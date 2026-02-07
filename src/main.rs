use rand::Rng;
use rand::seq::SliceRandom;
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, timeout};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod lightnodepb {
    tonic::include_proto!("lightnode");
}

const LIGHTNODE_FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("lightnode_descriptor");

const DEFAULT_PORT: u16 = 21841;
const DEFAULT_GRPC_PORT: u16 = 50051;
const HEADER_SIZE: usize = 8;
const MAX_FRAME_SIZE: usize = 0x00FF_FFFF;
const EXCHANGE_PUBLIC_PEERS_TYPE: u8 = 0;
const NUMBER_OF_EXCHANGED_PEERS: usize = 4;
const EXCHANGE_PUBLIC_PEERS_FRAME_SIZE: usize = HEADER_SIZE + NUMBER_OF_EXCHANGED_PEERS * 4;
const DEFAULT_DNS_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_API_TIMEOUT_MS: u64 = 6_000;

const BROADCAST_TICK_TYPE: u8 = 3;
const BROADCAST_TRANSACTION_TYPE: u8 = 24;
const RESPOND_CURRENT_TICK_INFO_TYPE: u8 = 28;
const REQUEST_TICK_TRANSACTIONS_TYPE: u8 = 29;
const REQUEST_ENTITY_TYPE: u8 = 31;
const RESPOND_ENTITY_TYPE: u8 = 32;
const END_RESPONSE_TYPE: u8 = 35;

const REQUEST_TICK_TRANSACTION_FLAGS_SIZE: usize = 1024 / 8;
const REQUEST_TICK_TRANSACTIONS_PAYLOAD_SIZE: usize = 4 + REQUEST_TICK_TRANSACTION_FLAGS_SIZE;
const RESPOND_CURRENT_TICK_INFO_PAYLOAD_SIZE: usize = 16;
const RESPOND_ENTITY_MIN_PAYLOAD_SIZE: usize = 72;
const TRANSACTION_BASE_SIZE: usize = 80;
const SIGNATURE_SIZE: usize = 64;
const MAX_PARALLEL_PEER_QUERIES: usize = 3;

#[derive(Clone, Copy, Debug, Serialize)]
struct TickStatus {
    epoch: u16,
    tick: u32,
    initial_tick: u32,
    tick_duration_ms: u16,
    aligned_votes: u16,
    misaligned_votes: u16,
}

#[derive(Clone, Debug)]
struct Config {
    listen_addr: SocketAddrV4,
    api_timeout: Duration,
    grpc_listen_addr: SocketAddr,
    grpc_enabled: bool,
    network_port: u16,
    target_outbound: usize,
    max_incoming: usize,
    max_seen: usize,
    reconnect_interval: Duration,
    relay_all: bool,
    dns_bootstrap: bool,
    dns_lite_peers: usize,
    dns_timeout: Duration,
    traffic_log: bool,
    seed_peers: Vec<SocketAddrV4>,
}

#[derive(Clone, Debug)]
struct Session {
    remote: SocketAddrV4,
    outbound: bool,
    tx: mpsc::UnboundedSender<Arc<[u8]>>,
}

#[derive(Debug)]
struct NodeState {
    sessions: HashMap<u64, Session>,
    connected_ip_refcount: HashMap<Ipv4Addr, usize>,
    known_peers: HashSet<SocketAddrV4>,
    pending_dials: HashSet<SocketAddrV4>,
    seen_order: VecDeque<[u8; 32]>,
    seen_set: HashSet<[u8; 32]>,
    latest_tick: Option<TickStatus>,
    max_seen: usize,
    next_peer_id: u64,
}

impl NodeState {
    fn new(max_seen: usize, seed_peers: &[SocketAddrV4]) -> Self {
        let mut known_peers = HashSet::new();
        for peer in seed_peers {
            known_peers.insert(*peer);
        }

        Self {
            sessions: HashMap::new(),
            connected_ip_refcount: HashMap::new(),
            known_peers,
            pending_dials: HashSet::new(),
            seen_order: VecDeque::new(),
            seen_set: HashSet::new(),
            latest_tick: None,
            max_seen: max_seen.max(1_000),
            next_peer_id: 1,
        }
    }

    fn outgoing_count(&self) -> usize {
        self.sessions.values().filter(|s| s.outbound).count()
    }

    fn incoming_count(&self) -> usize {
        self.sessions.values().filter(|s| !s.outbound).count()
    }

    fn is_ip_connected(&self, ip: Ipv4Addr) -> bool {
        self.connected_ip_refcount.contains_key(&ip)
    }

    fn register_session(
        &mut self,
        remote: SocketAddrV4,
        outbound: bool,
        tx: mpsc::UnboundedSender<Arc<[u8]>>,
        network_port: u16,
    ) -> Option<u64> {
        let remote_ip = *remote.ip();
        if self.is_ip_connected(remote_ip) {
            return None;
        }

        let peer_id = self.next_peer_id;
        self.next_peer_id = self.next_peer_id.wrapping_add(1);

        self.sessions.insert(
            peer_id,
            Session {
                remote,
                outbound,
                tx,
            },
        );
        *self.connected_ip_refcount.entry(remote_ip).or_insert(0) += 1;
        self.known_peers
            .insert(SocketAddrV4::new(remote_ip, network_port));
        self.pending_dials.remove(&remote);
        self.pending_dials
            .remove(&SocketAddrV4::new(remote_ip, network_port));

        Some(peer_id)
    }

    fn unregister_session(&mut self, peer_id: u64) -> Option<SocketAddrV4> {
        let session = self.sessions.remove(&peer_id)?;
        let remote_ip = *session.remote.ip();
        let remove_ip_entry = if let Some(counter) = self.connected_ip_refcount.get_mut(&remote_ip)
        {
            *counter -= 1;
            *counter == 0
        } else {
            false
        };
        if remove_ip_entry {
            self.connected_ip_refcount.remove(&remote_ip);
        }
        Some(session.remote)
    }

    fn mark_seen(&mut self, digest: [u8; 32]) -> bool {
        if self.seen_set.contains(&digest) {
            return false;
        }
        self.seen_set.insert(digest);
        self.seen_order.push_back(digest);

        while self.seen_order.len() > self.max_seen {
            if let Some(old) = self.seen_order.pop_front() {
                self.seen_set.remove(&old);
            }
        }
        true
    }

    fn collect_targets(&self, source_peer_id: u64) -> Vec<mpsc::UnboundedSender<Arc<[u8]>>> {
        self.sessions
            .iter()
            .filter_map(|(peer_id, session)| {
                if *peer_id == source_peer_id {
                    None
                } else {
                    Some(session.tx.clone())
                }
            })
            .collect()
    }

    fn add_discovered_peer(&mut self, peer: SocketAddrV4) -> bool {
        if is_bogon(peer.ip()) {
            return false;
        }
        self.known_peers.insert(peer)
    }

    fn choose_dial_targets(&mut self, needed: usize) -> Vec<SocketAddrV4> {
        let mut candidates: Vec<SocketAddrV4> = self
            .known_peers
            .iter()
            .copied()
            .filter(|peer| {
                !self.pending_dials.contains(peer)
                    && !self.is_ip_connected(*peer.ip())
                    && peer.ip().octets() != [0, 0, 0, 0]
            })
            .collect();

        let mut rng = rand::rng();
        candidates.shuffle(&mut rng);
        candidates.truncate(needed);
        for peer in &candidates {
            self.pending_dials.insert(*peer);
        }
        candidates
    }

    fn clear_pending_dial(&mut self, peer: SocketAddrV4) {
        self.pending_dials.remove(&peer);
    }

    fn choose_handshake_peers(&self) -> [Ipv4Addr; NUMBER_OF_EXCHANGED_PEERS] {
        let mut ips: Vec<Ipv4Addr> = self
            .known_peers
            .iter()
            .map(|peer| *peer.ip())
            .filter(|ip| !is_bogon(ip))
            .collect();
        ips.sort_unstable();
        ips.dedup();

        let mut rng = rand::rng();
        ips.shuffle(&mut rng);

        let mut selected = [Ipv4Addr::new(0, 0, 0, 0); NUMBER_OF_EXCHANGED_PEERS];
        for (idx, ip) in ips.into_iter().take(NUMBER_OF_EXCHANGED_PEERS).enumerate() {
            selected[idx] = ip;
        }
        selected
    }

    fn update_latest_tick(&mut self, tick_status: TickStatus) {
        if let Some(current) = self.latest_tick {
            let is_newer_epoch = tick_status.epoch > current.epoch;
            let is_same_epoch_newer_tick =
                tick_status.epoch == current.epoch && tick_status.tick >= current.tick;
            let enrich_same_tick = tick_status.epoch == current.epoch
                && tick_status.tick == current.tick
                && tick_status.initial_tick != 0
                && current.initial_tick == 0;

            if is_newer_epoch || is_same_epoch_newer_tick || enrich_same_tick {
                self.latest_tick = Some(tick_status);
            }
        } else {
            self.latest_tick = Some(tick_status);
        }
    }

    fn latest_tick(&self) -> Option<TickStatus> {
        self.latest_tick
    }

    fn peer_candidates(&self, network_port: u16) -> Vec<SocketAddrV4> {
        let mut peers = HashSet::<SocketAddrV4>::new();

        for session in self.sessions.values() {
            if is_bogon(session.remote.ip()) {
                continue;
            }
            if session.outbound {
                peers.insert(session.remote);
            } else {
                peers.insert(SocketAddrV4::new(*session.remote.ip(), network_port));
            }
        }

        for peer in &self.known_peers {
            if is_bogon(peer.ip()) {
                continue;
            }
            peers.insert(*peer);
        }

        peers.into_iter().collect()
    }
}

fn format_epoch_tick(status: Option<TickStatus>) -> String {
    match status {
        Some(status) => format!("epoch={} tick={}", status.epoch, status.tick),
        None => "epoch=? tick=?".to_string(),
    }
}

fn pack_epoch_tick(epoch: u16, tick: u32) -> u64 {
    ((epoch as u64) << 32) | (tick as u64)
}

fn unpack_epoch_tick(packed: u64) -> Option<(u16, u32)> {
    if packed == 0 {
        None
    } else {
        Some(((packed >> 32) as u16, packed as u32))
    }
}

fn format_epoch_tick_packed(packed: u64) -> String {
    match unpack_epoch_tick(packed) {
        Some((epoch, tick)) => format!("epoch={epoch} tick={tick}"),
        None => "epoch=? tick=?".to_string(),
    }
}

fn tick_status_from_packed(packed: u64) -> Option<TickStatus> {
    unpack_epoch_tick(packed).map(|(epoch, tick)| TickStatus {
        epoch,
        tick,
        initial_tick: 0,
        tick_duration_ms: 0,
        aligned_votes: 0,
        misaligned_votes: 0,
    })
}

#[derive(Clone)]
struct ApiState {
    node_state: Arc<Mutex<NodeState>>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
}

#[derive(Debug, Serialize)]
struct BalanceResponse {
    wallet: String,
    public_key_hex: String,
    tick: u32,
    spectrum_index: i32,
    incoming_amount: i64,
    outgoing_amount: i64,
    balance: i64,
    number_of_incoming_transfers: u32,
    number_of_outgoing_transfers: u32,
    latest_incoming_transfer_tick: u32,
    latest_outgoing_transfer_tick: u32,
}

#[derive(Debug, Serialize)]
struct TickTransaction {
    source_public_key_hex: String,
    destination_public_key_hex: String,
    amount: i64,
    tick: u32,
    input_type: u16,
    input_size: u16,
    input_hex: String,
    signature_hex: String,
}

#[derive(Clone)]
struct GrpcService {
    api: ApiState,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut config = match parse_config() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("{err}");
            print_usage();
            std::process::exit(2);
        }
    };

    if config.dns_bootstrap && config.seed_peers.is_empty() {
        let dns_lite_peers = if config.dns_lite_peers == 0 {
            (config.target_outbound * 3).max(8)
        } else {
            config.dns_lite_peers
        };

        match fetch_seed_peers_from_dns(config.network_port, dns_lite_peers, config.dns_timeout)
            .await
        {
            Ok(mut peers) => {
                if peers.is_empty() {
                    println!("DNS bootstrap returned no peers.");
                } else {
                    peers.sort_unstable();
                    peers.dedup();
                    println!("DNS bootstrap: loaded {} peers.", peers.len());
                    config.seed_peers.extend(peers);
                    config.seed_peers.sort_unstable();
                    config.seed_peers.dedup();
                }
            }
            Err(err) => {
                eprintln!("DNS bootstrap failed: {err}");
            }
        }
    }

    let state = Arc::new(Mutex::new(NodeState::new(
        config.max_seen,
        &config.seed_peers,
    )));
    let latest_epoch_tick = Arc::new(AtomicU64::new(0));
    let listener = TcpListener::bind(config.listen_addr).await?;

    println!(
        "Qubic light relay started at {} | target_outbound={} | max_incoming={} | seed_peers={} | traffic_log={} | grpc={}({})",
        config.listen_addr,
        config.target_outbound,
        config.max_incoming,
        config.seed_peers.len(),
        config.traffic_log,
        config.grpc_enabled,
        config.grpc_listen_addr
    );
    if config.seed_peers.is_empty() {
        println!("No seed peers configured. Use --peer <ip[:port]> to join the public network.");
    }

    let shared_config = Arc::new(config);

    if shared_config.grpc_enabled {
        let grpc_state = ApiState {
            node_state: Arc::clone(&state),
            latest_epoch_tick: Arc::clone(&latest_epoch_tick),
            config: Arc::clone(&shared_config),
        };
        tokio::spawn(async move {
            if let Err(err) = run_grpc_server(grpc_state).await {
                eprintln!("gRPC server stopped: {err}");
            }
        });
    }

    let accept_state = Arc::clone(&state);
    let accept_latest_epoch_tick = Arc::clone(&latest_epoch_tick);
    let accept_config = Arc::clone(&shared_config);
    tokio::spawn(async move {
        accept_loop(
            listener,
            accept_state,
            accept_latest_epoch_tick,
            accept_config,
        )
        .await;
    });

    let dial_state = Arc::clone(&state);
    let dial_latest_epoch_tick = Arc::clone(&latest_epoch_tick);
    let dial_config = Arc::clone(&shared_config);
    tokio::spawn(async move {
        dial_loop(dial_state, dial_latest_epoch_tick, dial_config).await;
    });

    tokio::signal::ctrl_c().await?;
    println!("Shutdown signal received, stopping.");
    Ok(())
}

async fn run_grpc_server(api_state: ApiState) -> std::io::Result<()> {
    let service = GrpcService {
        api: api_state.clone(),
    };
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(LIGHTNODE_FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    println!("gRPC listening on {}", api_state.config.grpc_listen_addr);

    Server::builder()
        .add_service(reflection)
        .add_service(lightnodepb::light_node_server::LightNodeServer::new(
            service,
        ))
        .serve(api_state.config.grpc_listen_addr)
        .await
        .map_err(|err| std::io::Error::other(err.to_string()))
}

#[tonic::async_trait]
impl lightnodepb::light_node_server::LightNode for GrpcService {
    async fn get_status(
        &self,
        _request: Request<lightnodepb::GetStatusRequest>,
    ) -> Result<Response<lightnodepb::GetStatusResponse>, Status> {
        let packed = self.api.latest_epoch_tick.load(Ordering::Relaxed);
        let cached = {
            let cached_from_state = self.api.node_state.lock().await.latest_tick();
            if cached_from_state.is_some() {
                cached_from_state
            } else {
                tick_status_from_packed(packed)
            }
        };
        if let Some(status) = cached {
            Ok(Response::new(lightnodepb::GetStatusResponse {
                ok: true,
                source: "cache".to_string(),
                status: Some(map_tick_status(status)),
                warning: String::new(),
                error: String::new(),
            }))
        } else {
            Ok(Response::new(lightnodepb::GetStatusResponse {
                ok: false,
                source: String::new(),
                status: None,
                warning: String::new(),
                error: "No tick data in local cache yet. Wait for incoming network messages."
                    .to_string(),
            }))
        }
    }

    async fn get_balance(
        &self,
        request: Request<lightnodepb::GetBalanceRequest>,
    ) -> Result<Response<lightnodepb::GetBalanceResponse>, Status> {
        let wallet = request.into_inner().wallet;
        let public_key = parse_wallet_public_key(&wallet).map_err(Status::invalid_argument)?;

        match query_balance(
            Arc::clone(&self.api.node_state),
            Arc::clone(&self.api.config),
            &wallet,
            public_key,
        )
        .await
        {
            Ok(balance) => Ok(Response::new(lightnodepb::GetBalanceResponse {
                ok: true,
                balance: Some(map_balance(balance)),
                error: String::new(),
            })),
            Err(err) => Ok(Response::new(lightnodepb::GetBalanceResponse {
                ok: false,
                balance: None,
                error: err,
            })),
        }
    }

    async fn get_tick_transactions(
        &self,
        request: Request<lightnodepb::GetTickTransactionsRequest>,
    ) -> Result<Response<lightnodepb::GetTickTransactionsResponse>, Status> {
        let tick = request.into_inner().tick;
        match query_tick_transactions(
            Arc::clone(&self.api.node_state),
            Arc::clone(&self.api.config),
            tick,
        )
        .await
        {
            Ok(transactions) => Ok(Response::new(lightnodepb::GetTickTransactionsResponse {
                ok: true,
                tick,
                transactions: transactions
                    .into_iter()
                    .map(map_transaction)
                    .collect::<Vec<_>>(),
                error: String::new(),
            })),
            Err(err) => Ok(Response::new(lightnodepb::GetTickTransactionsResponse {
                ok: false,
                tick,
                transactions: Vec::new(),
                error: err,
            })),
        }
    }
}

fn map_tick_status(status: TickStatus) -> lightnodepb::TickStatus {
    lightnodepb::TickStatus {
        epoch: status.epoch as u32,
        tick: status.tick,
        initial_tick: status.initial_tick,
        tick_duration_ms: status.tick_duration_ms as u32,
        aligned_votes: status.aligned_votes as u32,
        misaligned_votes: status.misaligned_votes as u32,
    }
}

fn map_balance(balance: BalanceResponse) -> lightnodepb::Balance {
    lightnodepb::Balance {
        wallet: balance.wallet,
        public_key_hex: balance.public_key_hex,
        tick: balance.tick,
        spectrum_index: balance.spectrum_index,
        incoming_amount: balance.incoming_amount,
        outgoing_amount: balance.outgoing_amount,
        balance: balance.balance,
        number_of_incoming_transfers: balance.number_of_incoming_transfers,
        number_of_outgoing_transfers: balance.number_of_outgoing_transfers,
        latest_incoming_transfer_tick: balance.latest_incoming_transfer_tick,
        latest_outgoing_transfer_tick: balance.latest_outgoing_transfer_tick,
    }
}

fn map_transaction(tx: TickTransaction) -> lightnodepb::Transaction {
    lightnodepb::Transaction {
        source_public_key_hex: tx.source_public_key_hex,
        destination_public_key_hex: tx.destination_public_key_hex,
        amount: tx.amount,
        tick: tx.tick,
        input_type: tx.input_type as u32,
        input_size: tx.input_size as u32,
        input_hex: tx.input_hex,
        signature_hex: tx.signature_hex,
    }
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<Mutex<NodeState>>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, remote_addr)) => {
                let remote_v4 = match remote_addr {
                    SocketAddr::V4(addr) => addr,
                    SocketAddr::V6(_) => {
                        continue;
                    }
                };

                if let Err(err) = stream.set_nodelay(true) {
                    eprintln!("Failed to set TCP_NODELAY for {remote_v4}: {err}");
                }

                let connection_state = Arc::clone(&state);
                let connection_latest_epoch_tick = Arc::clone(&latest_epoch_tick);
                let connection_config = Arc::clone(&config);
                tokio::spawn(async move {
                    establish_connection(
                        stream,
                        remote_v4,
                        false,
                        connection_state,
                        connection_latest_epoch_tick,
                        connection_config,
                    )
                    .await;
                });
            }
            Err(err) => {
                eprintln!("Accept failed: {err}");
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn dial_loop(
    state: Arc<Mutex<NodeState>>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
    loop {
        let targets = {
            let mut locked = state.lock().await;
            let current_outbound = locked.outgoing_count();
            if current_outbound >= config.target_outbound {
                Vec::new()
            } else {
                let needed = config.target_outbound - current_outbound;
                locked.choose_dial_targets(needed)
            }
        };

        for target in targets {
            let state_for_task = Arc::clone(&state);
            let latest_epoch_tick_for_task = Arc::clone(&latest_epoch_tick);
            let config_for_task = Arc::clone(&config);
            tokio::spawn(async move {
                match TcpStream::connect(target).await {
                    Ok(stream) => {
                        if let Err(err) = stream.set_nodelay(true) {
                            eprintln!("Failed to set TCP_NODELAY for {target}: {err}");
                        }
                        establish_connection(
                            stream,
                            target,
                            true,
                            state_for_task,
                            latest_epoch_tick_for_task,
                            config_for_task,
                        )
                        .await;
                    }
                    Err(err) => {
                        {
                            let mut locked = state_for_task.lock().await;
                            locked.clear_pending_dial(target);
                        }
                        eprintln!("Dial failed {target}: {err}");
                    }
                }
            });
        }

        sleep(config.reconnect_interval).await;
    }
}

async fn establish_connection(
    stream: TcpStream,
    remote: SocketAddrV4,
    outbound: bool,
    state: Arc<Mutex<NodeState>>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
    let (tx, rx) = mpsc::unbounded_channel::<Arc<[u8]>>();
    let (peer_id, handshake_payload, incoming_count, outgoing_count, latest_tick) = {
        let mut locked = state.lock().await;

        if !outbound && locked.incoming_count() >= config.max_incoming {
            println!("Rejecting incoming {remote}: incoming limit reached.");
            locked.clear_pending_dial(remote);
            locked.clear_pending_dial(SocketAddrV4::new(*remote.ip(), config.network_port));
            return;
        }

        let Some(peer_id) =
            locked.register_session(remote, outbound, tx.clone(), config.network_port)
        else {
            locked.clear_pending_dial(remote);
            locked.clear_pending_dial(SocketAddrV4::new(*remote.ip(), config.network_port));
            return;
        };

        let peers_for_handshake = locked.choose_handshake_peers();
        (
            peer_id,
            build_exchange_public_peers_frame(peers_for_handshake),
            locked.incoming_count(),
            locked.outgoing_count(),
            locked.latest_tick(),
        )
    };

    let _ = tx.send(Arc::<[u8]>::from(handshake_payload));
    println!(
        "Connected {} [{}] | in={} out={} | {}",
        remote,
        if outbound { "out" } else { "in" },
        incoming_count,
        outgoing_count,
        format_epoch_tick(latest_tick)
    );

    tokio::spawn(connection_worker(
        stream,
        peer_id,
        remote,
        rx,
        state,
        latest_epoch_tick,
        config,
    ));
}

async fn connection_worker(
    stream: TcpStream,
    peer_id: u64,
    remote: SocketAddrV4,
    mut rx: mpsc::UnboundedReceiver<Arc<[u8]>>,
    state: Arc<Mutex<NodeState>>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
    let (mut reader, mut writer) = stream.into_split();
    let traffic_log = config.traffic_log;
    let writer_latest_epoch_tick = Arc::clone(&latest_epoch_tick);

    let writer_task = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if traffic_log {
                let (size, message_type, dejavu) = frame_meta(&frame);
                let packed = writer_latest_epoch_tick.load(Ordering::Relaxed);
                println!(
                    "TX {} | size={} type={}({}) dejavu={} | {}",
                    remote,
                    size,
                    message_type_name(message_type),
                    message_type,
                    dejavu,
                    format_epoch_tick_packed(packed)
                );
            }
            if writer.write_all(&frame).await.is_err() {
                break;
            }
        }
        let _ = writer.shutdown().await;
    });

    let mut read_buffer = vec![0u8; 128 * 1024];
    let mut accumulated = Vec::<u8>::with_capacity(256 * 1024);

    loop {
        match reader.read(&mut read_buffer).await {
            Ok(0) => break,
            Ok(read_bytes) => {
                accumulated.extend_from_slice(&read_buffer[..read_bytes]);
                let mut offset = 0usize;

                while accumulated.len().saturating_sub(offset) >= HEADER_SIZE {
                    let frame_size = decode_frame_size(&accumulated[offset..offset + HEADER_SIZE]);
                    if !(HEADER_SIZE..=MAX_FRAME_SIZE).contains(&frame_size) {
                        eprintln!("Invalid frame size from {remote}: {frame_size}");
                        offset = accumulated.len();
                        break;
                    }
                    if accumulated.len() - offset < frame_size {
                        break;
                    }
                    let end = offset + frame_size;
                    let frame = accumulated[offset..end].to_vec();
                    offset = end;

                    if config.traffic_log {
                        let (size, message_type, dejavu) = frame_meta(&frame);
                        let packed = latest_epoch_tick.load(Ordering::Relaxed);
                        println!(
                            "RX {} | peer_id={} size={} type={}({}) dejavu={} | {}",
                            remote,
                            peer_id,
                            size,
                            message_type_name(message_type),
                            message_type,
                            dejavu,
                            format_epoch_tick_packed(packed)
                        );
                    }

                    process_incoming_frame(
                        peer_id,
                        frame,
                        Arc::clone(&state),
                        Arc::clone(&latest_epoch_tick),
                        Arc::clone(&config),
                    )
                    .await;
                }

                if offset > 0 {
                    accumulated.drain(0..offset);
                }
            }
            Err(err) => {
                eprintln!("Read failed from {remote}: {err}");
                break;
            }
        }
    }

    writer_task.abort();

    let (in_count, out_count, latest_tick) = {
        let mut locked = state.lock().await;
        let _ = locked.unregister_session(peer_id);
        (
            locked.incoming_count(),
            locked.outgoing_count(),
            locked.latest_tick(),
        )
    };
    println!(
        "Disconnected {remote} | in={in_count} out={out_count} | {}",
        format_epoch_tick(latest_tick)
    );
}

async fn process_incoming_frame(
    source_peer_id: u64,
    frame: Vec<u8>,
    state: Arc<Mutex<NodeState>>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
    if frame.len() < HEADER_SIZE {
        return;
    }

    let message_type = frame[3];
    let dejavu = u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]);

    let discovered = if message_type == EXCHANGE_PUBLIC_PEERS_TYPE {
        parse_exchange_public_peers(&frame, config.network_port)
    } else {
        Vec::new()
    };
    let tick_update = parse_tick_status_from_frame(&frame);
    if let Some(status) = tick_update {
        latest_epoch_tick.store(
            pack_epoch_tick(status.epoch, status.tick),
            Ordering::Relaxed,
        );
    }

    if !config.relay_all && dejavu != 0 {
        let mut locked = state.lock().await;
        if let Some(status) = tick_update {
            locked.update_latest_tick(status);
        }
        for peer in discovered {
            let _ = locked.add_discovered_peer(peer);
        }
        return;
    }

    let digest = *blake3::hash(&frame).as_bytes();
    let (targets, latest_tick) = {
        let mut locked = state.lock().await;
        if let Some(status) = tick_update {
            locked.update_latest_tick(status);
        }
        for peer in discovered {
            let _ = locked.add_discovered_peer(peer);
        }
        if !locked.mark_seen(digest) {
            if config.traffic_log {
                let (size, message_type, dejavu) = frame_meta(&frame);
                println!(
                    "DROP_DUP peer_id={} size={} type={}({}) dejavu={} | {}",
                    source_peer_id,
                    size,
                    message_type_name(message_type),
                    message_type,
                    dejavu,
                    format_epoch_tick(locked.latest_tick())
                );
            }
            return;
        }
        (locked.collect_targets(source_peer_id), locked.latest_tick())
    };

    if config.traffic_log {
        let (size, message_type, dejavu) = frame_meta(&frame);
        println!(
            "RELAY peer_id={} -> {} peers | size={} type={}({}) dejavu={} | {}",
            source_peer_id,
            targets.len(),
            size,
            message_type_name(message_type),
            message_type,
            dejavu,
            format_epoch_tick(latest_tick)
        );
    }

    let frame = Arc::<[u8]>::from(frame);
    for tx in targets {
        let _ = tx.send(Arc::clone(&frame));
    }
}

fn frame_meta(frame: &[u8]) -> (usize, u8, u32) {
    if frame.len() < HEADER_SIZE {
        return (frame.len(), 0, 0);
    }
    let size = decode_frame_size(frame);
    let message_type = frame[3];
    let dejavu = u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]);
    (size, message_type, dejavu)
}

fn message_type_name(message_type: u8) -> &'static str {
    match message_type {
        0 => "EXCHANGE_PUBLIC_PEERS",
        1 => "BROADCAST_MESSAGE",
        2 => "BROADCAST_COMPUTORS",
        3 => "BROADCAST_TICK",
        8 => "BROADCAST_FUTURE_TICK_DATA",
        11 => "REQUEST_COMPUTORS",
        14 => "REQUEST_QUORUM_TICK",
        16 => "REQUEST_TICK_DATA",
        24 => "BROADCAST_TRANSACTION",
        26 => "REQUEST_TRANSACTION_INFO",
        27 => "REQUEST_CURRENT_TICK_INFO",
        28 => "RESPOND_CURRENT_TICK_INFO",
        29 => "REQUEST_TICK_TRANSACTIONS",
        31 => "REQUEST_ENTITY",
        32 => "RESPOND_ENTITY",
        33 => "REQUEST_CONTRACT_IPO",
        34 => "RESPOND_CONTRACT_IPO",
        35 => "END_RESPONSE",
        36 => "REQUEST_ISSUED_ASSETS",
        37 => "RESPOND_ISSUED_ASSETS",
        38 => "REQUEST_OWNED_ASSETS",
        39 => "RESPOND_OWNED_ASSETS",
        40 => "REQUEST_POSSESSED_ASSETS",
        41 => "RESPOND_POSSESSED_ASSETS",
        42 => "REQUEST_CONTRACT_FUNCTION",
        43 => "RESPOND_CONTRACT_FUNCTION",
        44 => "REQUEST_LOG",
        45 => "RESPOND_LOG",
        46 => "REQUEST_SYSTEM_INFO",
        47 => "RESPOND_SYSTEM_INFO",
        48 => "REQUEST_LOG_ID_RANGE_FROM_TX",
        49 => "RESPOND_LOG_ID_RANGE_FROM_TX",
        50 => "REQUEST_ALL_LOG_ID_RANGES_FROM_TX",
        51 => "RESPOND_ALL_LOG_ID_RANGES_FROM_TX",
        52 => "REQUEST_ASSETS",
        53 => "RESPOND_ASSETS",
        54 => "TRY_AGAIN",
        56 => "REQUEST_PRUNING_LOG",
        57 => "RESPOND_PRUNING_LOG",
        58 => "REQUEST_LOG_STATE_DIGEST",
        59 => "RESPOND_LOG_STATE_DIGEST",
        60 => "REQUEST_CUSTOM_MINING_DATA",
        61 => "RESPOND_CUSTOM_MINING_DATA",
        62 => "REQUEST_CUSTOM_MINING_SOLUTION_VERIFICATION",
        63 => "RESPOND_CUSTOM_MINING_SOLUTION_VERIFICATION",
        64 => "REQUEST_ACTIVE_IPOS",
        65 => "RESPOND_ACTIVE_IPO",
        201 => "REQUEST_TX_STATUS",
        202 => "RESPOND_TX_STATUS",
        255 => "SPECIAL_COMMAND",
        _ => "UNKNOWN",
    }
}

fn build_exchange_public_peers_frame(peers: [Ipv4Addr; NUMBER_OF_EXCHANGED_PEERS]) -> Vec<u8> {
    let mut frame = vec![0u8; EXCHANGE_PUBLIC_PEERS_FRAME_SIZE];
    let size = EXCHANGE_PUBLIC_PEERS_FRAME_SIZE as u32;
    frame[0] = (size & 0xFF) as u8;
    frame[1] = ((size >> 8) & 0xFF) as u8;
    frame[2] = ((size >> 16) & 0xFF) as u8;
    frame[3] = EXCHANGE_PUBLIC_PEERS_TYPE;
    frame[4..8].copy_from_slice(&random_non_zero_u32().to_le_bytes());

    for (index, ip) in peers.into_iter().enumerate() {
        let offset = HEADER_SIZE + index * 4;
        frame[offset..offset + 4].copy_from_slice(&ip.octets());
    }

    frame
}

fn parse_exchange_public_peers(frame: &[u8], network_port: u16) -> Vec<SocketAddrV4> {
    if frame.len() < EXCHANGE_PUBLIC_PEERS_FRAME_SIZE {
        return Vec::new();
    }

    let mut peers = Vec::with_capacity(NUMBER_OF_EXCHANGED_PEERS);
    let payload = &frame[HEADER_SIZE..HEADER_SIZE + NUMBER_OF_EXCHANGED_PEERS * 4];
    for chunk in payload.chunks_exact(4) {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        if is_bogon(&ip) {
            continue;
        }
        peers.push(SocketAddrV4::new(ip, network_port));
    }
    peers
}

fn decode_frame_size(header: &[u8]) -> usize {
    (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16)
}

fn is_bogon(ip: &Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 0
        || octets[0] == 10
        || octets[0] == 127
        || (octets[0] == 172 && (16..=31).contains(&octets[1]))
        || (octets[0] == 192 && octets[1] == 168)
        || octets[0] == 255
}

fn random_non_zero_u32() -> u32 {
    let mut rng = rand::rng();
    loop {
        let value: u32 = rng.random();
        if value != 0 {
            return value;
        }
    }
}

async fn fetch_seed_peers_from_dns(
    network_port: u16,
    lite_peers: usize,
    timeout: Duration,
) -> Result<Vec<SocketAddrV4>, String> {
    let requested_lite = lite_peers.max(1);
    let url = format!(
        "https://api.qubic.global/random-peers?service=bobNode&litePeers={requested_lite}&bobPeers=4"
    );

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|err| format!("cannot build HTTP client: {err}"))?;

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("request failed: {err}"))?;

    if !response.status().is_success() {
        return Err(format!("HTTP status {}", response.status()));
    }

    let root: Value = response
        .json()
        .await
        .map_err(|err| format!("invalid JSON: {err}"))?;

    let mut peers = Vec::<SocketAddrV4>::new();
    collect_peers_from_json(&root, "litePeers", network_port, &mut peers);

    peers.sort_unstable();
    peers.dedup();
    Ok(peers)
}

fn collect_peers_from_json(
    root: &Value,
    field_name: &str,
    network_port: u16,
    out: &mut Vec<SocketAddrV4>,
) {
    let Some(array) = root.get(field_name).and_then(Value::as_array) else {
        return;
    };

    for item in array {
        let Some(ip_raw) = item.as_str() else {
            continue;
        };
        let Ok(ip) = ip_raw.parse::<Ipv4Addr>() else {
            continue;
        };
        if is_bogon(&ip) {
            continue;
        }
        out.push(SocketAddrV4::new(ip, network_port));
    }
}

fn build_request_frame(message_type: u8, dejavu: u32, payload: &[u8]) -> Result<Vec<u8>, String> {
    let size = HEADER_SIZE + payload.len();
    if !(HEADER_SIZE..=MAX_FRAME_SIZE).contains(&size) {
        return Err(format!("Invalid frame size {size}"));
    }

    let mut frame = Vec::with_capacity(size);
    frame.push((size & 0xFF) as u8);
    frame.push(((size >> 8) & 0xFF) as u8);
    frame.push(((size >> 16) & 0xFF) as u8);
    frame.push(message_type);
    frame.extend_from_slice(&dejavu.to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn frame_payload(frame: &[u8]) -> Result<&[u8], String> {
    if frame.len() < HEADER_SIZE {
        return Err("Frame is smaller than header".to_string());
    }

    let frame_size = decode_frame_size(frame);
    if frame_size != frame.len() {
        return Err(format!(
            "Frame length mismatch: header={frame_size} actual={}",
            frame.len()
        ));
    }
    Ok(&frame[HEADER_SIZE..])
}

fn parse_tick_status_from_frame(frame: &[u8]) -> Option<TickStatus> {
    if frame.len() < HEADER_SIZE {
        return None;
    }

    match frame[3] {
        RESPOND_CURRENT_TICK_INFO_TYPE => {
            let payload = frame_payload(frame).ok()?;
            parse_current_tick_info_payload(payload).ok()
        }
        BROADCAST_TICK_TYPE => {
            let payload = frame_payload(frame).ok()?;
            if payload.len() < 8 {
                return None;
            }
            Some(TickStatus {
                epoch: read_u16(payload, 2)?,
                tick: read_u32(payload, 4)?,
                initial_tick: 0,
                tick_duration_ms: 0,
                aligned_votes: 0,
                misaligned_votes: 0,
            })
        }
        _ => None,
    }
}

async fn query_balance(
    state: Arc<Mutex<NodeState>>,
    config: Arc<Config>,
    wallet: &str,
    public_key: [u8; 32],
) -> Result<BalanceResponse, String> {
    let mut peers = {
        let locked = state.lock().await;
        locked.peer_candidates(config.network_port)
    };
    if peers.is_empty() {
        return Err("No known peers available".to_string());
    }

    {
        let mut rng = rand::rng();
        peers.shuffle(&mut rng);
    }

    let wallet_owned = wallet.to_string();
    let parallelism = peers.len().clamp(1, MAX_PARALLEL_PEER_QUERIES);
    let mut join_set = JoinSet::<(SocketAddrV4, Result<BalanceResponse, String>)>::new();
    let mut peer_iter = peers.into_iter();

    for _ in 0..parallelism {
        if let Some(peer) = peer_iter.next() {
            let timeout_duration = config.api_timeout;
            let wallet = wallet_owned.clone();
            join_set.spawn(async move {
                (
                    peer,
                    query_balance_from_peer(peer, timeout_duration, &wallet, public_key).await,
                )
            });
        }
    }

    let mut last_err = String::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((_, Ok(balance))) => {
                join_set.abort_all();
                return Ok(balance);
            }
            Ok((peer, Err(err))) => {
                last_err = format!("{peer}: {err}");
                if let Some(next_peer) = peer_iter.next() {
                    let timeout_duration = config.api_timeout;
                    let wallet = wallet_owned.clone();
                    join_set.spawn(async move {
                        (
                            next_peer,
                            query_balance_from_peer(
                                next_peer,
                                timeout_duration,
                                &wallet,
                                public_key,
                            )
                            .await,
                        )
                    });
                }
            }
            Err(err) => {
                last_err = format!("task join error: {err}");
            }
        }
    }

    Err(format!(
        "Failed to query balance from peers (parallel): {last_err}"
    ))
}

async fn query_balance_from_peer(
    peer: SocketAddrV4,
    timeout_duration: Duration,
    wallet: &str,
    public_key: [u8; 32],
) -> Result<BalanceResponse, String> {
    let mut stream = connect_to_peer(peer, timeout_duration).await?;
    send_handshake_frame(&mut stream, timeout_duration).await?;

    let dejavu = random_non_zero_u32();
    let request = build_request_frame(REQUEST_ENTITY_TYPE, dejavu, &public_key)?;
    write_frame(&mut stream, &request, timeout_duration).await?;

    let deadline = Instant::now() + timeout_duration;
    loop {
        let frame = read_frame_until_deadline(&mut stream, deadline).await?;
        let response_dejavu = frame_dejavu(&frame)?;
        if response_dejavu != dejavu {
            continue;
        }

        match frame[3] {
            RESPOND_ENTITY_TYPE => {
                let payload = frame_payload(&frame)?;
                return parse_balance_payload(wallet, payload);
            }
            END_RESPONSE_TYPE => {
                return Err("Peer returned END_RESPONSE without balance data".to_string());
            }
            _ => {}
        }
    }
}

async fn query_tick_transactions(
    state: Arc<Mutex<NodeState>>,
    config: Arc<Config>,
    tick: u32,
) -> Result<Vec<TickTransaction>, String> {
    let mut peers = {
        let locked = state.lock().await;
        locked.peer_candidates(config.network_port)
    };
    if peers.is_empty() {
        return Err("No known peers available".to_string());
    }

    {
        let mut rng = rand::rng();
        peers.shuffle(&mut rng);
    }

    let parallelism = peers.len().clamp(1, MAX_PARALLEL_PEER_QUERIES);
    let mut join_set = JoinSet::<(SocketAddrV4, Result<Vec<TickTransaction>, String>)>::new();
    let mut peer_iter = peers.into_iter();

    for _ in 0..parallelism {
        if let Some(peer) = peer_iter.next() {
            let timeout_duration = config.api_timeout;
            join_set.spawn(async move {
                (
                    peer,
                    query_tick_transactions_from_peer(peer, timeout_duration, tick).await,
                )
            });
        }
    }

    let mut last_err = String::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((_, Ok(transactions))) => {
                join_set.abort_all();
                return Ok(transactions);
            }
            Ok((peer, Err(err))) => {
                last_err = format!("{peer}: {err}");
                if let Some(next_peer) = peer_iter.next() {
                    let timeout_duration = config.api_timeout;
                    join_set.spawn(async move {
                        (
                            next_peer,
                            query_tick_transactions_from_peer(next_peer, timeout_duration, tick)
                                .await,
                        )
                    });
                }
            }
            Err(err) => {
                last_err = format!("task join error: {err}");
            }
        }
    }

    Err(format!(
        "Failed to query tick transactions from peers (parallel): {last_err}"
    ))
}

async fn query_tick_transactions_from_peer(
    peer: SocketAddrV4,
    timeout_duration: Duration,
    tick: u32,
) -> Result<Vec<TickTransaction>, String> {
    let mut stream = connect_to_peer(peer, timeout_duration).await?;
    send_handshake_frame(&mut stream, timeout_duration).await?;

    let dejavu = random_non_zero_u32();
    let mut payload = vec![0u8; REQUEST_TICK_TRANSACTIONS_PAYLOAD_SIZE];
    payload[0..4].copy_from_slice(&tick.to_le_bytes());

    let request = build_request_frame(REQUEST_TICK_TRANSACTIONS_TYPE, dejavu, &payload)?;
    write_frame(&mut stream, &request, timeout_duration).await?;

    let deadline = Instant::now() + timeout_duration;
    let mut transactions = Vec::<TickTransaction>::new();
    loop {
        let frame = read_frame_until_deadline(&mut stream, deadline).await?;
        let response_dejavu = frame_dejavu(&frame)?;
        if response_dejavu != dejavu {
            continue;
        }

        match frame[3] {
            BROADCAST_TRANSACTION_TYPE => {
                let tx_payload = frame_payload(&frame)?;
                transactions.push(parse_transaction_payload(tx_payload)?);
            }
            END_RESPONSE_TYPE => return Ok(transactions),
            _ => {}
        }
    }
}

async fn connect_to_peer(
    peer: SocketAddrV4,
    timeout_duration: Duration,
) -> Result<TcpStream, String> {
    let stream = timeout(timeout_duration, TcpStream::connect(peer))
        .await
        .map_err(|_| format!("connect timeout after {} ms", timeout_duration.as_millis()))
        .and_then(|result| result.map_err(|err| err.to_string()))?;

    if let Err(err) = stream.set_nodelay(true) {
        return Err(format!("set_nodelay failed: {err}"));
    }
    Ok(stream)
}

async fn send_handshake_frame(
    stream: &mut TcpStream,
    timeout_duration: Duration,
) -> Result<(), String> {
    let handshake = build_exchange_public_peers_frame([
        Ipv4Addr::new(0, 0, 0, 0),
        Ipv4Addr::new(0, 0, 0, 0),
        Ipv4Addr::new(0, 0, 0, 0),
        Ipv4Addr::new(0, 0, 0, 0),
    ]);
    write_frame(stream, &handshake, timeout_duration).await
}

async fn write_frame(
    stream: &mut TcpStream,
    frame: &[u8],
    timeout_duration: Duration,
) -> Result<(), String> {
    timeout(timeout_duration, stream.write_all(frame))
        .await
        .map_err(|_| format!("write timeout after {} ms", timeout_duration.as_millis()))
        .and_then(|result| result.map_err(|err| err.to_string()))
}

async fn read_frame_until_deadline(
    stream: &mut TcpStream,
    deadline: Instant,
) -> Result<Vec<u8>, String> {
    let mut header = [0u8; HEADER_SIZE];
    read_exact_until_deadline(stream, &mut header, deadline).await?;

    let frame_size = decode_frame_size(&header);
    if !(HEADER_SIZE..=MAX_FRAME_SIZE).contains(&frame_size) {
        return Err(format!("Invalid frame size {frame_size}"));
    }

    let payload_size = frame_size - HEADER_SIZE;
    let mut frame = Vec::<u8>::with_capacity(frame_size);
    frame.extend_from_slice(&header);

    if payload_size > 0 {
        let mut payload = vec![0u8; payload_size];
        read_exact_until_deadline(stream, &mut payload, deadline).await?;
        frame.extend_from_slice(&payload);
    }

    Ok(frame)
}

async fn read_exact_until_deadline(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    deadline: Instant,
) -> Result<(), String> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| "request timeout".to_string())?;

    timeout(remaining, stream.read_exact(buffer))
        .await
        .map_err(|_| "read timeout".to_string())
        .and_then(|result| result.map(|_| ()).map_err(|err| err.to_string()))
}

fn frame_dejavu(frame: &[u8]) -> Result<u32, String> {
    if frame.len() < HEADER_SIZE {
        return Err("Frame too small".to_string());
    }
    Ok(u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]))
}

fn parse_current_tick_info_payload(payload: &[u8]) -> Result<TickStatus, String> {
    if payload.len() < RESPOND_CURRENT_TICK_INFO_PAYLOAD_SIZE {
        return Err(format!(
            "RespondCurrentTickInfo payload too small: {}",
            payload.len()
        ));
    }

    Ok(TickStatus {
        tick_duration_ms: read_u16(payload, 0).ok_or_else(|| "tickDuration missing".to_string())?,
        epoch: read_u16(payload, 2).ok_or_else(|| "epoch missing".to_string())?,
        tick: read_u32(payload, 4).ok_or_else(|| "tick missing".to_string())?,
        aligned_votes: read_u16(payload, 8).ok_or_else(|| "aligned votes missing".to_string())?,
        misaligned_votes: read_u16(payload, 10)
            .ok_or_else(|| "misaligned votes missing".to_string())?,
        initial_tick: read_u32(payload, 12).ok_or_else(|| "initial tick missing".to_string())?,
    })
}

fn parse_balance_payload(wallet: &str, payload: &[u8]) -> Result<BalanceResponse, String> {
    if payload.len() < RESPOND_ENTITY_MIN_PAYLOAD_SIZE {
        return Err(format!(
            "RespondEntity payload too small: {}",
            payload.len()
        ));
    }

    let incoming_amount =
        read_i64(payload, 32).ok_or_else(|| "incomingAmount missing".to_string())?;
    let outgoing_amount =
        read_i64(payload, 40).ok_or_else(|| "outgoingAmount missing".to_string())?;

    Ok(BalanceResponse {
        wallet: wallet.to_string(),
        public_key_hex: format!("0x{}", bytes_to_hex(&payload[0..32])),
        tick: read_u32(payload, 64).ok_or_else(|| "tick missing".to_string())?,
        spectrum_index: read_i32(payload, 68).ok_or_else(|| "spectrumIndex missing".to_string())?,
        incoming_amount,
        outgoing_amount,
        balance: incoming_amount - outgoing_amount,
        number_of_incoming_transfers: read_u32(payload, 48)
            .ok_or_else(|| "numberOfIncomingTransfers missing".to_string())?,
        number_of_outgoing_transfers: read_u32(payload, 52)
            .ok_or_else(|| "numberOfOutgoingTransfers missing".to_string())?,
        latest_incoming_transfer_tick: read_u32(payload, 56)
            .ok_or_else(|| "latestIncomingTransferTick missing".to_string())?,
        latest_outgoing_transfer_tick: read_u32(payload, 60)
            .ok_or_else(|| "latestOutgoingTransferTick missing".to_string())?,
    })
}

fn parse_transaction_payload(payload: &[u8]) -> Result<TickTransaction, String> {
    if payload.len() < TRANSACTION_BASE_SIZE + SIGNATURE_SIZE {
        return Err(format!("Transaction payload too small: {}", payload.len()));
    }

    let input_size = read_u16(payload, 78).ok_or_else(|| "inputSize missing".to_string())? as usize;
    let expected_size = TRANSACTION_BASE_SIZE + input_size + SIGNATURE_SIZE;
    if payload.len() != expected_size {
        return Err(format!(
            "Transaction payload size mismatch: expected {expected_size}, got {}",
            payload.len()
        ));
    }

    let input_start = TRANSACTION_BASE_SIZE;
    let input_end = input_start + input_size;
    let signature_start = input_end;

    Ok(TickTransaction {
        source_public_key_hex: format!("0x{}", bytes_to_hex(&payload[0..32])),
        destination_public_key_hex: format!("0x{}", bytes_to_hex(&payload[32..64])),
        amount: read_i64(payload, 64).ok_or_else(|| "amount missing".to_string())?,
        tick: read_u32(payload, 72).ok_or_else(|| "tick missing".to_string())?,
        input_type: read_u16(payload, 76).ok_or_else(|| "inputType missing".to_string())?,
        input_size: input_size as u16,
        input_hex: bytes_to_hex(&payload[input_start..input_end]),
        signature_hex: bytes_to_hex(&payload[signature_start..]),
    })
}

fn parse_wallet_public_key(input: &str) -> Result<[u8; 32], String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("Wallet is empty".to_string());
    }

    if let Ok(hex_key) = parse_public_key_hex(trimmed) {
        return Ok(hex_key);
    }
    parse_public_key_identity(trimmed)
}

fn parse_public_key_hex(input: &str) -> Result<[u8; 32], String> {
    let hex = if let Some(rest) = input.strip_prefix("0x") {
        rest
    } else if let Some(rest) = input.strip_prefix("0X") {
        rest
    } else {
        input
    };
    if hex.len() != 64 {
        return Err("Public key hex must have 64 hex characters".to_string());
    }

    let mut out = [0u8; 32];
    for (idx, slot) in out.iter_mut().enumerate() {
        let offset = idx * 2;
        let byte = u8::from_str_radix(&hex[offset..offset + 2], 16)
            .map_err(|_| format!("Invalid hex at position {offset}"))?;
        *slot = byte;
    }
    Ok(out)
}

fn parse_public_key_identity(identity: &str) -> Result<[u8; 32], String> {
    let identity_upper = identity.trim().to_ascii_uppercase();
    if identity_upper.len() != 60 {
        return Err("Wallet identity must be 60 chars (A-Z) or 0x + 64 hex public key".to_string());
    }

    let bytes = identity_upper.as_bytes();
    for ch in bytes {
        if !(*ch >= b'A' && *ch <= b'Z') {
            return Err("Identity contains invalid characters, expected only A-Z".to_string());
        }
    }

    let mut public_key = [0u8; 32];
    for fragment_idx in 0..4 {
        let mut fragment_value: u64 = 0;
        for char_idx in (0..14).rev() {
            let index = fragment_idx * 14 + char_idx;
            let value = (bytes[index] - b'A') as u64;
            fragment_value = fragment_value
                .checked_mul(26)
                .and_then(|v| v.checked_add(value))
                .ok_or_else(|| "Identity decoding overflow".to_string())?;
        }

        let offset = fragment_idx * 8;
        public_key[offset..offset + 8].copy_from_slice(&fragment_value.to_le_bytes());
    }

    Ok(public_key)
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn read_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    let chunk = bytes.get(offset..offset + 2)?;
    Some(u16::from_le_bytes([chunk[0], chunk[1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    let chunk = bytes.get(offset..offset + 4)?;
    Some(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
}

fn read_i32(bytes: &[u8], offset: usize) -> Option<i32> {
    let chunk = bytes.get(offset..offset + 4)?;
    Some(i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
}

fn read_i64(bytes: &[u8], offset: usize) -> Option<i64> {
    let chunk = bytes.get(offset..offset + 8)?;
    Some(i64::from_le_bytes([
        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
    ]))
}

fn parse_config() -> Result<Config, String> {
    let mut port = DEFAULT_PORT;
    let mut listen_ip = Ipv4Addr::new(0, 0, 0, 0);

    let mut api_timeout_ms = DEFAULT_API_TIMEOUT_MS;
    let mut grpc_enabled = true;
    let mut grpc_listen = SocketAddr::from(([127, 0, 0, 1], DEFAULT_GRPC_PORT));

    let mut target_outbound = 8usize;
    let mut max_incoming = 32usize;
    let mut max_seen = 65_536usize;
    let mut reconnect_ms = 2_000u64;
    let mut relay_all = false;
    let mut dns_bootstrap = true;
    let mut dns_lite_peers = 0usize;
    let mut dns_timeout_ms = DEFAULT_DNS_TIMEOUT_MS;
    let mut traffic_log = false;
    let mut seed_peer_args = Vec::<String>::new();

    let args: Vec<String> = env::args().collect();
    let mut index = 1usize;
    while index < args.len() {
        match args[index].as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--port" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --port".to_string())?;
                port = value
                    .parse::<u16>()
                    .map_err(|_| format!("Invalid port: {value}"))?;
            }
            "--listen-ip" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --listen-ip".to_string())?;
                listen_ip = value
                    .parse::<Ipv4Addr>()
                    .map_err(|_| format!("Invalid IPv4 address: {value}"))?;
            }
            "--api-timeout-ms" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --api-timeout-ms".to_string())?;
                api_timeout_ms = value
                    .parse::<u64>()
                    .map_err(|_| format!("Invalid API timeout value: {value}"))?;
            }
            "--grpc-listen" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --grpc-listen".to_string())?;
                grpc_listen = parse_socket_addr(value)?;
            }
            "--no-grpc" => {
                grpc_enabled = false;
            }
            "--peer" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --peer".to_string())?;
                seed_peer_args.push(value.clone());
            }
            "--target-outbound" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --target-outbound".to_string())?;
                target_outbound = value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid target outbound value: {value}"))?;
            }
            "--max-incoming" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --max-incoming".to_string())?;
                max_incoming = value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid max incoming value: {value}"))?;
            }
            "--max-seen" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --max-seen".to_string())?;
                max_seen = value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid max seen value: {value}"))?;
            }
            "--reconnect-ms" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --reconnect-ms".to_string())?;
                reconnect_ms = value
                    .parse::<u64>()
                    .map_err(|_| format!("Invalid reconnect interval: {value}"))?;
            }
            "--relay-all" => {
                relay_all = true;
            }
            "--no-dns-bootstrap" => {
                dns_bootstrap = false;
            }
            "--dns-lite-peers" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --dns-lite-peers".to_string())?;
                dns_lite_peers = value
                    .parse::<usize>()
                    .map_err(|_| format!("Invalid DNS lite peers value: {value}"))?;
            }
            "--dns-timeout-ms" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| "Missing value for --dns-timeout-ms".to_string())?;
                dns_timeout_ms = value
                    .parse::<u64>()
                    .map_err(|_| format!("Invalid DNS timeout value: {value}"))?;
            }
            "--traffic-log" => {
                traffic_log = true;
            }
            unknown => {
                return Err(format!("Unknown argument: {unknown}"));
            }
        }
        index += 1;
    }

    let mut seed_peers = Vec::<SocketAddrV4>::new();
    for value in seed_peer_args {
        seed_peers.push(parse_peer_arg(&value, port)?);
    }

    seed_peers.sort_unstable();
    seed_peers.dedup();

    Ok(Config {
        listen_addr: SocketAddrV4::new(listen_ip, port),
        api_timeout: Duration::from_millis(api_timeout_ms.max(1_000)),
        grpc_listen_addr: grpc_listen,
        grpc_enabled,
        network_port: port,
        target_outbound,
        max_incoming,
        max_seen,
        reconnect_interval: Duration::from_millis(reconnect_ms.max(200)),
        relay_all,
        dns_bootstrap,
        dns_lite_peers,
        dns_timeout: Duration::from_millis(dns_timeout_ms.max(500)),
        traffic_log,
        seed_peers,
    })
}

fn parse_peer_arg(value: &str, default_port: u16) -> Result<SocketAddrV4, String> {
    if let Ok(addr) = value.parse::<SocketAddrV4>() {
        return Ok(addr);
    }
    if let Ok(ip) = value.parse::<Ipv4Addr>() {
        return Ok(SocketAddrV4::new(ip, default_port));
    }
    Err(format!(
        "Invalid peer value: {value}. Expected ip or ip:port"
    ))
}

fn parse_socket_addr(value: &str) -> Result<SocketAddr, String> {
    value
        .parse::<SocketAddr>()
        .map_err(|_| format!("Invalid socket address: {value}"))
}

fn print_usage() {
    println!("Usage:");
    println!("  QubicLightNode [options]");
    println!();
    println!("Options:");
    println!("  --peer <ip[:port]>       Seed peer (repeatable).");
    println!("  --port <u16>             Network/listen port (default: 21841).");
    println!("  --listen-ip <ipv4>       Listen IP (default: 0.0.0.0).");
    println!("  --target-outbound <n>    Target outgoing connections (default: 8).");
    println!("  --max-incoming <n>       Max incoming connections (default: 32).");
    println!("  --max-seen <n>           Dedup window size (default: 65536).");
    println!("  --reconnect-ms <n>       Outgoing reconnect interval in ms (default: 2000).");
    println!("  --relay-all              Relay messages even with non-zero dejavu.");
    println!("  --no-dns-bootstrap       Disable peer bootstrap from api.qubic.global.");
    println!("  --dns-lite-peers <n>     Requested lite peers count from DNS API (default: auto).");
    println!("  --dns-timeout-ms <n>     DNS API request timeout in ms (default: 5000).");
    println!("  --traffic-log            Log RX/TX/relay events for network frames.");
    println!("  --api-timeout-ms <n>     API query timeout in ms (default: 6000).");
    println!("  --grpc-listen <ip:port>  gRPC bind address (default: 127.0.0.1:50051).");
    println!("  --no-grpc                Disable gRPC API.");
    println!("  -h, --help               Show this help.");
}
