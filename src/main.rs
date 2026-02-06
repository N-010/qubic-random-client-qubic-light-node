use rand::Rng;
use rand::seq::SliceRandom;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::time::sleep;

const DEFAULT_PORT: u16 = 21841;
const HEADER_SIZE: usize = 8;
const MAX_FRAME_SIZE: usize = 0x00FF_FFFF;
const EXCHANGE_PUBLIC_PEERS_TYPE: u8 = 0;
const NUMBER_OF_EXCHANGED_PEERS: usize = 4;
const EXCHANGE_PUBLIC_PEERS_FRAME_SIZE: usize = HEADER_SIZE + NUMBER_OF_EXCHANGED_PEERS * 4;
const DEFAULT_DNS_TIMEOUT_MS: u64 = 5_000;

#[derive(Clone, Debug)]
struct Config {
    listen_addr: SocketAddrV4,
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
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

#[derive(Debug)]
struct NodeState {
    sessions: HashMap<u64, Session>,
    connected_ip_refcount: HashMap<Ipv4Addr, usize>,
    known_peers: HashSet<SocketAddrV4>,
    pending_dials: HashSet<SocketAddrV4>,
    seen_order: VecDeque<[u8; 32]>,
    seen_set: HashSet<[u8; 32]>,
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
        tx: mpsc::UnboundedSender<Vec<u8>>,
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

    fn collect_targets(&self, source_peer_id: u64) -> Vec<mpsc::UnboundedSender<Vec<u8>>> {
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
    let listener = TcpListener::bind(config.listen_addr).await?;

    println!(
        "Qubic light relay started at {} | target_outbound={} | max_incoming={} | seed_peers={} | traffic_log={}",
        config.listen_addr,
        config.target_outbound,
        config.max_incoming,
        config.seed_peers.len(),
        config.traffic_log
    );
    if config.seed_peers.is_empty() {
        println!("No seed peers configured. Use --peer <ip[:port]> to join the public network.");
    }

    let shared_config = Arc::new(config);

    let accept_state = Arc::clone(&state);
    let accept_config = Arc::clone(&shared_config);
    tokio::spawn(async move {
        accept_loop(listener, accept_state, accept_config).await;
    });

    let dial_state = Arc::clone(&state);
    let dial_config = Arc::clone(&shared_config);
    tokio::spawn(async move {
        dial_loop(dial_state, dial_config).await;
    });

    tokio::signal::ctrl_c().await?;
    println!("Shutdown signal received, stopping.");
    Ok(())
}

async fn accept_loop(listener: TcpListener, state: Arc<Mutex<NodeState>>, config: Arc<Config>) {
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
                let connection_config = Arc::clone(&config);
                tokio::spawn(async move {
                    establish_connection(
                        stream,
                        remote_v4,
                        false,
                        connection_state,
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

async fn dial_loop(state: Arc<Mutex<NodeState>>, config: Arc<Config>) {
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
            let config_for_task = Arc::clone(&config);
            tokio::spawn(async move {
                match TcpStream::connect(target).await {
                    Ok(stream) => {
                        if let Err(err) = stream.set_nodelay(true) {
                            eprintln!("Failed to set TCP_NODELAY for {target}: {err}");
                        }
                        establish_connection(stream, target, true, state_for_task, config_for_task)
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
    config: Arc<Config>,
) {
    let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (peer_id, handshake_payload, incoming_count, outgoing_count) = {
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
        )
    };

    let _ = tx.send(handshake_payload);
    println!(
        "Connected {} [{}] | in={} out={}",
        remote,
        if outbound { "out" } else { "in" },
        incoming_count,
        outgoing_count
    );

    tokio::spawn(connection_worker(
        stream, peer_id, remote, rx, state, config,
    ));
}

async fn connection_worker(
    stream: TcpStream,
    peer_id: u64,
    remote: SocketAddrV4,
    mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
    state: Arc<Mutex<NodeState>>,
    config: Arc<Config>,
) {
    let (mut reader, mut writer) = stream.into_split();
    let traffic_log = config.traffic_log;

    let writer_task = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if traffic_log {
                let (size, message_type, dejavu) = frame_meta(&frame);
                println!(
                    "TX {} | size={} type={}({}) dejavu={}",
                    remote,
                    size,
                    message_type_name(message_type),
                    message_type,
                    dejavu
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
                        println!(
                            "RX {} | peer_id={} size={} type={}({}) dejavu={}",
                            remote,
                            peer_id,
                            size,
                            message_type_name(message_type),
                            message_type,
                            dejavu
                        );
                    }

                    process_incoming_frame(peer_id, frame, Arc::clone(&state), Arc::clone(&config))
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

    let (in_count, out_count) = {
        let mut locked = state.lock().await;
        let _ = locked.unregister_session(peer_id);
        (locked.incoming_count(), locked.outgoing_count())
    };
    println!("Disconnected {remote} | in={in_count} out={out_count}");
}

async fn process_incoming_frame(
    source_peer_id: u64,
    frame: Vec<u8>,
    state: Arc<Mutex<NodeState>>,
    config: Arc<Config>,
) {
    if frame.len() < HEADER_SIZE {
        return;
    }

    let message_type = frame[3];
    let dejavu = u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]);

    if message_type == EXCHANGE_PUBLIC_PEERS_TYPE {
        let discovered = parse_exchange_public_peers(&frame, config.network_port);
        if !discovered.is_empty() {
            let mut locked = state.lock().await;
            for peer in discovered {
                let _ = locked.add_discovered_peer(peer);
            }
        }
    }

    if !config.relay_all && dejavu != 0 {
        return;
    }

    let digest = *blake3::hash(&frame).as_bytes();
    let targets = {
        let mut locked = state.lock().await;
        if !locked.mark_seen(digest) {
            if config.traffic_log {
                let (size, message_type, dejavu) = frame_meta(&frame);
                println!(
                    "DROP_DUP peer_id={} size={} type={}({}) dejavu={}",
                    source_peer_id,
                    size,
                    message_type_name(message_type),
                    message_type,
                    dejavu
                );
            }
            return;
        }
        locked.collect_targets(source_peer_id)
    };

    if config.traffic_log {
        let (size, message_type, dejavu) = frame_meta(&frame);
        println!(
            "RELAY peer_id={} -> {} peers | size={} type={}({}) dejavu={}",
            source_peer_id,
            targets.len(),
            size,
            message_type_name(message_type),
            message_type,
            dejavu
        );
    }

    for tx in targets {
        let _ = tx.send(frame.clone());
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

fn parse_config() -> Result<Config, String> {
    let mut port = DEFAULT_PORT;
    let mut listen_ip = Ipv4Addr::new(0, 0, 0, 0);
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
    println!("  -h, --help               Show this help.");
}
