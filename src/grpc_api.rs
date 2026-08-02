use crate::LIGHTNODE_FILE_DESCRIPTOR_SET;
use crate::codec::{parse_wallet_public_key, tx_id_from_bytes};
use crate::frame::{
    MAX_CONTRACT_FUNCTION_INPUT_SIZE, MAX_NUMBER_OF_CONTRACTS, parse_transaction_layout,
};
use crate::lightnodepb;
use crate::network::broadcast_transaction_to_network;
use crate::peer_api::{
    PeerQueryError, query_balance, query_contract_function, stream_tick_transactions,
};
use crate::types::{ApiState, BalanceResponse, TickStatus, TickTransaction};
use std::collections::HashMap;
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::{Instant as TokioInstant, sleep_until, timeout_at};
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub(crate) struct GrpcService {
    pub(crate) api: ApiState,
    peer_query_slots: Arc<Semaphore>,
    tick_stream_slots: Arc<Semaphore>,
    broadcast_slots: Arc<Semaphore>,
    broadcast_rate: Arc<StdMutex<BroadcastRateLimiter>>,
    broadcasts_inflight: Arc<StdMutex<BroadcastInflight>>,
}

const MAX_CONCURRENT_PEER_QUERIES: usize = 64;
const MAX_CONCURRENT_TICK_STREAMS: usize = 8;
const MAX_CONCURRENT_BROADCASTS: usize = 32;
const PEER_QUERY_OVERLOADED_ERROR: &str =
    "Peer-backed API is overloaded; retry after an in-flight query completes";

#[derive(Debug)]
pub(crate) struct TickTransactionStream {
    data: mpsc::Receiver<lightnodepb::Transaction>,
    terminal: oneshot::Receiver<Option<Status>>,
    data_closed: bool,
    terminal_done: bool,
}

impl Stream for TickTransactionStream {
    type Item = Result<lightnodepb::Transaction, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.data_closed {
            match Pin::new(&mut self.data).poll_recv(context) {
                Poll::Ready(Some(transaction)) => return Poll::Ready(Some(Ok(transaction))),
                Poll::Ready(None) => self.data_closed = true,
                Poll::Pending => return Poll::Pending,
            }
        }
        if self.terminal_done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.terminal).poll(context) {
            Poll::Ready(Ok(None)) => {
                self.terminal_done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Ok(Some(status))) => {
                self.terminal_done = true;
                Poll::Ready(Some(Err(status)))
            }
            Poll::Ready(Err(_)) => {
                self.terminal_done = true;
                Poll::Ready(Some(Err(Status::internal(
                    "tick stream bridge stopped before reporting completion",
                ))))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn peer_query_status(error: PeerQueryError) -> Status {
    let message = error.to_string();
    match error {
        PeerQueryError::Deadline(_) => Status::deadline_exceeded(message),
        PeerQueryError::NoPeers | PeerQueryError::PeerUnavailable(_) => {
            Status::unavailable(message)
        }
        PeerQueryError::LocalOverload(_) => Status::resource_exhausted(message),
        PeerQueryError::Protocol(_) => Status::data_loss(message),
        PeerQueryError::Cancelled => Status::cancelled(message),
        PeerQueryError::Internal(_) => Status::internal(message),
    }
}

fn map_tick_query_result(
    result: Result<Result<(), PeerQueryError>, tokio::task::JoinError>,
) -> Option<Status> {
    match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(peer_query_status(error)),
        Err(error) if error.is_cancelled() => Some(Status::cancelled("tick stream task cancelled")),
        Err(error) => Some(Status::internal(format!(
            "tick stream task failed: {error}"
        ))),
    }
}

pub(crate) async fn run_grpc_server(api_state: ApiState) -> std::io::Result<()> {
    let service = GrpcService {
        api: api_state.clone(),
        peer_query_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_PEER_QUERIES)),
        tick_stream_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_TICK_STREAMS)),
        broadcast_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_BROADCASTS)),
        broadcast_rate: Arc::new(StdMutex::new(BroadcastRateLimiter::default())),
        broadcasts_inflight: Arc::new(StdMutex::new(BroadcastInflight::default())),
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
        let cached = self.api.trusted_network.status();
        if let Some(status) = cached {
            Ok(Response::new(lightnodepb::GetStatusResponse {
                ok: true,
                source: "verified_tick_quorum".to_string(),
                status: Some(map_tick_status(status)),
                warning: "Epoch and tick are backed by a verified 451-computor quorum; initial_tick and tick_duration_ms are unavailable and returned as zero."
                    .to_string(),
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
        let Some(_permit) = self.try_acquire_peer_query_slot() else {
            return Ok(Response::new(lightnodepb::GetBalanceResponse {
                ok: false,
                balance: None,
                error: PEER_QUERY_OVERLOADED_ERROR.to_string(),
            }));
        };

        match query_balance(
            Arc::clone(&self.api.node_state),
            Arc::clone(&self.api.pending_requests),
            Arc::clone(&self.api.outbound_budget),
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
                error: err.to_string(),
            })),
        }
    }

    type StreamTickTransactionsStream = TickTransactionStream;

    async fn stream_tick_transactions(
        &self,
        request: Request<lightnodepb::GetTickTransactionsRequest>,
    ) -> Result<Response<Self::StreamTickTransactionsStream>, Status> {
        let tick = request.into_inner().tick;
        let Ok(permit) = Arc::clone(&self.tick_stream_slots).try_acquire_owned() else {
            return Err(Status::resource_exhausted(
                "Tick transaction stream limit reached",
            ));
        };
        let deadline = TokioInstant::now() + self.api.config.api_timeout;
        let (rpc_tx, rpc_rx) = mpsc::channel(32);
        let (terminal_tx, terminal_rx) = oneshot::channel();
        let api = self.api.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let (domain_tx, mut domain_rx) = mpsc::channel(32);
            let mut query = tokio::spawn(stream_tick_transactions(
                Arc::clone(&api.node_state),
                Arc::clone(&api.pending_requests),
                Arc::clone(&api.outbound_budget),
                Arc::clone(&api.config),
                tick,
                domain_tx,
                deadline,
            ));
            let mut query_result = None;
            let mut query_joined = false;
            let mut domain_open = true;
            let terminal_status = loop {
                tokio::select! {
                    biased;
                    _ = rpc_tx.closed() => {
                        if query_result.is_none() {
                            query.abort();
                            let _ = (&mut query).await;
                        }
                        return;
                    }
                    _ = sleep_until(deadline) => {
                        if query_result.is_none() {
                            query.abort();
                            let _ = (&mut query).await;
                            query_joined = true;
                        }
                        break Some(Status::deadline_exceeded(
                            "Tick transaction stream deadline exceeded",
                        ));
                    }
                    result = &mut query, if query_result.is_none() => {
                        query_result = Some(result);
                        query_joined = true;
                        if !domain_open {
                            break map_tick_query_result(query_result.take().expect("query result was just stored"));
                        }
                    }
                    transaction = domain_rx.recv(), if domain_open => {
                        let Some(transaction) = transaction else {
                            domain_open = false;
                            if query_result.is_some() {
                                break map_tick_query_result(query_result.take().expect("completed query has a result"));
                            }
                            continue;
                        };
                        let rpc_item = match transaction {
                            Ok(transaction) => map_transaction(transaction),
                            Err(err) => break Some(peer_query_status(err)),
                        };
                        match timeout_at(deadline, rpc_tx.send(rpc_item)).await {
                            Ok(Ok(())) => {}
                            Ok(Err(_)) => {
                                if query_result.is_none() {
                                    query.abort();
                                    let _ = (&mut query).await;
                                }
                                return;
                            }
                            Err(_) => {
                                if query_result.is_none() {
                                    query.abort();
                                    let _ = (&mut query).await;
                                    query_joined = true;
                                }
                                break Some(Status::deadline_exceeded(
                                    "Tick transaction stream deadline exceeded",
                                ));
                            }
                        }
                    }
                }
            };
            if !query_joined {
                query.abort();
                let _ = (&mut query).await;
            }
            drop(rpc_tx);
            let _ = terminal_tx.send(terminal_status);
        });
        Ok(Response::new(TickTransactionStream {
            data: rpc_rx,
            terminal: terminal_rx,
            data_closed: false,
            terminal_done: false,
        }))
    }

    async fn query_contract_function(
        &self,
        request: Request<lightnodepb::QueryContractFunctionRequest>,
    ) -> Result<Response<lightnodepb::QueryContractFunctionResponse>, Status> {
        let request = request.into_inner();
        if !(1..MAX_NUMBER_OF_CONTRACTS).contains(&request.contract_index) {
            return Err(Status::invalid_argument(
                "contract_index must be between 1 and 1023",
            ));
        }
        let input_type = u16::try_from(request.input_type)
            .map_err(|_| Status::invalid_argument("input_type must fit into uint16"))?;
        if request.input.len() > MAX_CONTRACT_FUNCTION_INPUT_SIZE {
            return Err(Status::invalid_argument(format!(
                "input must not exceed {MAX_CONTRACT_FUNCTION_INPUT_SIZE} bytes"
            )));
        }
        let Some(_permit) = self.try_acquire_peer_query_slot() else {
            return Ok(Response::new(lightnodepb::QueryContractFunctionResponse {
                ok: false,
                output: Vec::new(),
                error: PEER_QUERY_OVERLOADED_ERROR.to_string(),
            }));
        };

        match query_contract_function(
            Arc::clone(&self.api.node_state),
            Arc::clone(&self.api.pending_requests),
            Arc::clone(&self.api.outbound_budget),
            Arc::clone(&self.api.config),
            request.contract_index,
            input_type,
            &request.input,
        )
        .await
        {
            Ok(output) => Ok(Response::new(lightnodepb::QueryContractFunctionResponse {
                ok: true,
                output,
                error: String::new(),
            })),
            Err(err) => Ok(Response::new(lightnodepb::QueryContractFunctionResponse {
                ok: false,
                output: Vec::new(),
                error: err.to_string(),
            })),
        }
    }

    async fn broadcast_transaction(
        &self,
        request: Request<lightnodepb::BroadcastTransactionRequest>,
    ) -> Result<Response<lightnodepb::BroadcastTransactionResponse>, Status> {
        let remote_ip = request.remote_addr().map(|address| address.ip());
        let tx_bytes = request.into_inner().tx_bytes;
        if tx_bytes.is_empty() {
            return Ok(Response::new(lightnodepb::BroadcastTransactionResponse {
                ok: false,
                tx_id: String::new(),
                error: "Transaction payload is empty".to_string(),
            }));
        }

        parse_transaction_layout(&tx_bytes, None).map_err(Status::invalid_argument)?;
        let _broadcast_permit = Arc::clone(&self.broadcast_slots)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("Broadcast concurrency limit reached"))?;
        if !self
            .broadcast_rate
            .lock()
            .expect("broadcast rate mutex should not be poisoned")
            .allow(remote_ip, Instant::now())
        {
            return Err(Status::resource_exhausted("Broadcast rate limit exceeded"));
        }

        let tx_id = tx_id_from_bytes(&tx_bytes);
        let digest = *blake3::hash(&tx_bytes).as_bytes();
        let inflight = self
            .broadcasts_inflight
            .lock()
            .expect("broadcast inflight mutex should not be poisoned")
            .begin(digest);
        let result = match inflight {
            InflightRole::Leader => {
                let leader =
                    BroadcastLeaderGuard::new(Arc::clone(&self.broadcasts_inflight), digest);
                let result = broadcast_transaction_to_network(
                    Arc::clone(&self.api.node_state),
                    Arc::clone(&self.api.dedup),
                    Arc::clone(&self.api.outbound_budget),
                    &tx_bytes,
                )
                .await;
                leader.complete(result.clone());
                result
            }
            InflightRole::Follower(receiver) => receiver
                .await
                .unwrap_or_else(|_| Err("Broadcast leader stopped unexpectedly".to_string())),
        };
        match result {
            Ok(_) => Ok(Response::new(lightnodepb::BroadcastTransactionResponse {
                ok: true,
                tx_id,
                error: String::new(),
            })),
            Err(err) => Ok(Response::new(lightnodepb::BroadcastTransactionResponse {
                ok: false,
                tx_id: String::new(),
                error: err,
            })),
        }
    }
}

#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    last_seen: Instant,
}

impl TokenBucket {
    fn refill(&mut self, now: Instant, rate: f64, burst: f64) {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        self.tokens = (self.tokens + elapsed * rate).min(burst);
        self.last_refill = now;
        self.last_seen = now;
    }

    fn has_token(&self) -> bool {
        self.tokens >= 1.0
    }

    fn consume(&mut self) {
        self.tokens -= 1.0;
    }
}

#[derive(Debug)]
struct BroadcastRateLimiter {
    global: TokenBucket,
    clients: HashMap<Option<IpAddr>, TokenBucket>,
}

impl Default for BroadcastRateLimiter {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            global: TokenBucket {
                tokens: 200.0,
                last_refill: now,
                last_seen: now,
            },
            clients: HashMap::new(),
        }
    }
}

impl BroadcastRateLimiter {
    fn allow(&mut self, client: Option<IpAddr>, now: Instant) -> bool {
        self.global.refill(now, 100.0, 200.0);
        if !self.global.has_token() {
            return false;
        }
        if self.clients.len() >= 4096 {
            let cutoff = now.checked_sub(Duration::from_secs(10 * 60)).unwrap_or(now);
            self.clients.retain(|_, bucket| bucket.last_seen >= cutoff);
            if self.clients.len() >= 4096 && !self.clients.contains_key(&client) {
                return false;
            }
        }
        if let Some(client_bucket) = self.clients.get_mut(&client) {
            client_bucket.refill(now, 10.0, 20.0);
            if !client_bucket.has_token() {
                return false;
            }
        }
        let client_bucket = self.clients.entry(client).or_insert(TokenBucket {
            tokens: 20.0,
            last_refill: now,
            last_seen: now,
        });
        self.global.consume();
        client_bucket.consume();
        true
    }
}

#[derive(Debug, Default)]
struct BroadcastInflight {
    waiters: HashMap<[u8; 32], Vec<oneshot::Sender<Result<(), String>>>>,
}

enum InflightRole {
    Leader,
    Follower(oneshot::Receiver<Result<(), String>>),
}

struct BroadcastLeaderGuard {
    inflight: Arc<StdMutex<BroadcastInflight>>,
    digest: [u8; 32],
    completed: bool,
}

impl BroadcastLeaderGuard {
    fn new(inflight: Arc<StdMutex<BroadcastInflight>>, digest: [u8; 32]) -> Self {
        Self {
            inflight,
            digest,
            completed: false,
        }
    }

    fn complete(mut self, result: Result<(), String>) {
        self.inflight
            .lock()
            .expect("broadcast inflight mutex should not be poisoned")
            .complete(self.digest, result);
        self.completed = true;
    }
}

impl Drop for BroadcastLeaderGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.inflight
                .lock()
                .expect("broadcast inflight mutex should not be poisoned")
                .complete(
                    self.digest,
                    Err("Broadcast leader stopped unexpectedly".to_string()),
                );
        }
    }
}

impl BroadcastInflight {
    fn begin(&mut self, digest: [u8; 32]) -> InflightRole {
        if let Some(waiters) = self.waiters.get_mut(&digest) {
            let (tx, rx) = oneshot::channel();
            waiters.push(tx);
            InflightRole::Follower(rx)
        } else {
            self.waiters.insert(digest, Vec::new());
            InflightRole::Leader
        }
    }

    fn complete(&mut self, digest: [u8; 32], result: Result<(), String>) {
        if let Some(waiters) = self.waiters.remove(&digest) {
            for waiter in waiters {
                let _ = waiter.send(result.clone());
            }
        }
    }
}

impl GrpcService {
    fn try_acquire_peer_query_slot(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.peer_query_slots).try_acquire_owned().ok()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DEFAULT_GRPC_PORT, DEFAULT_PORT};
    use crate::frame::{
        BROADCAST_TRANSACTION_TYPE, SIGNATURE_SIZE, TRANSACTION_BASE_SIZE, build_request_frame,
    };
    use crate::lightnodepb::light_node_server::LightNode;
    use crate::state::NodeState;
    use bytes::Bytes;
    use pretty_assertions::assert_eq;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::time::Duration;
    use tokio::sync::{Mutex, mpsc, watch};
    use tokio_stream::StreamExt;

    fn test_config() -> Arc<Config> {
        Arc::new(Config {
            listen_addr: SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), DEFAULT_PORT),
            api_timeout: Duration::from_secs(1),
            grpc_listen_addr: SocketAddr::from(([127, 0, 0, 1], DEFAULT_GRPC_PORT)),
            grpc_enabled: true,
            peer_port: DEFAULT_PORT,
            target_outbound: 8,
            max_incoming: 32,
            max_seen: 1_000,
            max_known_peers: 1_000,
            reconnect_interval: Duration::from_millis(2_000),
            peer_write_timeout: Duration::from_secs(5),
            peer_connect_timeout: Duration::from_secs(5),
            peer_handshake_timeout: Duration::from_secs(5),
            peer_frame_timeout: Duration::from_secs(30),
            max_frame_bytes: 1024 * 1024,
            relay_all: false,
            dns_bootstrap: false,
            dns_lite_peers: 0,
            dns_timeout: Duration::from_secs(1),
            traffic_log: false,
            seed_peers: Vec::new(),
            critical_peer_threshold: 4,
            emergency_dns_bootstrap: false,
            emergency_dns_backoff_initial_ms: 10_000,
            emergency_dns_backoff_max_ms: 300_000,
        })
    }

    fn test_service(node_state: Arc<Mutex<NodeState>>) -> GrpcService {
        let outbound_budget = Arc::new(Semaphore::new(crate::network::GLOBAL_OUTBOUND_QUEUE_BYTES));
        GrpcService {
            api: ApiState {
                node_state,
                dedup: Arc::new(crate::state::DedupWindow::new(1_000)),
                pending_requests: Arc::new(crate::pending::PendingRequests::default()),
                trusted_network: Arc::new(crate::verified::TrustedNetworkState::default()),
                outbound_budget,
                config: test_config(),
            },
            peer_query_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_PEER_QUERIES)),
            tick_stream_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_TICK_STREAMS)),
            broadcast_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_BROADCASTS)),
            broadcast_rate: Arc::new(StdMutex::new(BroadcastRateLimiter::default())),
            broadcasts_inflight: Arc::new(StdMutex::new(BroadcastInflight::default())),
        }
    }

    fn valid_transaction(marker: u8) -> Vec<u8> {
        let mut transaction = vec![0; TRANSACTION_BASE_SIZE + SIGNATURE_SIZE];
        transaction[0] = marker;
        transaction[64..72].copy_from_slice(&100i64.to_le_bytes());
        transaction[72..76].copy_from_slice(&123u32.to_le_bytes());
        transaction[TRANSACTION_BASE_SIZE] = marker;
        transaction
    }

    #[test]
    fn proto_exposes_only_the_streaming_tick_transaction_rpc() {
        let proto = include_str!("../proto/lightnode.proto");
        assert!(proto.contains(
            "rpc StreamTickTransactions(GetTickTransactionsRequest) returns (stream Transaction);"
        ));
        assert!(!proto.contains("rpc GetTickTransactions(GetTickTransactionsRequest)"));
    }

    #[tokio::test]
    async fn broadcast_transaction_success() {
        let node_state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let (peer_tx, mut peer_rx) = mpsc::channel(1);
        {
            let mut locked = node_state.lock().await;
            let peer = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), DEFAULT_PORT);
            let (disconnect_tx, _disconnect_rx) = watch::channel(false);
            let registered =
                locked.register_session(peer, true, peer_tx, disconnect_tx, DEFAULT_PORT);
            assert_eq!(registered.is_some(), true);
        }

        let service = test_service(Arc::clone(&node_state));
        let tx_bytes = valid_transaction(1);
        let request = Request::new(lightnodepb::BroadcastTransactionRequest {
            tx_bytes: tx_bytes.clone(),
        });

        let response = LightNode::broadcast_transaction(&service, request)
            .await
            .expect("broadcast_transaction should return grpc response")
            .into_inner();

        assert_eq!(
            response,
            lightnodepb::BroadcastTransactionResponse {
                ok: true,
                tx_id: tx_id_from_bytes(&tx_bytes),
                error: String::new(),
            }
        );

        let outbound_frame = peer_rx
            .recv()
            .await
            .expect("peer should receive broadcast frame");
        let expected_frame = build_request_frame(BROADCAST_TRANSACTION_TYPE, 0, &tx_bytes)
            .expect("broadcast frame should be buildable");
        assert_eq!(&*outbound_frame.bytes, expected_frame.as_slice());
    }

    #[tokio::test]
    async fn sequential_duplicate_broadcast_is_idempotent() {
        let node_state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let (peer_tx, mut peer_rx) = mpsc::channel(2);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        node_state
            .lock()
            .await
            .register_session(
                SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), DEFAULT_PORT),
                true,
                peer_tx,
                disconnect_tx,
                DEFAULT_PORT,
            )
            .expect("peer should register");
        let service = test_service(node_state);
        let tx_bytes = valid_transaction(2);

        for _ in 0..2 {
            let response = LightNode::broadcast_transaction(
                &service,
                Request::new(lightnodepb::BroadcastTransactionRequest {
                    tx_bytes: tx_bytes.clone(),
                }),
            )
            .await
            .expect("duplicate broadcast should return a response")
            .into_inner();
            assert!(response.ok);
        }

        assert!(peer_rx.recv().await.is_some());
        assert!(peer_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn broadcast_transaction_empty_payload() {
        let service = test_service(Arc::new(Mutex::new(NodeState::new(1_000, &[]))));
        let request = Request::new(lightnodepb::BroadcastTransactionRequest {
            tx_bytes: Vec::new(),
        });

        let response = LightNode::broadcast_transaction(&service, request)
            .await
            .expect("broadcast_transaction should return grpc response")
            .into_inner();

        assert_eq!(
            response,
            lightnodepb::BroadcastTransactionResponse {
                ok: false,
                tx_id: String::new(),
                error: "Transaction payload is empty".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn broadcast_transaction_rejects_malformed_payload() {
        let service = test_service(Arc::new(Mutex::new(NodeState::new(1_000, &[]))));

        let error = LightNode::broadcast_transaction(
            &service,
            Request::new(lightnodepb::BroadcastTransactionRequest {
                tx_bytes: vec![1, 2, 3],
            }),
        )
        .await
        .expect_err("malformed transaction should be rejected as invalid argument");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn query_contract_function_rejects_invalid_arguments() {
        let service = test_service(Arc::new(Mutex::new(NodeState::new(1_000, &[]))));

        let zero_contract = LightNode::query_contract_function(
            &service,
            Request::new(lightnodepb::QueryContractFunctionRequest {
                contract_index: 0,
                input_type: 2,
                input: Vec::new(),
            }),
        )
        .await
        .expect_err("zero contract index should be rejected");
        assert_eq!(zero_contract.code(), tonic::Code::InvalidArgument);

        let out_of_range_contract = LightNode::query_contract_function(
            &service,
            Request::new(lightnodepb::QueryContractFunctionRequest {
                contract_index: MAX_NUMBER_OF_CONTRACTS,
                input_type: 2,
                input: Vec::new(),
            }),
        )
        .await
        .expect_err("contract index 1024 should be rejected");
        assert_eq!(out_of_range_contract.code(), tonic::Code::InvalidArgument);

        let oversized = LightNode::query_contract_function(
            &service,
            Request::new(lightnodepb::QueryContractFunctionRequest {
                contract_index: 3,
                input_type: 2,
                input: vec![0; MAX_CONTRACT_FUNCTION_INPUT_SIZE + 1],
            }),
        )
        .await
        .expect_err("oversized input should be rejected");
        assert_eq!(oversized.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn sixty_fifth_peer_backed_request_is_rejected_without_blocking_other_methods() {
        let service = test_service(Arc::new(Mutex::new(NodeState::new(1_000, &[]))));
        let permits = (0..MAX_CONCURRENT_PEER_QUERIES)
            .map(|_| {
                service
                    .try_acquire_peer_query_slot()
                    .expect("test should acquire each available peer-query slot")
            })
            .collect::<Vec<_>>();

        let overloaded = LightNode::get_balance(
            &service,
            Request::new(lightnodepb::GetBalanceRequest {
                wallet: format!("0x{}", "00".repeat(32)),
            }),
        )
        .await
        .expect("overloaded request should return grpc response")
        .into_inner();
        assert_eq!(overloaded.ok, false);
        assert_eq!(overloaded.error, PEER_QUERY_OVERLOADED_ERROR);

        let status =
            LightNode::get_status(&service, Request::new(lightnodepb::GetStatusRequest {}))
                .await
                .expect("status should remain available")
                .into_inner();
        assert!(status.error.contains("No tick data"));

        let broadcast = LightNode::broadcast_transaction(
            &service,
            Request::new(lightnodepb::BroadcastTransactionRequest {
                tx_bytes: Vec::new(),
            }),
        )
        .await
        .expect("broadcast should remain available")
        .into_inner();
        assert_eq!(broadcast.error, "Transaction payload is empty");

        drop(permits);
    }

    #[test]
    fn fifteen_peer_backed_requests_fit_within_limit() {
        let service = test_service(Arc::new(Mutex::new(NodeState::new(1_000, &[]))));
        let permits = (0..15)
            .map(|_| service.try_acquire_peer_query_slot())
            .collect::<Vec<_>>();

        assert!(permits.iter().all(Option::is_some));
        assert_eq!(service.peer_query_slots.available_permits(), 49);
    }

    #[test]
    fn rejected_client_bucket_does_not_consume_global_tokens() {
        let now = Instant::now();
        let exhausted_client = Some(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
        let other_client = Some(IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)));
        let mut limiter = BroadcastRateLimiter {
            global: TokenBucket {
                tokens: 2.0,
                last_refill: now,
                last_seen: now,
            },
            ..BroadcastRateLimiter::default()
        };
        limiter.clients.insert(
            exhausted_client,
            TokenBucket {
                tokens: 0.0,
                last_refill: now,
                last_seen: now,
            },
        );

        assert!(!limiter.allow(exhausted_client, now));
        assert_eq!(limiter.global.tokens, 2.0);
        assert!(limiter.allow(other_client, now));
        assert_eq!(limiter.global.tokens, 1.0);
    }

    #[test]
    fn exhausted_global_bucket_does_not_insert_client() {
        let now = Instant::now();
        let mut limiter = BroadcastRateLimiter {
            global: TokenBucket {
                tokens: 0.0,
                last_refill: now,
                last_seen: now,
            },
            ..BroadcastRateLimiter::default()
        };

        assert!(!limiter.allow(Some(IpAddr::V4(Ipv4Addr::LOCALHOST)), now));
        assert!(limiter.clients.is_empty());
    }

    #[tokio::test]
    async fn terminal_error_survives_a_full_transaction_buffer() {
        let (data_tx, data_rx) = mpsc::channel(1);
        data_tx
            .send(lightnodepb::Transaction::default())
            .await
            .expect("test stream should accept its buffered transaction");
        drop(data_tx);
        let (terminal_tx, terminal_rx) = oneshot::channel();
        terminal_tx
            .send(Some(Status::deadline_exceeded("deadline")))
            .expect("test stream should accept its terminal status");
        let mut stream = TickTransactionStream {
            data: data_rx,
            terminal: terminal_rx,
            data_closed: false,
            terminal_done: false,
        };

        assert!(
            stream
                .next()
                .await
                .expect("buffered item should exist")
                .is_ok()
        );
        let terminal = stream
            .next()
            .await
            .expect("terminal error should be emitted")
            .expect_err("terminal item should be an error");
        assert_eq!(terminal.code(), tonic::Code::DeadlineExceeded);
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn local_peer_query_overload_is_retryable_resource_exhaustion() {
        let status = peer_query_status(PeerQueryError::LocalOverload("overloaded"));
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn cancelled_tick_client_releases_stream_permit_and_pending_route() {
        let node_state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let (peer_tx, mut peer_rx) = mpsc::channel(16);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        node_state
            .lock()
            .await
            .register_session(
                SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), DEFAULT_PORT),
                true,
                peer_tx,
                disconnect_tx,
                DEFAULT_PORT,
            )
            .expect("peer should register");
        let service = test_service(node_state);
        let mut streams = Vec::new();
        for tick in 1..=MAX_CONCURRENT_TICK_STREAMS as u32 {
            streams.push(
                LightNode::stream_tick_transactions(
                    &service,
                    Request::new(lightnodepb::GetTickTransactionsRequest { tick }),
                )
                .await
                .expect("first eight streams should start")
                .into_inner(),
            );
        }
        for _ in 0..MAX_CONCURRENT_TICK_STREAMS {
            peer_rx
                .recv()
                .await
                .expect("each stream should issue one peer request");
        }
        let overloaded = LightNode::stream_tick_transactions(
            &service,
            Request::new(lightnodepb::GetTickTransactionsRequest { tick: 9 }),
        )
        .await
        .expect_err("ninth stream should be rejected");
        assert_eq!(overloaded.code(), tonic::Code::ResourceExhausted);

        drop(streams.pop());
        tokio::time::timeout(Duration::from_secs(1), async {
            while service.tick_stream_slots.available_permits() == 0
                || service.api.pending_requests.active_count() == MAX_CONCURRENT_TICK_STREAMS
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled stream should release its resources");

        let replacement = LightNode::stream_tick_transactions(
            &service,
            Request::new(lightnodepb::GetTickTransactionsRequest { tick: 9 }),
        )
        .await
        .expect("replacement stream should start");
        drop(replacement);
        drop(streams);
    }

    #[tokio::test]
    async fn cancelled_broadcast_leader_releases_followers_and_registry() {
        let inflight = Arc::new(StdMutex::new(BroadcastInflight::default()));
        let digest = [9; 32];
        assert!(matches!(
            inflight.lock().unwrap().begin(digest),
            InflightRole::Leader
        ));
        let InflightRole::Follower(follower) = inflight.lock().unwrap().begin(digest) else {
            panic!("second request should follow the leader");
        };

        drop(BroadcastLeaderGuard::new(Arc::clone(&inflight), digest));

        assert_eq!(
            follower.await.unwrap(),
            Err("Broadcast leader stopped unexpectedly".to_string())
        );
        assert!(matches!(
            inflight.lock().unwrap().begin(digest),
            InflightRole::Leader
        ));
    }

    #[tokio::test]
    async fn grpc_bind_failure_is_returned_to_supervisor() {
        let occupied = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = occupied.local_addr().unwrap();
        let service = test_service(Arc::new(Mutex::new(NodeState::new(1_000, &[]))));
        let mut config = (*service.api.config).clone();
        config.grpc_listen_addr = address;
        let mut api = service.api;
        api.config = Arc::new(config);

        let result = tokio::time::timeout(Duration::from_secs(1), run_grpc_server(api))
            .await
            .expect("occupied port should fail promptly");

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_status_without_quorum_does_not_wait_for_node_state_lock() {
        let node_state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let service = test_service(Arc::clone(&node_state));
        let _guard = node_state.lock().await;

        let response = tokio::time::timeout(
            Duration::from_millis(100),
            LightNode::get_status(&service, Request::new(lightnodepb::GetStatusRequest {})),
        )
        .await
        .expect("status must not wait for NodeState")
        .expect("status should return")
        .into_inner();

        assert_eq!(response.ok, false);
        assert!(response.error.contains("No tick data"));
    }

    #[tokio::test]
    async fn broadcast_transaction_no_peers_maps_to_error_response() {
        let service = test_service(Arc::new(Mutex::new(NodeState::new(1_000, &[]))));
        let request = Request::new(lightnodepb::BroadcastTransactionRequest {
            tx_bytes: valid_transaction(7),
        });

        let response = LightNode::broadcast_transaction(&service, request)
            .await
            .expect("broadcast_transaction should return grpc response")
            .into_inner();

        assert_eq!(
            response,
            lightnodepb::BroadcastTransactionResponse {
                ok: false,
                tx_id: String::new(),
                error: "No connected peers available for broadcast".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn broadcast_transaction_sends_to_at_most_six_peers() {
        let node_state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let mut peer_receivers = Vec::new();
        {
            let mut locked = node_state.lock().await;
            for last_octet in 1..=8 {
                let (peer_tx, peer_rx) = mpsc::channel(1);
                let (disconnect_tx, _disconnect_rx) = watch::channel(false);
                let peer = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, last_octet), DEFAULT_PORT);
                let registered =
                    locked.register_session(peer, true, peer_tx, disconnect_tx, DEFAULT_PORT);
                assert_eq!(registered.is_some(), true);
                peer_receivers.push(peer_rx);
            }
        }

        let service = test_service(node_state);
        let request = Request::new(lightnodepb::BroadcastTransactionRequest {
            tx_bytes: valid_transaction(4),
        });
        let response = LightNode::broadcast_transaction(&service, request)
            .await
            .expect("broadcast_transaction should return grpc response")
            .into_inner();

        assert_eq!(response.ok, true);
        let received_count = peer_receivers
            .iter_mut()
            .map(|peer_rx| peer_rx.try_recv().is_ok())
            .filter(|received| *received)
            .count();
        assert_eq!(received_count, 6);
    }

    #[tokio::test]
    async fn failed_broadcast_can_be_retried_after_full_peer_is_disconnected() {
        let node_state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let (full_peer_tx, _full_peer_rx) = mpsc::channel(1);
        full_peer_tx
            .try_send(
                crate::state::OutboundFrame::try_new(
                    Bytes::from_static(&[0]),
                    Arc::new(Semaphore::new(crate::state::PEER_OUTBOUND_QUEUE_BYTES)),
                    Arc::new(Semaphore::new(crate::network::GLOBAL_OUTBOUND_QUEUE_BYTES)),
                )
                .expect("test budgets should accept a frame"),
            )
            .expect("test peer queue should accept its first frame");
        let (disconnect_tx, mut disconnect_rx) = watch::channel(false);
        {
            let mut locked = node_state.lock().await;
            let peer = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), DEFAULT_PORT);
            let registered =
                locked.register_session(peer, true, full_peer_tx, disconnect_tx, DEFAULT_PORT);
            assert_eq!(registered.is_some(), true);
        }

        let service = test_service(Arc::clone(&node_state));
        let tx_bytes = valid_transaction(7);
        let first_response = LightNode::broadcast_transaction(
            &service,
            Request::new(lightnodepb::BroadcastTransactionRequest {
                tx_bytes: tx_bytes.clone(),
            }),
        )
        .await
        .expect("broadcast_transaction should return grpc response")
        .into_inner();

        assert_eq!(
            first_response,
            lightnodepb::BroadcastTransactionResponse {
                ok: false,
                tx_id: String::new(),
                error: "Failed to broadcast transaction: all peer outbound queues are full"
                    .to_string(),
            }
        );
        disconnect_rx
            .changed()
            .await
            .expect("full peer should receive a disconnect signal");
        assert_eq!(*disconnect_rx.borrow(), true);
        assert_eq!(node_state.lock().await.outgoing_count(), 0);

        let (replacement_tx, mut replacement_rx) = mpsc::channel(1);
        let (replacement_disconnect_tx, _replacement_disconnect_rx) = watch::channel(false);
        {
            let mut locked = node_state.lock().await;
            let peer = SocketAddrV4::new(Ipv4Addr::new(2, 2, 2, 2), DEFAULT_PORT);
            let registered = locked.register_session(
                peer,
                true,
                replacement_tx,
                replacement_disconnect_tx,
                DEFAULT_PORT,
            );
            assert_eq!(registered.is_some(), true);
        }

        let retry_response = LightNode::broadcast_transaction(
            &service,
            Request::new(lightnodepb::BroadcastTransactionRequest {
                tx_bytes: tx_bytes.clone(),
            }),
        )
        .await
        .expect("broadcast_transaction retry should return grpc response")
        .into_inner();

        assert_eq!(retry_response.ok, true);
        let retried_frame = replacement_rx
            .recv()
            .await
            .expect("replacement peer should receive the retried transaction");
        let expected_frame = build_request_frame(BROADCAST_TRANSACTION_TYPE, 0, &tx_bytes)
            .expect("broadcast frame should be buildable");
        assert_eq!(&*retried_frame.bytes, expected_frame.as_slice());
    }
}
