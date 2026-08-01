use crate::codec::{bytes_to_hex, read_i32, read_i64, read_u32};
use crate::config::Config;
use crate::frame::{
    BROADCAST_TRANSACTION_TYPE, END_RESPONSE_TYPE, MAX_CONTRACT_FUNCTION_OUTPUT_SIZE,
    NUMBER_OF_TRANSACTIONS_PER_TICK, REQUEST_ENTITY_TYPE, RESPOND_CONTRACT_FUNCTION_TYPE,
    RESPOND_ENTITY_PAYLOAD_SIZE, RESPOND_ENTITY_TYPE, SPECTRUM_CAPACITY, TRY_AGAIN_TYPE,
    build_request_contract_function_frame, build_request_frame,
    build_request_tick_transactions_frame, frame_payload, parse_transaction_layout,
};
use crate::pending::{PendingEvent, PendingRequests};
use crate::state::NodeState;
use crate::types::{BalanceResponse, TickTransaction};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout_at};

const MAX_PARALLEL_PEER_QUERIES: usize = 3;

pub(crate) async fn query_balance(
    state: Arc<Mutex<NodeState>>,
    pending: Arc<PendingRequests>,
    config: Arc<Config>,
    wallet: &str,
    public_key: [u8; 32],
) -> Result<BalanceResponse, String> {
    let timeout_duration = config.api_timeout;
    let deadline = Instant::now() + timeout_duration;
    let targets = timeout_at(deadline, state.lock())
        .await
        .map_err(|_| api_timeout_error(timeout_duration))?
        .collect_all_targets(MAX_PARALLEL_PEER_QUERIES);
    if targets.is_empty() {
        return Err("No connected peers available".to_string());
    }
    let (registration, receivers) = pending.register(targets.iter().map(|target| target.peer_id));
    let request = Bytes::from(build_request_frame(
        REQUEST_ENTITY_TYPE,
        registration.dejavu(),
        &public_key,
    )?);
    timeout_at(
        deadline,
        send_request_to_targets(&state, &pending, targets, &request),
    )
    .await
    .map_err(|_| api_timeout_error(timeout_duration))?;

    let mut join_set = JoinSet::new();
    for (peer_id, receiver) in receivers {
        let wallet = wallet.to_string();
        join_set.spawn(async move {
            (
                peer_id,
                receive_balance(receiver, &wallet, public_key).await,
            )
        });
    }

    let mut last_err = String::new();
    loop {
        let result = tokio::select! {
            biased;
            _ = sleep_until(deadline) => {
                join_set.abort_all();
                return Err(api_timeout_error(timeout_duration));
            }
            result = join_set.join_next() => result,
        };
        let Some(result) = result else {
            break;
        };
        match result {
            Ok((_peer_id, Ok(balance))) => {
                join_set.abort_all();
                return Ok(balance);
            }
            Ok((peer_id, Err(err))) => {
                last_err = format!("peer {peer_id}: {err}");
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

pub(crate) async fn query_tick_transactions(
    state: Arc<Mutex<NodeState>>,
    pending: Arc<PendingRequests>,
    config: Arc<Config>,
    tick: u32,
) -> Result<Vec<TickTransaction>, String> {
    let timeout_duration = config.api_timeout;
    let deadline = Instant::now() + timeout_duration;
    let targets = timeout_at(deadline, state.lock())
        .await
        .map_err(|_| api_timeout_error(timeout_duration))?
        .collect_all_targets(MAX_PARALLEL_PEER_QUERIES);
    if targets.is_empty() {
        return Err("No connected peers available".to_string());
    }
    let (registration, receivers) = pending.register(targets.iter().map(|target| target.peer_id));
    let request = Bytes::from(build_request_tick_transactions_frame(
        registration.dejavu(),
        tick,
    )?);
    timeout_at(
        deadline,
        send_request_to_targets(&state, &pending, targets, &request),
    )
    .await
    .map_err(|_| api_timeout_error(timeout_duration))?;

    let mut join_set = JoinSet::new();
    for (peer_id, receiver) in receivers {
        join_set.spawn(async move { (peer_id, receive_tick_transactions(receiver, tick).await) });
    }

    let mut last_err = String::new();
    loop {
        let result = tokio::select! {
            biased;
            _ = sleep_until(deadline) => {
                join_set.abort_all();
                return Err(api_timeout_error(timeout_duration));
            }
            result = join_set.join_next() => result,
        };
        let Some(result) = result else {
            break;
        };
        match result {
            Ok((_peer_id, Ok(transactions))) => {
                join_set.abort_all();
                return Ok(transactions);
            }
            Ok((peer_id, Err(err))) => {
                last_err = format!("peer {peer_id}: {err}");
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

pub(crate) async fn query_contract_function(
    state: Arc<Mutex<NodeState>>,
    pending: Arc<PendingRequests>,
    config: Arc<Config>,
    contract_index: u32,
    input_type: u16,
    input: &[u8],
) -> Result<Vec<u8>, String> {
    let timeout_duration = config.api_timeout;
    let deadline = Instant::now() + timeout_duration;
    let targets = timeout_at(deadline, state.lock())
        .await
        .map_err(|_| api_timeout_error(timeout_duration))?
        .collect_all_targets(MAX_PARALLEL_PEER_QUERIES);
    if targets.is_empty() {
        return Err("No connected peers available".to_string());
    }
    let (registration, receivers) = pending.register(targets.iter().map(|target| target.peer_id));
    let request = Bytes::from(build_request_contract_function_frame(
        registration.dejavu(),
        contract_index,
        input_type,
        input,
    )?);
    timeout_at(
        deadline,
        send_request_to_targets(&state, &pending, targets, &request),
    )
    .await
    .map_err(|_| api_timeout_error(timeout_duration))?;

    let mut join_set = JoinSet::new();
    for (peer_id, receiver) in receivers {
        join_set.spawn(async move { (peer_id, receive_contract_function(receiver).await) });
    }

    let mut last_err = String::new();
    loop {
        let result = tokio::select! {
            biased;
            _ = sleep_until(deadline) => {
                join_set.abort_all();
                return Err(api_timeout_error(timeout_duration));
            }
            result = join_set.join_next() => result,
        };
        let Some(result) = result else {
            break;
        };
        match result {
            Ok((_peer_id, Ok(output))) => {
                join_set.abort_all();
                return Ok(output);
            }
            Ok((peer_id, Err(err))) => {
                last_err = format!("peer {peer_id}: {err}");
            }
            Err(err) => {
                last_err = format!("task join error: {err}");
            }
        }
    }

    Err(format!(
        "Failed to query contract function from peers (parallel): {last_err}"
    ))
}

async fn receive_balance(
    mut receiver: mpsc::UnboundedReceiver<PendingEvent>,
    wallet: &str,
    public_key: [u8; 32],
) -> Result<BalanceResponse, String> {
    while let Some(event) = receiver.recv().await {
        match event {
            PendingEvent::Frame(frame) => match frame[3] {
                RESPOND_ENTITY_TYPE => {
                    let payload = frame_payload(&frame)?;
                    return parse_balance_payload(wallet, public_key, payload);
                }
                END_RESPONSE_TYPE => {
                    return Err("Peer returned END_RESPONSE without balance data".to_string());
                }
                TRY_AGAIN_TYPE => return Err("peer requested retry".to_string()),
                _ => {}
            },
            PendingEvent::PeerDisconnected => return Err("peer disconnected".to_string()),
        }
    }
    Err("pending response channel closed".to_string())
}

async fn receive_tick_transactions(
    mut receiver: mpsc::UnboundedReceiver<PendingEvent>,
    requested_tick: u32,
) -> Result<Vec<TickTransaction>, String> {
    let mut transactions = Vec::<TickTransaction>::new();
    while let Some(event) = receiver.recv().await {
        match event {
            PendingEvent::Frame(frame) => match frame[3] {
                BROADCAST_TRANSACTION_TYPE => {
                    if transactions.len() >= NUMBER_OF_TRANSACTIONS_PER_TICK {
                        return Err(format!(
                            "Peer returned more than {NUMBER_OF_TRANSACTIONS_PER_TICK} transactions"
                        ));
                    }
                    let tx_payload = frame_payload(&frame)?;
                    transactions.push(parse_transaction_payload(tx_payload, requested_tick)?);
                }
                END_RESPONSE_TYPE => return Ok(transactions),
                TRY_AGAIN_TYPE => return Err("peer requested retry".to_string()),
                _ => {}
            },
            PendingEvent::PeerDisconnected => return Err("peer disconnected".to_string()),
        }
    }
    Err("pending response channel closed".to_string())
}

async fn receive_contract_function(
    mut receiver: mpsc::UnboundedReceiver<PendingEvent>,
) -> Result<Vec<u8>, String> {
    while let Some(event) = receiver.recv().await {
        match event {
            PendingEvent::Frame(frame) => match frame[3] {
                RESPOND_CONTRACT_FUNCTION_TYPE => {
                    let payload = frame_payload(&frame)?;
                    if payload.is_empty() {
                        return Err("contract function invocation failed".to_string());
                    }
                    if payload.len() > MAX_CONTRACT_FUNCTION_OUTPUT_SIZE {
                        return Err(format!(
                            "Contract function output is too large: maximum {MAX_CONTRACT_FUNCTION_OUTPUT_SIZE}, got {}",
                            payload.len()
                        ));
                    }
                    return Ok(payload.to_vec());
                }
                TRY_AGAIN_TYPE => return Err("peer requested retry".to_string()),
                END_RESPONSE_TYPE => {
                    return Err(
                        "Peer returned END_RESPONSE without contract function data".to_string()
                    );
                }
                _ => {}
            },
            PendingEvent::PeerDisconnected => return Err("peer disconnected".to_string()),
        }
    }
    Err("pending response channel closed".to_string())
}

async fn send_request_to_targets(
    state: &Arc<Mutex<NodeState>>,
    pending: &PendingRequests,
    targets: Vec<crate::state::RelayTarget>,
    frame: &Bytes,
) {
    for target in targets {
        if target.tx.try_send(frame.clone()).is_err() {
            pending.peer_disconnected(target.peer_id);
            let _ = state.lock().await.disconnect_session(target.peer_id);
        }
    }
}

fn api_timeout_error(timeout_duration: std::time::Duration) -> String {
    format!(
        "Peer-backed API query timed out after {} ms",
        timeout_duration.as_millis()
    )
}

fn parse_balance_payload(
    wallet: &str,
    requested_public_key: [u8; 32],
    payload: &[u8],
) -> Result<BalanceResponse, String> {
    if payload.len() != RESPOND_ENTITY_PAYLOAD_SIZE {
        return Err(format!(
            "RespondEntity payload size mismatch: expected {RESPOND_ENTITY_PAYLOAD_SIZE}, got {}",
            payload.len()
        ));
    }
    if payload[..32] != requested_public_key {
        return Err("RespondEntity public key does not match request".to_string());
    }

    let incoming_amount =
        read_i64(payload, 32).ok_or_else(|| "incomingAmount missing".to_string())?;
    let outgoing_amount =
        read_i64(payload, 40).ok_or_else(|| "outgoingAmount missing".to_string())?;
    let balance = incoming_amount
        .checked_sub(outgoing_amount)
        .ok_or_else(|| "RespondEntity balance overflows int64".to_string())?;
    let spectrum_index =
        read_i32(payload, 68).ok_or_else(|| "spectrumIndex missing".to_string())?;
    if spectrum_index != -1 && !(0..SPECTRUM_CAPACITY).contains(&spectrum_index) {
        return Err(format!(
            "RespondEntity spectrumIndex is out of range: {spectrum_index}"
        ));
    }

    Ok(BalanceResponse {
        wallet: wallet.to_string(),
        public_key_hex: format!("0x{}", bytes_to_hex(&payload[0..32])),
        tick: read_u32(payload, 64).ok_or_else(|| "tick missing".to_string())?,
        spectrum_index,
        incoming_amount,
        outgoing_amount,
        balance,
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

fn parse_transaction_payload(
    payload: &[u8],
    expected_tick: u32,
) -> Result<TickTransaction, String> {
    let layout = parse_transaction_layout(payload, Some(expected_tick))?;
    let input_end = layout.input_start + layout.input_size as usize;

    Ok(TickTransaction {
        source_public_key_hex: format!("0x{}", bytes_to_hex(&payload[0..32])),
        destination_public_key_hex: format!("0x{}", bytes_to_hex(&payload[32..64])),
        amount: layout.amount,
        tick: layout.tick,
        input_type: layout.input_type,
        input_size: layout.input_size,
        input_hex: bytes_to_hex(&payload[layout.input_start..input_end]),
        signature_hex: bytes_to_hex(&payload[layout.signature_start..]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DEFAULT_GRPC_PORT, DEFAULT_PORT};
    use crate::frame::{SIGNATURE_SIZE, TRANSACTION_BASE_SIZE, build_request_frame};
    use pretty_assertions::assert_eq;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::time::Duration;
    use tokio::sync::watch;

    fn test_config(api_timeout: Duration) -> Arc<Config> {
        Arc::new(Config {
            listen_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, DEFAULT_PORT),
            api_timeout,
            grpc_listen_addr: SocketAddr::from(([127, 0, 0, 1], DEFAULT_GRPC_PORT)),
            grpc_enabled: true,
            peer_port: DEFAULT_PORT,
            target_outbound: 8,
            max_incoming: 32,
            max_seen: 1_000,
            max_known_peers: 1_000,
            reconnect_interval: Duration::from_secs(2),
            peer_write_timeout: Duration::from_secs(5),
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

    async fn state_with_peer() -> (Arc<Mutex<NodeState>>, u64, mpsc::Receiver<Bytes>) {
        let state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let (tx, rx) = mpsc::channel(2);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        let peer_id = state
            .lock()
            .await
            .register_session(
                SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), DEFAULT_PORT),
                true,
                tx,
                disconnect_tx,
                DEFAULT_PORT,
            )
            .unwrap();
        (state, peer_id, rx)
    }

    #[test]
    fn rejects_transaction_with_inconsistent_input_size() {
        let mut payload = vec![0; TRANSACTION_BASE_SIZE + SIGNATURE_SIZE];
        payload[78..80].copy_from_slice(&1u16.to_le_bytes());

        assert_eq!(
            parse_transaction_payload(&payload, 0).unwrap_err(),
            "Transaction payload size mismatch: expected 145, got 144"
        );
    }

    fn transaction_frame(dejavu: u32, amount: i64) -> Bytes {
        let mut payload = vec![0; TRANSACTION_BASE_SIZE + SIGNATURE_SIZE];
        payload[64..72].copy_from_slice(&amount.to_le_bytes());
        payload[72..76].copy_from_slice(&42u32.to_le_bytes());
        Bytes::from(
            build_request_frame(BROADCAST_TRANSACTION_TYPE, dejavu, &payload)
                .expect("transaction frame should build"),
        )
    }

    fn entity_payload(public_key: [u8; 32]) -> [u8; RESPOND_ENTITY_PAYLOAD_SIZE] {
        let mut payload = [0; RESPOND_ENTITY_PAYLOAD_SIZE];
        payload[..32].copy_from_slice(&public_key);
        payload
    }

    #[tokio::test]
    async fn tick_transactions_from_different_peers_do_not_mix() {
        let dejavu = 7;
        let (peer_1_tx, peer_1_rx) = mpsc::unbounded_channel();
        let (peer_2_tx, peer_2_rx) = mpsc::unbounded_channel();
        peer_1_tx
            .send(PendingEvent::Frame(transaction_frame(dejavu, 11)))
            .unwrap();
        peer_2_tx
            .send(PendingEvent::Frame(transaction_frame(dejavu, 22)))
            .unwrap();
        peer_2_tx
            .send(PendingEvent::Frame(Bytes::from(
                build_request_frame(END_RESPONSE_TYPE, dejavu, &[]).unwrap(),
            )))
            .unwrap();

        let peer_2_transactions = receive_tick_transactions(peer_2_rx, 42).await.unwrap();

        assert_eq!(peer_2_transactions.len(), 1);
        assert_eq!(peer_2_transactions[0].amount, 22);
        assert!(!peer_1_rx.is_empty());
    }

    #[test]
    fn balance_response_is_bound_to_request_and_exact_core_layout() {
        let public_key = [9; 32];
        let payload = entity_payload(public_key);

        assert!(parse_balance_payload("wallet", public_key, &payload).is_ok());
        assert_eq!(
            parse_balance_payload("wallet", public_key, &payload[..72]).unwrap_err(),
            "RespondEntity payload size mismatch: expected 840, got 72"
        );
        assert_eq!(
            parse_balance_payload("wallet", [8; 32], &payload).unwrap_err(),
            "RespondEntity public key does not match request"
        );
    }

    #[test]
    fn balance_response_rejects_invalid_index_and_overflow() {
        let public_key = [9; 32];
        let mut invalid_index = entity_payload(public_key);
        invalid_index[68..72].copy_from_slice(&SPECTRUM_CAPACITY.to_le_bytes());
        assert_eq!(
            parse_balance_payload("wallet", public_key, &invalid_index).unwrap_err(),
            "RespondEntity spectrumIndex is out of range: 16777216"
        );

        let mut overflowing = entity_payload(public_key);
        overflowing[32..40].copy_from_slice(&i64::MAX.to_le_bytes());
        overflowing[40..48].copy_from_slice(&(-1i64).to_le_bytes());
        assert_eq!(
            parse_balance_payload("wallet", public_key, &overflowing).unwrap_err(),
            "RespondEntity balance overflows int64"
        );
    }

    #[tokio::test]
    async fn tick_response_rejects_more_than_core_transaction_limit() {
        let (tx, rx) = mpsc::unbounded_channel();
        for _ in 0..=NUMBER_OF_TRANSACTIONS_PER_TICK {
            tx.send(PendingEvent::Frame(transaction_frame(7, 0)))
                .unwrap();
        }

        assert_eq!(
            receive_tick_transactions(rx, 42).await.unwrap_err(),
            "Peer returned more than 4096 transactions"
        );
    }

    #[tokio::test]
    async fn first_balance_response_completes_and_cleans_pending_request() {
        let (state, peer_id, mut peer_rx) = state_with_peer().await;
        let pending = Arc::new(PendingRequests::default());
        let query = tokio::spawn(query_balance(
            state,
            Arc::clone(&pending),
            test_config(Duration::from_secs(1)),
            "wallet",
            [9; 32],
        ));
        let request = peer_rx.recv().await.unwrap();
        let dejavu = u32::from_le_bytes(request[4..8].try_into().unwrap());
        let mut payload = [0; RESPOND_ENTITY_PAYLOAD_SIZE];
        payload[..32].copy_from_slice(&[9; 32]);
        payload[32..40].copy_from_slice(&100i64.to_le_bytes());
        payload[40..48].copy_from_slice(&40i64.to_le_bytes());
        payload[64..68].copy_from_slice(&123u32.to_le_bytes());
        let response = Bytes::from(
            build_request_frame(RESPOND_ENTITY_TYPE, dejavu, &payload)
                .expect("balance response should build"),
        );

        assert!(pending.deliver(peer_id, dejavu, response));
        let balance = query.await.unwrap().unwrap();

        assert_eq!(balance.balance, 60);
        assert_eq!(balance.tick, 123);
        assert_eq!(pending.active_count(), 0);
        assert!(pending.deliver(peer_id, dejavu, Bytes::new()));
    }

    #[tokio::test]
    async fn timeout_cleans_pending_request() {
        let (state, _peer_id, mut peer_rx) = state_with_peer().await;
        let pending = Arc::new(PendingRequests::default());
        let query = query_balance(
            state,
            Arc::clone(&pending),
            test_config(Duration::from_millis(20)),
            "wallet",
            [9; 32],
        );
        let (_, result) = tokio::join!(peer_rx.recv(), query);

        assert_eq!(
            result.unwrap_err(),
            "Peer-backed API query timed out after 20 ms"
        );
        assert_eq!(pending.active_count(), 0);
    }

    #[tokio::test]
    async fn contract_function_returns_first_peer_response_and_cleans_pending_request() {
        let (state, peer_id, mut peer_rx) = state_with_peer().await;
        let pending = Arc::new(PendingRequests::default());
        let query = tokio::spawn(query_contract_function(
            state,
            Arc::clone(&pending),
            test_config(Duration::from_secs(1)),
            3,
            2,
            &[9; 32],
        ));
        let request = peer_rx.recv().await.unwrap();
        let dejavu = u32::from_le_bytes(request[4..8].try_into().unwrap());
        let response = Bytes::from(
            build_request_frame(RESPOND_CONTRACT_FUNCTION_TYPE, dejavu, &[1, 2, 3])
                .expect("contract response should build"),
        );

        assert!(pending.deliver(peer_id, dejavu, response));
        assert_eq!(query.await.unwrap().unwrap(), vec![1, 2, 3]);
        assert_eq!(pending.active_count(), 0);
    }

    #[tokio::test]
    async fn contract_function_maps_try_again_to_peer_error() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(PendingEvent::Frame(Bytes::from(
            build_request_frame(TRY_AGAIN_TYPE, 7, &[]).unwrap(),
        )))
        .unwrap();

        assert_eq!(
            receive_contract_function(rx).await.unwrap_err(),
            "peer requested retry"
        );
    }

    #[tokio::test]
    async fn contract_function_rejects_empty_response_as_core_failure() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(PendingEvent::Frame(Bytes::from(
            build_request_frame(RESPOND_CONTRACT_FUNCTION_TYPE, 7, &[]).unwrap(),
        )))
        .unwrap();

        assert_eq!(
            receive_contract_function(rx).await.unwrap_err(),
            "contract function invocation failed"
        );
    }

    #[tokio::test]
    async fn contract_function_rejects_output_larger_than_core_u16_size() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(PendingEvent::Frame(Bytes::from(
            build_request_frame(
                RESPOND_CONTRACT_FUNCTION_TYPE,
                7,
                &vec![0; MAX_CONTRACT_FUNCTION_OUTPUT_SIZE + 1],
            )
            .unwrap(),
        )))
        .unwrap();

        assert_eq!(
            receive_contract_function(rx).await.unwrap_err(),
            "Contract function output is too large: maximum 65535, got 65536"
        );
    }
}
