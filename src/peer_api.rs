use crate::codec::{bytes_to_hex, read_i32, read_i64, read_u16, read_u32};
use crate::config::Config;
use crate::frame::{
    BROADCAST_TRANSACTION_TYPE, END_RESPONSE_TYPE, REQUEST_ENTITY_TYPE, RESPOND_ENTITY_TYPE,
    build_request_frame, build_request_tick_transactions_frame, frame_payload,
};
use crate::pending::{PendingEvent, PendingRequests};
use crate::state::NodeState;
use crate::types::{BalanceResponse, TickTransaction};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout_at};

const RESPOND_ENTITY_MIN_PAYLOAD_SIZE: usize = 72;
const TRANSACTION_BASE_SIZE: usize = 80;
const SIGNATURE_SIZE: usize = 64;
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
        join_set.spawn(async move { (peer_id, receive_balance(receiver, &wallet).await) });
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
        join_set.spawn(async move { (peer_id, receive_tick_transactions(receiver).await) });
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

async fn receive_balance(
    mut receiver: mpsc::UnboundedReceiver<PendingEvent>,
    wallet: &str,
) -> Result<BalanceResponse, String> {
    while let Some(event) = receiver.recv().await {
        match event {
            PendingEvent::Frame(frame) => match frame[3] {
                RESPOND_ENTITY_TYPE => {
                    let payload = frame_payload(&frame)?;
                    return parse_balance_payload(wallet, payload);
                }
                END_RESPONSE_TYPE => {
                    return Err("Peer returned END_RESPONSE without balance data".to_string());
                }
                _ => {}
            },
            PendingEvent::PeerDisconnected => return Err("peer disconnected".to_string()),
        }
    }
    Err("pending response channel closed".to_string())
}

async fn receive_tick_transactions(
    mut receiver: mpsc::UnboundedReceiver<PendingEvent>,
) -> Result<Vec<TickTransaction>, String> {
    let mut transactions = Vec::<TickTransaction>::new();
    while let Some(event) = receiver.recv().await {
        match event {
            PendingEvent::Frame(frame) => match frame[3] {
                BROADCAST_TRANSACTION_TYPE => {
                    let tx_payload = frame_payload(&frame)?;
                    transactions.push(parse_transaction_payload(tx_payload)?);
                }
                END_RESPONSE_TYPE => return Ok(transactions),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DEFAULT_GRPC_PORT, DEFAULT_PORT};
    use crate::frame::build_request_frame;
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
            parse_transaction_payload(&payload).unwrap_err(),
            "Transaction payload size mismatch: expected 145, got 144"
        );
    }

    fn transaction_frame(dejavu: u32, amount: i64) -> Bytes {
        let mut payload = vec![0; TRANSACTION_BASE_SIZE + SIGNATURE_SIZE];
        payload[64..72].copy_from_slice(&amount.to_le_bytes());
        Bytes::from(
            build_request_frame(BROADCAST_TRANSACTION_TYPE, dejavu, &payload)
                .expect("transaction frame should build"),
        )
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

        let peer_2_transactions = receive_tick_transactions(peer_2_rx).await.unwrap();

        assert_eq!(peer_2_transactions.len(), 1);
        assert_eq!(peer_2_transactions[0].amount, 22);
        assert!(!peer_1_rx.is_empty());
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
        let mut payload = [0; RESPOND_ENTITY_MIN_PAYLOAD_SIZE];
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
}
