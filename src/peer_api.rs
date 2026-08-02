use crate::codec::{bytes_to_hex, kangaroo_twelve, read_i32, read_i64, read_u32};
use crate::config::Config;
use crate::frame::{
    END_RESPONSE_TYPE, HEADER_SIZE, MAX_CONTRACT_FUNCTION_OUTPUT_SIZE, REQUEST_ENTITY_TYPE,
    RESPOND_CONTRACT_FUNCTION_TYPE, RESPOND_ENTITY_PAYLOAD_SIZE, RESPOND_ENTITY_TYPE,
    SPECTRUM_CAPACITY, SPECTRUM_DEPTH, TRY_AGAIN_TYPE, build_request_contract_function_frame,
    build_request_frame, frame_payload,
};
use crate::pending::{PendingEvent, PendingRequests};
use crate::pending::{PendingSpec, ResponseRule};
use crate::state::{DisconnectReason, NodeState, OutboundAdmissionError, OutboundFrame};
use crate::types::BalanceResponse;
use crate::verified::TrustedNetworkState;
use bytes::Bytes;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout_at};

const MAX_PARALLEL_PEER_QUERIES: usize = 3;
const BALANCE_RESPONSE_RULES: &[ResponseRule] = &[
    ResponseRule {
        message_type: RESPOND_ENTITY_TYPE,
        min_frame_bytes: HEADER_SIZE + RESPOND_ENTITY_PAYLOAD_SIZE,
        max_frame_bytes: HEADER_SIZE + RESPOND_ENTITY_PAYLOAD_SIZE,
        max_frames: 1,
        terminal: true,
    },
    ResponseRule {
        message_type: END_RESPONSE_TYPE,
        min_frame_bytes: HEADER_SIZE,
        max_frame_bytes: HEADER_SIZE,
        max_frames: 1,
        terminal: true,
    },
    ResponseRule {
        message_type: TRY_AGAIN_TYPE,
        min_frame_bytes: HEADER_SIZE,
        max_frame_bytes: HEADER_SIZE,
        max_frames: 1,
        terminal: true,
    },
];
const CONTRACT_RESPONSE_RULES: &[ResponseRule] = &[
    ResponseRule {
        message_type: RESPOND_CONTRACT_FUNCTION_TYPE,
        min_frame_bytes: HEADER_SIZE,
        max_frame_bytes: HEADER_SIZE + MAX_CONTRACT_FUNCTION_OUTPUT_SIZE,
        max_frames: 1,
        terminal: true,
    },
    ResponseRule {
        message_type: END_RESPONSE_TYPE,
        min_frame_bytes: HEADER_SIZE,
        max_frame_bytes: HEADER_SIZE,
        max_frames: 1,
        terminal: true,
    },
    ResponseRule {
        message_type: TRY_AGAIN_TYPE,
        min_frame_bytes: HEADER_SIZE,
        max_frame_bytes: HEADER_SIZE,
        max_frames: 1,
        terminal: true,
    },
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PeerQueryError {
    Deadline(std::time::Duration),
    NoPeers,
    LocalOverload(&'static str),
    PeerUnavailable(String),
    Protocol(String),
    Internal(String),
}

impl fmt::Display for PeerQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deadline(duration) => write!(
                formatter,
                "Peer-backed API query timed out after {} ms",
                duration.as_millis()
            ),
            Self::NoPeers => formatter.write_str("No connected peers available"),
            Self::LocalOverload(message) => formatter.write_str(message),
            Self::PeerUnavailable(message) | Self::Protocol(message) | Self::Internal(message) => {
                formatter.write_str(message)
            }
        }
    }
}

impl std::error::Error for PeerQueryError {}

impl From<String> for PeerQueryError {
    fn from(message: String) -> Self {
        Self::Protocol(message)
    }
}

pub(crate) async fn query_balance(
    state: Arc<Mutex<NodeState>>,
    pending: Arc<PendingRequests>,
    outbound_budget: Arc<Semaphore>,
    trusted_network: Arc<TrustedNetworkState>,
    config: Arc<Config>,
    wallet: &str,
    public_key: [u8; 32],
) -> Result<BalanceResponse, PeerQueryError> {
    let timeout_duration = config.api_timeout;
    let deadline = Instant::now() + timeout_duration;
    let targets = timeout_at(deadline, state.lock())
        .await
        .map_err(|_| api_timeout_error(timeout_duration))?
        .collect_all_targets(MAX_PARALLEL_PEER_QUERIES);
    if targets.is_empty() {
        return Err(PeerQueryError::NoPeers);
    }
    let (registration, receivers) = pending.register_with_spec(
        targets.iter().map(|target| target.peer_id),
        PendingSpec {
            response_rules: BALANCE_RESPONSE_RULES,
            max_response_frames: 1,
            max_response_bytes: HEADER_SIZE + RESPOND_ENTITY_PAYLOAD_SIZE,
        },
    );
    let request = Bytes::from(
        build_request_frame(REQUEST_ENTITY_TYPE, registration.dejavu(), &public_key)
            .map_err(PeerQueryError::Internal)?,
    );
    ensure_local_request_size(&request, &config)?;
    let accepted = timeout_at(
        deadline,
        send_request_to_targets(&state, &pending, targets, &request, &outbound_budget),
    )
    .await
    .map_err(|_| api_timeout_error(timeout_duration))??;
    registration.retain_peers(&accepted);

    let mut join_set = JoinSet::new();
    for (peer_id, receiver) in receivers
        .into_iter()
        .filter(|(peer_id, _)| accepted.contains(peer_id))
    {
        let wallet = wallet.to_string();
        let trusted_network = Arc::clone(&trusted_network);
        join_set.spawn(async move {
            (
                peer_id,
                receive_balance(receiver, &wallet, public_key, &trusted_network).await,
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
                if matches!(err, PeerQueryError::Protocol(_)) {
                    penalize_protocol_peer(&state, peer_id).await;
                }
                last_err = format!("peer {peer_id}: {err}");
            }
            Err(err) => {
                last_err = format!("task join error: {err}");
            }
        }
    }

    Err(PeerQueryError::PeerUnavailable(format!(
        "Failed to query balance from peers (parallel): {last_err}"
    )))
}

pub(crate) async fn query_contract_function(
    state: Arc<Mutex<NodeState>>,
    pending: Arc<PendingRequests>,
    outbound_budget: Arc<Semaphore>,
    config: Arc<Config>,
    contract_index: u32,
    input_type: u16,
    input: &[u8],
) -> Result<Vec<u8>, PeerQueryError> {
    let timeout_duration = config.api_timeout;
    let deadline = Instant::now() + timeout_duration;
    let targets = timeout_at(deadline, state.lock())
        .await
        .map_err(|_| api_timeout_error(timeout_duration))?
        .collect_all_targets(MAX_PARALLEL_PEER_QUERIES);
    if targets.is_empty() {
        return Err(PeerQueryError::NoPeers);
    }
    let (registration, receivers) = pending.register_with_spec(
        targets.iter().map(|target| target.peer_id),
        PendingSpec {
            response_rules: CONTRACT_RESPONSE_RULES,
            max_response_frames: 1,
            max_response_bytes: HEADER_SIZE + MAX_CONTRACT_FUNCTION_OUTPUT_SIZE,
        },
    );
    let request = Bytes::from(
        build_request_contract_function_frame(
            registration.dejavu(),
            contract_index,
            input_type,
            input,
        )
        .map_err(PeerQueryError::Internal)?,
    );
    ensure_local_request_size(&request, &config)?;
    let accepted = timeout_at(
        deadline,
        send_request_to_targets(&state, &pending, targets, &request, &outbound_budget),
    )
    .await
    .map_err(|_| api_timeout_error(timeout_duration))??;
    registration.retain_peers(&accepted);

    let mut join_set = JoinSet::new();
    for (peer_id, receiver) in receivers
        .into_iter()
        .filter(|(peer_id, _)| accepted.contains(peer_id))
    {
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
                if matches!(err, PeerQueryError::Protocol(_)) {
                    penalize_protocol_peer(&state, peer_id).await;
                }
                last_err = format!("peer {peer_id}: {err}");
            }
            Err(err) => {
                last_err = format!("task join error: {err}");
            }
        }
    }

    Err(PeerQueryError::PeerUnavailable(format!(
        "Failed to query contract function from peers (parallel): {last_err}"
    )))
}

async fn receive_balance(
    mut receiver: mpsc::Receiver<PendingEvent>,
    wallet: &str,
    public_key: [u8; 32],
    trusted_network: &TrustedNetworkState,
) -> Result<BalanceResponse, PeerQueryError> {
    while let Some(event) = receiver.recv().await {
        match event {
            PendingEvent::Frame(frame) => match frame[3] {
                RESPOND_ENTITY_TYPE => {
                    let payload = frame_payload(&frame)?;
                    let parsed = parse_balance_payload(wallet, public_key, payload)
                        .map_err(PeerQueryError::Protocol)?;
                    if parsed.response.spectrum_index < 0 {
                        return Err(PeerQueryError::PeerUnavailable(
                            "RespondEntity cannot prove that the entity is absent".to_string(),
                        ));
                    }
                    let Some(root) = trusted_network.spectrum_root(parsed.response.tick) else {
                        return Err(PeerQueryError::PeerUnavailable(format!(
                            "no verified spectrum root for entity tick {}",
                            parsed.response.tick
                        )));
                    };
                    if parsed.proof_root() != root {
                        return Err(PeerQueryError::Protocol(
                            "RespondEntity Merkle proof does not match the verified spectrum root"
                                .to_string(),
                        ));
                    }
                    return Ok(parsed.response);
                }
                END_RESPONSE_TYPE => {
                    return Err(PeerQueryError::Protocol(
                        "Peer returned END_RESPONSE without balance data".to_string(),
                    ));
                }
                TRY_AGAIN_TYPE => {
                    return Err(PeerQueryError::PeerUnavailable(
                        "peer requested retry".to_string(),
                    ));
                }
                _ => {}
            },
            PendingEvent::PeerDisconnected => {
                return Err(PeerQueryError::PeerUnavailable(
                    "peer disconnected".to_string(),
                ));
            }
        }
    }
    Err(PeerQueryError::PeerUnavailable(
        "pending response channel closed".to_string(),
    ))
}

async fn receive_contract_function(
    mut receiver: mpsc::Receiver<PendingEvent>,
) -> Result<Vec<u8>, PeerQueryError> {
    while let Some(event) = receiver.recv().await {
        match event {
            PendingEvent::Frame(frame) => match frame[3] {
                RESPOND_CONTRACT_FUNCTION_TYPE => {
                    let payload = frame_payload(&frame)?;
                    if payload.is_empty() {
                        return Err(PeerQueryError::PeerUnavailable(
                            "contract function invocation failed".to_string(),
                        ));
                    }
                    if payload.len() > MAX_CONTRACT_FUNCTION_OUTPUT_SIZE {
                        return Err(PeerQueryError::Protocol(format!(
                            "Contract function output is too large: maximum {MAX_CONTRACT_FUNCTION_OUTPUT_SIZE}, got {}",
                            payload.len()
                        )));
                    }
                    return Ok(payload.to_vec());
                }
                TRY_AGAIN_TYPE => {
                    return Err(PeerQueryError::PeerUnavailable(
                        "peer requested retry".to_string(),
                    ));
                }
                END_RESPONSE_TYPE => {
                    return Err(PeerQueryError::Protocol(
                        "Peer returned END_RESPONSE without contract function data".to_string(),
                    ));
                }
                _ => {}
            },
            PendingEvent::PeerDisconnected => {
                return Err(PeerQueryError::PeerUnavailable(
                    "peer disconnected".to_string(),
                ));
            }
        }
    }
    Err(PeerQueryError::PeerUnavailable(
        "pending response channel closed".to_string(),
    ))
}

async fn send_request_to_targets(
    state: &Arc<Mutex<NodeState>>,
    pending: &PendingRequests,
    targets: Vec<crate::state::RelayTarget>,
    frame: &Bytes,
    outbound_budget: &Arc<Semaphore>,
) -> Result<HashSet<u64>, PeerQueryError> {
    let mut accepted = HashSet::new();
    for target in targets {
        let queued = match OutboundFrame::try_new(
            frame.clone(),
            Arc::clone(&target.byte_budget),
            Arc::clone(outbound_budget),
        ) {
            Ok(queued) => queued,
            Err(OutboundAdmissionError::GlobalBudgetExhausted) => {
                if accepted.is_empty() {
                    return Err(PeerQueryError::LocalOverload(
                        "Global outbound queue is overloaded",
                    ));
                }
                break;
            }
            Err(OutboundAdmissionError::FrameTooLarge) => {
                return Err(PeerQueryError::Internal(
                    "Outbound request frame is too large".to_string(),
                ));
            }
            Err(OutboundAdmissionError::PeerBudgetExhausted) => {
                pending.peer_disconnected(target.peer_id);
                let _ = state.lock().await.disconnect_session_with_reason(
                    target.peer_id,
                    DisconnectReason::PeerQueueFull,
                );
                continue;
            }
        };
        match target.tx.try_send(queued) {
            Ok(()) => {
                accepted.insert(target.peer_id);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                pending.peer_disconnected(target.peer_id);
                let _ = state.lock().await.disconnect_session_with_reason(
                    target.peer_id,
                    DisconnectReason::PeerQueueFull,
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                pending.peer_disconnected(target.peer_id);
                let _ = state.lock().await.disconnect_session_with_reason(
                    target.peer_id,
                    DisconnectReason::PeerQueueClosed,
                );
            }
        }
    }
    if accepted.is_empty() {
        Err(PeerQueryError::PeerUnavailable(
            "No selected peer accepted the request".to_string(),
        ))
    } else {
        Ok(accepted)
    }
}

fn api_timeout_error(timeout_duration: std::time::Duration) -> PeerQueryError {
    PeerQueryError::Deadline(timeout_duration)
}

fn ensure_local_request_size(frame: &[u8], config: &Config) -> Result<(), PeerQueryError> {
    if frame.len() > config.max_frame_bytes {
        return Err(PeerQueryError::Internal(format!(
            "Outbound request frame is {} bytes, exceeding local --max-frame-bytes {}",
            frame.len(),
            config.max_frame_bytes
        )));
    }
    Ok(())
}

async fn penalize_protocol_peer(state: &Arc<Mutex<NodeState>>, peer_id: u64) {
    let _ = state
        .lock()
        .await
        .disconnect_session_with_reason(peer_id, DisconnectReason::ProtocolViolation);
}

#[derive(Debug)]
struct ParsedBalance {
    response: BalanceResponse,
    entity_record: [u8; 64],
    siblings: [[u8; 32]; SPECTRUM_DEPTH],
}

impl ParsedBalance {
    fn proof_root(&self) -> [u8; 32] {
        let mut digest = kangaroo_twelve(&self.entity_record);
        let mut index = usize::try_from(self.response.spectrum_index)
            .expect("proof root is only computed for a non-negative spectrum index");
        for sibling in self.siblings {
            let mut pair = [0; 64];
            if index & 1 == 0 {
                pair[..32].copy_from_slice(&digest);
                pair[32..].copy_from_slice(&sibling);
            } else {
                pair[..32].copy_from_slice(&sibling);
                pair[32..].copy_from_slice(&digest);
            }
            digest = kangaroo_twelve(&pair);
            index >>= 1;
        }
        digest
    }
}

fn parse_balance_payload(
    wallet: &str,
    requested_public_key: [u8; 32],
    payload: &[u8],
) -> Result<ParsedBalance, String> {
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

    let entity_record = payload[..64]
        .try_into()
        .expect("RespondEntity contains an exact-size entity record");
    let siblings = core::array::from_fn(|index| {
        let offset = 72 + index * 32;
        payload[offset..offset + 32]
            .try_into()
            .expect("RespondEntity contains an exact-size sibling path")
    });

    Ok(ParsedBalance {
        response: BalanceResponse {
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
        },
        entity_record,
        siblings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DEFAULT_GRPC_PORT, DEFAULT_PORT};
    use crate::frame::build_request_frame;
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::time::Duration;
    use tokio::sync::watch;

    fn pending_frame(bytes: Bytes) -> PendingEvent {
        PendingEvent::Frame(crate::pending::PendingFrame::for_test(bytes))
    }

    fn test_config(api_timeout: Duration) -> Arc<Config> {
        Arc::new(Config {
            api_timeout,
            grpc_listen_addr: SocketAddr::from(([127, 0, 0, 1], DEFAULT_GRPC_PORT)),
            peer_port: DEFAULT_PORT,
            target_outbound: 8,
            max_known_peers: 1_000,
            reconnect_interval: Duration::from_secs(2),
            peer_write_timeout: Duration::from_secs(5),
            peer_connect_timeout: Duration::from_secs(5),
            peer_handshake_timeout: Duration::from_secs(5),
            peer_frame_timeout: Duration::from_secs(30),
            max_frame_bytes: 1024 * 1024,
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

    async fn state_with_peer() -> (
        Arc<Mutex<NodeState>>,
        u64,
        mpsc::Receiver<crate::state::OutboundFrame>,
    ) {
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

    fn outbound_budget() -> Arc<Semaphore> {
        Arc::new(Semaphore::new(crate::network::GLOBAL_OUTBOUND_QUEUE_BYTES))
    }

    fn entity_payload(public_key: [u8; 32]) -> [u8; RESPOND_ENTITY_PAYLOAD_SIZE] {
        let mut payload = [0; RESPOND_ENTITY_PAYLOAD_SIZE];
        payload[..32].copy_from_slice(&public_key);
        payload
    }

    #[tokio::test]
    async fn partial_global_admission_closes_unsent_pending_routes() {
        let state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let mut peer_receivers = Vec::new();
        {
            let mut locked = state.lock().await;
            for last_octet in 1..=2 {
                let (tx, rx) = mpsc::channel(2);
                let (disconnect_tx, _disconnect_rx) = watch::channel(false);
                let peer_id = locked
                    .register_session(
                        SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, last_octet), DEFAULT_PORT),
                        true,
                        tx,
                        disconnect_tx,
                        DEFAULT_PORT,
                    )
                    .expect("test peer should register");
                peer_receivers.push((peer_id, rx));
            }
        }
        let pending = Arc::new(PendingRequests::default());
        let targets = state.lock().await.collect_all_targets(2);
        let (registration, receivers) = pending.register_with_spec(
            targets.iter().map(|target| target.peer_id),
            PendingSpec {
                response_rules: &[ResponseRule {
                    message_type: END_RESPONSE_TYPE,
                    min_frame_bytes: HEADER_SIZE,
                    max_frame_bytes: HEADER_SIZE,
                    max_frames: 1,
                    terminal: true,
                }],
                max_response_frames: 1,
                max_response_bytes: HEADER_SIZE,
            },
        );
        let request = Bytes::from_static(&[8, 0, 0, 1, 1, 0, 0, 0]);
        let accepted = send_request_to_targets(
            &state,
            &pending,
            targets,
            &request,
            &Arc::new(Semaphore::new(request.len())),
        )
        .await
        .expect("one peer should accept before global pressure");
        registration.retain_peers(&accepted);

        assert_eq!(accepted.len(), 1);
        for (peer_id, mut receiver) in receivers {
            if accepted.contains(&peer_id) {
                assert!(matches!(
                    receiver.try_recv(),
                    Err(mpsc::error::TryRecvError::Empty)
                ));
            } else {
                assert!(receiver.recv().await.is_none());
            }
        }
        let sent_count = peer_receivers
            .iter_mut()
            .map(|(_, receiver)| receiver.try_recv().is_ok())
            .filter(|received| *received)
            .count();
        assert_eq!(sent_count, 1);
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
    async fn absent_entity_is_not_reported_as_a_verified_zero_balance() {
        let public_key = [9; 32];
        let mut payload = entity_payload(public_key);
        payload[68..72].copy_from_slice(&(-1i32).to_le_bytes());
        let (tx, rx) = mpsc::channel(1);
        tx.send(pending_frame(Bytes::from(
            build_request_frame(RESPOND_ENTITY_TYPE, 7, &payload).unwrap(),
        )))
        .await
        .unwrap();

        let error = receive_balance(rx, "wallet", public_key, &TrustedNetworkState::default())
            .await
            .unwrap_err();

        assert!(matches!(error, PeerQueryError::PeerUnavailable(_)));
        assert!(
            error
                .to_string()
                .contains("cannot prove that the entity is absent")
        );
    }

    #[test]
    fn balance_proof_changes_when_any_sibling_changes() {
        let public_key = [9; 32];
        let mut payload = entity_payload(public_key);
        payload[68..72].copy_from_slice(&0i32.to_le_bytes());
        let original = parse_balance_payload("wallet", public_key, &payload)
            .expect("entity should parse")
            .proof_root();

        for sibling in 0..SPECTRUM_DEPTH {
            let mut mutated = payload;
            mutated[72 + sibling * 32] ^= 1;
            let root = parse_balance_payload("wallet", public_key, &mutated)
                .expect("mutated entity should parse")
                .proof_root();
            assert_ne!(root, original);
        }
    }

    proptest! {
        #[test]
        fn arbitrary_nonzero_merkle_path_mutation_changes_root(
            sibling in 0usize..SPECTRUM_DEPTH,
            byte in 0usize..32,
            mask in 1u8..=u8::MAX,
        ) {
            let public_key = [9; 32];
            let mut payload = entity_payload(public_key);
            payload[68..72].copy_from_slice(&0i32.to_le_bytes());
            let original = parse_balance_payload("wallet", public_key, &payload)
                .expect("entity should parse")
                .proof_root();
            payload[72 + sibling * 32 + byte] ^= mask;
            let mutated = parse_balance_payload("wallet", public_key, &payload)
                .expect("mutated entity should parse")
                .proof_root();
            prop_assert_ne!(mutated, original);
        }
    }

    #[test]
    fn local_request_larger_than_configured_ceiling_is_internal_not_peer_fault() {
        let mut config = (*test_config(Duration::from_secs(1))).clone();
        config.max_frame_bytes = crate::frame::MIN_OPERATIONAL_FRAME_BYTES;
        let frame = vec![0; config.max_frame_bytes + 1];

        assert!(matches!(
            ensure_local_request_size(&frame, &config),
            Err(PeerQueryError::Internal(_))
        ));
    }

    #[tokio::test]
    async fn first_balance_response_completes_and_cleans_pending_request() {
        let (state, peer_id, mut peer_rx) = state_with_peer().await;
        let pending = Arc::new(PendingRequests::default());
        let trusted_network = Arc::new(TrustedNetworkState::default());
        let query = tokio::spawn(query_balance(
            state,
            Arc::clone(&pending),
            outbound_budget(),
            Arc::clone(&trusted_network),
            test_config(Duration::from_secs(1)),
            "wallet",
            [9; 32],
        ));
        let request = peer_rx.recv().await.unwrap().bytes;
        let dejavu = u32::from_le_bytes(request[4..8].try_into().unwrap());
        let mut payload = [0; RESPOND_ENTITY_PAYLOAD_SIZE];
        payload[..32].copy_from_slice(&[9; 32]);
        payload[32..40].copy_from_slice(&100i64.to_le_bytes());
        payload[40..48].copy_from_slice(&40i64.to_le_bytes());
        payload[64..68].copy_from_slice(&123u32.to_le_bytes());
        let root = parse_balance_payload("wallet", [9; 32], &payload)
            .expect("test entity should parse")
            .proof_root();
        trusted_network.set_spectrum_root_for_test(123, root);
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
    async fn malformed_bound_balance_response_disconnects_and_cools_peer() {
        let (state, peer_id, mut peer_rx) = state_with_peer().await;
        let pending = Arc::new(PendingRequests::default());
        let query = tokio::spawn(query_balance(
            Arc::clone(&state),
            Arc::clone(&pending),
            outbound_budget(),
            Arc::new(TrustedNetworkState::default()),
            test_config(Duration::from_secs(1)),
            "wallet",
            [9; 32],
        ));
        let request = peer_rx.recv().await.unwrap().bytes;
        let dejavu = u32::from_le_bytes(request[4..8].try_into().unwrap());
        let payload = entity_payload([8; 32]);
        let response =
            Bytes::from(build_request_frame(RESPOND_ENTITY_TYPE, dejavu, &payload).unwrap());

        assert_eq!(
            pending.deliver(peer_id, dejavu, response),
            crate::pending::DeliveryOutcome::Delivered
        );
        let error = query.await.unwrap().unwrap_err();

        assert!(error.to_string().contains("does not match request"));
        let locked = state.lock().await;
        assert_eq!(locked.outgoing_count(), 0);
        assert_eq!(locked.pool_stats(std::time::Instant::now()).cooldown, 1);
    }

    #[tokio::test]
    async fn timeout_cleans_pending_request() {
        let (state, _peer_id, mut peer_rx) = state_with_peer().await;
        let pending = Arc::new(PendingRequests::default());
        let query = query_balance(
            state,
            Arc::clone(&pending),
            outbound_budget(),
            Arc::new(TrustedNetworkState::default()),
            test_config(Duration::from_millis(20)),
            "wallet",
            [9; 32],
        );
        let (_, result) = tokio::join!(peer_rx.recv(), query);

        assert_eq!(
            result.unwrap_err().to_string(),
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
            outbound_budget(),
            test_config(Duration::from_secs(1)),
            3,
            2,
            &[9; 32],
        ));
        let request = peer_rx.recv().await.unwrap().bytes;
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
    async fn documented_empty_contract_result_does_not_penalize_peer() {
        let (state, peer_id, mut peer_rx) = state_with_peer().await;
        let pending = Arc::new(PendingRequests::default());
        let query = tokio::spawn(query_contract_function(
            Arc::clone(&state),
            Arc::clone(&pending),
            outbound_budget(),
            test_config(Duration::from_secs(1)),
            3,
            2,
            &[],
        ));
        let request = peer_rx.recv().await.unwrap().bytes;
        let dejavu = u32::from_le_bytes(request[4..8].try_into().unwrap());
        let response =
            Bytes::from(build_request_frame(RESPOND_CONTRACT_FUNCTION_TYPE, dejavu, &[]).unwrap());
        assert_eq!(
            pending.deliver(peer_id, dejavu, response),
            crate::pending::DeliveryOutcome::Delivered
        );

        assert_eq!(
            query.await.unwrap().unwrap_err().to_string(),
            "Failed to query contract function from peers (parallel): peer 1: contract function invocation failed"
        );
        let locked = state.lock().await;
        assert_eq!(locked.outgoing_count(), 1);
        assert_eq!(locked.pool_stats(std::time::Instant::now()).cooldown, 0);
    }

    #[tokio::test]
    async fn contract_function_maps_try_again_to_peer_error() {
        let (tx, rx) = mpsc::channel(1);
        tx.send(pending_frame(Bytes::from(
            build_request_frame(TRY_AGAIN_TYPE, 7, &[]).unwrap(),
        )))
        .await
        .unwrap();

        assert_eq!(
            receive_contract_function(rx).await.unwrap_err().to_string(),
            "peer requested retry"
        );
    }

    #[tokio::test]
    async fn contract_function_rejects_empty_response_as_core_failure() {
        let (tx, rx) = mpsc::channel(1);
        tx.send(pending_frame(Bytes::from(
            build_request_frame(RESPOND_CONTRACT_FUNCTION_TYPE, 7, &[]).unwrap(),
        )))
        .await
        .unwrap();

        assert_eq!(
            receive_contract_function(rx).await.unwrap_err().to_string(),
            "contract function invocation failed"
        );
    }

    #[tokio::test]
    async fn contract_function_rejects_output_larger_than_core_u16_size() {
        let (tx, rx) = mpsc::channel(1);
        tx.send(pending_frame(Bytes::from(
            build_request_frame(
                RESPOND_CONTRACT_FUNCTION_TYPE,
                7,
                &vec![0; MAX_CONTRACT_FUNCTION_OUTPUT_SIZE + 1],
            )
            .unwrap(),
        )))
        .await
        .unwrap();

        assert_eq!(
            receive_contract_function(rx).await.unwrap_err().to_string(),
            "Contract function output is too large: maximum 65535, got 65536"
        );
    }
}
