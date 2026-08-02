use crate::config::Config;
use crate::frame::{
    BROADCAST_COMPUTORS_PAYLOAD_SIZE, BROADCAST_COMPUTORS_TYPE, BROADCAST_TICK_PAYLOAD_SIZE,
    BROADCAST_TICK_TYPE, BROADCAST_TRANSACTION_TYPE, END_RESPONSE_TYPE,
    EXCHANGE_PUBLIC_PEERS_FRAME_SIZE, EXCHANGE_PUBLIC_PEERS_TYPE, HEADER_SIZE,
    OC_MACHINE_INVOCATION_TYPE, ORACLE_MACHINE_QUERY_TYPE, ORACLE_MACHINE_REPLY_TYPE,
    REQUEST_COMPUTORS_TYPE, build_exchange_public_peers_frame, build_request_frame,
    decode_frame_size, frame_meta, frame_payload, message_type_name, parse_exchange_public_peers,
    parse_transaction_layout,
};
use crate::pending::{DeliveryOutcome, PendingEvent, PendingRequests, PendingSpec, ResponseRule};
use crate::state::{
    DedupReservationError, DedupWindow, DisconnectReason, NodeState, OutboundAdmissionError,
    OutboundFrame, PeerPoolStats, RelayTarget,
};
use crate::types::{format_epoch_tick_packed, pack_epoch_tick};
use crate::verified::{
    ComputorVerification, TickVerification, TrustedNetworkState, unix_time_millis,
};
use bytes::{Bytes, BytesMut};
use std::collections::HashSet;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::time::{Instant as TokioInstant, sleep, timeout, timeout_at};

const OUTBOUND_QUEUE_CAPACITY: usize = 1024;
pub(crate) const GLOBAL_OUTBOUND_QUEUE_BYTES: usize = 256 * 1024 * 1024;
const DISSEMINATION_MULTIPLIER: usize = 6;
const READ_BUFFER_SIZE: usize = 128 * 1024;
const ACCUMULATED_INITIAL_CAPACITY: usize = 256 * 1024;
const ACCUMULATED_RETAIN_CAPACITY: usize = 512 * 1024;
const ACCUMULATED_SHRINK_THRESHOLD: usize = 2 * 1024 * 1024;

#[cfg(unix)]
fn configure_tcp_keepalive(stream: &TcpStream) -> std::io::Result<()> {
    let sock_ref = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(60)) // Начать проверку через 60с простоя
        .with_interval(Duration::from_secs(10)) // Проверять каждые 10с
        .with_retries(3); // 3 неудачи = мёртвое соединение
    sock_ref.set_tcp_keepalive(&keepalive)?;
    Ok(())
}

#[cfg(windows)]
fn configure_tcp_keepalive(stream: &TcpStream) -> std::io::Result<()> {
    let sock_ref = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(60))
        .with_interval(Duration::from_secs(10));
    sock_ref.set_tcp_keepalive(&keepalive)?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn configure_tcp_keepalive(_stream: &TcpStream) -> std::io::Result<()> {
    Ok(())
}

fn calculate_backoff(attempt: u32, initial_ms: u64, max_ms: u64) -> Duration {
    let backoff_ms = initial_ms
        .saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)))
        .min(max_ms);
    Duration::from_millis(backoff_ms)
}

fn reset_emergency_dns_backoff(
    attempt: &mut u32,
    next_emergency_dns: &mut Instant,
    now: Instant,
    initial_ms: u64,
) {
    *attempt = 0;
    *next_emergency_dns = now + Duration::from_millis(initial_ms);
}

fn emergency_dns_needed(
    outgoing: usize,
    critical_threshold: usize,
    emergency_dns_bootstrap: bool,
    dns_bootstrap: bool,
) -> bool {
    emergency_dns_bootstrap && dns_bootstrap && outgoing < critical_threshold
}

fn log_pool_stats(stats: PeerPoolStats, target: usize) {
    println!(
        "Peer pool | known={} dialable={} cooldown={} pending={} incoming={} outgoing={} target={target}",
        stats.known, stats.dialable, stats.cooldown, stats.pending, stats.incoming, stats.outgoing,
    );
}

fn apply_emergency_dns_result(
    state: &mut NodeState,
    mut peers: Vec<SocketAddrV4>,
    attempt: &mut u32,
) -> usize {
    peers.sort_unstable();
    peers.dedup();
    let added_count = peers
        .into_iter()
        .filter(|peer| {
            let admission = state.add_dns_peer(*peer);
            admission.retained && (admission.inserted || admission.promoted)
        })
        .count();
    if added_count == 0 {
        *attempt = attempt.saturating_add(1);
    } else {
        *attempt = 0;
    };
    added_count
}

#[derive(Debug, Default)]
struct DispatchResult {
    sent_count: usize,
    full_peer_ids: Vec<u64>,
    closed_peer_ids: Vec<u64>,
    global_pressure: bool,
    frame_too_large: bool,
}

#[derive(Clone)]
pub(crate) struct NetworkResources {
    state: Arc<Mutex<NodeState>>,
    dedup: Arc<DedupWindow>,
    pending: Arc<PendingRequests>,
    latest_epoch_tick: Arc<AtomicU64>,
    trusted_network: Arc<TrustedNetworkState>,
    signed_replay: Arc<DedupWindow>,
    signed_invalid: Arc<DedupWindow>,
    verification_slots: Arc<Semaphore>,
    outbound_budget: Arc<Semaphore>,
    config: Arc<Config>,
}

impl NetworkResources {
    pub(crate) fn new(
        state: Arc<Mutex<NodeState>>,
        dedup: Arc<DedupWindow>,
        pending: Arc<PendingRequests>,
        latest_epoch_tick: Arc<AtomicU64>,
        trusted_network: Arc<TrustedNetworkState>,
        outbound_budget: Arc<Semaphore>,
        config: Arc<Config>,
    ) -> Self {
        let verification_parallelism = std::thread::available_parallelism()
            .map_or(2, usize::from)
            .clamp(2, 8);
        Self {
            state,
            dedup,
            pending,
            latest_epoch_tick,
            trusted_network,
            signed_replay: Arc::new(DedupWindow::new(config.max_seen)),
            signed_invalid: Arc::new(DedupWindow::new(config.max_seen)),
            verification_slots: Arc::new(Semaphore::new(verification_parallelism)),
            outbound_budget,
            config,
        }
    }
}

struct ConnectionContext {
    peer_id: u64,
    remote: SocketAddrV4,
    outbound: bool,
    _incoming_permit: Option<OwnedSemaphorePermit>,
    _incoming_ip_reservation: Option<IncomingIpReservation>,
    resources: NetworkResources,
}

struct IncomingIpReservation {
    ip: std::net::Ipv4Addr,
    reserved: Arc<StdMutex<HashSet<std::net::Ipv4Addr>>>,
}

impl IncomingIpReservation {
    fn try_new(
        ip: std::net::Ipv4Addr,
        reserved: &Arc<StdMutex<HashSet<std::net::Ipv4Addr>>>,
    ) -> Option<Self> {
        let mut locked = reserved
            .lock()
            .expect("incoming IP reservation mutex should not be poisoned");
        if !locked.insert(ip) {
            return None;
        }
        Some(Self {
            ip,
            reserved: Arc::clone(reserved),
        })
    }
}

impl Drop for IncomingIpReservation {
    fn drop(&mut self) {
        self.reserved
            .lock()
            .expect("incoming IP reservation mutex should not be poisoned")
            .remove(&self.ip);
    }
}

fn dispatch_frame(
    targets: Vec<RelayTarget>,
    frame: &Bytes,
    global_budget: &Arc<Semaphore>,
) -> DispatchResult {
    let mut result = DispatchResult::default();

    for target in targets {
        let queued = match OutboundFrame::try_new(
            frame.clone(),
            Arc::clone(&target.byte_budget),
            Arc::clone(global_budget),
        ) {
            Ok(queued) => queued,
            Err(OutboundAdmissionError::PeerBudgetExhausted) => {
                result.full_peer_ids.push(target.peer_id);
                continue;
            }
            Err(OutboundAdmissionError::GlobalBudgetExhausted) => {
                result.global_pressure = true;
                break;
            }
            Err(OutboundAdmissionError::FrameTooLarge) => {
                result.frame_too_large = true;
                break;
            }
        };
        match target.tx.try_send(queued) {
            Ok(()) => {
                result.sent_count += 1;
            }
            Err(TrySendError::Full(_)) => {
                result.full_peer_ids.push(target.peer_id);
            }
            Err(TrySendError::Closed(_)) => {
                result.closed_peer_ids.push(target.peer_id);
            }
        }
    }

    result
}

async fn disconnect_failed_targets(state: &Arc<Mutex<NodeState>>, result: &DispatchResult) {
    if result.full_peer_ids.is_empty() && result.closed_peer_ids.is_empty() {
        return;
    }

    let mut locked = state.lock().await;
    for peer_id in &result.full_peer_ids {
        if let Some(remote) =
            locked.disconnect_session_with_reason(*peer_id, DisconnectReason::PeerQueueFull)
        {
            eprintln!("Disconnecting {remote}: outbound queue is full");
        }
    }
    for peer_id in &result.closed_peer_ids {
        let _ = locked.disconnect_session_with_reason(*peer_id, DisconnectReason::PeerQueueClosed);
    }
}

pub(crate) async fn broadcast_transaction_to_network(
    state: Arc<Mutex<NodeState>>,
    dedup: Arc<DedupWindow>,
    outbound_budget: Arc<Semaphore>,
    tx_bytes: &[u8],
) -> Result<(), String> {
    if tx_bytes.is_empty() {
        return Err("Transaction payload is empty".to_string());
    }
    parse_transaction_layout(tx_bytes, None)?;

    let frame = build_request_frame(BROADCAST_TRANSACTION_TYPE, 0, tx_bytes)?;
    let digest = *blake3::hash(&frame).as_bytes();
    let reservation = match dedup.reserve(digest) {
        Ok(reservation) => reservation,
        Err(DedupReservationError::AlreadyCommitted) => return Ok(()),
        Err(DedupReservationError::InFlight) => {
            return Err("Transaction broadcast is already in progress".to_string());
        }
    };

    let targets = {
        let locked = state.lock().await;
        let targets = locked.collect_all_targets(DISSEMINATION_MULTIPLIER);
        if targets.is_empty() {
            return Err("No connected peers available for broadcast".to_string());
        }
        targets
    };

    let frame = Bytes::from(frame);
    let result = dispatch_frame(targets, &frame, &outbound_budget);
    disconnect_failed_targets(&state, &result).await;

    if result.sent_count == 0 {
        if result.global_pressure {
            return Err(
                "Failed to broadcast transaction: global outbound queue is overloaded".to_string(),
            );
        }
        if result.frame_too_large {
            return Err("Failed to broadcast transaction: frame is too large".to_string());
        }
        let full_count = result.full_peer_ids.len();
        let closed_count = result.closed_peer_ids.len();
        let peer_count = full_count + closed_count;
        if full_count == peer_count {
            return Err(
                "Failed to broadcast transaction: all peer outbound queues are full".to_string(),
            );
        }
        if closed_count == peer_count {
            return Err(
                "Failed to broadcast transaction: all peer sessions are closed".to_string(),
            );
        }
        return Err(
            "Failed to broadcast transaction: no peer accepted the transaction".to_string(),
        );
    }

    reservation.commit();

    Ok(())
}

pub(crate) async fn accept_loop(listener: TcpListener, resources: NetworkResources) {
    let incoming_slots = Arc::new(Semaphore::new(resources.config.max_incoming));
    let incoming_ips = Arc::new(StdMutex::new(HashSet::new()));
    loop {
        match listener.accept().await {
            Ok((stream, remote_addr)) => {
                let remote_v4 = match remote_addr {
                    SocketAddr::V4(addr) => addr,
                    SocketAddr::V6(_) => {
                        continue;
                    }
                };

                let Some(incoming_ip_reservation) =
                    IncomingIpReservation::try_new(*remote_v4.ip(), &incoming_ips)
                else {
                    println!("Rejecting incoming {remote_v4}: source IP already reserved.");
                    continue;
                };

                if let Err(err) = stream.set_nodelay(true) {
                    eprintln!("Failed to set TCP_NODELAY for {remote_v4}: {err}");
                }
                if let Err(err) = configure_tcp_keepalive(&stream) {
                    eprintln!("Failed to set TCP keepalive for {remote_v4}: {err}");
                }

                let Ok(incoming_permit) = Arc::clone(&incoming_slots).try_acquire_owned() else {
                    println!("Rejecting incoming {remote_v4}: incoming limit reached.");
                    continue;
                };
                let connection_resources = resources.clone();
                tokio::spawn(async move {
                    establish_connection(
                        stream,
                        remote_v4,
                        false,
                        Some(incoming_permit),
                        Some(incoming_ip_reservation),
                        connection_resources,
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

pub(crate) async fn dial_loop(resources: NetworkResources) {
    let state = Arc::clone(&resources.state);
    let config = Arc::clone(&resources.config);
    let mut emergency_dns_attempt = 0u32;
    let mut next_emergency_dns =
        Instant::now() + Duration::from_millis(config.emergency_dns_backoff_initial_ms);
    let mut last_pool_stats = None;
    let mut completed_dial_cycle = false;

    loop {
        let cycle_now = Instant::now();
        // Проверить здоровье пиров и запустить аварийный DNS при необходимости
        let current_outbound = {
            let locked = state.lock().await;
            locked.outgoing_count()
        };

        // Логика аварийного DNS bootstrap
        if completed_dial_cycle
            && emergency_dns_needed(
                current_outbound,
                config.critical_peer_threshold,
                config.emergency_dns_bootstrap,
                config.dns_bootstrap,
            )
        {
            if cycle_now >= next_emergency_dns {
                eprintln!(
                    "CRITICAL: Only {current_outbound} outbound peers connected (threshold: {}). Attempting emergency DNS bootstrap...",
                    config.critical_peer_threshold
                );

                let dns_lite_peers = if config.dns_lite_peers == 0 {
                    (config.target_outbound * 3).max(8)
                } else {
                    config.dns_lite_peers
                };

                match crate::dns::fetch_seed_peers_from_dns(
                    config.peer_port,
                    dns_lite_peers,
                    config.dns_timeout,
                )
                .await
                {
                    Ok(peers) => {
                        let mut locked = state.lock().await;
                        let added_count = apply_emergency_dns_result(
                            &mut locked,
                            peers,
                            &mut emergency_dns_attempt,
                        );
                        drop(locked);

                        if added_count == 0 {
                            eprintln!("Emergency DNS bootstrap returned no new or promoted peers.");
                        } else {
                            println!(
                                "Emergency DNS bootstrap: added or promoted {added_count} peers."
                            );

                            // Сбросить backoff при успехе
                        }
                        let completed_at = Instant::now();
                        next_emergency_dns = completed_at
                            + calculate_backoff(
                                emergency_dns_attempt,
                                config.emergency_dns_backoff_initial_ms,
                                config.emergency_dns_backoff_max_ms,
                            );
                    }
                    Err(err) => {
                        eprintln!("Emergency DNS bootstrap failed: {err}");
                        emergency_dns_attempt = emergency_dns_attempt.saturating_add(1);
                        let completed_at = Instant::now();
                        next_emergency_dns = completed_at
                            + calculate_backoff(
                                emergency_dns_attempt,
                                config.emergency_dns_backoff_initial_ms,
                                config.emergency_dns_backoff_max_ms,
                            );
                    }
                }
            }
        } else if current_outbound >= config.critical_peer_threshold {
            // Сбросить состояние аварии когда восстановились
            if emergency_dns_attempt > 0 {
                println!(
                    "Outbound peer count recovered to {current_outbound}. Resetting emergency DNS backoff."
                );
            }
            reset_emergency_dns_backoff(
                &mut emergency_dns_attempt,
                &mut next_emergency_dns,
                cycle_now,
                config.emergency_dns_backoff_initial_ms,
            );
        }

        // Обычная логика подключения
        let targets = {
            let mut locked = state.lock().await;
            let current_outbound = locked.outgoing_count();
            let occupied = current_outbound.saturating_add(locked.pending_dial_count());
            if occupied >= config.target_outbound {
                Vec::new()
            } else {
                let needed = config.target_outbound - occupied;
                locked.choose_dial_targets_at(needed, cycle_now)
            }
        };

        let pool_stats = state.lock().await.pool_stats(cycle_now);
        if last_pool_stats != Some(pool_stats) {
            log_pool_stats(pool_stats, config.target_outbound);
            last_pool_stats = Some(pool_stats);
        }

        for target in targets {
            let resources_for_task = resources.clone();
            tokio::spawn(async move {
                match timeout(
                    resources_for_task.config.peer_connect_timeout,
                    TcpStream::connect(target),
                )
                .await
                {
                    Ok(Ok(stream)) => {
                        if let Err(err) = stream.set_nodelay(true) {
                            eprintln!("Failed to set TCP_NODELAY for {target}: {err}");
                        }
                        if let Err(err) = configure_tcp_keepalive(&stream) {
                            eprintln!("Failed to set TCP keepalive for {target}: {err}");
                        }
                        establish_connection(stream, target, true, None, None, resources_for_task)
                            .await;
                    }
                    Ok(Err(err)) => {
                        {
                            let mut locked = resources_for_task.state.lock().await;
                            locked.record_peer_failure(
                                target,
                                resources_for_task.config.reconnect_interval,
                                Instant::now(),
                            );
                        }
                        eprintln!("Dial failed {target}: {err}");
                    }
                    Err(_) => {
                        resources_for_task.state.lock().await.record_peer_failure(
                            target,
                            resources_for_task.config.reconnect_interval,
                            Instant::now(),
                        );
                        eprintln!(
                            "Dial timed out {target} after {} ms",
                            resources_for_task.config.peer_connect_timeout.as_millis()
                        );
                    }
                }
            });
        }

        completed_dial_cycle = true;
        sleep(config.reconnect_interval).await;
    }
}

async fn establish_connection(
    mut stream: TcpStream,
    remote: SocketAddrV4,
    outbound: bool,
    incoming_permit: Option<OwnedSemaphorePermit>,
    incoming_ip_reservation: Option<IncomingIpReservation>,
    resources: NetworkResources,
) {
    let handshake_payload = {
        let locked = resources.state.lock().await;
        build_exchange_public_peers_frame(locked.choose_handshake_peers())
    };
    let handshake_frame = match perform_handshake(
        &mut stream,
        &handshake_payload,
        resources.config.peer_handshake_timeout,
        resources.config.max_frame_bytes,
    )
    .await
    {
        Ok(frame) => frame,
        Err(err) => {
            if outbound {
                resources.state.lock().await.record_peer_failure(
                    remote,
                    resources.config.reconnect_interval,
                    Instant::now(),
                );
            }
            eprintln!("Handshake failed {remote}: {err}");
            return;
        }
    };

    update_discovered_peers(
        &resources.state,
        &handshake_frame,
        resources.config.peer_port,
        *remote.ip(),
    )
    .await;

    let (tx, rx) = mpsc::channel::<OutboundFrame>(OUTBOUND_QUEUE_CAPACITY);
    let (disconnect_tx, disconnect_rx) = watch::channel(false);
    let (peer_id, incoming_count, outgoing_count) = {
        let mut locked = resources.state.lock().await;

        if outbound && locked.outgoing_count() >= resources.config.target_outbound {
            println!("Rejecting outbound {remote}: outbound target already reached.");
            locked.clear_pending_dial(remote);
            return;
        }

        let Some(peer_id) = locked.register_session(
            remote,
            outbound,
            tx.clone(),
            disconnect_tx,
            resources.config.peer_port,
        ) else {
            locked.clear_pending_dial(remote);
            return;
        };

        (peer_id, locked.incoming_count(), locked.outgoing_count())
    };

    println!(
        "Connected {} [{}] | in={} out={} | {}",
        remote,
        if outbound { "out" } else { "in" },
        incoming_count,
        outgoing_count,
        format_epoch_tick_packed(resources.latest_epoch_tick.load(Ordering::Relaxed))
    );

    let pending = Arc::clone(&resources.pending);
    let state = Arc::clone(&resources.state);
    let worker = tokio::spawn(connection_worker(
        stream,
        rx,
        disconnect_rx,
        ConnectionContext {
            peer_id,
            remote,
            outbound,
            _incoming_permit: incoming_permit,
            _incoming_ip_reservation: incoming_ip_reservation,
            resources,
        },
    ));
    if let Err(error) = worker.await {
        state.lock().await.unregister_session(peer_id);
        pending.peer_disconnected(peer_id);
        eprintln!("Connection worker failed for {remote}: {error}");
    }
}

async fn perform_handshake(
    stream: &mut TcpStream,
    local_handshake: &[u8],
    handshake_timeout: Duration,
    max_frame_bytes: usize,
) -> Result<Bytes, String> {
    let deadline = TokioInstant::now() + handshake_timeout;
    timeout_at(deadline, stream.write_all(local_handshake))
        .await
        .map_err(|_| "handshake write timed out".to_string())?
        .map_err(|err| format!("handshake write failed: {err}"))?;

    let mut header = [0u8; HEADER_SIZE];
    timeout_at(deadline, stream.read_exact(&mut header))
        .await
        .map_err(|_| "handshake read timed out".to_string())?
        .map_err(|err| format!("handshake header read failed: {err}"))?;
    let size = decode_frame_size(&header);
    if size != EXCHANGE_PUBLIC_PEERS_FRAME_SIZE
        || size > max_frame_bytes
        || header[3] != EXCHANGE_PUBLIC_PEERS_TYPE
    {
        return Err(format!(
            "expected {EXCHANGE_PUBLIC_PEERS_FRAME_SIZE}-byte EXCHANGE_PUBLIC_PEERS, got size={size} type={}",
            header[3]
        ));
    }
    let mut frame = vec![0u8; size];
    frame[..HEADER_SIZE].copy_from_slice(&header);
    timeout_at(deadline, stream.read_exact(&mut frame[HEADER_SIZE..]))
        .await
        .map_err(|_| "handshake payload read timed out".to_string())?
        .map_err(|err| format!("handshake payload read failed: {err}"))?;
    Ok(Bytes::from(frame))
}

const COMPUTOR_RESPONSE_RULES: &[ResponseRule] = &[
    ResponseRule {
        message_type: BROADCAST_COMPUTORS_TYPE,
        min_frame_bytes: HEADER_SIZE + BROADCAST_COMPUTORS_PAYLOAD_SIZE,
        max_frame_bytes: HEADER_SIZE + BROADCAST_COMPUTORS_PAYLOAD_SIZE,
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
];

async fn computor_bootstrap(
    peer_id: u64,
    remote: SocketAddrV4,
    mut disconnect_rx: watch::Receiver<bool>,
    resources: NetworkResources,
) {
    let mut retry_delay = resources.config.reconnect_interval;
    let max_retry_delay = Duration::from_secs(30);
    loop {
        if resources.trusted_network.has_computors() {
            return;
        }
        let Some(target) = resources.state.lock().await.target(peer_id) else {
            return;
        };
        let (registration, mut receiver) = resources.pending.register_control(
            peer_id,
            PendingSpec {
                response_rules: COMPUTOR_RESPONSE_RULES,
                max_response_frames: 1,
                max_response_bytes: HEADER_SIZE + BROADCAST_COMPUTORS_PAYLOAD_SIZE,
            },
        );
        let request = Bytes::from(
            build_request_frame(REQUEST_COMPUTORS_TYPE, registration.dejavu(), &[])
                .expect("request computors frame has a fixed valid size"),
        );
        let dispatch = dispatch_frame(vec![target], &request, &resources.outbound_budget);
        disconnect_failed_targets(&resources.state, &dispatch).await;
        let retry = if dispatch.sent_count == 0 {
            if !dispatch.full_peer_ids.is_empty() || !dispatch.closed_peer_ids.is_empty() {
                return;
            }
            true
        } else {
            match timeout(resources.config.peer_frame_timeout, receiver.recv()).await {
                Ok(Some(PendingEvent::Frame(frame))) if frame[3] == BROADCAST_COMPUTORS_TYPE => {
                    sleep(Duration::from_millis(10)).await;
                    !resources.trusted_network.has_computors()
                }
                Ok(Some(PendingEvent::Frame(frame))) if frame[3] == END_RESPONSE_TYPE => true,
                Ok(Some(PendingEvent::Frame(_))) => false,
                Ok(Some(PendingEvent::PeerDisconnected)) | Ok(None) => return,
                Err(_) => true,
            }
        };
        drop(registration);

        if !retry || resources.trusted_network.has_computors() {
            return;
        }
        println!(
            "Computor bootstrap unavailable from {remote}; retrying in {} ms",
            retry_delay.as_millis()
        );
        tokio::select! {
            _ = sleep(retry_delay) => {}
            changed = disconnect_rx.changed() => {
                if changed.is_err() || *disconnect_rx.borrow() {
                    return;
                }
            }
        }
        retry_delay = retry_delay.saturating_mul(2).min(max_retry_delay);
    }
}

async fn connection_worker(
    stream: TcpStream,
    rx: mpsc::Receiver<OutboundFrame>,
    mut disconnect_rx: watch::Receiver<bool>,
    context: ConnectionContext,
) {
    enum ConnectionExit {
        Io(String),
        Protocol(String),
        LocalPolicy(String),
        Requested,
    }

    let ConnectionContext {
        peer_id,
        remote,
        outbound,
        _incoming_permit,
        _incoming_ip_reservation,
        resources,
    } = context;
    let connection_exit = {
        let bootstrap_task = tokio::spawn(computor_bootstrap(
            peer_id,
            remote,
            disconnect_rx.clone(),
            resources.clone(),
        ));
        let (reader, mut writer) = stream.into_split();
        let writer_future = write_peer_frames(
            &mut writer,
            rx,
            remote,
            resources.config.traffic_log,
            Arc::clone(&resources.latest_epoch_tick),
            resources.config.peer_write_timeout,
        );
        let read_future = read_peer_frames(reader, peer_id, remote, resources.clone());
        tokio::pin!(writer_future);
        tokio::pin!(read_future);

        let exit = tokio::select! {
            result = &mut writer_future => ConnectionExit::Io(match result {
                Ok(()) => "outbound queue closed".to_string(),
                Err(err) => err,
            }),
            result = &mut read_future => match result {
                Ok(()) => ConnectionExit::Io("reader stopped".to_string()),
                Err(PeerReadError::Io(err)) => ConnectionExit::Io(err),
                Err(PeerReadError::Protocol(err)) => ConnectionExit::Protocol(err),
                Err(PeerReadError::LocalPolicy(err)) => ConnectionExit::LocalPolicy(err),
            },
            _ = disconnect_rx.changed() => {
                ConnectionExit::Requested
            },
        };
        bootstrap_task.abort();
        let _ = bootstrap_task.await;
        exit
    };

    let (in_count, out_count, disconnect_reason) = {
        let mut locked = resources.state.lock().await;
        let removed_endpoint = locked.unregister_session(peer_id);
        let requested_reason = locked.take_disconnect_reason(peer_id);
        let (disconnect_reason, penalize) = match connection_exit {
            ConnectionExit::Io(reason) => requested_reason
                .map_or((reason, outbound), |requested| {
                    (requested.to_string(), requested.penalizes_peer())
                }),
            ConnectionExit::Protocol(reason) => requested_reason
                .map_or((reason, true), |requested| {
                    (requested.to_string(), requested.penalizes_peer())
                }),
            ConnectionExit::LocalPolicy(reason) => (reason, false),
            ConnectionExit::Requested => {
                let reason = requested_reason.unwrap_or(DisconnectReason::Administrative);
                (reason.to_string(), reason.penalizes_peer())
            }
        };
        if penalize && let Some(endpoint) = removed_endpoint {
            locked.record_peer_failure(
                endpoint,
                resources.config.reconnect_interval,
                Instant::now(),
            );
        }
        (
            locked.incoming_count(),
            locked.outgoing_count(),
            disconnect_reason,
        )
    };
    resources.pending.peer_disconnected(peer_id);
    eprintln!("Disconnecting {remote}: {disconnect_reason}");
    println!(
        "Disconnected {remote} | in={in_count} out={out_count} | {}",
        format_epoch_tick_packed(resources.latest_epoch_tick.load(Ordering::Relaxed))
    );
}

async fn write_peer_frames<W: AsyncWrite + Unpin>(
    writer: &mut W,
    mut rx: mpsc::Receiver<OutboundFrame>,
    remote: SocketAddrV4,
    traffic_log: bool,
    latest_epoch_tick: Arc<AtomicU64>,
    peer_write_timeout: Duration,
) -> Result<(), String> {
    while let Some(frame) = rx.recv().await {
        if traffic_log {
            let (size, message_type, dejavu) = frame_meta(&frame.bytes);
            let packed = latest_epoch_tick.load(Ordering::Relaxed);
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
        match timeout(peer_write_timeout, writer.write_all(&frame.bytes)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(format!("write failed: {err}")),
            Err(_) => {
                return Err(format!(
                    "write timed out after {} ms",
                    peer_write_timeout.as_millis()
                ));
            }
        }
    }
    Ok(())
}

async fn read_peer_frames(
    mut reader: OwnedReadHalf,
    peer_id: u64,
    remote: SocketAddrV4,
    resources: NetworkResources,
) -> Result<(), PeerReadError> {
    let initial_capacity = ACCUMULATED_INITIAL_CAPACITY.min(resources.config.max_frame_bytes);
    let mut accumulated = BytesMut::with_capacity(initial_capacity);
    let mut scratch = vec![0u8; READ_BUFFER_SIZE];
    let mut frame_deadline = None;

    loop {
        let read_result = if let Some(deadline) = frame_deadline {
            timeout_at(deadline, reader.read(&mut scratch))
                .await
                .map_err(|_| {
                    PeerReadError::Io(format!(
                        "frame read timed out after {} ms",
                        resources.config.peer_frame_timeout.as_millis()
                    ))
                })?
        } else {
            reader.read(&mut scratch).await
        };
        match read_result {
            Ok(0) => return Err(PeerReadError::Io("peer closed the connection".to_string())),
            Ok(read) => {
                accumulated.extend_from_slice(&scratch[..read]);
                loop {
                    let frame =
                        match extract_frame(&mut accumulated, resources.config.max_frame_bytes) {
                            Ok(Some(frame)) => frame,
                            Ok(None) => break,
                            Err(FrameExtractError::MalformedSize(size)) => {
                                return Err(PeerReadError::Protocol(format!(
                                    "invalid declared frame size {size}"
                                )));
                            }
                            Err(FrameExtractError::LocalPolicyLimit { declared, limit }) => {
                                return Err(PeerReadError::LocalPolicy(format!(
                                    "declared frame size {declared} exceeds local limit {limit}"
                                )));
                            }
                        };
                    frame_deadline = None;
                    if resources.config.traffic_log {
                        let (size, message_type, dejavu) = frame_meta(&frame);
                        let packed = resources.latest_epoch_tick.load(Ordering::Relaxed);
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

                    if process_incoming_frame(peer_id, frame, resources.clone()).await
                        == FrameProcessingOutcome::ConnectionFatal
                    {
                        return Err(PeerReadError::Protocol(
                            "peer protocol violation".to_string(),
                        ));
                    }
                }

                if accumulated.len() > resources.config.max_frame_bytes {
                    return Err(PeerReadError::LocalPolicy(format!(
                        "incomplete frame buffered {} bytes (limit={})",
                        accumulated.len(),
                        resources.config.max_frame_bytes
                    )));
                }

                if !accumulated.is_empty() && frame_deadline.is_none() {
                    frame_deadline =
                        Some(TokioInstant::now() + resources.config.peer_frame_timeout);
                }

                if accumulated.capacity() > ACCUMULATED_SHRINK_THRESHOLD
                    && accumulated.len() < ACCUMULATED_RETAIN_CAPACITY
                {
                    let mut compacted = BytesMut::with_capacity(ACCUMULATED_RETAIN_CAPACITY);
                    compacted.extend_from_slice(&accumulated);
                    accumulated = compacted;
                }
            }
            Err(err) => {
                return Err(PeerReadError::Io(format!("read failed: {err}")));
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FrameExtractError {
    MalformedSize(usize),
    LocalPolicyLimit { declared: usize, limit: usize },
}

#[derive(Debug)]
enum PeerReadError {
    Io(String),
    Protocol(String),
    LocalPolicy(String),
}

fn extract_frame(
    accumulated: &mut BytesMut,
    max_frame_bytes: usize,
) -> Result<Option<Bytes>, FrameExtractError> {
    if accumulated.len() < HEADER_SIZE {
        return Ok(None);
    }
    let frame_size = decode_frame_size(&accumulated[..HEADER_SIZE]);
    if frame_size < HEADER_SIZE {
        return Err(FrameExtractError::MalformedSize(frame_size));
    }
    if frame_size > max_frame_bytes {
        return Err(FrameExtractError::LocalPolicyLimit {
            declared: frame_size,
            limit: max_frame_bytes,
        });
    }
    if accumulated.len() < frame_size {
        return Ok(None);
    }
    Ok(Some(accumulated.split_to(frame_size).freeze()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FrameProcessingOutcome {
    Continue,
    ConnectionFatal,
}

fn computor_verification_digest(payload: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"qubic-computors\0");
    hasher.update(payload);
    *hasher.finalize().as_bytes()
}

fn tick_verification_digest(payload: &[u8], generation: u64) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"qubic-tick\0");
    hasher.update(payload);
    hasher.update(&generation.to_le_bytes());
    *hasher.finalize().as_bytes()
}

fn commit_dedup(dedup: &Arc<DedupWindow>, digest: [u8; 32]) {
    if let Ok(reservation) = dedup.reserve(digest) {
        reservation.commit();
    }
}

async fn protocol_violation(
    source_peer_id: u64,
    resources: &NetworkResources,
) -> FrameProcessingOutcome {
    let _ = resources
        .state
        .lock()
        .await
        .disconnect_session_with_reason(source_peer_id, DisconnectReason::ProtocolViolation);
    FrameProcessingOutcome::ConnectionFatal
}

async fn process_incoming_frame(
    source_peer_id: u64,
    frame: Bytes,
    resources: NetworkResources,
) -> FrameProcessingOutcome {
    if frame.len() < HEADER_SIZE {
        return protocol_violation(source_peer_id, &resources).await;
    }

    let message_type = frame[3];
    let dejavu = u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]);

    if dejavu != 0 {
        match resources
            .pending
            .deliver(source_peer_id, dejavu, frame.clone())
        {
            DeliveryOutcome::Delivered | DeliveryOutcome::Ignored => {
                return FrameProcessingOutcome::Continue;
            }
            DeliveryOutcome::PassThrough => {}
            DeliveryOutcome::ReceiverClosed | DeliveryOutcome::LocalSaturation => {
                return FrameProcessingOutcome::Continue;
            }
            DeliveryOutcome::ProtocolViolation => {
                return protocol_violation(source_peer_id, &resources).await;
            }
            DeliveryOutcome::NotPending => {}
        }
    }

    if message_type == BROADCAST_COMPUTORS_TYPE {
        let Ok(payload) = frame_payload(&frame) else {
            return protocol_violation(source_peer_id, &resources).await;
        };
        let computors = match resources.trusted_network.parse_computors(payload) {
            Ok(computors) => computors,
            Err(ComputorVerification::Malformed) => {
                return protocol_violation(source_peer_id, &resources).await;
            }
            Err(
                ComputorVerification::BadSignature
                | ComputorVerification::AuthenticatedStale
                | ComputorVerification::AuthenticatedConflict
                | ComputorVerification::Duplicate
                | ComputorVerification::Accepted,
            ) => unreachable!("computor parsing only reports malformed input"),
        };
        let digest = computor_verification_digest(payload);
        if resources.signed_invalid.contains(&digest) {
            return protocol_violation(source_peer_id, &resources).await;
        }
        let outcome = match resources.signed_replay.reserve(digest) {
            Err(DedupReservationError::AlreadyCommitted) => {
                resources.trusted_network.apply_computors(computors)
            }
            Err(DedupReservationError::InFlight) => return FrameProcessingOutcome::Continue,
            Ok(replay_reservation) => {
                let Ok(verification_permit) =
                    Arc::clone(&resources.verification_slots).try_acquire_owned()
                else {
                    return FrameProcessingOutcome::Continue;
                };
                let trusted = Arc::clone(&resources.trusted_network);
                let invalid = Arc::clone(&resources.signed_invalid);
                let payload = payload.to_vec();
                let Ok(outcome) = tokio::task::spawn_blocking(move || {
                    let _verification_permit = verification_permit;
                    if trusted.computor_signature_is_valid(&payload) {
                        replay_reservation.commit();
                        trusted.apply_computors(computors)
                    } else {
                        drop(replay_reservation);
                        commit_dedup(&invalid, digest);
                        ComputorVerification::BadSignature
                    }
                })
                .await
                else {
                    return FrameProcessingOutcome::Continue;
                };
                outcome
            }
        };
        if matches!(
            outcome,
            ComputorVerification::Malformed | ComputorVerification::BadSignature
        ) {
            return protocol_violation(source_peer_id, &resources).await;
        }
    }

    if message_type == BROADCAST_TICK_TYPE {
        let Ok(payload) = frame_payload(&frame) else {
            return protocol_violation(source_peer_id, &resources).await;
        };
        if payload.len() != BROADCAST_TICK_PAYLOAD_SIZE {
            return protocol_violation(source_peer_id, &resources).await;
        }
        let received_at = unix_time_millis();
        let context = match resources.trusted_network.tick_context(payload, received_at) {
            Ok(context) => context,
            Err(TickVerification::Malformed) => {
                return protocol_violation(source_peer_id, &resources).await;
            }
            Err(TickVerification::Deferred | TickVerification::AuthenticatedStale) => {
                return FrameProcessingOutcome::Continue;
            }
            Err(
                TickVerification::BadSignature
                | TickVerification::Duplicate
                | TickVerification::Equivocation
                | TickVerification::Accepted
                | TickVerification::Quorum(_),
            ) => unreachable!("tick context cannot report a post-authentication outcome"),
        };
        let digest = tick_verification_digest(payload, context.generation());
        if resources.signed_invalid.contains(&digest) {
            return protocol_violation(source_peer_id, &resources).await;
        }
        let verification = match resources.signed_replay.reserve(digest) {
            Err(DedupReservationError::AlreadyCommitted) => {
                resources.trusted_network.apply_tick(context, payload)
            }
            Err(DedupReservationError::InFlight) => return FrameProcessingOutcome::Continue,
            Ok(replay_reservation) => {
                let Ok(verification_permit) =
                    Arc::clone(&resources.verification_slots).try_acquire_owned()
                else {
                    return FrameProcessingOutcome::Continue;
                };
                let trusted = Arc::clone(&resources.trusted_network);
                let invalid = Arc::clone(&resources.signed_invalid);
                let payload = payload.to_vec();
                let Ok(verification) = tokio::task::spawn_blocking(move || {
                    let _verification_permit = verification_permit;
                    if trusted.tick_signature_is_valid(&payload, context) {
                        replay_reservation.commit();
                        trusted.apply_tick(context, &payload)
                    } else {
                        drop(replay_reservation);
                        commit_dedup(&invalid, digest);
                        TickVerification::BadSignature
                    }
                })
                .await
                else {
                    return FrameProcessingOutcome::Continue;
                };
                verification
            }
        };
        match verification {
            TickVerification::Malformed
            | TickVerification::BadSignature
            | TickVerification::Equivocation => {
                return protocol_violation(source_peer_id, &resources).await;
            }
            TickVerification::Quorum(status) => {
                update_latest_epoch_tick(&resources.latest_epoch_tick, status.epoch, status.tick);
            }
            TickVerification::Deferred
            | TickVerification::AuthenticatedStale
            | TickVerification::Duplicate
            | TickVerification::Accepted => {}
        }
    }

    if message_type == EXCHANGE_PUBLIC_PEERS_TYPE {
        let source_ip = {
            let locked = resources.state.lock().await;
            locked.remote_ip(source_peer_id)
        };
        if let Some(source_ip) = source_ip {
            update_discovered_peers(
                &resources.state,
                &frame,
                resources.config.peer_port,
                source_ip,
            )
            .await;
        }
        return FrameProcessingOutcome::Continue;
    }

    if matches!(
        message_type,
        ORACLE_MACHINE_QUERY_TYPE | ORACLE_MACHINE_REPLY_TYPE | OC_MACHINE_INVOCATION_TYPE
    ) {
        return FrameProcessingOutcome::Continue;
    }

    if message_type == BROADCAST_TRANSACTION_TYPE {
        let Ok(payload) = frame_payload(&frame) else {
            return FrameProcessingOutcome::Continue;
        };
        if parse_transaction_layout(payload, None).is_err() {
            return FrameProcessingOutcome::Continue;
        }
    }

    if !resources.config.relay_all && dejavu != 0 {
        return FrameProcessingOutcome::Continue;
    }

    let digest = *blake3::hash(&frame).as_bytes();
    let Ok(reservation) = resources.dedup.reserve(digest) else {
        if resources.config.traffic_log {
            let (size, message_type, dejavu) = frame_meta(&frame);
            println!(
                "DROP_DUP peer_id={} size={} type={}({}) dejavu={} | {}",
                source_peer_id,
                size,
                message_type_name(message_type),
                message_type,
                dejavu,
                format_epoch_tick_packed(resources.latest_epoch_tick.load(Ordering::Relaxed))
            );
        }
        return FrameProcessingOutcome::Continue;
    };

    let locked = resources.state.lock().await;
    let targets = locked.collect_targets(source_peer_id, DISSEMINATION_MULTIPLIER);
    drop(locked);

    let result = dispatch_frame(targets, &frame, &resources.outbound_budget);
    disconnect_failed_targets(&resources.state, &result).await;
    if result.sent_count > 0 {
        reservation.commit();
    }

    if resources.config.traffic_log {
        let (size, message_type, dejavu) = frame_meta(&frame);
        println!(
            "RELAY peer_id={} -> {} peers | size={} type={}({}) dejavu={} | {}",
            source_peer_id,
            result.sent_count,
            size,
            message_type_name(message_type),
            message_type,
            dejavu,
            format_epoch_tick_packed(resources.latest_epoch_tick.load(Ordering::Relaxed))
        );
        if !result.full_peer_ids.is_empty() {
            println!(
                "DROP_BACKPRESSURE peer_id={} dropped={} size={} type={}({}) dejavu={}",
                source_peer_id,
                result.full_peer_ids.len(),
                size,
                message_type_name(message_type),
                message_type,
                dejavu
            );
        }
    }
    FrameProcessingOutcome::Continue
}
fn update_latest_epoch_tick(latest: &AtomicU64, epoch: u16, tick: u32) {
    let candidate = pack_epoch_tick(epoch, tick);
    let _ = latest.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        (candidate >= current).then_some(candidate)
    });
}

async fn update_discovered_peers(
    state: &Arc<Mutex<NodeState>>,
    frame: &[u8],
    peer_port: u16,
    source_ip: std::net::Ipv4Addr,
) {
    let mut locked = state.lock().await;
    add_discovered_peers(&mut locked, frame, peer_port, source_ip);
}

fn add_discovered_peers(
    locked: &mut NodeState,
    frame: &[u8],
    peer_port: u16,
    source_ip: std::net::Ipv4Addr,
) {
    for peer in parse_exchange_public_peers(frame, peer_port) {
        if peer.ip() == &source_ip {
            continue;
        }
        let _ = locked.add_gossip_peer(peer, source_ip);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DEFAULT_GRPC_PORT, DEFAULT_PORT};
    use crate::frame::{COMPUTORS_PUBLIC_KEYS_SIZE, REQUEST_ENTITY_TYPE};
    use pretty_assertions::assert_eq;
    use std::net::{Ipv4Addr, SocketAddr};

    fn peer(last_octet: u8) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, last_octet), DEFAULT_PORT)
    }

    fn test_config() -> Arc<Config> {
        Arc::new(Config {
            listen_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, DEFAULT_PORT),
            api_timeout: Duration::from_secs(1),
            grpc_listen_addr: SocketAddr::from(([127, 0, 0, 1], DEFAULT_GRPC_PORT)),
            grpc_enabled: true,
            peer_port: DEFAULT_PORT,
            target_outbound: 8,
            max_incoming: 32,
            max_seen: 1_000,
            max_known_peers: 1_000,
            reconnect_interval: Duration::from_secs(2),
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

    fn trusted_network() -> Arc<TrustedNetworkState> {
        Arc::new(TrustedNetworkState::default())
    }

    fn outbound_budget() -> Arc<Semaphore> {
        Arc::new(Semaphore::new(GLOBAL_OUTBOUND_QUEUE_BYTES))
    }

    fn network_resources(
        state: Arc<Mutex<NodeState>>,
        pending: Arc<PendingRequests>,
        latest_epoch_tick: Arc<AtomicU64>,
        config: Arc<Config>,
    ) -> NetworkResources {
        NetworkResources::new(
            state,
            Arc::new(DedupWindow::new(1_000)),
            pending,
            latest_epoch_tick,
            trusted_network(),
            outbound_budget(),
            config,
        )
    }

    #[test]
    fn frame_extraction_waits_for_partial_header_and_payload() {
        let expected = build_request_frame(REQUEST_ENTITY_TYPE, 7, &[1, 2, 3, 4])
            .expect("test frame should build");
        let mut buffered = BytesMut::new();
        buffered.extend_from_slice(&expected[..4]);
        assert_eq!(extract_frame(&mut buffered, 1024), Ok(None));

        buffered.extend_from_slice(&expected[4..10]);
        assert_eq!(extract_frame(&mut buffered, 1024), Ok(None));

        buffered.extend_from_slice(&expected[10..]);
        assert_eq!(
            extract_frame(&mut buffered, 1024),
            Ok(Some(Bytes::from(expected)))
        );
        assert!(buffered.is_empty());
    }

    #[test]
    fn frame_extraction_returns_multiple_frames_from_one_read() {
        let first = build_request_frame(REQUEST_ENTITY_TYPE, 1, &[1])
            .expect("first test frame should build");
        let second = build_request_frame(BROADCAST_TRANSACTION_TYPE, 0, &[2, 3])
            .expect("second test frame should build");
        let mut buffered =
            BytesMut::from([first.as_slice(), second.as_slice()].concat().as_slice());

        assert_eq!(
            extract_frame(&mut buffered, 1024),
            Ok(Some(Bytes::from(first)))
        );
        assert_eq!(
            extract_frame(&mut buffered, 1024),
            Ok(Some(Bytes::from(second)))
        );
        assert_eq!(extract_frame(&mut buffered, 1024), Ok(None));
    }

    #[test]
    fn frame_extraction_accepts_maximum_frame_size() {
        let mut buffered = BytesMut::zeroed(crate::frame::MAX_FRAME_SIZE);
        buffered[..3].copy_from_slice(&[0xff, 0xff, 0xff]);

        let frame = extract_frame(&mut buffered, crate::frame::MAX_FRAME_SIZE)
            .expect("maximum frame should be valid")
            .expect("maximum frame should be complete");

        assert_eq!(frame.len(), crate::frame::MAX_FRAME_SIZE);
        assert!(buffered.is_empty());
    }

    #[test]
    fn maximum_frame_and_next_partial_header_share_one_read() {
        let mut buffered = BytesMut::zeroed(crate::frame::MAX_FRAME_SIZE + 4);
        buffered[..3].copy_from_slice(&[0xff, 0xff, 0xff]);
        buffered[crate::frame::MAX_FRAME_SIZE..].copy_from_slice(&[8, 0, 0, 35]);

        let frame = extract_frame(&mut buffered, crate::frame::MAX_FRAME_SIZE)
            .expect("maximum frame should be valid")
            .expect("maximum frame should be complete");

        assert_eq!(frame.len(), crate::frame::MAX_FRAME_SIZE);
        assert_eq!(&buffered[..], &[8, 0, 0, 35]);
        assert_eq!(
            extract_frame(&mut buffered, crate::frame::MAX_FRAME_SIZE),
            Ok(None)
        );
    }

    #[test]
    fn frame_extraction_rejects_size_smaller_than_header() {
        let mut buffered = BytesMut::from(&[7, 0, 0, 0, 0, 0, 0, 0][..]);

        assert_eq!(
            extract_frame(&mut buffered, 1024),
            Err(FrameExtractError::MalformedSize(7))
        );
    }

    #[test]
    fn frame_extraction_distinguishes_local_ceiling_from_malformed_size() {
        let declared = crate::frame::MIN_OPERATIONAL_FRAME_BYTES + 1;
        let mut header = vec![0u8; HEADER_SIZE];
        header[..3].copy_from_slice(&[
            (declared & 0xff) as u8,
            ((declared >> 8) & 0xff) as u8,
            ((declared >> 16) & 0xff) as u8,
        ]);
        let mut buffered = BytesMut::from(header.as_slice());

        assert_eq!(
            extract_frame(&mut buffered, crate::frame::MIN_OPERATIONAL_FRAME_BYTES),
            Err(FrameExtractError::LocalPolicyLimit {
                declared,
                limit: crate::frame::MIN_OPERATIONAL_FRAME_BYTES,
            })
        );
    }

    #[test]
    fn incoming_ip_reservation_precedes_and_outlives_global_admission() {
        let reserved = Arc::new(StdMutex::new(HashSet::new()));
        let ip = Ipv4Addr::new(1, 2, 3, 4);
        let first = IncomingIpReservation::try_new(ip, &reserved).unwrap();

        assert!(IncomingIpReservation::try_new(ip, &reserved).is_none());
        assert!(IncomingIpReservation::try_new(Ipv4Addr::new(1, 2, 3, 5), &reserved).is_some());
        drop(first);
        assert!(IncomingIpReservation::try_new(ip, &reserved).is_some());
    }

    #[tokio::test]
    async fn unrelated_non_relay_frame_does_not_wait_for_state_lock() {
        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let _guard = state.lock().await;
        let frame = Bytes::from(
            build_request_frame(REQUEST_ENTITY_TYPE, 1, &[]).expect("test frame should build"),
        );

        tokio::time::timeout(
            Duration::from_millis(100),
            process_incoming_frame(
                1,
                frame,
                network_resources(
                    Arc::clone(&state),
                    Arc::new(PendingRequests::default()),
                    Arc::new(AtomicU64::new(0)),
                    test_config(),
                ),
            ),
        )
        .await
        .expect("unrelated response must not wait for NodeState");
    }

    #[tokio::test]
    async fn contract_function_response_is_routed_to_pending_request() {
        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let _guard = state.lock().await;
        let pending = Arc::new(PendingRequests::default());
        let (registration, mut receivers) = pending.register([1]);
        let dejavu = registration.dejavu();
        let mut receiver = receivers.remove(0).1;
        let frame = Bytes::from(
            build_request_frame(
                crate::frame::RESPOND_CONTRACT_FUNCTION_TYPE,
                dejavu,
                &[1, 2, 3],
            )
            .expect("contract response should build"),
        );

        tokio::time::timeout(
            Duration::from_millis(100),
            process_incoming_frame(
                1,
                frame.clone(),
                network_resources(
                    Arc::clone(&state),
                    Arc::clone(&pending),
                    Arc::new(AtomicU64::new(0)),
                    test_config(),
                ),
            ),
        )
        .await
        .expect("contract response must not wait for NodeState");

        let Some(crate::pending::PendingEvent::Frame(delivered)) = receiver.recv().await else {
            panic!("contract response should be delivered");
        };
        assert_eq!(delivered, frame);
    }

    #[tokio::test]
    async fn empty_computor_end_response_releases_route_and_retries_with_new_dejavu() {
        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let (tx, mut rx) = mpsc::channel(4);
        let (disconnect_tx, disconnect_rx) = watch::channel(false);
        let peer_id = state
            .lock()
            .await
            .register_session(peer(80), true, tx, disconnect_tx.clone(), DEFAULT_PORT)
            .unwrap();
        let pending = Arc::new(PendingRequests::default());
        let mut config = (*test_config()).clone();
        config.reconnect_interval = Duration::from_millis(20);
        config.peer_frame_timeout = Duration::from_millis(100);
        let resources = network_resources(
            state,
            Arc::clone(&pending),
            Arc::new(AtomicU64::new(0)),
            Arc::new(config),
        );
        let task = tokio::spawn(computor_bootstrap(
            peer_id,
            peer(80),
            disconnect_rx,
            resources,
        ));

        let first = rx.recv().await.unwrap().bytes;
        let first_dejavu = u32::from_le_bytes(first[4..8].try_into().unwrap());
        let end = Bytes::from(build_request_frame(END_RESPONSE_TYPE, first_dejavu, &[]).unwrap());
        assert_eq!(
            pending.deliver(peer_id, first_dejavu, end),
            DeliveryOutcome::PassThrough
        );
        let second = tokio::time::timeout(Duration::from_millis(250), rx.recv())
            .await
            .expect("empty computor response should schedule retry")
            .unwrap()
            .bytes;
        let second_dejavu = u32::from_le_bytes(second[4..8].try_into().unwrap());
        assert_ne!(first_dejavu, second_dejavu);

        disconnect_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_millis(250), task)
            .await
            .expect("bootstrap task should observe session cancellation")
            .unwrap();
    }

    #[tokio::test]
    async fn unverified_tick_does_not_update_atomic_cache() {
        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let _guard = state.lock().await;
        let latest = Arc::new(AtomicU64::new(0));
        let mut payload = [0; crate::frame::BROADCAST_TICK_PAYLOAD_SIZE];
        payload[2..4].copy_from_slice(&7u16.to_le_bytes());
        payload[4..8].copy_from_slice(&123u32.to_le_bytes());
        payload[13] = 1;
        payload[14] = 1;
        payload[15] = 26;
        let frame = Bytes::from(
            build_request_frame(crate::frame::BROADCAST_TICK_TYPE, 1, &payload)
                .expect("tick frame should build"),
        );

        tokio::time::timeout(
            Duration::from_millis(100),
            process_incoming_frame(
                1,
                frame,
                network_resources(
                    Arc::clone(&state),
                    Arc::new(PendingRequests::default()),
                    Arc::clone(&latest),
                    test_config(),
                ),
            ),
        )
        .await
        .expect("tick response must not wait for NodeState");

        assert_eq!(latest.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn peer_exchange_updates_known_peer_state() {
        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let frame = Bytes::from(build_exchange_public_peers_frame([
            Ipv4Addr::new(1, 1, 1, 1),
            Ipv4Addr::UNSPECIFIED,
            Ipv4Addr::UNSPECIFIED,
            Ipv4Addr::UNSPECIFIED,
        ]));

        {
            let mut locked = state.lock().await;
            add_discovered_peers(&mut locked, &frame, DEFAULT_PORT, Ipv4Addr::new(9, 9, 9, 9));
        }

        assert_eq!(state.lock().await.pool_stats(Instant::now()).known, 1);
    }

    #[tokio::test]
    async fn late_api_response_is_not_relayed_in_relay_all_mode() {
        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let (target_tx, mut target_rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        state
            .lock()
            .await
            .register_session(peer(20), true, target_tx, disconnect_tx, DEFAULT_PORT)
            .unwrap();
        let pending = Arc::new(PendingRequests::default());
        let (registration, _receivers) = pending.register([999]);
        let dejavu = registration.dejavu();
        drop(registration);
        let mut config = (*test_config()).clone();
        config.relay_all = true;
        let frame = Bytes::from(
            build_request_frame(crate::frame::RESPOND_ENTITY_TYPE, dejavu, &[]).unwrap(),
        );

        process_incoming_frame(
            999,
            frame,
            network_resources(
                Arc::clone(&state),
                pending,
                Arc::new(AtomicU64::new(0)),
                Arc::new(config),
            ),
        )
        .await;

        assert!(target_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn fatal_frame_stops_processing_later_frames_from_same_read() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (server, remote) = listener.accept().await.unwrap();
        let SocketAddr::V4(remote) = remote else {
            panic!("test listener must be IPv4");
        };
        let (reader, _writer) = server.into_split();

        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let (source_tx, _source_rx) = mpsc::channel(2);
        let (target_tx, mut target_rx) = mpsc::channel(2);
        let (source_disconnect_tx, _source_disconnect_rx) = watch::channel(false);
        let (target_disconnect_tx, _target_disconnect_rx) = watch::channel(false);
        let source_peer_id = {
            let mut locked = state.lock().await;
            let source = locked
                .register_session(
                    peer(70),
                    true,
                    source_tx,
                    source_disconnect_tx,
                    DEFAULT_PORT,
                )
                .unwrap();
            locked
                .register_session(
                    peer(71),
                    true,
                    target_tx,
                    target_disconnect_tx,
                    DEFAULT_PORT,
                )
                .unwrap();
            source
        };
        let pending = Arc::new(PendingRequests::default());
        let (registration, _receiver) = pending.register_control(
            source_peer_id,
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
        let malformed =
            build_request_frame(END_RESPONSE_TYPE, registration.dejavu(), &[1]).unwrap();
        let relayable = build_request_frame(1, 0, &[9]).unwrap();
        client
            .write_all(&[malformed, relayable].concat())
            .await
            .unwrap();

        let error = read_peer_frames(
            reader,
            source_peer_id,
            remote,
            network_resources(
                Arc::clone(&state),
                Arc::clone(&pending),
                Arc::new(AtomicU64::new(0)),
                test_config(),
            ),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, PeerReadError::Protocol(_)));
        assert!(target_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn relay_filters_handshake_internal_channels_and_malformed_transactions() {
        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let (target_tx, mut target_rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        state
            .lock()
            .await
            .register_session(peer(20), true, target_tx, disconnect_tx, DEFAULT_PORT)
            .unwrap();
        let mut config = (*test_config()).clone();
        config.relay_all = true;
        let config = Arc::new(config);
        let dedup = Arc::new(DedupWindow::new(1_000));
        let pending = Arc::new(PendingRequests::default());
        let latest = Arc::new(AtomicU64::new(0));
        let resources = NetworkResources::new(
            Arc::clone(&state),
            Arc::clone(&dedup),
            Arc::clone(&pending),
            Arc::clone(&latest),
            trusted_network(),
            outbound_budget(),
            Arc::clone(&config),
        );

        for message_type in [
            ORACLE_MACHINE_QUERY_TYPE,
            ORACLE_MACHINE_REPLY_TYPE,
            OC_MACHINE_INVOCATION_TYPE,
        ] {
            process_incoming_frame(
                999,
                Bytes::from(build_request_frame(message_type, 0, &[1]).unwrap()),
                resources.clone(),
            )
            .await;
        }

        let exchange_frame = build_exchange_public_peers_frame([
            Ipv4Addr::new(2, 2, 2, 2),
            Ipv4Addr::UNSPECIFIED,
            Ipv4Addr::UNSPECIFIED,
            Ipv4Addr::UNSPECIFIED,
        ]);
        let mut locked = state.lock().await;
        add_discovered_peers(
            &mut locked,
            &exchange_frame,
            DEFAULT_PORT,
            Ipv4Addr::new(9, 9, 9, 9),
        );
        drop(locked);

        process_incoming_frame(
            999,
            Bytes::from(build_request_frame(BROADCAST_TRANSACTION_TYPE, 0, &[1, 2, 3]).unwrap()),
            resources,
        )
        .await;

        assert!(target_rx.try_recv().is_err());
        assert_eq!(state.lock().await.pool_stats(Instant::now()).known, 2);
    }

    #[test]
    fn incoming_connections_do_not_suppress_emergency_dns() {
        let mut state = NodeState::new(10, &[]);
        let (tx, _rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        state
            .register_session(peer(1), false, tx, disconnect_tx, DEFAULT_PORT)
            .expect("incoming peer should connect");

        assert_eq!(state.incoming_count(), 1);
        assert_eq!(state.outgoing_count(), 0);
        assert!(emergency_dns_needed(state.outgoing_count(), 1, true, true));
    }

    #[test]
    fn empty_and_duplicate_dns_results_back_off_but_new_peer_recovers() {
        let existing = peer(2);
        let discovered = peer(3);
        let mut state = NodeState::new(10, &[existing]);
        let mut attempt = 0;

        assert_eq!(
            apply_emergency_dns_result(&mut state, Vec::new(), &mut attempt),
            0
        );
        assert_eq!(attempt, 1);
        assert_eq!(
            calculate_backoff(attempt, 1_000, 10_000),
            Duration::from_secs(1)
        );

        assert_eq!(
            apply_emergency_dns_result(&mut state, vec![existing], &mut attempt),
            0
        );
        assert_eq!(attempt, 2);
        assert_eq!(
            calculate_backoff(attempt, 1_000, 10_000),
            Duration::from_secs(2)
        );

        assert_eq!(
            apply_emergency_dns_result(&mut state, vec![discovered], &mut attempt),
            1
        );
        assert_eq!(attempt, 0);
        assert_eq!(
            calculate_backoff(attempt, 1_000, 10_000),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn dns_promotion_counts_as_recovery_progress() {
        let target = peer(4);
        let mut state = NodeState::new(10, &[]);
        let _ = state.add_gossip_peer(target, Ipv4Addr::new(9, 9, 9, 9));
        let mut attempt = 3;

        assert_eq!(
            apply_emergency_dns_result(&mut state, vec![target], &mut attempt),
            1
        );
        assert_eq!(attempt, 0);
    }

    #[test]
    fn recovery_replaces_stale_absolute_dns_deadline() {
        let now = Instant::now();
        let mut attempt = 7;
        let mut next = now + Duration::from_secs(5 * 60);

        reset_emergency_dns_backoff(&mut attempt, &mut next, now, 10_000);

        assert_eq!(attempt, 0);
        assert_eq!(next, now + Duration::from_secs(10));
    }

    #[tokio::test]
    async fn global_outbound_pressure_does_not_disconnect_a_healthy_peer() {
        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let (tx, _rx) = mpsc::channel(1);
        let (disconnect_tx, disconnect_rx) = watch::channel(false);
        state
            .lock()
            .await
            .register_session(peer(5), true, tx, disconnect_tx, DEFAULT_PORT)
            .expect("peer should register");
        let transaction =
            vec![0; crate::frame::TRANSACTION_BASE_SIZE + crate::frame::SIGNATURE_SIZE];

        let result = broadcast_transaction_to_network(
            Arc::clone(&state),
            Arc::new(DedupWindow::new(1_000)),
            Arc::new(Semaphore::new(0)),
            &transaction,
        )
        .await;

        assert_eq!(
            result,
            Err("Failed to broadcast transaction: global outbound queue is overloaded".to_string())
        );
        assert_eq!(state.lock().await.outgoing_count(), 1);
        assert!(disconnect_rx.has_changed().is_ok_and(|changed| !changed));
    }

    #[tokio::test]
    async fn saturated_signature_gate_defers_without_queueing_blocking_work() {
        let resources = network_resources(
            Arc::new(Mutex::new(NodeState::new(10, &[]))),
            Arc::new(PendingRequests::default()),
            Arc::new(AtomicU64::new(0)),
            test_config(),
        );
        let permit_count = resources.verification_slots.available_permits() as u32;
        let _all_permits = Arc::clone(&resources.verification_slots)
            .try_acquire_many_owned(permit_count)
            .expect("test should saturate the verification gate");
        let mut payload = vec![0; BROADCAST_COMPUTORS_PAYLOAD_SIZE];
        payload[2..2 + COMPUTORS_PUBLIC_KEYS_SIZE].fill(1);
        let digest = computor_verification_digest(&payload);
        let frame = Bytes::from(
            build_request_frame(BROADCAST_COMPUTORS_TYPE, 0, &payload)
                .expect("test computor frame should build"),
        );

        let verification = tokio::spawn(process_incoming_frame(1, frame, resources.clone()));
        tokio::task::yield_now().await;
        assert!(verification.is_finished());
        drop(_all_permits);
        verification.await.expect("verification task should finish");

        assert!(!resources.signed_invalid.contains(&digest));
    }

    #[tokio::test]
    async fn invalid_signed_frame_replay_is_committed_after_one_verification() {
        let resources = network_resources(
            Arc::new(Mutex::new(NodeState::new(10, &[]))),
            Arc::new(PendingRequests::default()),
            Arc::new(AtomicU64::new(0)),
            test_config(),
        );
        let mut payload = vec![0; BROADCAST_COMPUTORS_PAYLOAD_SIZE];
        payload[2..2 + COMPUTORS_PUBLIC_KEYS_SIZE].fill(1);
        let digest = computor_verification_digest(&payload);
        let frame = Bytes::from(
            build_request_frame(BROADCAST_COMPUTORS_TYPE, 0, &payload)
                .expect("test computor frame should build"),
        );

        process_incoming_frame(1, frame.clone(), resources.clone()).await;
        assert!(resources.signed_invalid.contains(&digest));
        process_incoming_frame(1, frame, resources).await;
    }

    #[tokio::test]
    async fn cancelled_async_waiter_does_not_release_blocking_verification_guards() {
        let semaphore = Arc::new(Semaphore::new(1));
        let dedup = Arc::new(DedupWindow::new(1_000));
        let digest = [9; 32];
        let reservation = dedup
            .reserve(digest)
            .expect("test reservation should succeed");
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .expect("test semaphore should be open");
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let blocking = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            started_tx.send(()).expect("test should report start");
            release_rx.recv().expect("test should release verification");
            reservation.commit();
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking verification should start");
        drop(blocking);

        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(
            dedup.reserve(digest).unwrap_err(),
            DedupReservationError::InFlight
        );
        release_tx
            .send(())
            .expect("test should release verification");
        tokio::time::timeout(Duration::from_secs(1), async {
            while semaphore.available_permits() == 0 || !dedup.contains(&digest) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking guards should complete independently of the waiter");
    }

    #[tokio::test]
    async fn cached_valid_signature_still_obeys_each_frames_relay_header() {
        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let (source_tx, _source_rx) = mpsc::channel(2);
        let (target_tx, mut target_rx) = mpsc::channel(2);
        let (source_disconnect_tx, _source_disconnect_rx) = watch::channel(false);
        let (target_disconnect_tx, _target_disconnect_rx) = watch::channel(false);
        let source_peer_id = {
            let mut locked = state.lock().await;
            let source_peer_id = locked
                .register_session(
                    peer(30),
                    true,
                    source_tx,
                    source_disconnect_tx,
                    DEFAULT_PORT,
                )
                .expect("source peer should register");
            locked
                .register_session(
                    peer(31),
                    true,
                    target_tx,
                    target_disconnect_tx,
                    DEFAULT_PORT,
                )
                .expect("target peer should register");
            source_peer_id
        };
        let resources = network_resources(
            state,
            Arc::new(PendingRequests::default()),
            Arc::new(AtomicU64::new(0)),
            test_config(),
        );
        let mut payload = vec![0; BROADCAST_COMPUTORS_PAYLOAD_SIZE];
        payload[2..2 + COMPUTORS_PUBLIC_KEYS_SIZE].fill(1);
        let digest = computor_verification_digest(&payload);
        resources
            .signed_replay
            .reserve(digest)
            .expect("test signature cache should be empty")
            .commit();
        let non_relayable = Bytes::from(
            build_request_frame(BROADCAST_COMPUTORS_TYPE, 7, &payload)
                .expect("test frame should build"),
        );
        let relayable = Bytes::from(
            build_request_frame(BROADCAST_COMPUTORS_TYPE, 0, &payload)
                .expect("test frame should build"),
        );

        process_incoming_frame(source_peer_id, non_relayable, resources.clone()).await;
        assert!(matches!(
            target_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        process_incoming_frame(source_peer_id, relayable.clone(), resources).await;
        assert_eq!(
            target_rx
                .recv()
                .await
                .expect("relayable replay should reach the target")
                .bytes,
            relayable
        );
    }

    #[tokio::test]
    async fn tcp_reset_unregisters_outbound_session_and_allows_replacement() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("test listener should bind");
        let listener_addr = listener.local_addr().expect("listener should have address");
        let (worker_stream, reset_stream) = tokio::join!(
            async {
                TcpStream::connect(listener_addr)
                    .await
                    .expect("worker stream should connect")
            },
            async {
                listener
                    .accept()
                    .await
                    .expect("test listener should accept")
                    .0
            }
        );

        let remote = peer(10);
        let state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let (tx, rx) = mpsc::channel(1);
        let (disconnect_tx, disconnect_rx) = watch::channel(false);
        let peer_id = state
            .lock()
            .await
            .register_session(remote, true, tx, disconnect_tx, DEFAULT_PORT)
            .expect("outbound session should register");
        let worker = tokio::spawn(connection_worker(
            worker_stream,
            rx,
            disconnect_rx,
            ConnectionContext {
                peer_id,
                remote,
                outbound: true,
                _incoming_permit: None,
                _incoming_ip_reservation: None,
                resources: network_resources(
                    Arc::clone(&state),
                    Arc::new(PendingRequests::default()),
                    Arc::new(AtomicU64::new(0)),
                    test_config(),
                ),
            },
        ));

        socket2::SockRef::from(&reset_stream)
            .set_linger(Some(Duration::ZERO))
            .expect("test peer should configure reset-on-close");
        drop(reset_stream);
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("connection worker should observe reset")
            .expect("connection worker task should finish");

        let mut locked = state.lock().await;
        assert_eq!(locked.outgoing_count(), 0);
        let (replacement_tx, _replacement_rx) = mpsc::channel(1);
        let (replacement_disconnect_tx, _replacement_disconnect_rx) = watch::channel(false);
        assert!(
            locked
                .register_session(
                    remote,
                    true,
                    replacement_tx,
                    replacement_disconnect_tx,
                    DEFAULT_PORT,
                )
                .is_some()
        );
    }

    #[tokio::test]
    async fn stalled_peer_write_times_out() {
        let (mut writer, _reader) = tokio::io::duplex(1);
        let (tx, rx) = mpsc::channel(1);
        tx.send(
            OutboundFrame::try_new(
                Bytes::from(vec![0; 1_024]),
                Arc::new(Semaphore::new(crate::state::PEER_OUTBOUND_QUEUE_BYTES)),
                outbound_budget(),
            )
            .expect("test budgets should accept a frame"),
        )
        .await
        .expect("test frame should queue");

        let result = write_peer_frames(
            &mut writer,
            rx,
            peer(11),
            false,
            Arc::new(AtomicU64::new(0)),
            Duration::from_millis(20),
        )
        .await;

        assert_eq!(result, Err("write timed out after 20 ms".to_string()));
    }

    #[tokio::test]
    async fn partial_header_is_subject_to_frame_timeout() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (server, remote) = listener.accept().await.unwrap();
        let SocketAddr::V4(remote) = remote else {
            panic!("test listener must be IPv4");
        };
        let (reader, _writer) = server.into_split();
        let mut config = (*test_config()).clone();
        config.peer_frame_timeout = Duration::from_millis(25);
        let resources = network_resources(
            Arc::new(Mutex::new(NodeState::new(10, &[]))),
            Arc::new(PendingRequests::default()),
            Arc::new(AtomicU64::new(0)),
            Arc::new(config),
        );

        client.write_all(&[1]).await.unwrap();
        let err = tokio::time::timeout(
            Duration::from_millis(250),
            read_peer_frames(reader, 1, remote, resources),
        )
        .await
        .expect("partial header should not wait forever")
        .unwrap_err();

        assert!(
            matches!(err, PeerReadError::Io(message) if message.contains("frame read timed out"))
        );
    }

    #[tokio::test]
    async fn local_frame_ceiling_disconnect_does_not_cool_peer() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (server, _remote) = listener.accept().await.unwrap();
        let remote = peer(91);
        let state = Arc::new(Mutex::new(NodeState::new(10, &[remote])));
        let (tx, rx) = mpsc::channel(2);
        let (disconnect_tx, disconnect_rx) = watch::channel(false);
        let peer_id = state
            .lock()
            .await
            .register_session(remote, true, tx, disconnect_tx, DEFAULT_PORT)
            .unwrap();
        let mut config = (*test_config()).clone();
        config.max_frame_bytes = crate::frame::MIN_OPERATIONAL_FRAME_BYTES;
        let worker = tokio::spawn(connection_worker(
            server,
            rx,
            disconnect_rx,
            ConnectionContext {
                peer_id,
                remote,
                outbound: true,
                _incoming_permit: None,
                _incoming_ip_reservation: None,
                resources: network_resources(
                    Arc::clone(&state),
                    Arc::new(PendingRequests::default()),
                    Arc::new(AtomicU64::new(0)),
                    Arc::new(config),
                ),
            },
        ));
        let declared = crate::frame::MIN_OPERATIONAL_FRAME_BYTES + 1;
        let mut header = [0u8; HEADER_SIZE];
        header[..3].copy_from_slice(&[
            (declared & 0xff) as u8,
            ((declared >> 8) & 0xff) as u8,
            ((declared >> 16) & 0xff) as u8,
        ]);
        client.write_all(&header).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("local ceiling should stop the connection")
            .unwrap();

        let locked = state.lock().await;
        assert_eq!(locked.outgoing_count(), 0);
        assert_eq!(locked.pool_stats(Instant::now()).cooldown, 0);
    }

    #[tokio::test]
    async fn explicit_disconnect_unregisters_session_without_waiting_for_writer() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("test listener should bind");
        let listener_addr = listener.local_addr().expect("listener should have address");
        let (worker_stream, _peer_stream) = tokio::join!(
            async {
                TcpStream::connect(listener_addr)
                    .await
                    .expect("worker stream should connect")
            },
            async {
                listener
                    .accept()
                    .await
                    .expect("test listener should accept")
                    .0
            }
        );

        let remote = peer(12);
        let state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let (tx, rx) = mpsc::channel(1);
        let (disconnect_tx, disconnect_rx) = watch::channel(false);
        let peer_id = state
            .lock()
            .await
            .register_session(remote, true, tx, disconnect_tx.clone(), DEFAULT_PORT)
            .expect("outbound session should register");
        let worker = tokio::spawn(connection_worker(
            worker_stream,
            rx,
            disconnect_rx,
            ConnectionContext {
                peer_id,
                remote,
                outbound: true,
                _incoming_permit: None,
                _incoming_ip_reservation: None,
                resources: network_resources(
                    Arc::clone(&state),
                    Arc::new(PendingRequests::default()),
                    Arc::new(AtomicU64::new(0)),
                    test_config(),
                ),
            },
        ));

        disconnect_tx
            .send(true)
            .expect("disconnect request should send");
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker should stop immediately")
            .expect("worker task should finish");

        assert_eq!(state.lock().await.outgoing_count(), 0);
    }

    #[tokio::test]
    async fn peer_fault_disconnect_enters_reconnect_cooldown() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("test listener should bind");
        let listener_addr = listener.local_addr().expect("listener should have address");
        let (worker_stream, _peer_stream) = tokio::join!(
            async {
                TcpStream::connect(listener_addr)
                    .await
                    .expect("worker stream should connect")
            },
            async {
                listener
                    .accept()
                    .await
                    .expect("test listener should accept")
                    .0
            }
        );
        let remote = peer(40);
        let state = Arc::new(Mutex::new(NodeState::new(1_000, &[])));
        let (tx, rx) = mpsc::channel(1);
        let (disconnect_tx, disconnect_rx) = watch::channel(false);
        let peer_id = state
            .lock()
            .await
            .register_session(remote, true, tx, disconnect_tx, DEFAULT_PORT)
            .expect("outbound session should register");
        let worker = tokio::spawn(connection_worker(
            worker_stream,
            rx,
            disconnect_rx,
            ConnectionContext {
                peer_id,
                remote,
                outbound: true,
                _incoming_permit: None,
                _incoming_ip_reservation: None,
                resources: network_resources(
                    Arc::clone(&state),
                    Arc::new(PendingRequests::default()),
                    Arc::new(AtomicU64::new(0)),
                    test_config(),
                ),
            },
        ));
        state
            .lock()
            .await
            .disconnect_session_with_reason(peer_id, DisconnectReason::ProtocolViolation);
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker should observe the disconnect")
            .expect("worker task should finish");

        assert_eq!(state.lock().await.pool_stats(Instant::now()).cooldown, 1);
    }
}
