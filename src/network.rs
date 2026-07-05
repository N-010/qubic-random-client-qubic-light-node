use crate::config::Config;
use crate::frame::{
    BROADCAST_TRANSACTION_TYPE, EXCHANGE_PUBLIC_PEERS_TYPE, HEADER_SIZE, MAX_FRAME_SIZE,
    NUMBER_OF_TRANSACTIONS_PER_TICK, build_exchange_public_peers_frame, build_request_frame,
    decode_frame_size, frame_meta, message_type_name, parse_exchange_public_peers,
    parse_tick_status_from_frame,
};
use crate::pending::PendingRequests;
use crate::state::{DedupWindow, NodeState, PeerPoolStats, RelayTarget};
use crate::types::{format_epoch_tick_packed, pack_epoch_tick};
use bytes::{Bytes, BytesMut};
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::time::{sleep, timeout};

const OUTBOUND_QUEUE_CAPACITY: usize = NUMBER_OF_TRANSACTIONS_PER_TICK * 2;
const DISSEMINATION_MULTIPLIER: usize = 6;
const READ_BUFFER_SIZE: usize = 128 * 1024;
const ACCUMULATED_INITIAL_CAPACITY: usize = 256 * 1024;
const ACCUMULATED_RETAIN_CAPACITY: usize = 512 * 1024;
const ACCUMULATED_SHRINK_THRESHOLD: usize = 2 * 1024 * 1024;
const MAX_ACCUMULATED_BUFFER: usize = MAX_FRAME_SIZE * 2;

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
        .filter(|peer| state.add_discovered_peer(*peer))
        .count();
    if added_count == 0 {
        *attempt = attempt.saturating_add(1);
    } else {
        *attempt = 0;
    }
    added_count
}

#[derive(Debug, Default)]
struct DispatchResult {
    sent_count: usize,
    full_peer_ids: Vec<u64>,
    closed_peer_ids: Vec<u64>,
}

#[derive(Clone)]
struct NetworkResources {
    state: Arc<Mutex<NodeState>>,
    dedup: Arc<DedupWindow>,
    pending: Arc<PendingRequests>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
}

struct ConnectionContext {
    peer_id: u64,
    remote: SocketAddrV4,
    resources: NetworkResources,
}

fn dispatch_frame(targets: Vec<RelayTarget>, frame: &Bytes) -> DispatchResult {
    let mut result = DispatchResult::default();

    for target in targets {
        match target.tx.try_send(frame.clone()) {
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
        if let Some(remote) = locked.disconnect_session(*peer_id) {
            eprintln!("Disconnecting {remote}: outbound queue is full");
        }
    }
    for peer_id in &result.closed_peer_ids {
        let _ = locked.disconnect_session(*peer_id);
    }
}

pub(crate) async fn broadcast_transaction_to_network(
    state: Arc<Mutex<NodeState>>,
    dedup: Arc<DedupWindow>,
    tx_bytes: &[u8],
) -> Result<(), String> {
    if tx_bytes.is_empty() {
        return Err("Transaction payload is empty".to_string());
    }

    let frame = build_request_frame(BROADCAST_TRANSACTION_TYPE, 0, tx_bytes)?;
    let digest = *blake3::hash(&frame).as_bytes();

    let targets = {
        let locked = state.lock().await;
        let targets = locked.collect_all_targets(DISSEMINATION_MULTIPLIER);
        if targets.is_empty() {
            return Err("No connected peers available for broadcast".to_string());
        }
        if dedup.contains(&digest) {
            return Ok(());
        }
        targets
    };

    let frame = Bytes::from(frame);
    let result = dispatch_frame(targets, &frame);
    disconnect_failed_targets(&state, &result).await;

    if result.sent_count == 0 {
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

    let _ = dedup.mark_seen(digest);

    Ok(())
}

pub(crate) async fn accept_loop(
    listener: TcpListener,
    state: Arc<Mutex<NodeState>>,
    dedup: Arc<DedupWindow>,
    pending: Arc<PendingRequests>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
    let resources = NetworkResources {
        state: Arc::clone(&state),
        dedup: Arc::clone(&dedup),
        pending: Arc::clone(&pending),
        latest_epoch_tick: Arc::clone(&latest_epoch_tick),
        config: Arc::clone(&config),
    };
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
                if let Err(err) = configure_tcp_keepalive(&stream) {
                    eprintln!("Failed to set TCP keepalive for {remote_v4}: {err}");
                }

                let connection_resources = resources.clone();
                tokio::spawn(async move {
                    establish_connection(stream, remote_v4, false, connection_resources).await;
                });
            }
            Err(err) => {
                eprintln!("Accept failed: {err}");
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

pub(crate) async fn dial_loop(
    state: Arc<Mutex<NodeState>>,
    dedup: Arc<DedupWindow>,
    pending: Arc<PendingRequests>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
    let resources = NetworkResources {
        state: Arc::clone(&state),
        dedup: Arc::clone(&dedup),
        pending: Arc::clone(&pending),
        latest_epoch_tick: Arc::clone(&latest_epoch_tick),
        config: Arc::clone(&config),
    };
    let mut emergency_dns_attempt = 0u32;
    let mut last_emergency_dns = Instant::now()
        .checked_sub(Duration::from_secs(3600))
        .unwrap_or_else(Instant::now);
    let mut last_pool_stats = None;

    loop {
        // Проверить здоровье пиров и запустить аварийный DNS при необходимости
        let current_outbound = {
            let locked = state.lock().await;
            locked.outgoing_count()
        };

        // Логика аварийного DNS bootstrap
        if emergency_dns_needed(
            current_outbound,
            config.critical_peer_threshold,
            config.emergency_dns_bootstrap,
            config.dns_bootstrap,
        ) {
            let backoff_duration = calculate_backoff(
                emergency_dns_attempt,
                config.emergency_dns_backoff_initial_ms,
                config.emergency_dns_backoff_max_ms,
            );

            if last_emergency_dns.elapsed() >= backoff_duration {
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
                            eprintln!("Emergency DNS bootstrap returned no new peers.");
                        } else {
                            println!(
                                "Emergency DNS bootstrap: discovered {added_count} new peers."
                            );

                            // Сбросить backoff при успехе
                        }
                        last_emergency_dns = Instant::now();
                    }
                    Err(err) => {
                        eprintln!("Emergency DNS bootstrap failed: {err}");
                        emergency_dns_attempt = emergency_dns_attempt.saturating_add(1);
                        last_emergency_dns = Instant::now();
                    }
                }
            }
        } else if current_outbound >= config.critical_peer_threshold {
            // Сбросить состояние аварии когда восстановились
            if emergency_dns_attempt > 0 {
                println!(
                    "Outbound peer count recovered to {current_outbound}. Resetting emergency DNS backoff."
                );
                emergency_dns_attempt = 0;
            }
        }

        // Обычная логика подключения
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

        let pool_stats = state.lock().await.pool_stats(Instant::now());
        if last_pool_stats != Some(pool_stats) {
            log_pool_stats(pool_stats, config.target_outbound);
            last_pool_stats = Some(pool_stats);
        }

        for target in targets {
            let resources_for_task = resources.clone();
            tokio::spawn(async move {
                match TcpStream::connect(target).await {
                    Ok(stream) => {
                        if let Err(err) = stream.set_nodelay(true) {
                            eprintln!("Failed to set TCP_NODELAY for {target}: {err}");
                        }
                        if let Err(err) = configure_tcp_keepalive(&stream) {
                            eprintln!("Failed to set TCP keepalive for {target}: {err}");
                        }
                        establish_connection(stream, target, true, resources_for_task).await;
                    }
                    Err(err) => {
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
    resources: NetworkResources,
) {
    let (tx, rx) = mpsc::channel::<Bytes>(OUTBOUND_QUEUE_CAPACITY);
    let (disconnect_tx, disconnect_rx) = watch::channel(false);
    let (peer_id, handshake_payload, incoming_count, outgoing_count) = {
        let mut locked = resources.state.lock().await;

        if !outbound && locked.incoming_count() >= resources.config.max_incoming {
            println!("Rejecting incoming {remote}: incoming limit reached.");
            locked.clear_pending_dial(remote);
            locked.clear_pending_dial(SocketAddrV4::new(*remote.ip(), resources.config.peer_port));
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
            locked.clear_pending_dial(SocketAddrV4::new(*remote.ip(), resources.config.peer_port));
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

    if tx.try_send(Bytes::from(handshake_payload)).is_err() {
        eprintln!("Failed to queue handshake frame for {remote}");
    }
    println!(
        "Connected {} [{}] | in={} out={} | {}",
        remote,
        if outbound { "out" } else { "in" },
        incoming_count,
        outgoing_count,
        format_epoch_tick_packed(resources.latest_epoch_tick.load(Ordering::Relaxed))
    );

    tokio::spawn(connection_worker(
        stream,
        rx,
        disconnect_rx,
        ConnectionContext {
            peer_id,
            remote,
            resources,
        },
    ));
}

async fn connection_worker(
    stream: TcpStream,
    rx: mpsc::Receiver<Bytes>,
    mut disconnect_rx: watch::Receiver<bool>,
    context: ConnectionContext,
) {
    let ConnectionContext {
        peer_id,
        remote,
        resources,
    } = context;
    let disconnect_reason = {
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

        tokio::select! {
            result = &mut writer_future => match result {
                Ok(()) => "outbound queue closed".to_string(),
                Err(err) => err,
            },
            result = &mut read_future => match result {
                Ok(()) => "reader stopped".to_string(),
                Err(err) => err,
            },
            result = disconnect_rx.changed() => match result {
                Ok(()) => "disconnect requested".to_string(),
                Err(err) => format!("disconnect channel closed: {err}"),
            },
        }
    };

    let (in_count, out_count) = {
        let mut locked = resources.state.lock().await;
        let _ = locked.unregister_session(peer_id);
        (locked.incoming_count(), locked.outgoing_count())
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
    mut rx: mpsc::Receiver<Bytes>,
    remote: SocketAddrV4,
    traffic_log: bool,
    latest_epoch_tick: Arc<AtomicU64>,
    peer_write_timeout: Duration,
) -> Result<(), String> {
    while let Some(frame) = rx.recv().await {
        if traffic_log {
            let (size, message_type, dejavu) = frame_meta(&frame);
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
        match timeout(peer_write_timeout, writer.write_all(&frame)).await {
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
) -> Result<(), String> {
    let mut accumulated = BytesMut::with_capacity(ACCUMULATED_INITIAL_CAPACITY);

    loop {
        accumulated.reserve(READ_BUFFER_SIZE);
        match reader.read_buf(&mut accumulated).await {
            Ok(0) => return Err("peer closed the connection".to_string()),
            Ok(_) => {
                if accumulated.len() > MAX_ACCUMULATED_BUFFER {
                    return Err(format!(
                        "buffered {} bytes without full parse (limit={MAX_ACCUMULATED_BUFFER})",
                        accumulated.len()
                    ));
                }
                while let Some(frame) = extract_frame(&mut accumulated)? {
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

                    process_incoming_frame(
                        peer_id,
                        frame,
                        Arc::clone(&resources.state),
                        Arc::clone(&resources.dedup),
                        Arc::clone(&resources.pending),
                        Arc::clone(&resources.latest_epoch_tick),
                        Arc::clone(&resources.config),
                    )
                    .await;
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
                return Err(format!("read failed: {err}"));
            }
        }
    }
}

fn extract_frame(accumulated: &mut BytesMut) -> Result<Option<Bytes>, String> {
    if accumulated.len() < HEADER_SIZE {
        return Ok(None);
    }
    let frame_size = decode_frame_size(&accumulated[..HEADER_SIZE]);
    if !(HEADER_SIZE..=MAX_FRAME_SIZE).contains(&frame_size) {
        return Err(format!("invalid frame size {frame_size}"));
    }
    if accumulated.len() < frame_size {
        return Ok(None);
    }
    Ok(Some(accumulated.split_to(frame_size).freeze()))
}

async fn process_incoming_frame(
    source_peer_id: u64,
    frame: Bytes,
    state: Arc<Mutex<NodeState>>,
    dedup: Arc<DedupWindow>,
    pending: Arc<PendingRequests>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
    if frame.len() < HEADER_SIZE {
        return;
    }

    let message_type = frame[3];
    let dejavu = u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]);

    if matches!(
        message_type,
        crate::frame::RESPOND_ENTITY_TYPE
            | BROADCAST_TRANSACTION_TYPE
            | crate::frame::END_RESPONSE_TYPE
    ) && pending.deliver(source_peer_id, dejavu, frame.clone())
    {
        return;
    }

    let tick_update = parse_tick_status_from_frame(&frame);
    if let Some(status) = tick_update {
        update_latest_epoch_tick(&latest_epoch_tick, status.epoch, status.tick);
    }

    if !config.relay_all && dejavu != 0 {
        if message_type == EXCHANGE_PUBLIC_PEERS_TYPE {
            update_discovered_peers(&state, &frame, config.peer_port).await;
        }
        return;
    }

    let digest = *blake3::hash(&frame).as_bytes();
    if !dedup.mark_seen(digest) {
        if config.traffic_log {
            let (size, message_type, dejavu) = frame_meta(&frame);
            println!(
                "DROP_DUP peer_id={} size={} type={}({}) dejavu={} | {}",
                source_peer_id,
                size,
                message_type_name(message_type),
                message_type,
                dejavu,
                format_epoch_tick_packed(latest_epoch_tick.load(Ordering::Relaxed))
            );
        }
        return;
    }

    let mut locked = state.lock().await;
    if message_type == EXCHANGE_PUBLIC_PEERS_TYPE {
        add_discovered_peers(&mut locked, &frame, config.peer_port);
    }
    let targets = locked.collect_targets(source_peer_id, DISSEMINATION_MULTIPLIER);
    drop(locked);

    let result = dispatch_frame(targets, &frame);
    disconnect_failed_targets(&state, &result).await;

    if config.traffic_log {
        let (size, message_type, dejavu) = frame_meta(&frame);
        println!(
            "RELAY peer_id={} -> {} peers | size={} type={}({}) dejavu={} | {}",
            source_peer_id,
            result.sent_count,
            size,
            message_type_name(message_type),
            message_type,
            dejavu,
            format_epoch_tick_packed(latest_epoch_tick.load(Ordering::Relaxed))
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
}

fn update_latest_epoch_tick(latest: &AtomicU64, epoch: u16, tick: u32) {
    let candidate = pack_epoch_tick(epoch, tick);
    let _ = latest.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        (candidate >= current).then_some(candidate)
    });
}

async fn update_discovered_peers(state: &Arc<Mutex<NodeState>>, frame: &[u8], peer_port: u16) {
    let mut locked = state.lock().await;
    add_discovered_peers(&mut locked, frame, peer_port);
}

fn add_discovered_peers(locked: &mut NodeState, frame: &[u8], peer_port: u16) {
    for peer in parse_exchange_public_peers(frame, peer_port) {
        let _ = locked.add_discovered_peer(peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DEFAULT_GRPC_PORT, DEFAULT_PORT};
    use crate::frame::REQUEST_ENTITY_TYPE;
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

    #[test]
    fn frame_extraction_waits_for_partial_header_and_payload() {
        let expected = build_request_frame(REQUEST_ENTITY_TYPE, 7, &[1, 2, 3, 4])
            .expect("test frame should build");
        let mut buffered = BytesMut::new();
        buffered.extend_from_slice(&expected[..4]);
        assert_eq!(extract_frame(&mut buffered), Ok(None));

        buffered.extend_from_slice(&expected[4..10]);
        assert_eq!(extract_frame(&mut buffered), Ok(None));

        buffered.extend_from_slice(&expected[10..]);
        assert_eq!(
            extract_frame(&mut buffered),
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

        assert_eq!(extract_frame(&mut buffered), Ok(Some(Bytes::from(first))));
        assert_eq!(extract_frame(&mut buffered), Ok(Some(Bytes::from(second))));
        assert_eq!(extract_frame(&mut buffered), Ok(None));
    }

    #[test]
    fn frame_extraction_accepts_maximum_frame_size() {
        let mut buffered = BytesMut::zeroed(MAX_FRAME_SIZE);
        buffered[..3].copy_from_slice(&[0xff, 0xff, 0xff]);

        let frame = extract_frame(&mut buffered)
            .expect("maximum frame should be valid")
            .expect("maximum frame should be complete");

        assert_eq!(frame.len(), MAX_FRAME_SIZE);
        assert!(buffered.is_empty());
    }

    #[test]
    fn frame_extraction_rejects_size_smaller_than_header() {
        let mut buffered = BytesMut::from(&[7, 0, 0, 0, 0, 0, 0, 0][..]);

        assert_eq!(
            extract_frame(&mut buffered),
            Err("invalid frame size 7".to_string())
        );
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
                Arc::clone(&state),
                Arc::new(DedupWindow::new(1_000)),
                Arc::new(PendingRequests::default()),
                Arc::new(AtomicU64::new(0)),
                test_config(),
            ),
        )
        .await
        .expect("unrelated response must not wait for NodeState");
    }

    #[tokio::test]
    async fn non_relay_tick_updates_atomic_cache_without_state_lock() {
        let state = Arc::new(Mutex::new(NodeState::new(10, &[])));
        let _guard = state.lock().await;
        let latest = Arc::new(AtomicU64::new(0));
        let mut payload = [0; 8];
        payload[2..4].copy_from_slice(&7u16.to_le_bytes());
        payload[4..8].copy_from_slice(&123u32.to_le_bytes());
        let frame = Bytes::from(
            build_request_frame(crate::frame::BROADCAST_TICK_TYPE, 1, &payload)
                .expect("tick frame should build"),
        );

        tokio::time::timeout(
            Duration::from_millis(100),
            process_incoming_frame(
                1,
                frame,
                Arc::clone(&state),
                Arc::new(DedupWindow::new(1_000)),
                Arc::new(PendingRequests::default()),
                Arc::clone(&latest),
                test_config(),
            ),
        )
        .await
        .expect("tick response must not wait for NodeState");

        assert_eq!(latest.load(Ordering::Relaxed), pack_epoch_tick(7, 123));
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

        process_incoming_frame(
            1,
            frame,
            Arc::clone(&state),
            Arc::new(DedupWindow::new(1_000)),
            Arc::new(PendingRequests::default()),
            Arc::new(AtomicU64::new(0)),
            test_config(),
        )
        .await;

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
            Arc::clone(&state),
            Arc::new(DedupWindow::new(1_000)),
            pending,
            Arc::new(AtomicU64::new(0)),
            Arc::new(config),
        )
        .await;

        assert!(target_rx.try_recv().is_err());
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
                resources: NetworkResources {
                    state: Arc::clone(&state),
                    dedup: Arc::new(DedupWindow::new(1_000)),
                    pending: Arc::new(PendingRequests::default()),
                    latest_epoch_tick: Arc::new(AtomicU64::new(0)),
                    config: test_config(),
                },
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
        tx.send(Bytes::from(vec![0; 1_024]))
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
                resources: NetworkResources {
                    state: Arc::clone(&state),
                    dedup: Arc::new(DedupWindow::new(1_000)),
                    pending: Arc::new(PendingRequests::default()),
                    latest_epoch_tick: Arc::new(AtomicU64::new(0)),
                    config: test_config(),
                },
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
}
