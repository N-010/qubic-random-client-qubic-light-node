use crate::config::Config;
use crate::frame::{
    BROADCAST_TRANSACTION_TYPE, EXCHANGE_PUBLIC_PEERS_TYPE, HEADER_SIZE, MAX_FRAME_SIZE,
    NUMBER_OF_TRANSACTIONS_PER_TICK, build_exchange_public_peers_frame, build_request_frame,
    decode_frame_size, frame_meta, message_type_name, parse_exchange_public_peers,
    parse_tick_status_from_frame,
};
use crate::state::{NodeState, PeerPoolStats, RelayTarget};
use crate::types::{TickStatus, format_epoch_tick, format_epoch_tick_packed, pack_epoch_tick};
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::time::sleep;

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

struct ConnectionContext {
    peer_id: u64,
    remote: SocketAddrV4,
    state: Arc<Mutex<NodeState>>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
}

fn dispatch_frame(targets: Vec<RelayTarget>, frame: &Arc<[u8]>) -> DispatchResult {
    let mut result = DispatchResult::default();

    for target in targets {
        if result.sent_count == DISSEMINATION_MULTIPLIER {
            break;
        }
        match target.tx.try_send(Arc::clone(frame)) {
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
    tx_bytes: &[u8],
) -> Result<(), String> {
    if tx_bytes.is_empty() {
        return Err("Transaction payload is empty".to_string());
    }

    let frame = build_request_frame(BROADCAST_TRANSACTION_TYPE, 0, tx_bytes)?;
    let digest = *blake3::hash(&frame).as_bytes();

    let targets = {
        let locked = state.lock().await;
        let targets = locked.collect_all_targets();
        if targets.is_empty() {
            return Err("No connected peers available for broadcast".to_string());
        }
        if locked.is_seen(&digest) {
            return Ok(());
        }
        targets
    };

    let frame = Arc::<[u8]>::from(frame);
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

    let mut locked = state.lock().await;
    let _ = locked.mark_seen(digest);

    Ok(())
}

pub(crate) async fn accept_loop(
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
                if let Err(err) = configure_tcp_keepalive(&stream) {
                    eprintln!("Failed to set TCP keepalive for {remote_v4}: {err}");
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

pub(crate) async fn dial_loop(
    state: Arc<Mutex<NodeState>>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
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
            let state_for_task = Arc::clone(&state);
            let latest_epoch_tick_for_task = Arc::clone(&latest_epoch_tick);
            let config_for_task = Arc::clone(&config);
            tokio::spawn(async move {
                match TcpStream::connect(target).await {
                    Ok(stream) => {
                        if let Err(err) = stream.set_nodelay(true) {
                            eprintln!("Failed to set TCP_NODELAY for {target}: {err}");
                        }
                        if let Err(err) = configure_tcp_keepalive(&stream) {
                            eprintln!("Failed to set TCP keepalive for {target}: {err}");
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
                            locked.record_peer_failure(
                                target,
                                config_for_task.reconnect_interval,
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
    state: Arc<Mutex<NodeState>>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
    let (tx, rx) = mpsc::channel::<Arc<[u8]>>(OUTBOUND_QUEUE_CAPACITY);
    let (disconnect_tx, disconnect_rx) = watch::channel(false);
    let (peer_id, handshake_payload, incoming_count, outgoing_count, latest_tick) = {
        let mut locked = state.lock().await;

        if !outbound && locked.incoming_count() >= config.max_incoming {
            println!("Rejecting incoming {remote}: incoming limit reached.");
            locked.clear_pending_dial(remote);
            locked.clear_pending_dial(SocketAddrV4::new(*remote.ip(), config.peer_port));
            return;
        }

        let Some(peer_id) = locked.register_session(
            remote,
            outbound,
            tx.clone(),
            disconnect_tx,
            config.peer_port,
        ) else {
            locked.clear_pending_dial(remote);
            locked.clear_pending_dial(SocketAddrV4::new(*remote.ip(), config.peer_port));
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

    if tx.try_send(Arc::<[u8]>::from(handshake_payload)).is_err() {
        eprintln!("Failed to queue handshake frame for {remote}");
    }
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
        rx,
        disconnect_rx,
        ConnectionContext {
            peer_id,
            remote,
            state,
            latest_epoch_tick,
            config,
        },
    ));
}

async fn connection_worker(
    stream: TcpStream,
    mut rx: mpsc::Receiver<Arc<[u8]>>,
    mut disconnect_rx: watch::Receiver<bool>,
    context: ConnectionContext,
) {
    let ConnectionContext {
        peer_id,
        remote,
        state,
        latest_epoch_tick,
        config,
    } = context;
    let (reader, mut writer) = stream.into_split();
    let traffic_log = config.traffic_log;
    let writer_latest_epoch_tick = Arc::clone(&latest_epoch_tick);

    let mut writer_task = tokio::spawn(async move {
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
            if let Err(err) = writer.write_all(&frame).await {
                eprintln!("Write failed to {remote}: {err}");
                return;
            }
        }
        let _ = writer.shutdown().await;
    });

    let read_future = read_peer_frames(
        reader,
        peer_id,
        remote,
        Arc::clone(&state),
        latest_epoch_tick,
        config,
    );
    tokio::pin!(read_future);

    let writer_finished = tokio::select! {
        result = &mut writer_task => {
            if let Err(err) = result {
                eprintln!("Writer task failed for {remote}: {err}");
            }
            true
        }
        _ = &mut read_future => false,
        _ = disconnect_rx.changed() => false,
    };

    if !writer_finished {
        writer_task.abort();
        let _ = writer_task.await;
    }

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

async fn read_peer_frames(
    mut reader: OwnedReadHalf,
    peer_id: u64,
    remote: SocketAddrV4,
    state: Arc<Mutex<NodeState>>,
    latest_epoch_tick: Arc<AtomicU64>,
    config: Arc<Config>,
) {
    let mut read_buffer = vec![0u8; READ_BUFFER_SIZE];
    let mut accumulated = Vec::<u8>::with_capacity(ACCUMULATED_INITIAL_CAPACITY);

    loop {
        match reader.read(&mut read_buffer).await {
            Ok(0) => break,
            Ok(read_bytes) => {
                accumulated.extend_from_slice(&read_buffer[..read_bytes]);
                if accumulated.len() > MAX_ACCUMULATED_BUFFER {
                    eprintln!(
                        "Dropping {remote}: buffered {} bytes without full parse (limit={MAX_ACCUMULATED_BUFFER})",
                        accumulated.len()
                    );
                    break;
                }
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

                if accumulated.capacity() > ACCUMULATED_SHRINK_THRESHOLD
                    && accumulated.len() < ACCUMULATED_RETAIN_CAPACITY
                {
                    accumulated.shrink_to(ACCUMULATED_RETAIN_CAPACITY);
                }
            }
            Err(err) => {
                eprintln!("Read failed from {remote}: {err}");
                break;
            }
        }
    }
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
        parse_exchange_public_peers(&frame, config.peer_port)
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
        update_state_without_relay(&mut locked, tick_update, discovered);
        return;
    }

    let digest = *blake3::hash(&frame).as_bytes();
    let (targets, latest_tick) = {
        let mut locked = state.lock().await;
        update_state_without_relay(&mut locked, tick_update, discovered);
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

    let frame = Arc::<[u8]>::from(frame);
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
            format_epoch_tick(latest_tick)
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

fn update_state_without_relay(
    locked: &mut NodeState,
    tick_update: Option<TickStatus>,
    discovered: Vec<SocketAddrV4>,
) {
    if let Some(status) = tick_update {
        locked.update_latest_tick(status);
    }
    for peer in discovered {
        let _ = locked.add_discovered_peer(peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_PORT;
    use pretty_assertions::assert_eq;
    use std::net::Ipv4Addr;

    fn peer(last_octet: u8) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, last_octet), DEFAULT_PORT)
    }

    #[test]
    fn incoming_connections_do_not_suppress_emergency_dns() {
        let mut state = NodeState::new(1_000, 10, &[]);
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
        let mut state = NodeState::new(1_000, 10, &[existing]);
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
}
