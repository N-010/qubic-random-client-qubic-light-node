use crate::config::Config;
use crate::frame::{
    BROADCAST_TRANSACTION_TYPE, EXCHANGE_PUBLIC_PEERS_TYPE, HEADER_SIZE, MAX_FRAME_SIZE,
    build_exchange_public_peers_frame, build_request_frame, decode_frame_size, frame_meta,
    message_type_name, parse_exchange_public_peers, parse_tick_status_from_frame,
};
use crate::state::NodeState;
use crate::types::{TickStatus, format_epoch_tick, format_epoch_tick_packed, pack_epoch_tick};
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, mpsc};
use tokio::time::sleep;

const OUTBOUND_QUEUE_CAPACITY: usize = 1_024;
const READ_BUFFER_SIZE: usize = 128 * 1024;
const ACCUMULATED_INITIAL_CAPACITY: usize = 256 * 1024;
const ACCUMULATED_RETAIN_CAPACITY: usize = 512 * 1024;
const ACCUMULATED_SHRINK_THRESHOLD: usize = 2 * 1024 * 1024;
const MAX_ACCUMULATED_BUFFER: usize = MAX_FRAME_SIZE * 2;

pub(crate) async fn broadcast_transaction_to_network(
    state: Arc<Mutex<NodeState>>,
    tx_bytes: &[u8],
) -> Result<(), String> {
    if tx_bytes.is_empty() {
        return Err("Transaction payload is empty".to_string());
    }

    let frame = build_request_frame(BROADCAST_TRANSACTION_TYPE, 0, tx_bytes)?;
    let digest = *blake3::hash(&frame).as_bytes();

    let (targets, is_new) = {
        let mut locked = state.lock().await;
        let targets = locked.collect_all_targets();
        if targets.is_empty() {
            return Err("No connected peers available for broadcast".to_string());
        }
        let is_new = locked.mark_seen(digest);
        (targets, is_new)
    };

    if !is_new {
        return Ok(());
    }

    let frame = Arc::<[u8]>::from(frame);
    let mut sent_count = 0usize;
    let mut full_count = 0usize;
    let mut closed_count = 0usize;

    for tx in targets {
        match tx.try_send(Arc::clone(&frame)) {
            Ok(()) => {
                sent_count += 1;
            }
            Err(TrySendError::Full(_)) => {
                full_count += 1;
            }
            Err(TrySendError::Closed(_)) => {
                closed_count += 1;
            }
        }
    }

    if sent_count == 0 {
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
    let (tx, rx) = mpsc::channel::<Arc<[u8]>>(OUTBOUND_QUEUE_CAPACITY);
    let (peer_id, handshake_payload, incoming_count, outgoing_count, latest_tick) = {
        let mut locked = state.lock().await;

        if !outbound && locked.incoming_count() >= config.max_incoming {
            println!("Rejecting incoming {remote}: incoming limit reached.");
            locked.clear_pending_dial(remote);
            locked.clear_pending_dial(SocketAddrV4::new(*remote.ip(), config.peer_port));
            return;
        }

        let Some(peer_id) = locked.register_session(remote, outbound, tx.clone(), config.peer_port)
        else {
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
    mut rx: mpsc::Receiver<Arc<[u8]>>,
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

    writer_task.abort();
    let _ = writer_task.await;

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
    let mut dropped_due_to_full = 0usize;
    for tx in targets {
        match tx.try_send(Arc::clone(&frame)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                dropped_due_to_full += 1;
            }
            Err(TrySendError::Closed(_)) => {}
        }
    }

    if config.traffic_log && dropped_due_to_full > 0 {
        let (size, message_type, dejavu) = frame_meta(&frame);
        println!(
            "DROP_BACKPRESSURE peer_id={} dropped={} size={} type={}({}) dejavu={}",
            source_peer_id,
            dropped_due_to_full,
            size,
            message_type_name(message_type),
            message_type,
            dejavu
        );
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
