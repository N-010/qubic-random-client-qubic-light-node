use crate::codec::{bytes_to_hex, read_i32, read_i64, read_u16, read_u32};
use crate::config::Config;
use crate::frame::{
    BROADCAST_TRANSACTION_TYPE, END_RESPONSE_TYPE, HEADER_SIZE, MAX_FRAME_SIZE,
    REQUEST_ENTITY_TYPE, REQUEST_TICK_TRANSACTIONS_PAYLOAD_SIZE, REQUEST_TICK_TRANSACTIONS_TYPE,
    RESPOND_ENTITY_TYPE, build_exchange_public_peers_frame, build_request_frame, decode_frame_size,
    frame_dejavu, frame_payload, random_non_zero_u32,
};
use crate::state::NodeState;
use crate::types::{BalanceResponse, TickTransaction};
use rand::seq::SliceRandom;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};

const RESPOND_ENTITY_MIN_PAYLOAD_SIZE: usize = 72;
const TRANSACTION_BASE_SIZE: usize = 80;
const SIGNATURE_SIZE: usize = 64;
const MAX_PARALLEL_PEER_QUERIES: usize = 3;

pub(crate) async fn query_balance(
    state: Arc<Mutex<NodeState>>,
    config: Arc<Config>,
    wallet: &str,
    public_key: [u8; 32],
) -> Result<BalanceResponse, String> {
    let mut peers = {
        let locked = state.lock().await;
        locked.peer_candidates(config.peer_port)
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

pub(crate) async fn query_tick_transactions(
    state: Arc<Mutex<NodeState>>,
    config: Arc<Config>,
    tick: u32,
) -> Result<Vec<TickTransaction>, String> {
    let mut peers = {
        let locked = state.lock().await;
        locked.peer_candidates(config.peer_port)
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
