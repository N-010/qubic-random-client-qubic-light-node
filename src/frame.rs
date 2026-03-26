use crate::codec::{read_u16, read_u32};
use crate::types::TickStatus;
use rand::Rng;
use std::net::{Ipv4Addr, SocketAddrV4};

pub(crate) const HEADER_SIZE: usize = 8;
pub(crate) const MAX_FRAME_SIZE: usize = 0x00FF_FFFF;
pub(crate) const EXCHANGE_PUBLIC_PEERS_TYPE: u8 = 0;
pub(crate) const NUMBER_OF_EXCHANGED_PEERS: usize = 4;
pub(crate) const EXCHANGE_PUBLIC_PEERS_FRAME_SIZE: usize =
    HEADER_SIZE + NUMBER_OF_EXCHANGED_PEERS * 4;

pub(crate) const BROADCAST_TICK_TYPE: u8 = 3;
pub(crate) const BROADCAST_TRANSACTION_TYPE: u8 = 24;
pub(crate) const RESPOND_CURRENT_TICK_INFO_TYPE: u8 = 28;
pub(crate) const REQUEST_TICK_TRANSACTIONS_TYPE: u8 = 29;
pub(crate) const REQUEST_ENTITY_TYPE: u8 = 31;
pub(crate) const RESPOND_ENTITY_TYPE: u8 = 32;
pub(crate) const END_RESPONSE_TYPE: u8 = 35;

pub(crate) const REQUEST_TICK_TRANSACTION_FLAGS_SIZE: usize = 1024 / 8;
pub(crate) const REQUEST_TICK_TRANSACTIONS_PAYLOAD_SIZE: usize =
    4 + REQUEST_TICK_TRANSACTION_FLAGS_SIZE;
pub(crate) const RESPOND_CURRENT_TICK_INFO_PAYLOAD_SIZE: usize = 16;

pub(crate) fn frame_meta(frame: &[u8]) -> (usize, u8, u32) {
    if frame.len() < HEADER_SIZE {
        return (frame.len(), 0, 0);
    }
    let size = decode_frame_size(frame);
    let message_type = frame[3];
    let dejavu = u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]);
    (size, message_type, dejavu)
}

pub(crate) fn message_type_name(message_type: u8) -> &'static str {
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

pub(crate) fn build_exchange_public_peers_frame(
    peers: [Ipv4Addr; NUMBER_OF_EXCHANGED_PEERS],
) -> Vec<u8> {
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

pub(crate) fn parse_exchange_public_peers(frame: &[u8], peer_port: u16) -> Vec<SocketAddrV4> {
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
        peers.push(SocketAddrV4::new(ip, peer_port));
    }
    peers
}

pub(crate) fn build_request_frame(
    message_type: u8,
    dejavu: u32,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let size = HEADER_SIZE + payload.len();
    if !(HEADER_SIZE..=MAX_FRAME_SIZE).contains(&size) {
        return Err(format!("Invalid frame size {size}"));
    }

    let mut frame = Vec::with_capacity(size);
    frame.push((size & 0xFF) as u8);
    frame.push(((size >> 8) & 0xFF) as u8);
    frame.push(((size >> 16) & 0xFF) as u8);
    frame.push(message_type);
    frame.extend_from_slice(&dejavu.to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

pub(crate) fn frame_payload(frame: &[u8]) -> Result<&[u8], String> {
    if frame.len() < HEADER_SIZE {
        return Err("Frame is smaller than header".to_string());
    }

    let frame_size = decode_frame_size(frame);
    if frame_size != frame.len() {
        return Err(format!(
            "Frame length mismatch: header={frame_size} actual={}",
            frame.len()
        ));
    }
    Ok(&frame[HEADER_SIZE..])
}

pub(crate) fn frame_dejavu(frame: &[u8]) -> Result<u32, String> {
    if frame.len() < HEADER_SIZE {
        return Err("Frame too small".to_string());
    }
    Ok(u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]))
}

pub(crate) fn parse_tick_status_from_frame(frame: &[u8]) -> Option<TickStatus> {
    if frame.len() < HEADER_SIZE {
        return None;
    }

    match frame[3] {
        RESPOND_CURRENT_TICK_INFO_TYPE => {
            let payload = frame_payload(frame).ok()?;
            parse_current_tick_info_payload(payload).ok()
        }
        BROADCAST_TICK_TYPE => {
            let payload = frame_payload(frame).ok()?;
            if payload.len() < 8 {
                return None;
            }
            Some(TickStatus {
                epoch: read_u16(payload, 2)?,
                tick: read_u32(payload, 4)?,
                initial_tick: 0,
                tick_duration_ms: 0,
                aligned_votes: 0,
                misaligned_votes: 0,
            })
        }
        _ => None,
    }
}

pub(crate) fn decode_frame_size(header: &[u8]) -> usize {
    (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16)
}

pub(crate) fn is_bogon(ip: &Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 0
        || octets[0] == 10
        || octets[0] == 127
        || (octets[0] == 172 && (16..=31).contains(&octets[1]))
        || (octets[0] == 192 && octets[1] == 168)
        || octets[0] == 255
}

pub(crate) fn random_non_zero_u32() -> u32 {
    let mut rng = rand::rng();
    loop {
        let value: u32 = rng.random();
        if value != 0 {
            return value;
        }
    }
}

fn parse_current_tick_info_payload(payload: &[u8]) -> Result<TickStatus, String> {
    if payload.len() < RESPOND_CURRENT_TICK_INFO_PAYLOAD_SIZE {
        return Err(format!(
            "RespondCurrentTickInfo payload too small: {}",
            payload.len()
        ));
    }

    Ok(TickStatus {
        tick_duration_ms: read_u16(payload, 0).ok_or_else(|| "tickDuration missing".to_string())?,
        epoch: read_u16(payload, 2).ok_or_else(|| "epoch missing".to_string())?,
        tick: read_u32(payload, 4).ok_or_else(|| "tick missing".to_string())?,
        aligned_votes: read_u16(payload, 8).ok_or_else(|| "aligned votes missing".to_string())?,
        misaligned_votes: read_u16(payload, 10)
            .ok_or_else(|| "misaligned votes missing".to_string())?,
        initial_tick: read_u32(payload, 12).ok_or_else(|| "initial tick missing".to_string())?,
    })
}
