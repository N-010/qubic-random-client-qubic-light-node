use crate::codec::{read_i64, read_u16, read_u32};
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
pub(crate) const REQUEST_CONTRACT_FUNCTION_TYPE: u8 = 42;
pub(crate) const RESPOND_CONTRACT_FUNCTION_TYPE: u8 = 43;
pub(crate) const TRY_AGAIN_TYPE: u8 = 54;
pub(crate) const ORACLE_MACHINE_QUERY_TYPE: u8 = 190;
pub(crate) const ORACLE_MACHINE_REPLY_TYPE: u8 = 191;
pub(crate) const OC_MACHINE_INVOCATION_TYPE: u8 = 192;

pub(crate) const NUMBER_OF_TRANSACTIONS_PER_TICK: usize = 4096;
pub(crate) const NUMBER_OF_COMPUTORS: usize = 676;
pub(crate) const MAX_NUMBER_OF_CONTRACTS: u32 = 1024;
pub(crate) const MAX_INPUT_SIZE: usize = 1024;
pub(crate) const MAX_CONTRACT_FUNCTION_INPUT_SIZE: usize = MAX_INPUT_SIZE;
pub(crate) const MAX_CONTRACT_FUNCTION_OUTPUT_SIZE: usize = u16::MAX as usize;
pub(crate) const MAX_AMOUNT: i64 = 1_000_000_000_000_000;
pub(crate) const TRANSACTION_BASE_SIZE: usize = 80;
pub(crate) const SIGNATURE_SIZE: usize = 64;
pub(crate) const SPECTRUM_DEPTH: usize = 24;
pub(crate) const SPECTRUM_CAPACITY: i32 = 1 << SPECTRUM_DEPTH;
pub(crate) const RESPOND_ENTITY_PAYLOAD_SIZE: usize = 64 + 8 + 32 * SPECTRUM_DEPTH;
pub(crate) const BROADCAST_TICK_PAYLOAD_SIZE: usize = 344;
pub(crate) const REQUEST_TICK_TRANSACTION_FLAGS_SIZE: usize = NUMBER_OF_TRANSACTIONS_PER_TICK / 8;
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
        64 => "REQUEST_ACTIVE_IPOS",
        65 => "RESPOND_ACTIVE_IPO",
        66 => "REQUEST_ORACLE_DATA",
        67 => "RESPOND_ORACLE_DATA",
        68 => "BROADCAST_CUSTOM_MINING_TASK",
        69 => "BROADCAST_CUSTOM_MINING_SOLUTION",
        70 => "REQUEST_REVENUE_DATA",
        71 => "RESPOND_REVENUE_DATA",
        190 => "ORACLE_MACHINE_QUERY",
        191 => "ORACLE_MACHINE_REPLY",
        192 => "OC_MACHINE_INVOCATION",
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
    if frame.len() != EXCHANGE_PUBLIC_PEERS_FRAME_SIZE {
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

pub(crate) fn build_request_tick_transactions_frame(
    dejavu: u32,
    tick: u32,
) -> Result<Vec<u8>, String> {
    let mut payload = [0u8; REQUEST_TICK_TRANSACTIONS_PAYLOAD_SIZE];
    payload[..4].copy_from_slice(&tick.to_le_bytes());
    build_request_frame(REQUEST_TICK_TRANSACTIONS_TYPE, dejavu, &payload)
}

pub(crate) fn build_request_contract_function_frame(
    dejavu: u32,
    contract_index: u32,
    input_type: u16,
    input: &[u8],
) -> Result<Vec<u8>, String> {
    if !(1..MAX_NUMBER_OF_CONTRACTS).contains(&contract_index) {
        return Err(format!(
            "Contract index must be between 1 and {}",
            MAX_NUMBER_OF_CONTRACTS - 1
        ));
    }
    if input.len() > MAX_CONTRACT_FUNCTION_INPUT_SIZE {
        return Err(format!(
            "Contract function input is too large: maximum {}, got {}",
            MAX_CONTRACT_FUNCTION_INPUT_SIZE,
            input.len()
        ));
    }

    let input_size = u16::try_from(input.len())
        .map_err(|_| format!("Contract function input is too large: {}", input.len()))?;
    let mut payload = Vec::with_capacity(8 + input.len());
    payload.extend_from_slice(&contract_index.to_le_bytes());
    payload.extend_from_slice(&input_type.to_le_bytes());
    payload.extend_from_slice(&input_size.to_le_bytes());
    payload.extend_from_slice(input);
    build_request_frame(REQUEST_CONTRACT_FUNCTION_TYPE, dejavu, &payload)
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
            if payload.len() != BROADCAST_TICK_PAYLOAD_SIZE {
                return None;
            }
            let computor_index = read_u16(payload, 0)? as usize;
            if computor_index >= NUMBER_OF_COMPUTORS {
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
    if payload.len() != RESPOND_CURRENT_TICK_INFO_PAYLOAD_SIZE {
        return Err(format!(
            "RespondCurrentTickInfo payload size mismatch: expected {RESPOND_CURRENT_TICK_INFO_PAYLOAD_SIZE}, got {}",
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TransactionLayout {
    pub(crate) amount: i64,
    pub(crate) tick: u32,
    pub(crate) input_type: u16,
    pub(crate) input_size: u16,
    pub(crate) input_start: usize,
    pub(crate) signature_start: usize,
}

pub(crate) fn parse_transaction_layout(
    payload: &[u8],
    expected_tick: Option<u32>,
) -> Result<TransactionLayout, String> {
    if payload.len() < TRANSACTION_BASE_SIZE + SIGNATURE_SIZE {
        return Err(format!("Transaction payload too small: {}", payload.len()));
    }

    let amount = read_i64(payload, 64).ok_or_else(|| "amount missing".to_string())?;
    if !(0..=MAX_AMOUNT).contains(&amount) {
        return Err(format!("Transaction amount is out of range: {amount}"));
    }

    let tick = read_u32(payload, 72).ok_or_else(|| "tick missing".to_string())?;
    if let Some(expected_tick) = expected_tick
        && tick != expected_tick
    {
        return Err(format!(
            "Transaction tick mismatch: expected {expected_tick}, got {tick}"
        ));
    }

    let input_type = read_u16(payload, 76).ok_or_else(|| "inputType missing".to_string())?;
    let input_size = read_u16(payload, 78).ok_or_else(|| "inputSize missing".to_string())?;
    if input_size as usize > MAX_INPUT_SIZE {
        return Err(format!(
            "Transaction input is too large: maximum {MAX_INPUT_SIZE}, got {input_size}"
        ));
    }

    let signature_start = TRANSACTION_BASE_SIZE + input_size as usize;
    let expected_size = signature_start + SIGNATURE_SIZE;
    if payload.len() != expected_size {
        return Err(format!(
            "Transaction payload size mismatch: expected {expected_size}, got {}",
            payload.len()
        ));
    }

    Ok(TransactionLayout {
        amount,
        tick,
        input_type,
        input_size,
        input_start: TRANSACTION_BASE_SIZE,
        signature_start,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn builds_request_tick_transactions_frame_for_current_core_layout() {
        let dejavu = 0x90AB_CDEF;
        let tick = 0x1234_5678;

        let frame = build_request_tick_transactions_frame(dejavu, tick).unwrap();
        let mut expected = vec![0u8; 524];
        expected[..3].copy_from_slice(&[0x0C, 0x02, 0x00]);
        expected[3] = REQUEST_TICK_TRANSACTIONS_TYPE;
        expected[4..8].copy_from_slice(&dejavu.to_le_bytes());
        expected[8..12].copy_from_slice(&tick.to_le_bytes());

        assert_eq!(REQUEST_TICK_TRANSACTIONS_PAYLOAD_SIZE, 516);
        assert_eq!(frame, expected);
    }

    #[test]
    fn builds_request_contract_function_frame_for_current_core_layout() {
        let frame =
            build_request_contract_function_frame(0x1122_3344, 3, 2, &[0xAA, 0xBB]).unwrap();

        assert_eq!(
            frame,
            vec![
                18,
                0,
                0,
                REQUEST_CONTRACT_FUNCTION_TYPE,
                0x44,
                0x33,
                0x22,
                0x11,
                3,
                0,
                0,
                0,
                2,
                0,
                2,
                0,
                0xAA,
                0xBB,
            ]
        );
    }

    #[test]
    fn rejects_invalid_contract_function_requests() {
        assert_eq!(
            build_request_contract_function_frame(1, 0, 2, &[]).unwrap_err(),
            "Contract index must be between 1 and 1023"
        );
        assert_eq!(
            build_request_contract_function_frame(1, MAX_NUMBER_OF_CONTRACTS, 2, &[]).unwrap_err(),
            "Contract index must be between 1 and 1023"
        );
        assert_eq!(
            build_request_contract_function_frame(
                1,
                3,
                2,
                &vec![0; MAX_CONTRACT_FUNCTION_INPUT_SIZE + 1],
            )
            .unwrap_err(),
            "Contract function input is too large: maximum 1024, got 1025"
        );
    }

    #[test]
    fn maps_current_core_message_types() {
        let actual = [
            message_type_name(60),
            message_type_name(61),
            message_type_name(62),
            message_type_name(63),
            message_type_name(66),
            message_type_name(67),
            message_type_name(68),
            message_type_name(69),
            message_type_name(70),
            message_type_name(71),
            message_type_name(190),
            message_type_name(191),
            message_type_name(192),
        ];

        assert_eq!(
            actual,
            [
                "UNKNOWN",
                "UNKNOWN",
                "UNKNOWN",
                "UNKNOWN",
                "REQUEST_ORACLE_DATA",
                "RESPOND_ORACLE_DATA",
                "BROADCAST_CUSTOM_MINING_TASK",
                "BROADCAST_CUSTOM_MINING_SOLUTION",
                "REQUEST_REVENUE_DATA",
                "RESPOND_REVENUE_DATA",
                "ORACLE_MACHINE_QUERY",
                "ORACLE_MACHINE_REPLY",
                "OC_MACHINE_INVOCATION",
            ]
        );
    }

    fn transaction_payload(input_size: usize, amount: i64, tick: u32) -> Vec<u8> {
        let mut payload = vec![0; TRANSACTION_BASE_SIZE + input_size + SIGNATURE_SIZE];
        payload[64..72].copy_from_slice(&amount.to_le_bytes());
        payload[72..76].copy_from_slice(&tick.to_le_bytes());
        payload[78..80].copy_from_slice(&(input_size as u16).to_le_bytes());
        payload
    }

    #[test]
    fn parses_transaction_using_core_validity_limits() {
        let payload = transaction_payload(MAX_INPUT_SIZE, MAX_AMOUNT, 123);

        assert_eq!(
            parse_transaction_layout(&payload, Some(123)),
            Ok(TransactionLayout {
                amount: MAX_AMOUNT,
                tick: 123,
                input_type: 0,
                input_size: MAX_INPUT_SIZE as u16,
                input_start: TRANSACTION_BASE_SIZE,
                signature_start: TRANSACTION_BASE_SIZE + MAX_INPUT_SIZE,
            })
        );
    }

    #[test]
    fn rejects_transaction_outside_core_validity_limits() {
        assert_eq!(
            parse_transaction_layout(&transaction_payload(0, -1, 123), Some(123)).unwrap_err(),
            "Transaction amount is out of range: -1"
        );
        assert_eq!(
            parse_transaction_layout(&transaction_payload(0, MAX_AMOUNT + 1, 123), Some(123))
                .unwrap_err(),
            "Transaction amount is out of range: 1000000000000001"
        );
        assert_eq!(
            parse_transaction_layout(&transaction_payload(0, 0, 124), Some(123)).unwrap_err(),
            "Transaction tick mismatch: expected 123, got 124"
        );

        let mut oversized_input = transaction_payload(MAX_INPUT_SIZE + 1, 0, 123);
        oversized_input[78..80].copy_from_slice(&((MAX_INPUT_SIZE + 1) as u16).to_le_bytes());
        assert_eq!(
            parse_transaction_layout(&oversized_input, Some(123)).unwrap_err(),
            "Transaction input is too large: maximum 1024, got 1025"
        );
    }

    #[test]
    fn tick_status_requires_exact_core_layout_and_valid_computor() {
        let mut payload = [0; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[2..4].copy_from_slice(&7u16.to_le_bytes());
        payload[4..8].copy_from_slice(&123u32.to_le_bytes());
        let valid = build_request_frame(BROADCAST_TICK_TYPE, 0, &payload).unwrap();
        assert_eq!(
            parse_tick_status_from_frame(&valid),
            Some(TickStatus {
                epoch: 7,
                tick: 123,
                initial_tick: 0,
                tick_duration_ms: 0,
                aligned_votes: 0,
                misaligned_votes: 0,
            })
        );

        let truncated = build_request_frame(BROADCAST_TICK_TYPE, 0, &payload[..8]).unwrap();
        assert_eq!(parse_tick_status_from_frame(&truncated), None);

        payload[..2].copy_from_slice(&(NUMBER_OF_COMPUTORS as u16).to_le_bytes());
        let invalid_computor = build_request_frame(BROADCAST_TICK_TYPE, 0, &payload).unwrap();
        assert_eq!(parse_tick_status_from_frame(&invalid_computor), None);

        let current_tick_payload = [0; RESPOND_CURRENT_TICK_INFO_PAYLOAD_SIZE];
        let current_tick =
            build_request_frame(RESPOND_CURRENT_TICK_INFO_TYPE, 1, &current_tick_payload).unwrap();
        assert!(parse_tick_status_from_frame(&current_tick).is_some());
        let oversized_current_tick =
            build_request_frame(RESPOND_CURRENT_TICK_INFO_TYPE, 1, &[0; 17]).unwrap();
        assert_eq!(parse_tick_status_from_frame(&oversized_current_tick), None);
    }

    #[test]
    fn peer_exchange_requires_exact_core_layout() {
        let mut frame = build_exchange_public_peers_frame([Ipv4Addr::new(1, 1, 1, 1); 4]);
        assert_eq!(parse_exchange_public_peers(&frame, 21841).len(), 4);

        frame.push(0);
        assert_eq!(parse_exchange_public_peers(&frame, 21841), Vec::new());
    }
}
