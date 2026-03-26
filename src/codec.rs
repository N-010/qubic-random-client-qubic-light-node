use blake3::hash;

pub(crate) fn parse_wallet_public_key(input: &str) -> Result<[u8; 32], String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("Wallet is empty".to_string());
    }

    if let Ok(hex_key) = parse_public_key_hex(trimmed) {
        return Ok(hex_key);
    }
    parse_public_key_identity(trimmed)
}

fn parse_public_key_hex(input: &str) -> Result<[u8; 32], String> {
    let hex = if let Some(rest) = input.strip_prefix("0x") {
        rest
    } else if let Some(rest) = input.strip_prefix("0X") {
        rest
    } else {
        input
    };
    if hex.len() != 64 {
        return Err("Public key hex must have 64 hex characters".to_string());
    }

    let mut out = [0u8; 32];
    for (idx, slot) in out.iter_mut().enumerate() {
        let offset = idx * 2;
        let byte = u8::from_str_radix(&hex[offset..offset + 2], 16)
            .map_err(|_| format!("Invalid hex at position {offset}"))?;
        *slot = byte;
    }
    Ok(out)
}

fn parse_public_key_identity(identity: &str) -> Result<[u8; 32], String> {
    let identity_upper = identity.trim().to_ascii_uppercase();
    if identity_upper.len() != 60 {
        return Err("Wallet identity must be 60 chars (A-Z) or 0x + 64 hex public key".to_string());
    }

    let bytes = identity_upper.as_bytes();
    for ch in bytes {
        if !(*ch >= b'A' && *ch <= b'Z') {
            return Err("Identity contains invalid characters, expected only A-Z".to_string());
        }
    }

    let mut public_key = [0u8; 32];
    for fragment_idx in 0..4 {
        let mut fragment_value: u64 = 0;
        for char_idx in (0..14).rev() {
            let index = fragment_idx * 14 + char_idx;
            let value = (bytes[index] - b'A') as u64;
            fragment_value = fragment_value
                .checked_mul(26)
                .and_then(|v| v.checked_add(value))
                .ok_or_else(|| "Identity decoding overflow".to_string())?;
        }

        let offset = fragment_idx * 8;
        public_key[offset..offset + 8].copy_from_slice(&fragment_value.to_le_bytes());
    }

    Ok(public_key)
}

pub(crate) fn tx_id_from_bytes(tx_bytes: &[u8]) -> String {
    bytes_to_hex(hash(tx_bytes).as_bytes())
}

pub(crate) fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

pub(crate) fn read_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    let chunk = bytes.get(offset..offset + 2)?;
    Some(u16::from_le_bytes([chunk[0], chunk[1]]))
}

pub(crate) fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    let chunk = bytes.get(offset..offset + 4)?;
    Some(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
}

pub(crate) fn read_i32(bytes: &[u8], offset: usize) -> Option<i32> {
    let chunk = bytes.get(offset..offset + 4)?;
    Some(i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
}

pub(crate) fn read_i64(bytes: &[u8], offset: usize) -> Option<i64> {
    let chunk = bytes.get(offset..offset + 8)?;
    Some(i64::from_le_bytes([
        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
    ]))
}
