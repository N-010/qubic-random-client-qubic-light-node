use tiny_keccak::{Hasher, KangarooTwelve};

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

    let expected_identity = qubic_identity(&public_key, false);
    if identity_upper.as_bytes()[56..] != expected_identity.as_bytes()[56..] {
        return Err("Identity checksum is invalid".to_string());
    }

    Ok(public_key)
}

pub(crate) fn tx_id_from_bytes(tx_bytes: &[u8]) -> String {
    qubic_identity(&kangaroo_twelve(tx_bytes), true)
}

pub(crate) fn kangaroo_twelve(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = KangarooTwelve::new(&[]);
    hasher.update(bytes);
    let mut digest = [0u8; 32];
    hasher.finalize(&mut digest);
    digest
}

fn qubic_identity(public_key: &[u8; 32], lowercase: bool) -> String {
    let base = if lowercase { b'a' } else { b'A' };
    let mut identity = [0u8; 60];

    for (fragment_index, fragment) in public_key.chunks_exact(8).enumerate() {
        let mut value = u64::from_le_bytes(
            fragment
                .try_into()
                .expect("public key fragment must contain eight bytes"),
        );
        for character_index in 0..14 {
            identity[fragment_index * 14 + character_index] = (value % 26) as u8 + base;
            value /= 26;
        }
    }

    let checksum = kangaroo_twelve(public_key);
    let mut checksum_value =
        u32::from_le_bytes([checksum[0], checksum[1], checksum[2], 0]) & 0x3_FFFF;
    for character in &mut identity[56..] {
        *character = (checksum_value % 26) as u8 + base;
        checksum_value /= 26;
    }

    String::from_utf8(identity.to_vec()).expect("Qubic identity contains only ASCII letters")
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

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    const DOGE_DISPATCHER_PUBLIC_KEY: [u8; 32] = [
        0x25, 0x98, 0x6d, 0x38, 0xa6, 0x3d, 0xd6, 0x45, 0x0c, 0x07, 0x34, 0xd8, 0xaa, 0x47, 0x95,
        0x27, 0xd7, 0x2c, 0x0f, 0x9b, 0x3a, 0x86, 0x0a, 0xa8, 0x9e, 0x9f, 0xb1, 0xf3, 0xfd, 0x3d,
        0x1f, 0x95,
    ];
    const DOGE_DISPATCHER_IDENTITY: &str =
        "XPILPIJYHRBTACMMIRSJLIZWCXDBHWVEOTZBQFBXWEUXDZGGDEKDQPIEQKQK";

    #[test]
    fn kangaroo_twelve_matches_standard_empty_message_vector() {
        assert_eq!(
            kangaroo_twelve(&[]),
            [
                0x1a, 0xc2, 0xd4, 0x50, 0xfc, 0x3b, 0x42, 0x05, 0xd1, 0x9d, 0xa7, 0xbf, 0xca, 0x1b,
                0x37, 0x51, 0x3c, 0x08, 0x03, 0x57, 0x7a, 0xc7, 0x16, 0x7f, 0x06, 0xfe, 0x2c, 0xe1,
                0xf0, 0xef, 0x39, 0xe5,
            ]
        );
    }

    #[test]
    fn encodes_and_decodes_core_identity_vector() {
        assert_eq!(
            qubic_identity(&DOGE_DISPATCHER_PUBLIC_KEY, false),
            DOGE_DISPATCHER_IDENTITY
        );
        assert_eq!(
            parse_wallet_public_key(DOGE_DISPATCHER_IDENTITY),
            Ok(DOGE_DISPATCHER_PUBLIC_KEY)
        );
        assert_eq!(
            parse_wallet_public_key(&DOGE_DISPATCHER_IDENTITY.to_ascii_lowercase()),
            Ok(DOGE_DISPATCHER_PUBLIC_KEY)
        );
    }

    #[test]
    fn rejects_identity_with_invalid_checksum() {
        let mut identity = DOGE_DISPATCHER_IDENTITY.to_string();
        identity.replace_range(59..60, "A");

        assert_eq!(
            parse_wallet_public_key(&identity),
            Err("Identity checksum is invalid".to_string())
        );
    }

    #[test]
    fn transaction_id_is_canonical_lowercase_qubic_identity() {
        let digest = kangaroo_twelve(b"transaction");

        assert_eq!(
            tx_id_from_bytes(b"transaction"),
            qubic_identity(&digest, true)
        );
        assert_eq!(tx_id_from_bytes(b"transaction").len(), 60);
        assert!(
            tx_id_from_bytes(b"transaction")
                .bytes()
                .all(|byte| byte.is_ascii_lowercase())
        );

        let mut transaction = [0u8; 144];
        let first_id = tx_id_from_bytes(&transaction);
        transaction[143] = 1;
        assert_ne!(tx_id_from_bytes(&transaction), first_id);
    }
}
