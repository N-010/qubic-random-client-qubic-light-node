//! Minimal Qubic SchnorrQ/FourQ signature verifier.
//!
//! Derived from SCAPI commit `3403107b8acfe0534faa809108906c1d6f2d8cee`
//! under the MIT license. The API deliberately accepts only fixed-size public
//! keys, message digests, and signatures.

#[allow(dead_code)]
mod four_q;

use four_q::ops::{decode, ecc_mul_double, encode};
use four_q::types::PointAffine;
use tiny_keccak::{Hasher, IntoXof, KangarooTwelve, Xof};

/// Verifies a 64-byte Qubic SchnorrQ signature over an already-hashed digest.
#[must_use]
pub fn verify_digest(
    public_key: &[u8; 32],
    message_digest: &[u8; 32],
    signature: &[u8; 64],
) -> bool {
    if public_key[15] & 0x80 != 0
        || signature[15] & 0x80 != 0
        || signature[62] & 0xC0 != 0
        || signature[63] != 0
    {
        return false;
    }

    let mut public_point = PointAffine::default();
    if !decode(public_key, &mut public_point) {
        return false;
    }

    let mut challenge_input = [0u8; 96];
    challenge_input[..32].copy_from_slice(&signature[..32]);
    challenge_input[32..64].copy_from_slice(public_key);
    challenge_input[64..].copy_from_slice(message_digest);

    let mut challenge_bytes = [0u8; 64];
    let mut hasher = KangarooTwelve::new(b"");
    hasher.update(&challenge_input);
    hasher.into_xof().squeeze(&mut challenge_bytes);

    let mut signature_words = bytes_to_words(signature);
    let mut challenge_words = bytes_to_words(&challenge_bytes);
    if !ecc_mul_double(
        &mut signature_words[4..],
        &mut challenge_words,
        &mut public_point,
    ) {
        return false;
    }

    let mut encoded_point = [0u8; 32];
    encode(&mut public_point, &mut encoded_point);
    signature[..32] == encoded_point
}

fn bytes_to_words(bytes: &[u8; 64]) -> [u64; 8] {
    core::array::from_fn(|index| {
        let offset = index * 8;
        u64::from_le_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("word chunk has exact size"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PUBLIC_KEY: [u8; 32] = [
        31, 89, 13, 3, 230, 19, 189, 222, 211, 139, 76, 8, 32, 172, 68, 97, 95, 145, 175, 18, 67,
        89, 128, 179, 237, 227, 192, 140, 49, 90, 37, 68,
    ];
    const DIGEST: [u8; 32] = [
        11, 60, 219, 230, 234, 238, 25, 35, 41, 53, 209, 37, 204, 58, 151, 184, 66, 77, 185, 82,
        154, 146, 140, 32, 204, 63, 90, 227, 241, 158, 84, 169,
    ];
    const SIGNATURE: [u8; 64] = [
        116, 16, 247, 173, 230, 4, 114, 33, 228, 85, 163, 207, 187, 152, 221, 26, 73, 119, 136, 33,
        32, 4, 74, 137, 126, 239, 31, 114, 192, 131, 185, 67, 153, 83, 82, 98, 171, 19, 40, 107,
        91, 158, 204, 242, 98, 106, 171, 3, 122, 212, 11, 206, 20, 229, 129, 244, 29, 66, 148, 213,
        202, 220, 21, 0,
    ];

    #[test]
    fn accepts_scapi_reference_vector() {
        assert!(verify_digest(&PUBLIC_KEY, &DIGEST, &SIGNATURE));
    }

    #[test]
    fn rejects_mutated_key_digest_and_signature() {
        for index in 0..PUBLIC_KEY.len() {
            let mut public_key = PUBLIC_KEY;
            public_key[index] ^= 1;
            assert!(!verify_digest(&public_key, &DIGEST, &SIGNATURE));
        }
        for index in 0..DIGEST.len() {
            let mut digest = DIGEST;
            digest[index] ^= 1;
            assert!(!verify_digest(&PUBLIC_KEY, &digest, &SIGNATURE));
        }
        for index in 0..SIGNATURE.len() {
            let mut signature = SIGNATURE;
            signature[index] ^= 1;
            assert!(!verify_digest(&PUBLIC_KEY, &DIGEST, &signature));
        }
    }
}
