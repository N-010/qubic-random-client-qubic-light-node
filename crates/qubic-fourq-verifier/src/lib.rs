//! Minimal Qubic SchnorrQ/FourQ signature verifier.
//!
//! Derived from SCAPI commit `3403107b8acfe0534faa809108906c1d6f2d8cee`
//! under the MIT license. The API deliberately accepts only fixed-size public
//! keys, message digests, and signatures.

#[allow(dead_code)]
mod four_q;

use four_q::consts::CURVE_ORDER;
use four_q::ops::{cofactor_clearing, decode, ecc_mul_double, encode, mod1271, point_setup};
use four_q::types::{PointAffine, PointExtproj};
use std::cmp::Ordering;
use tiny_keccak::{Hasher, IntoXof, KangarooTwelve, Xof};

/// Verifies a 64-byte Qubic SchnorrQ signature over an already-hashed digest.
#[must_use]
pub fn verify_digest(
    public_key: &[u8; 32],
    message_digest: &[u8; 32],
    signature: &[u8; 64],
) -> bool {
    if public_key[15] & 0x80 != 0 || signature[15] & 0x80 != 0 {
        return false;
    }

    let signature_scalar = bytes_to_words_32(
        signature[32..]
            .try_into()
            .expect("signature scalar has exact size"),
    );
    if signature_scalar.iter().rev().cmp(CURVE_ORDER.iter().rev()) != Ordering::Less {
        return false;
    }

    let public_key_words = bytes_to_words_32(public_key);
    if public_key_words[1] == 0
        && public_key_words[2] == 0
        && public_key_words[3] & 0x7FFF_FFFF_FFFF_FFFF == 0
    {
        return false;
    }

    let mut public_point = PointAffine::default();
    if !decode(public_key, &mut public_point) {
        return false;
    }
    if is_low_order(&public_point) {
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

fn bytes_to_words_32(bytes: &[u8; 32]) -> [u64; 4] {
    core::array::from_fn(|index| {
        let offset = index * 8;
        u64::from_le_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("word chunk has exact size"),
        )
    })
}

fn is_low_order(point: &PointAffine) -> bool {
    let mut cofactor_multiple = PointExtproj::default();
    point_setup(point, &mut cofactor_multiple);
    cofactor_clearing(&mut cofactor_multiple);
    mod1271(&mut cofactor_multiple.x[0]);
    mod1271(&mut cofactor_multiple.x[1]);
    cofactor_multiple.x.iter().flatten().all(|word| *word == 0)
}

#[cfg(feature = "test-signing")]
pub mod test_signing {
    //! Deterministic signing helpers available only to dependent test targets.

    use super::four_q::consts::{CURVE_ORDER, MONTGOMERY_R_PRIME, ONE};
    use super::four_q::ops::{ecc_mul_fixed, encode, montgomery_multiply_mod_order};
    use super::four_q::types::PointAffine;
    use tiny_keccak::{Hasher, IntoXof, KangarooTwelve, Xof};

    /// Produces the public key and SchnorrQ signature used by transaction tests.
    #[must_use]
    pub fn sign_digest(subseed: &[u8; 32], message_digest: &[u8; 32]) -> ([u8; 32], [u8; 64]) {
        let private_key = k12::<32>(subseed);
        let mut public_point = PointAffine::default();
        ecc_mul_fixed(&bytes_to_words::<4>(&private_key), &mut public_point);
        let mut public_key = [0; 32];
        encode(&mut public_point, &mut public_key);

        let secret = k12::<64>(subseed);
        let mut nonce_input = [0; 64];
        nonce_input[..32].copy_from_slice(&secret[32..]);
        nonce_input[32..].copy_from_slice(message_digest);
        let nonce_bytes = k12::<64>(&nonce_input);
        let mut nonce_point = PointAffine::default();
        ecc_mul_fixed(&bytes_to_words::<8>(&nonce_bytes), &mut nonce_point);

        let mut signature = [0; 64];
        encode(&mut nonce_point, &mut signature[..32]);

        let mut challenge_input = [0; 96];
        challenge_input[..32].copy_from_slice(&signature[..32]);
        challenge_input[32..64].copy_from_slice(&public_key);
        challenge_input[64..].copy_from_slice(message_digest);
        let challenge_bytes = k12::<64>(&challenge_input);

        let nonce = reduce_scalar(&bytes_to_words::<8>(&nonce_bytes));
        let private = reduce_scalar(&bytes_to_words::<8>(&secret));
        let challenge = reduce_scalar(&bytes_to_words::<8>(&challenge_bytes));
        let product = multiply_scalars(&private, &challenge);
        let scalar = subtract_scalars(&nonce, &product);
        for (index, word) in scalar.into_iter().enumerate() {
            let offset = 32 + index * 8;
            signature[offset..offset + 8].copy_from_slice(&word.to_le_bytes());
        }
        (public_key, signature)
    }

    fn k12<const N: usize>(input: &[u8]) -> [u8; N] {
        let mut output = [0; N];
        let mut hasher = KangarooTwelve::new(b"");
        hasher.update(input);
        hasher.into_xof().squeeze(&mut output);
        output
    }

    fn bytes_to_words<const N: usize>(bytes: &[u8]) -> [u64; N] {
        core::array::from_fn(|index| {
            let offset = index * 8;
            u64::from_le_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("word chunk has exact size"),
            )
        })
    }

    fn reduce_scalar(words: &[u64; 8]) -> [u64; 4] {
        let mut montgomery = [0; 4];
        montgomery_multiply_mod_order(words, &MONTGOMERY_R_PRIME, &mut montgomery);
        let mut reduced = [0; 4];
        montgomery_multiply_mod_order(&montgomery, &ONE, &mut reduced);
        reduced
    }

    fn multiply_scalars(left: &[u64; 4], right: &[u64; 4]) -> [u64; 4] {
        let mut left_montgomery = [0; 4];
        montgomery_multiply_mod_order(left, &MONTGOMERY_R_PRIME, &mut left_montgomery);
        let mut right_montgomery = [0; 4];
        montgomery_multiply_mod_order(right, &MONTGOMERY_R_PRIME, &mut right_montgomery);
        let mut product_montgomery = [0; 4];
        montgomery_multiply_mod_order(&left_montgomery, &right_montgomery, &mut product_montgomery);
        let mut product = [0; 4];
        montgomery_multiply_mod_order(&product_montgomery, &ONE, &mut product);
        product
    }

    fn subtract_scalars(left: &[u64; 4], right: &[u64; 4]) -> [u64; 4] {
        let mut output = [0; 4];
        let mut borrow = false;
        for index in 0..4 {
            let (difference, first_borrow) = left[index].overflowing_sub(right[index]);
            let (difference, second_borrow) = difference.overflowing_sub(u64::from(borrow));
            output[index] = difference;
            borrow = first_borrow || second_borrow;
        }
        if borrow {
            let mut carry = false;
            for index in 0..4 {
                let (sum, first_carry) = output[index].overflowing_add(CURVE_ORDER[index]);
                let (sum, second_carry) = sum.overflowing_add(u64::from(carry));
                output[index] = sum;
                carry = first_carry || second_carry;
            }
        }
        output
    }
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

    #[test]
    fn rejects_non_canonical_signature_scalars_from_core_regressions() {
        let mut at_order = SIGNATURE;
        for (index, word) in CURVE_ORDER.into_iter().enumerate() {
            let offset = 32 + index * 8;
            at_order[offset..offset + 8].copy_from_slice(&word.to_le_bytes());
        }
        assert!(!verify_digest(&PUBLIC_KEY, &DIGEST, &at_order));

        let original_scalar = bytes_to_words_32(
            SIGNATURE[32..]
                .try_into()
                .expect("signature scalar has exact size"),
        );
        let mut carry = false;
        let malleable_scalar: [u64; 4] = core::array::from_fn(|index| {
            let (sum, first_overflow) = original_scalar[index].overflowing_add(CURVE_ORDER[index]);
            let (sum, carry_overflow) = sum.overflowing_add(u64::from(carry));
            carry = first_overflow || carry_overflow;
            sum
        });
        let mut malleable = SIGNATURE;
        for (index, word) in malleable_scalar.into_iter().enumerate() {
            let offset = 32 + index * 8;
            malleable[offset..offset + 8].copy_from_slice(&word.to_le_bytes());
        }
        assert!(!verify_digest(&PUBLIC_KEY, &DIGEST, &malleable));
    }

    #[test]
    fn rejects_core_low_order_public_keys() {
        let weak_keys: [[u64; 4]; 4] = [
            [1, 0, 0, 0],
            [0, 0, 0, 0],
            [0, 0, 0, 0x8000_0000_0000_0000],
            [0xFFFF_FFFF_FFFF_FFFE, 0x7FFF_FFFF_FFFF_FFFF, 0, 0],
        ];

        for words in weak_keys {
            let mut public_key = [0; 32];
            for (index, word) in words.into_iter().enumerate() {
                let offset = index * 8;
                public_key[offset..offset + 8].copy_from_slice(&word.to_le_bytes());
            }
            assert!(!verify_digest(&public_key, &DIGEST, &SIGNATURE));
        }
    }

    #[cfg(feature = "test-signing")]
    #[test]
    fn test_signing_helper_produces_a_valid_signature() {
        let subseed = [0xA5; 32];
        let digest = [0x5A; 32];
        let (public_key, signature) = test_signing::sign_digest(&subseed, &digest);
        assert!(verify_digest(&public_key, &digest, &signature));
    }
}
