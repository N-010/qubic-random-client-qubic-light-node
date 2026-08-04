use crate::codec::parse_wallet_public_key;
use crate::frame::{
    BROADCAST_COMPUTORS_PAYLOAD_SIZE, BROADCAST_FUTURE_TICK_DATA_TYPE, COMPUTORS_PUBLIC_KEYS_SIZE,
    NUMBER_OF_COMPUTORS, NUMBER_OF_TRANSACTIONS_PER_TICK, SIGNATURE_SIZE, TICK_DATA_PAYLOAD_SIZE,
    TICK_DATA_TRANSACTION_DIGESTS_OFFSET, TICK_DATA_UNSIGNED_SIZE,
};
use qubic_fourq_verifier::verify_digest;
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tiny_keccak::{Hasher, IntoXof, KangarooTwelve, Xof};

const ARBITRATOR_IDENTITY: &str = "AFZPUAIYVPNUYGJRQVLUKOPPVLHAZQTGLYAAUUNBXFTVTAMSBKQBLEIEPCVJ";
const MAX_TICK_DATA_FUTURE_SKEW_MILLIS: i64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComputorVerification {
    Malformed,
    BadSignature,
    AuthenticatedStale,
    AuthenticatedConflict,
    Duplicate,
    Accepted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TickDataVerification {
    Malformed,
    Unavailable,
    BadSignature,
    Authenticated { has_transactions: bool },
}

#[derive(Debug)]
pub(crate) struct AuthenticatedComputors {
    epoch: u16,
    keys: Box<[[u8; 32]]>,
}

#[derive(Debug, Default)]
struct TrustedState {
    computor_epoch: Option<u16>,
    computor_keys: Box<[[u8; 32]]>,
}

#[derive(Debug, Default)]
pub(crate) struct TrustedNetworkState {
    inner: Mutex<TrustedState>,
}

impl TrustedNetworkState {
    pub(crate) fn has_computors(&self) -> bool {
        self.inner
            .lock()
            .expect("trusted network mutex should not be poisoned")
            .computor_epoch
            .is_some()
    }

    pub(crate) fn verify_tick_data(
        &self,
        payload: &[u8],
        requested_tick: u32,
    ) -> TickDataVerification {
        if payload.len() != TICK_DATA_PAYLOAD_SIZE {
            return TickDataVerification::Malformed;
        }

        let computor_index = u16::from_le_bytes(
            payload[0..2]
                .try_into()
                .expect("validated TickData has a computor index"),
        );
        let epoch = u16::from_le_bytes(
            payload[2..4]
                .try_into()
                .expect("validated TickData has an epoch"),
        );
        let tick = u32::from_le_bytes(
            payload[4..8]
                .try_into()
                .expect("validated TickData has a tick"),
        );
        let millisecond = u16::from_le_bytes(
            payload[8..10]
                .try_into()
                .expect("validated TickData has milliseconds"),
        );
        let second = payload[10];
        let minute = payload[11];
        let hour = payload[12];
        let day = payload[13];
        let month = payload[14];
        let wire_year = payload[15];
        if tick != requested_tick
            || usize::from(computor_index) >= NUMBER_OF_COMPUTORS
            || tick % NUMBER_OF_COMPUTORS as u32 != u32::from(computor_index)
            || millisecond > 999
            || second > 59
            || minute > 59
            || hour > 23
            || !(1..=12).contains(&month)
            || day == 0
            || day > days_in_month(wire_year, month)
        {
            return TickDataVerification::Malformed;
        }
        let timestamp_millis = utc_millis(
            2000 + u16::from(wire_year),
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
        );
        if timestamp_millis > unix_time_millis().saturating_add(MAX_TICK_DATA_FUTURE_SKEW_MILLIS) {
            return TickDataVerification::Malformed;
        }

        let digests_end =
            TICK_DATA_TRANSACTION_DIGESTS_OFFSET + NUMBER_OF_TRANSACTIONS_PER_TICK * 32;
        let zero_digest = [0u8; 32];
        let mut non_zero_digests = HashSet::new();
        for digest in payload[TICK_DATA_TRANSACTION_DIGESTS_OFFSET..digests_end].chunks_exact(32) {
            let digest: [u8; 32] = digest
                .try_into()
                .expect("TickData transaction digest has an exact size");
            if digest != zero_digest && !non_zero_digests.insert(digest) {
                return TickDataVerification::Malformed;
            }
        }

        let signature = signature_from_slice(&payload[TICK_DATA_UNSIGNED_SIZE..])
            .expect("validated TickData has an exact-size signature");
        let public_key = {
            let inner = self
                .inner
                .lock()
                .expect("trusted network mutex should not be poisoned");
            if inner.computor_epoch != Some(epoch) {
                return TickDataVerification::Unavailable;
            }
            let Some(public_key) = inner.computor_keys.get(usize::from(computor_index)) else {
                return TickDataVerification::Unavailable;
            };
            *public_key
        };

        let mut signed_body = payload[..TICK_DATA_UNSIGNED_SIZE].to_vec();
        signed_body[0] ^= BROADCAST_FUTURE_TICK_DATA_TYPE;
        if !verify_digest(&public_key, &k12(&signed_body), &signature) {
            return TickDataVerification::BadSignature;
        }

        TickDataVerification::Authenticated {
            has_transactions: !non_zero_digests.is_empty(),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_computor_key_for_test(
        &self,
        epoch: u16,
        computor_index: u16,
        public_key: [u8; 32],
    ) {
        let mut inner = self
            .inner
            .lock()
            .expect("trusted network mutex should not be poisoned");
        inner.computor_epoch = Some(epoch);
        inner.computor_keys = vec![[0; 32]; NUMBER_OF_COMPUTORS].into_boxed_slice();
        inner.computor_keys[usize::from(computor_index)] = public_key;
    }

    pub(crate) fn parse_computors(
        &self,
        payload: &[u8],
    ) -> Result<AuthenticatedComputors, ComputorVerification> {
        if !(BROADCAST_COMPUTORS_PAYLOAD_SIZE..=BROADCAST_COMPUTORS_PAYLOAD_SIZE + 4)
            .contains(&payload.len())
        {
            return Err(ComputorVerification::Malformed);
        }

        let signature_offset = 2 + COMPUTORS_PUBLIC_KEYS_SIZE;
        if payload[2..signature_offset]
            .chunks_exact(32)
            .any(|key| key.iter().all(|byte| *byte == 0))
        {
            return Err(ComputorVerification::Malformed);
        }

        let epoch = u16::from_le_bytes([payload[0], payload[1]]);
        let keys = payload[2..signature_offset]
            .chunks_exact(32)
            .map(|key| key.try_into().expect("computor key chunk has exact size"))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(AuthenticatedComputors { epoch, keys })
    }

    pub(crate) fn computor_signature_is_valid(&self, payload: &[u8]) -> bool {
        let signature_offset = 2 + COMPUTORS_PUBLIC_KEYS_SIZE;
        let Some(signed_payload) = payload.get(..signature_offset) else {
            return false;
        };
        let Some(signature) = payload
            .get(signature_offset..signature_offset + SIGNATURE_SIZE)
            .and_then(signature_from_slice)
        else {
            return false;
        };
        let Ok(arbitrator) = parse_wallet_public_key(ARBITRATOR_IDENTITY) else {
            return false;
        };
        verify_digest(&arbitrator, &k12(signed_payload), &signature)
    }

    pub(crate) fn apply_computors(
        &self,
        computors: AuthenticatedComputors,
    ) -> ComputorVerification {
        let mut inner = self
            .inner
            .lock()
            .expect("trusted network mutex should not be poisoned");
        if inner
            .computor_epoch
            .is_some_and(|current| computors.epoch < current)
        {
            return ComputorVerification::AuthenticatedStale;
        }
        if inner.computor_epoch == Some(computors.epoch) {
            return if inner.computor_keys == computors.keys {
                ComputorVerification::Duplicate
            } else {
                ComputorVerification::AuthenticatedConflict
            };
        }

        inner.computor_epoch = Some(computors.epoch);
        inner.computor_keys = computors.keys;
        ComputorVerification::Accepted
    }
}

fn days_in_month(wire_year: u8, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if wire_year.is_multiple_of(4) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_qubic_leap_year(year: u16) -> bool {
    year.is_multiple_of(4)
}

fn utc_millis(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    millisecond: u16,
) -> i64 {
    let days_before_year: i64 = (1970..year)
        .map(|candidate| {
            if is_qubic_leap_year(candidate) {
                366
            } else {
                365
            }
        })
        .sum();
    let days_before_month: i64 = (1..month)
        .map(|candidate| i64::from(days_in_month((year - 2000) as u8, candidate)))
        .sum();
    let days = days_before_year + days_before_month + i64::from(day - 1);
    (((days * 24 + i64::from(hour)) * 60 + i64::from(minute)) * 60 + i64::from(second)) * 1_000
        + i64::from(millisecond)
}

pub(crate) fn unix_time_millis() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn signature_from_slice(bytes: &[u8]) -> Option<[u8; SIGNATURE_SIZE]> {
    bytes.get(..SIGNATURE_SIZE)?.try_into().ok()
}

fn k12(bytes: &[u8]) -> [u8; 32] {
    let mut digest = [0u8; 32];
    let mut hasher = KangarooTwelve::new(b"");
    hasher.update(bytes);
    hasher.into_xof().squeeze(&mut digest);
    digest
}

#[cfg(test)]
pub(crate) fn signed_tick_data_for_test(
    epoch: u16,
    tick: u32,
    has_transactions: bool,
) -> (Vec<u8>, u16, [u8; 32]) {
    let computor_index = (tick % NUMBER_OF_COMPUTORS as u32) as u16;
    let subseed = [7; 32];
    let (public_key, _) = qubic_fourq_verifier::test_signing::sign_digest(&subseed, &[0; 32]);
    let mut payload = vec![0; TICK_DATA_PAYLOAD_SIZE];
    payload[0..2].copy_from_slice(&computor_index.to_le_bytes());
    payload[2..4].copy_from_slice(&epoch.to_le_bytes());
    payload[4..8].copy_from_slice(&tick.to_le_bytes());
    payload[8..10].copy_from_slice(&123u16.to_le_bytes());
    payload[10..16].copy_from_slice(&[4, 3, 2, 1, 8, 26]);
    if has_transactions {
        payload[TICK_DATA_TRANSACTION_DIGESTS_OFFSET..TICK_DATA_TRANSACTION_DIGESTS_OFFSET + 32]
            .fill(9);
    }
    let mut signed_body = payload[..TICK_DATA_UNSIGNED_SIZE].to_vec();
    signed_body[0] ^= BROADCAST_FUTURE_TICK_DATA_TYPE;
    let (_, signature) =
        qubic_fourq_verifier::test_signing::sign_digest(&subseed, &k12(&signed_body));
    payload[TICK_DATA_UNSIGNED_SIZE..].copy_from_slice(&signature);
    (payload, computor_index, public_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn tick_data_verification_distinguishes_empty_and_non_empty_ticks() {
        let epoch = 301;
        let tick = 12_345;
        for (has_transactions, expected) in [(false, false), (true, true)] {
            let (payload, computor_index, public_key) =
                signed_tick_data_for_test(epoch, tick, has_transactions);
            let trusted = TrustedNetworkState::default();
            trusted.set_computor_key_for_test(epoch, computor_index, public_key);

            assert_eq!(
                trusted.verify_tick_data(&payload, tick),
                TickDataVerification::Authenticated {
                    has_transactions: expected
                }
            );
        }
    }

    #[test]
    fn tick_data_verification_rejects_wrong_tick_duplicate_digest_and_signature() {
        let epoch = 301;
        let tick = 12_345;
        let (payload, computor_index, public_key) = signed_tick_data_for_test(epoch, tick, true);
        let trusted = TrustedNetworkState::default();
        trusted.set_computor_key_for_test(epoch, computor_index, public_key);

        assert_eq!(
            trusted.verify_tick_data(&payload, tick + 1),
            TickDataVerification::Malformed
        );

        let mut duplicate = payload.clone();
        let first_digest = duplicate
            [TICK_DATA_TRANSACTION_DIGESTS_OFFSET..TICK_DATA_TRANSACTION_DIGESTS_OFFSET + 32]
            .to_vec();
        duplicate
            [TICK_DATA_TRANSACTION_DIGESTS_OFFSET + 32..TICK_DATA_TRANSACTION_DIGESTS_OFFSET + 64]
            .copy_from_slice(&first_digest);
        assert_eq!(
            trusted.verify_tick_data(&duplicate, tick),
            TickDataVerification::Malformed
        );

        let mut future_timestamp = payload.clone();
        future_timestamp[15] = u8::MAX;
        assert_eq!(
            trusted.verify_tick_data(&future_timestamp, tick),
            TickDataVerification::Malformed
        );

        let mut bad_signature = payload;
        bad_signature[TICK_DATA_UNSIGNED_SIZE] ^= 1;
        assert_eq!(
            trusted.verify_tick_data(&bad_signature, tick),
            TickDataVerification::BadSignature
        );
        assert_eq!(
            TrustedNetworkState::default().verify_tick_data(&bad_signature, tick),
            TickDataVerification::Unavailable
        );
    }

    #[test]
    fn computor_payload_accepts_core_struct_padding() {
        for padding in 1..=4 {
            let mut payload = vec![0; BROADCAST_COMPUTORS_PAYLOAD_SIZE + padding];
            payload[2..2 + COMPUTORS_PUBLIC_KEYS_SIZE].fill(1);
            assert!(
                TrustedNetworkState::default()
                    .parse_computors(&payload)
                    .is_ok()
            );
        }
    }

    #[test]
    fn same_epoch_conflict_does_not_replace_verified_keys() {
        let trusted = TrustedNetworkState::default();
        let first_keys = vec![[1; 32]; NUMBER_OF_COMPUTORS].into_boxed_slice();
        assert_eq!(
            trusted.apply_computors(AuthenticatedComputors {
                epoch: 7,
                keys: first_keys.clone(),
            }),
            ComputorVerification::Accepted
        );
        assert_eq!(
            trusted.apply_computors(AuthenticatedComputors {
                epoch: 7,
                keys: vec![[2; 32]; NUMBER_OF_COMPUTORS].into_boxed_slice(),
            }),
            ComputorVerification::AuthenticatedConflict
        );

        let inner = trusted.inner.lock().unwrap();
        assert_eq!(inner.computor_keys, first_keys);
    }
}
