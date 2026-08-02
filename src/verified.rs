use crate::codec::parse_wallet_public_key;
use crate::frame::{
    BROADCAST_COMPUTORS_PAYLOAD_SIZE, BROADCAST_TICK_PAYLOAD_SIZE, BROADCAST_TICK_TYPE,
    COMPUTORS_PUBLIC_KEYS_SIZE, NUMBER_OF_COMPUTORS, SIGNATURE_SIZE,
};
use crate::types::TickStatus;
use qubic_fourq_verifier::verify_digest;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tiny_keccak::{Hasher, IntoXof, KangarooTwelve, Xof};

const ARBITRATOR_IDENTITY: &str = "AFZPUAIYVPNUYGJRQVLUKOPPVLHAZQTGLYAAUUNBXFTVTAMSBKQBLEIEPCVJ";
const TARGET_TICK_VOTE_SIGNATURE: u32 = 0x0002_42EC;
const QUORUM: usize = 451;
const TICK_UNSIGNED_SIZE: usize = BROADCAST_TICK_PAYLOAD_SIZE - SIGNATURE_SIZE;
const MAX_TICK_AGE_MILLIS: i64 = 120_000;
const MAX_TICK_FUTURE_SKEW_MILLIS: i64 = 30_000;
const MAX_UNCONFIRMED_TICKS: usize = 8;

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
pub(crate) enum TickVerification {
    Malformed,
    BadSignature,
    Deferred,
    AuthenticatedStale,
    Duplicate,
    Equivocation,
    Accepted,
    Quorum(TickStatus),
}

#[derive(Debug)]
struct VoteCandidate {
    signers: HashSet<u16>,
}

#[derive(Debug, Default)]
struct TickVotes {
    candidates: HashMap<[u8; 32], VoteCandidate>,
    computor_votes: HashMap<u16, [u8; 32]>,
    last_updated: u64,
}

#[derive(Debug)]
pub(crate) struct AuthenticatedComputors {
    epoch: u16,
    keys: Box<[[u8; 32]]>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TickContext {
    parsed: ParsedTick,
    public_key: [u8; 32],
    generation: u64,
    time_stale: bool,
}

impl TickContext {
    pub(crate) fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Debug, Default)]
struct TrustedState {
    computor_epoch: Option<u16>,
    computor_keys: Box<[[u8; 32]]>,
    computor_generation: u64,
    votes: BTreeMap<u32, TickVotes>,
    vote_clock: u64,
    status: Option<TickStatus>,
    status_digest: Option<[u8; 32]>,
}

#[derive(Debug, Default)]
pub(crate) struct TrustedNetworkState {
    inner: Mutex<TrustedState>,
}

impl TrustedNetworkState {
    pub(crate) fn status(&self) -> Option<TickStatus> {
        self.inner
            .lock()
            .expect("trusted network mutex should not be poisoned")
            .status
    }

    pub(crate) fn has_computors(&self) -> bool {
        self.inner
            .lock()
            .expect("trusted network mutex should not be poisoned")
            .computor_epoch
            .is_some()
    }

    pub(crate) fn parse_computors(
        &self,
        payload: &[u8],
    ) -> Result<AuthenticatedComputors, ComputorVerification> {
        if payload.len() != BROADCAST_COMPUTORS_PAYLOAD_SIZE {
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
            .get(signature_offset..)
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
        inner.computor_generation = inner.computor_generation.wrapping_add(1);
        inner.votes.clear();
        inner.vote_clock = 0;
        inner.status = None;
        inner.status_digest = None;
        ComputorVerification::Accepted
    }

    pub(crate) fn tick_context(
        &self,
        payload: &[u8],
        now_millis: i64,
    ) -> Result<TickContext, TickVerification> {
        let Some(parsed) = ParsedTick::parse(payload) else {
            return Err(TickVerification::Malformed);
        };
        if !has_acceptable_signature_score(&parsed.signature) {
            return Err(TickVerification::Malformed);
        }
        if parsed.timestamp_millis > now_millis.saturating_add(MAX_TICK_FUTURE_SKEW_MILLIS) {
            return Err(TickVerification::Deferred);
        }

        let inner = self
            .inner
            .lock()
            .expect("trusted network mutex should not be poisoned");
        match inner.computor_epoch {
            Some(epoch) if epoch == parsed.epoch => {}
            Some(epoch) if epoch > parsed.epoch => {
                return Err(TickVerification::AuthenticatedStale);
            }
            Some(_) | None => return Err(TickVerification::Deferred),
        }
        let Some(public_key) = inner.computor_keys.get(parsed.computor_index as usize) else {
            return Err(TickVerification::Malformed);
        };
        Ok(TickContext {
            parsed,
            public_key: *public_key,
            generation: inner.computor_generation,
            time_stale: parsed.timestamp_millis < now_millis.saturating_sub(MAX_TICK_AGE_MILLIS),
        })
    }

    pub(crate) fn tick_signature_is_valid(&self, payload: &[u8], context: TickContext) -> bool {
        verify_digest(
            &context.public_key,
            &tick_signature_digest(payload),
            &context.parsed.signature,
        )
    }

    pub(crate) fn apply_tick(&self, context: TickContext, payload: &[u8]) -> TickVerification {
        self.record_vote(context, payload)
    }

    fn record_vote(&self, context: TickContext, payload: &[u8]) -> TickVerification {
        let parsed = context.parsed;
        let body_digest = consensus_digest(payload);
        let mut inner = self
            .inner
            .lock()
            .expect("trusted network mutex should not be poisoned");
        if inner.computor_epoch != Some(parsed.epoch)
            || inner.computor_generation != context.generation
            || context.time_stale
            || inner.status.is_some_and(|status| parsed.tick < status.tick)
        {
            return TickVerification::AuthenticatedStale;
        }

        if let Some(previous) = inner
            .votes
            .get(&parsed.tick)
            .and_then(|votes| votes.computor_votes.get(&parsed.computor_index))
        {
            return if *previous == body_digest {
                TickVerification::Duplicate
            } else {
                TickVerification::Equivocation
            };
        }

        if !inner.votes.contains_key(&parsed.tick) && inner.votes.len() >= MAX_UNCONFIRMED_TICKS {
            let confirmed_tick = inner.status.map(|status| status.tick);
            let evict = inner
                .votes
                .iter()
                .filter(|(tick, _)| Some(**tick) != confirmed_tick)
                .min_by_key(|(tick, votes)| {
                    (votes.computor_votes.len(), votes.last_updated, **tick)
                })
                .map(|(tick, _)| *tick);
            if let Some(evict) = evict {
                inner.votes.remove(&evict);
            }
        }

        inner.vote_clock = inner.vote_clock.wrapping_add(1);
        let updated_at = inner.vote_clock;
        let (aligned, total) = {
            let tick_votes = inner.votes.entry(parsed.tick).or_default();
            tick_votes.last_updated = updated_at;
            tick_votes
                .computor_votes
                .insert(parsed.computor_index, body_digest);
            let candidate =
                tick_votes
                    .candidates
                    .entry(body_digest)
                    .or_insert_with(|| VoteCandidate {
                        signers: HashSet::with_capacity(QUORUM),
                    });
            candidate.signers.insert(parsed.computor_index);
            (candidate.signers.len(), tick_votes.computor_votes.len())
        };

        let establishes_quorum = aligned >= QUORUM
            && inner
                .status
                .is_none_or(|current| (parsed.epoch, parsed.tick) > (current.epoch, current.tick));
        if establishes_quorum {
            let status = tick_status(parsed.epoch, parsed.tick, aligned, total);
            inner.status = Some(status);
            inner.status_digest = Some(body_digest);
            inner.votes.retain(|tick, _| *tick >= status.tick);
            return TickVerification::Quorum(status);
        }

        if inner
            .status
            .is_some_and(|status| status.tick == parsed.tick)
            && let Some(status_digest) = inner.status_digest
            && let Some(tick_votes) = inner.votes.get(&parsed.tick)
        {
            let aligned = tick_votes
                .candidates
                .get(&status_digest)
                .map_or(0, |candidate| candidate.signers.len());
            let total = tick_votes.computor_votes.len();
            inner.status = Some(tick_status(parsed.epoch, parsed.tick, aligned, total));
        }

        TickVerification::Accepted
    }

    #[cfg(test)]
    pub(crate) fn verify_computors(&self, payload: &[u8]) -> ComputorVerification {
        let computors = match self.parse_computors(payload) {
            Ok(computors) => computors,
            Err(outcome) => return outcome,
        };
        if !self.computor_signature_is_valid(payload) {
            return ComputorVerification::BadSignature;
        }
        self.apply_computors(computors)
    }

    #[cfg(test)]
    pub(crate) fn verify_tick_at(&self, payload: &[u8], now_millis: i64) -> TickVerification {
        let context = match self.tick_context(payload, now_millis) {
            Ok(context) => context,
            Err(outcome) => return outcome,
        };
        if !self.tick_signature_is_valid(payload, context) {
            return TickVerification::BadSignature;
        }
        self.apply_tick(context, payload)
    }

    #[cfg(test)]
    fn record_vote_for_test(
        &self,
        parsed: ParsedTick,
        payload: &[u8],
        generation: u64,
    ) -> TickVerification {
        self.record_vote(
            TickContext {
                parsed,
                public_key: [0; 32],
                generation,
                time_stale: false,
            },
            payload,
        )
    }
}

fn tick_status(epoch: u16, tick: u32, aligned: usize, total: usize) -> TickStatus {
    TickStatus {
        epoch,
        tick,
        initial_tick: 0,
        tick_duration_ms: 0,
        aligned_votes: u16::try_from(aligned).expect("aligned votes fit into u16"),
        misaligned_votes: u16::try_from(total.saturating_sub(aligned))
            .expect("misaligned votes fit into u16"),
    }
}

fn has_acceptable_signature_score(signature: &[u8; SIGNATURE_SIZE]) -> bool {
    let signature_score = u32::from_be_bytes(
        signature[..4]
            .try_into()
            .expect("signature score has exact size"),
    );
    signature_score <= TARGET_TICK_VOTE_SIGNATURE
}

#[derive(Clone, Copy, Debug)]
struct ParsedTick {
    computor_index: u16,
    epoch: u16,
    tick: u32,
    timestamp_millis: i64,
    signature: [u8; SIGNATURE_SIZE],
}

impl ParsedTick {
    fn parse(payload: &[u8]) -> Option<Self> {
        if payload.len() != BROADCAST_TICK_PAYLOAD_SIZE {
            return None;
        }
        let wire_computor_index = u16::from_le_bytes(payload[0..2].try_into().ok()?);
        if wire_computor_index as usize >= NUMBER_OF_COMPUTORS {
            return None;
        }
        // Qubic Core XORs the index with the message type after signing and
        // restores it before hashing and selecting the computor public key.
        let computor_index = wire_computor_index ^ u16::from(BROADCAST_TICK_TYPE);
        if computor_index as usize >= NUMBER_OF_COMPUTORS {
            return None;
        }
        let millisecond = u16::from_le_bytes(payload[8..10].try_into().ok()?);
        let second = payload[10];
        let minute = payload[11];
        let hour = payload[12];
        let day = payload[13];
        let month = payload[14];
        let wire_year = payload[15];
        let year = 2000 + u16::from(wire_year);
        if millisecond > 999
            || second > 59
            || minute > 59
            || hour > 23
            || !(1..=12).contains(&month)
            || day == 0
            || day > days_in_month(year, month)
        {
            return None;
        }
        Some(Self {
            computor_index,
            epoch: u16::from_le_bytes(payload[2..4].try_into().ok()?),
            tick: u32::from_le_bytes(payload[4..8].try_into().ok()?),
            timestamp_millis: utc_millis(year, month, day, hour, minute, second, millisecond),
            signature: signature_from_slice(&payload[TICK_UNSIGNED_SIZE..])?,
        })
    }
}

fn consensus_digest(payload: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    // These are the fields Qubic Core compares for aligned current-tick
    // votes. Salted fields are computor-specific and must not split quorum.
    hasher.update(&payload[8..16]);
    hasher.update(&payload[32..128]);
    hasher.update(&payload[224..256]);
    *hasher.finalize().as_bytes()
}

fn tick_signature_digest(payload: &[u8]) -> [u8; 32] {
    let mut signed_body = [0u8; TICK_UNSIGNED_SIZE];
    signed_body.copy_from_slice(&payload[..TICK_UNSIGNED_SIZE]);
    signed_body[0] ^= BROADCAST_TICK_TYPE;
    k12(&signed_body)
}

fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: u16) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
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
        .map(|candidate| if is_leap_year(candidate) { 366 } else { 365 })
        .sum();
    let days_before_month: i64 = (1..month)
        .map(|candidate| i64::from(days_in_month(year, candidate)))
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
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn trusted_for_votes(epoch: u16) -> TrustedNetworkState {
        let trusted = TrustedNetworkState::default();
        {
            let mut inner = trusted.inner.lock().unwrap();
            inner.computor_epoch = Some(epoch);
            inner.computor_generation = 1;
        }
        trusted
    }

    fn parsed_tick(computor_index: u16, epoch: u16, tick: u32) -> ParsedTick {
        ParsedTick {
            computor_index,
            epoch,
            tick,
            timestamp_millis: 0,
            signature: [0; SIGNATURE_SIZE],
        }
    }

    #[test]
    fn rejects_old_tick_payload_size() {
        assert_eq!(
            ParsedTick::parse(&[0; 344]).is_none(),
            true,
            "the obsolete 344-byte layout must not be accepted"
        );
    }

    #[test]
    fn validates_calendar_bounds() {
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[13] = 29;
        payload[14] = 2;
        payload[15] = 4;
        assert!(ParsedTick::parse(&payload).is_some());
        payload[15] = 3;
        assert!(ParsedTick::parse(&payload).is_none());
        payload[15] = 100;
        assert!(ParsedTick::parse(&payload).is_none());
    }

    #[test]
    fn decodes_wire_computor_index_xor() {
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[0..2].copy_from_slice(&(42u16 ^ u16::from(BROADCAST_TICK_TYPE)).to_le_bytes());
        payload[13] = 1;
        payload[14] = 1;

        assert_eq!(ParsedTick::parse(&payload).unwrap().computor_index, 42);
    }

    #[test]
    fn consensus_digest_ignores_computor_specific_fields() {
        let mut first = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        first[8..16].fill(1);
        first[32..128].fill(2);
        first[224..256].fill(3);
        let mut second = first;
        second[0..2].copy_from_slice(&99u16.to_le_bytes());
        second[20..24].fill(4);
        second[28..32].fill(5);
        second[128..224].fill(6);

        assert_eq!(consensus_digest(&first), consensus_digest(&second));
        second[224] ^= 1;
        assert_ne!(consensus_digest(&first), consensus_digest(&second));
    }

    #[test]
    fn verifies_core_wire_index_signature_digest() {
        const PUBLIC_KEY: [u8; 32] = [
            31, 89, 13, 3, 230, 19, 189, 222, 211, 139, 76, 8, 32, 172, 68, 97, 95, 145, 175, 18,
            67, 89, 128, 179, 237, 227, 192, 140, 49, 90, 37, 68,
        ];
        const SIGNATURE: [u8; 64] = [
            116, 16, 247, 173, 230, 4, 114, 33, 228, 85, 163, 207, 187, 152, 221, 26, 73, 119, 136,
            33, 32, 4, 74, 137, 126, 239, 31, 114, 192, 131, 185, 67, 153, 83, 82, 98, 171, 19, 40,
            107, 91, 158, 204, 242, 98, 106, 171, 3, 122, 212, 11, 206, 20, 229, 129, 244, 29, 66,
            148, 213, 202, 220, 21, 0,
        ];
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[0..2].copy_from_slice(&u16::from(BROADCAST_TICK_TYPE).to_le_bytes());
        payload[13] = 1;
        payload[14] = 1;
        assert!(verify_digest(
            &PUBLIC_KEY,
            &tick_signature_digest(&payload),
            &SIGNATURE,
        ));
        payload[224] ^= 1;
        assert!(!verify_digest(
            &PUBLIC_KEY,
            &tick_signature_digest(&payload),
            &SIGNATURE,
        ));
    }

    #[test]
    fn signature_score_prefix_is_big_endian_like_core() {
        let mut accepted = [0; SIGNATURE_SIZE];
        accepted[..4].copy_from_slice(&TARGET_TICK_VOTE_SIGNATURE.to_be_bytes());
        assert!(has_acceptable_signature_score(&accepted));

        let mut rejected = [0; SIGNATURE_SIZE];
        rejected[..4].copy_from_slice(&TARGET_TICK_VOTE_SIGNATURE.saturating_add(1).to_be_bytes());
        assert!(!has_acceptable_signature_score(&rejected));
    }

    #[test]
    fn computor_payload_padding_is_rejected_before_signature_verification() {
        for padding in 1..=4 {
            let payload = vec![0; BROADCAST_COMPUTORS_PAYLOAD_SIZE + padding];
            assert_eq!(
                TrustedNetworkState::default().verify_computors(&payload),
                ComputorVerification::Malformed
            );
        }
    }

    #[test]
    fn same_epoch_conflict_does_not_replace_verified_state() {
        let trusted = TrustedNetworkState::default();
        let first_keys = vec![[1; 32]; NUMBER_OF_COMPUTORS].into_boxed_slice();
        assert_eq!(
            trusted.apply_computors(AuthenticatedComputors {
                epoch: 7,
                keys: first_keys.clone(),
            }),
            ComputorVerification::Accepted
        );
        {
            let mut inner = trusted.inner.lock().unwrap();
            inner.status = Some(tick_status(7, 50, QUORUM, QUORUM));
        }
        let generation = trusted.inner.lock().unwrap().computor_generation;

        assert_eq!(
            trusted.apply_computors(AuthenticatedComputors {
                epoch: 7,
                keys: vec![[2; 32]; NUMBER_OF_COMPUTORS].into_boxed_slice(),
            }),
            ComputorVerification::AuthenticatedConflict
        );
        let inner = trusted.inner.lock().unwrap();
        assert_eq!(inner.computor_keys, first_keys);
        assert_eq!(inner.computor_generation, generation);
        assert_eq!(inner.status.map(|status| status.tick), Some(50));
    }

    #[test]
    fn signer_can_vote_in_multiple_ticks_but_not_equivocate_within_one_tick() {
        let trusted = trusted_for_votes(7);
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[13] = 1;
        payload[14] = 1;

        assert_eq!(
            trusted.record_vote_for_test(parsed_tick(10, 7, 101), &payload, 1),
            TickVerification::Accepted
        );
        assert_eq!(
            trusted.record_vote_for_test(parsed_tick(10, 7, 100), &payload, 1),
            TickVerification::Accepted
        );
        payload[224] = 1;
        assert_eq!(
            trusted.record_vote_for_test(parsed_tick(10, 7, 100), &payload, 1),
            TickVerification::Equivocation
        );
    }

    #[test]
    fn ninth_unconfirmed_tick_evicts_the_weakest_oldest_bucket() {
        let trusted = trusted_for_votes(7);
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[13] = 1;
        payload[14] = 1;
        for tick in 1..=MAX_UNCONFIRMED_TICKS as u32 {
            for signer in 0..tick {
                let _ =
                    trusted.record_vote_for_test(parsed_tick(signer as u16, 7, tick), &payload, 1);
            }
        }
        let _ = trusted.record_vote_for_test(parsed_tick(500, 7, 99), &payload, 1);

        let inner = trusted.inner.lock().unwrap();
        assert_eq!(inner.votes.len(), MAX_UNCONFIRMED_TICKS);
        assert!(!inner.votes.contains_key(&1));
        assert!(inner.votes.contains_key(&99));
    }

    #[test]
    fn quorum_requires_distinct_computors_on_same_consensus_fields() {
        let trusted = trusted_for_votes(7);
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[13] = 1;
        payload[14] = 1;

        for computor_index in 0..10u16 {
            payload[224] = computor_index as u8 + 1;
            assert_eq!(
                trusted.record_vote_for_test(parsed_tick(computor_index, 7, 123), &payload, 1,),
                TickVerification::Accepted
            );
        }

        payload[224] = 0;
        let mut result = TickVerification::Accepted;
        for computor_index in 10..10 + QUORUM as u16 {
            result = trusted.record_vote_for_test(parsed_tick(computor_index, 7, 123), &payload, 1);
        }

        let TickVerification::Quorum(status) = result else {
            panic!("451 matching distinct computors should establish quorum");
        };
        assert_eq!(status.aligned_votes, QUORUM as u16);
        assert_eq!(status.misaligned_votes, 10);
        assert_eq!(trusted.status(), Some(status));
    }

    #[test]
    fn signer_churn_cannot_evict_other_computors_votes() {
        let trusted = trusted_for_votes(7);
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[13] = 1;
        payload[14] = 1;

        for computor_index in 1..QUORUM as u16 {
            assert_eq!(
                trusted.record_vote_for_test(parsed_tick(computor_index, 7, 9), &payload, 1),
                TickVerification::Accepted
            );
        }
        for tick in 10..=1_000 {
            let _ = trusted.record_vote_for_test(parsed_tick(0, 7, tick), &payload, 1);
        }
        let result = trusted.record_vote_for_test(parsed_tick(500, 7, 9), &payload, 1);

        assert!(matches!(result, TickVerification::Quorum(_)));
        let inner = trusted.inner.lock().unwrap();
        assert!(inner.votes.contains_key(&9));
        assert!(inner.votes.len() <= MAX_UNCONFIRMED_TICKS);
    }

    #[test]
    fn distant_quorum_catches_up_but_one_future_vote_does_not() {
        let trusted = trusted_for_votes(7);
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[13] = 1;
        payload[14] = 1;
        for computor_index in 0..QUORUM as u16 {
            let _ = trusted.record_vote_for_test(parsed_tick(computor_index, 7, 10), &payload, 1);
        }
        assert_eq!(trusted.status().map(|status| status.tick), Some(10));

        assert_eq!(
            trusted.record_vote_for_test(parsed_tick(500, 7, 1_000), &payload, 1),
            TickVerification::Accepted
        );
        assert_eq!(trusted.status().map(|status| status.tick), Some(10));

        for computor_index in 0..QUORUM as u16 {
            let _ =
                trusted.record_vote_for_test(parsed_tick(computor_index, 7, 1_000), &payload, 1);
        }
        assert_eq!(trusted.status().map(|status| status.tick), Some(1_000));
    }

    #[test]
    fn misaligned_votes_refresh_after_quorum() {
        let trusted = trusted_for_votes(7);
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[13] = 1;
        payload[14] = 1;
        for computor_index in 0..QUORUM as u16 {
            let _ = trusted.record_vote_for_test(parsed_tick(computor_index, 7, 50), &payload, 1);
        }
        assert_eq!(trusted.status().unwrap().misaligned_votes, 0);

        payload[224] = 1;
        assert_eq!(
            trusted.record_vote_for_test(parsed_tick(500, 7, 50), &payload, 1),
            TickVerification::Accepted
        );
        assert_eq!(trusted.status().unwrap().misaligned_votes, 1);
    }

    #[test]
    fn stale_generation_cannot_record_a_verified_vote() {
        let trusted = trusted_for_votes(7);
        trusted.inner.lock().unwrap().computor_generation = 2;
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[13] = 1;
        payload[14] = 1;

        assert_eq!(
            trusted.record_vote_for_test(parsed_tick(0, 7, 50), &payload, 1),
            TickVerification::AuthenticatedStale
        );
        assert!(trusted.inner.lock().unwrap().votes.is_empty());
    }

    #[test]
    fn tick_timestamp_window_is_enforced_at_millisecond_boundaries() {
        let trusted = TrustedNetworkState::default();
        let mut payload = [0u8; BROADCAST_TICK_PAYLOAD_SIZE];
        payload[13] = 1;
        payload[14] = 1;
        payload[15] = 25;
        let timestamp = ParsedTick::parse(&payload).unwrap().timestamp_millis;

        assert_eq!(
            trusted.verify_tick_at(&payload, timestamp + MAX_TICK_AGE_MILLIS + 1),
            TickVerification::Deferred
        );
        assert_eq!(
            trusted.verify_tick_at(&payload, timestamp - MAX_TICK_FUTURE_SKEW_MILLIS - 1),
            TickVerification::Deferred
        );
        assert_eq!(
            trusted.verify_tick_at(&payload, timestamp + MAX_TICK_AGE_MILLIS),
            TickVerification::Deferred,
            "the boundary passes freshness and then defers because no computor list is installed"
        );
    }
}
