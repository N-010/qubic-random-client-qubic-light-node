use crate::frame::is_bogon;
use bytes::Bytes;
use rand::seq::{IteratorRandom, SliceRandom};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

const MAX_PEER_COOLDOWN: Duration = Duration::from_secs(5 * 60);
const HEALTHY_SESSION_DURATION: Duration = Duration::from_secs(60);
pub(crate) const PEER_OUTBOUND_QUEUE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PeerOrigin {
    Manual,
    Dns,
    Gossip(Ipv4Addr),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PeerAdmission {
    pub(crate) inserted: bool,
    pub(crate) retained: bool,
    pub(crate) promoted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutboundAdmissionError {
    FrameTooLarge,
    PeerBudgetExhausted,
    GlobalBudgetExhausted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DisconnectReason {
    Administrative,
    PeerQueueFull,
    PeerQueueClosed,
    ProtocolViolation,
}

impl DisconnectReason {
    pub(crate) fn penalizes_peer(self) -> bool {
        match self {
            Self::Administrative => false,
            Self::PeerQueueFull | Self::PeerQueueClosed | Self::ProtocolViolation => true,
        }
    }
}

impl std::fmt::Display for DisconnectReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Administrative => formatter.write_str("administrative disconnect"),
            Self::PeerQueueFull => formatter.write_str("peer outbound queue is full"),
            Self::PeerQueueClosed => formatter.write_str("peer outbound queue is closed"),
            Self::ProtocolViolation => formatter.write_str("peer protocol violation"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct OutboundFrame {
    pub(crate) bytes: Bytes,
    _peer_bytes: OwnedSemaphorePermit,
    _global_bytes: OwnedSemaphorePermit,
}

impl OutboundFrame {
    pub(crate) fn try_new(
        bytes: Bytes,
        peer_budget: Arc<Semaphore>,
        global_budget: Arc<Semaphore>,
    ) -> Result<Self, OutboundAdmissionError> {
        let permits =
            u32::try_from(bytes.len()).map_err(|_| OutboundAdmissionError::FrameTooLarge)?;
        let peer_bytes = peer_budget
            .try_acquire_many_owned(permits)
            .map_err(|_| OutboundAdmissionError::PeerBudgetExhausted)?;
        let global_bytes = global_budget
            .try_acquire_many_owned(permits)
            .map_err(|_| OutboundAdmissionError::GlobalBudgetExhausted)?;
        Ok(Self {
            bytes,
            _peer_bytes: peer_bytes,
            _global_bytes: global_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct PeerHealth {
    consecutive_failures: u32,
    retry_after: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PeerPoolStats {
    pub(crate) known: usize,
    pub(crate) dialable: usize,
    pub(crate) cooldown: usize,
    pub(crate) pending: usize,
    pub(crate) outgoing: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct RelayTarget {
    pub(crate) peer_id: u64,
    pub(crate) tx: mpsc::Sender<OutboundFrame>,
    pub(crate) byte_budget: Arc<Semaphore>,
}

#[derive(Clone, Debug)]
struct Session {
    transport_remote: SocketAddrV4,
    dial_endpoint: SocketAddrV4,
    connected_at: Instant,
    tx: mpsc::Sender<OutboundFrame>,
    byte_budget: Arc<Semaphore>,
    disconnect_tx: watch::Sender<bool>,
}

#[derive(Debug)]
pub(crate) struct NodeState {
    sessions: HashMap<u64, Session>,
    connected_ip_refcount: HashMap<Ipv4Addr, usize>,
    known_peers: HashSet<SocketAddrV4>,
    known_peers_order: VecDeque<SocketAddrV4>,
    peer_origins: HashMap<SocketAddrV4, PeerOrigin>,
    peer_health: HashMap<SocketAddrV4, PeerHealth>,
    pending_dials: HashSet<SocketAddrV4>,
    disconnect_reasons: HashMap<u64, DisconnectReason>,
    reconnect_interval: Duration,
    max_known_peers: usize,
    next_peer_id: u64,
}

impl NodeState {
    pub(crate) fn new(max_known_peers: usize, seed_peers: &[SocketAddrV4]) -> Self {
        let mut node_state = Self {
            sessions: HashMap::new(),
            connected_ip_refcount: HashMap::new(),
            known_peers: HashSet::new(),
            known_peers_order: VecDeque::new(),
            peer_origins: HashMap::new(),
            peer_health: HashMap::new(),
            pending_dials: HashSet::new(),
            disconnect_reasons: HashMap::new(),
            reconnect_interval: Duration::from_secs(2),
            max_known_peers,
            next_peer_id: 1,
        };
        for peer in seed_peers {
            let _ = node_state.add_peer(*peer, PeerOrigin::Manual);
        }
        node_state
    }

    pub(crate) fn outgoing_count(&self) -> usize {
        self.sessions.len()
    }

    pub(crate) fn set_reconnect_interval(&mut self, reconnect_interval: Duration) {
        self.reconnect_interval = reconnect_interval;
    }

    pub(crate) fn remote_ip(&self, peer_id: u64) -> Option<Ipv4Addr> {
        self.sessions
            .get(&peer_id)
            .map(|session| *session.transport_remote.ip())
    }

    fn is_ip_connected(&self, ip: Ipv4Addr) -> bool {
        self.connected_ip_refcount.contains_key(&ip)
    }

    pub(crate) fn register_session(
        &mut self,
        remote: SocketAddrV4,
        _outbound: bool,
        tx: mpsc::Sender<OutboundFrame>,
        disconnect_tx: watch::Sender<bool>,
        _peer_port: u16,
    ) -> Option<u64> {
        let remote_ip = *remote.ip();
        if self.is_ip_connected(remote_ip) {
            return None;
        }

        let peer_id = self.next_peer_id;
        self.next_peer_id = self.next_peer_id.wrapping_add(1);

        let dial_endpoint = remote;
        self.sessions.insert(
            peer_id,
            Session {
                transport_remote: remote,
                dial_endpoint,
                connected_at: Instant::now(),
                tx,
                byte_budget: Arc::new(Semaphore::new(PEER_OUTBOUND_QUEUE_BYTES)),
                disconnect_tx,
            },
        );
        *self.connected_ip_refcount.entry(remote_ip).or_insert(0) += 1;
        let connected_peer = dial_endpoint;
        if !self.known_peers.contains(&connected_peer) {
            let _ = self.add_peer(connected_peer, PeerOrigin::Gossip(remote_ip));
        }
        self.pending_dials.remove(&dial_endpoint);

        Some(peer_id)
    }

    pub(crate) fn unregister_session(&mut self, peer_id: u64) -> Option<SocketAddrV4> {
        let session = self.sessions.remove(&peer_id)?;
        let remote_ip = *session.transport_remote.ip();
        let remove_ip_entry = if let Some(counter) = self.connected_ip_refcount.get_mut(&remote_ip)
        {
            *counter -= 1;
            *counter == 0
        } else {
            false
        };
        if remove_ip_entry {
            self.connected_ip_refcount.remove(&remote_ip);
        }
        if session.connected_at.elapsed() >= HEALTHY_SESSION_DURATION {
            self.peer_health.remove(&session.dial_endpoint);
        }
        self.enforce_known_peer_limit();
        Some(session.dial_endpoint)
    }

    pub(crate) fn disconnect_session_with_reason(
        &mut self,
        peer_id: u64,
        reason: DisconnectReason,
    ) -> Option<SocketAddrV4> {
        let now = Instant::now();
        let session = self.sessions.get(&peer_id)?;
        let disconnect_tx = session.disconnect_tx.clone();
        let dial_endpoint = session.dial_endpoint;
        let connected_at = session.connected_at;
        self.disconnect_reasons.insert(peer_id, reason);
        let remote = self.unregister_session(peer_id)?;
        if reason.penalizes_peer() {
            if now.saturating_duration_since(connected_at) >= HEALTHY_SESSION_DURATION {
                self.peer_health.remove(&dial_endpoint);
            }
            self.record_peer_failure(dial_endpoint, self.reconnect_interval, now);
        }
        let _ = disconnect_tx.send(true);
        Some(remote)
    }

    pub(crate) fn take_disconnect_reason(&mut self, peer_id: u64) -> Option<DisconnectReason> {
        self.disconnect_reasons.remove(&peer_id)
    }

    pub(crate) fn collect_all_targets(&self, limit: usize) -> Vec<RelayTarget> {
        let limit = limit.min(self.sessions.len());
        self.sessions
            .iter()
            .map(|(peer_id, session)| RelayTarget {
                peer_id: *peer_id,
                tx: session.tx.clone(),
                byte_budget: Arc::clone(&session.byte_budget),
            })
            .choose_multiple(&mut rand::rng(), limit)
    }

    pub(crate) fn target(&self, peer_id: u64) -> Option<RelayTarget> {
        let session = self.sessions.get(&peer_id)?;
        Some(RelayTarget {
            peer_id,
            tx: session.tx.clone(),
            byte_budget: Arc::clone(&session.byte_budget),
        })
    }

    #[cfg(test)]
    fn set_connected_at(&mut self, peer_id: u64, connected_at: Instant) {
        self.sessions
            .get_mut(&peer_id)
            .expect("test session should exist")
            .connected_at = connected_at;
    }

    #[cfg(test)]
    pub(crate) fn add_discovered_peer(&mut self, peer: SocketAddrV4) -> bool {
        self.add_peer(peer, PeerOrigin::Gossip(Ipv4Addr::UNSPECIFIED))
            .inserted
    }

    pub(crate) fn add_gossip_peer(
        &mut self,
        peer: SocketAddrV4,
        source_ip: Ipv4Addr,
    ) -> PeerAdmission {
        self.add_peer(peer, PeerOrigin::Gossip(source_ip))
    }

    pub(crate) fn add_dns_peer(&mut self, peer: SocketAddrV4) -> PeerAdmission {
        self.add_peer(peer, PeerOrigin::Dns)
    }

    fn add_peer(&mut self, peer: SocketAddrV4, origin: PeerOrigin) -> PeerAdmission {
        if !matches!(origin, PeerOrigin::Manual) && is_bogon(peer.ip()) {
            return PeerAdmission::default();
        }
        if matches!(origin, PeerOrigin::Manual)
            && (peer.ip().is_unspecified() || peer.ip().is_multicast())
        {
            return PeerAdmission::default();
        }
        if !self.known_peers.insert(peer) {
            let current_origin = self
                .peer_origins
                .get(&peer)
                .copied()
                .expect("known peer has an origin");
            let promoted = origin_priority(origin) > origin_priority(current_origin);
            if promoted {
                self.peer_origins.insert(peer, origin);
                self.enforce_origin_quota(origin);
                self.enforce_known_peer_limit();
            }
            let retained = self.known_peers.contains(&peer);
            if retained {
                self.touch_peer(peer);
            }
            return PeerAdmission {
                inserted: false,
                retained,
                promoted: promoted && retained,
            };
        }
        self.peer_origins.insert(peer, origin);
        self.known_peers_order.push_back(peer);
        self.enforce_origin_quota(origin);
        self.enforce_known_peer_limit();
        PeerAdmission {
            inserted: true,
            retained: self.known_peers.contains(&peer),
            promoted: false,
        }
    }

    fn enforce_origin_quota(&mut self, origin: PeerOrigin) {
        let limit = match origin {
            PeerOrigin::Manual | PeerOrigin::Dns => return,
            PeerOrigin::Gossip(_) => (self.max_known_peers / 8).clamp(4, 32),
        };
        while self
            .peer_origins
            .values()
            .filter(|candidate| **candidate == origin)
            .count()
            > limit
        {
            let Some(index) = self.known_peers_order.iter().position(|peer| {
                self.peer_origins.get(peer) == Some(&origin) && !self.is_protected_session(*peer)
            }) else {
                break;
            };
            let peer = self
                .known_peers_order
                .remove(index)
                .expect("peer index should exist");
            self.remove_known_peer(peer);
        }
    }

    fn touch_peer(&mut self, peer: SocketAddrV4) {
        self.known_peers_order
            .retain(|candidate| *candidate != peer);
        self.known_peers_order.push_back(peer);
    }

    fn is_protected_session(&self, peer: SocketAddrV4) -> bool {
        self.pending_dials.contains(&peer) || self.is_ip_connected(*peer.ip())
    }

    fn is_protected_peer(&self, peer: SocketAddrV4) -> bool {
        matches!(self.peer_origins.get(&peer), Some(PeerOrigin::Manual))
            || self.is_protected_session(peer)
    }

    fn remove_known_peer(&mut self, peer: SocketAddrV4) {
        self.known_peers.remove(&peer);
        self.known_peers_order
            .retain(|candidate| *candidate != peer);
        self.peer_origins.remove(&peer);
        self.peer_health.remove(&peer);
    }

    fn enforce_known_peer_limit(&mut self) {
        while self.known_peers.len() > self.max_known_peers {
            let Some(index) = self
                .known_peers_order
                .iter()
                .position(|peer| !self.is_protected_peer(*peer))
            else {
                break;
            };
            let peer = self
                .known_peers_order
                .remove(index)
                .expect("peer index should exist");
            self.remove_known_peer(peer);
        }
    }

    fn is_cooling_down(&self, peer: SocketAddrV4, now: Instant) -> bool {
        self.peer_health
            .get(&peer)
            .is_some_and(|health| health.retry_after > now)
    }

    pub(crate) fn record_peer_failure(
        &mut self,
        peer: SocketAddrV4,
        reconnect_interval: Duration,
        now: Instant,
    ) {
        let health = self.peer_health.entry(peer).or_insert(PeerHealth {
            consecutive_failures: 0,
            retry_after: now,
        });
        health.consecutive_failures = health.consecutive_failures.saturating_add(1);
        let exponent = health.consecutive_failures.saturating_sub(1).min(31);
        let cooldown = reconnect_interval
            .saturating_mul(2u32.saturating_pow(exponent))
            .min(MAX_PEER_COOLDOWN);
        health.retry_after = now + cooldown;
        self.pending_dials.remove(&peer);
        self.enforce_known_peer_limit();
    }

    #[cfg(test)]
    pub(crate) fn record_peer_success(&mut self, peer: SocketAddrV4) {
        self.peer_health.remove(&peer);
    }

    pub(crate) fn choose_dial_targets_at(
        &mut self,
        needed: usize,
        now: Instant,
    ) -> Vec<SocketAddrV4> {
        let mut candidates: Vec<SocketAddrV4> = self
            .known_peers
            .iter()
            .copied()
            .filter(|peer| {
                !self.pending_dials.contains(peer)
                    && !self.is_ip_connected(*peer.ip())
                    && !self.is_cooling_down(*peer, now)
                    && peer.ip().octets() != [0, 0, 0, 0]
            })
            .collect();

        let mut rng = rand::rng();
        candidates.shuffle(&mut rng);
        candidates.truncate(needed);
        for peer in &candidates {
            self.pending_dials.insert(*peer);
        }
        candidates
    }

    pub(crate) fn clear_pending_dial(&mut self, peer: SocketAddrV4) {
        self.pending_dials.remove(&peer);
        self.enforce_known_peer_limit();
    }

    pub(crate) fn pending_dial_count(&self) -> usize {
        self.pending_dials.len()
    }

    pub(crate) fn choose_handshake_peers(
        &self,
    ) -> [Ipv4Addr; crate::frame::NUMBER_OF_EXCHANGED_PEERS] {
        let mut ips: Vec<Ipv4Addr> = self
            .known_peers
            .iter()
            .map(|peer| *peer.ip())
            .filter(|ip| !is_bogon(ip))
            .collect();
        ips.sort_unstable();
        ips.dedup();

        let mut rng = rand::rng();
        ips.shuffle(&mut rng);

        let mut selected = [Ipv4Addr::new(0, 0, 0, 0); crate::frame::NUMBER_OF_EXCHANGED_PEERS];
        for (idx, ip) in ips
            .into_iter()
            .take(crate::frame::NUMBER_OF_EXCHANGED_PEERS)
            .enumerate()
        {
            selected[idx] = ip;
        }
        selected
    }

    pub(crate) fn pool_stats(&self, now: Instant) -> PeerPoolStats {
        PeerPoolStats {
            known: self.known_peers.len(),
            dialable: self
                .known_peers
                .iter()
                .filter(|peer| {
                    !self.pending_dials.contains(peer)
                        && !self.is_ip_connected(*peer.ip())
                        && !self.is_cooling_down(**peer, now)
                })
                .count(),
            cooldown: self
                .known_peers
                .iter()
                .filter(|peer| self.is_cooling_down(**peer, now))
                .count(),
            pending: self.pending_dials.len(),
            outgoing: self.outgoing_count(),
        }
    }
}

fn origin_priority(origin: PeerOrigin) -> u8 {
    match origin {
        PeerOrigin::Gossip(_) => 0,
        PeerOrigin::Dns => 1,
        PeerOrigin::Manual => 2,
    }
}

#[derive(Debug)]
pub(crate) struct DedupWindow {
    inner: Mutex<DedupState>,
}

#[derive(Debug)]
struct DedupState {
    order: VecDeque<[u8; 32]>,
    set: HashSet<[u8; 32]>,
    reserved: HashSet<[u8; 32]>,
    max_seen: usize,
}

#[derive(Debug)]
pub(crate) struct DedupReservation {
    digest: [u8; 32],
    dedup: Weak<DedupWindow>,
    committed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DedupReservationError {
    AlreadyCommitted,
    InFlight,
}

impl DedupWindow {
    pub(crate) fn new(max_seen: usize) -> Self {
        Self {
            inner: Mutex::new(DedupState {
                order: VecDeque::new(),
                set: HashSet::new(),
                reserved: HashSet::new(),
                max_seen: max_seen.max(1_000),
            }),
        }
    }

    pub(crate) fn contains(&self, digest: &[u8; 32]) -> bool {
        self.inner
            .lock()
            .expect("dedup mutex should not be poisoned")
            .set
            .contains(digest)
    }

    #[cfg(test)]
    pub(crate) fn mark_seen(&self, digest: [u8; 32]) -> bool {
        let mut inner = self
            .inner
            .lock()
            .expect("dedup mutex should not be poisoned");
        if inner.set.contains(&digest) {
            return false;
        }
        inner.set.insert(digest);
        inner.order.push_back(digest);

        while inner.order.len() > inner.max_seen {
            if let Some(old) = inner.order.pop_front() {
                inner.set.remove(&old);
            }
        }
        true
    }

    pub(crate) fn reserve(
        self: &Arc<Self>,
        digest: [u8; 32],
    ) -> Result<DedupReservation, DedupReservationError> {
        let mut inner = self
            .inner
            .lock()
            .expect("dedup mutex should not be poisoned");
        if inner.set.contains(&digest) {
            return Err(DedupReservationError::AlreadyCommitted);
        }
        if !inner.reserved.insert(digest) {
            return Err(DedupReservationError::InFlight);
        }
        Ok(DedupReservation {
            digest,
            dedup: Arc::downgrade(self),
            committed: false,
        })
    }
}

impl DedupReservation {
    pub(crate) fn commit(mut self) {
        if let Some(dedup) = self.dedup.upgrade() {
            let mut inner = dedup
                .inner
                .lock()
                .expect("dedup mutex should not be poisoned");
            inner.reserved.remove(&self.digest);
            if inner.set.insert(self.digest) {
                inner.order.push_back(self.digest);
                while inner.order.len() > inner.max_seen {
                    if let Some(old) = inner.order.pop_front() {
                        inner.set.remove(&old);
                    }
                }
            }
        }
        self.committed = true;
    }
}

impl Drop for DedupReservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some(dedup) = self.dedup.upgrade() {
            dedup
                .inner
                .lock()
                .expect("dedup mutex should not be poisoned")
                .reserved
                .remove(&self.digest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_PORT;
    use pretty_assertions::assert_eq;

    fn peer(a: u8, b: u8, c: u8, d: u8) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::new(a, b, c, d), DEFAULT_PORT)
    }

    #[test]
    fn broadcast_targets_are_limited_to_six() {
        let mut state = NodeState::new(1_000, &[]);
        for last_octet in 1..=8 {
            let (tx, _rx) = mpsc::channel(1);
            let (disconnect_tx, _disconnect_rx) = watch::channel(false);
            state
                .register_session(
                    peer(1, 1, 1, last_octet),
                    true,
                    tx,
                    disconnect_tx,
                    DEFAULT_PORT,
                )
                .expect("test peer should be registered");
        }

        assert_eq!(state.collect_all_targets(6).len(), 6);
    }

    #[test]
    fn broadcast_targets_include_every_available_peer_below_limit() {
        let mut state = NodeState::new(1_000, &[]);
        for last_octet in 1..=3 {
            let (tx, _rx) = mpsc::channel(1);
            let (disconnect_tx, _disconnect_rx) = watch::channel(false);
            state
                .register_session(
                    peer(1, 1, 1, last_octet),
                    true,
                    tx,
                    disconnect_tx,
                    DEFAULT_PORT,
                )
                .expect("test peer should be registered");
        }

        assert_eq!(state.collect_all_targets(6).len(), 3);
    }

    #[test]
    fn dedup_window_evicts_entries_in_fifo_order() {
        let dedup = DedupWindow::new(1_000);
        for value in 0..1_000u32 {
            let mut digest = [0; 32];
            digest[..4].copy_from_slice(&value.to_le_bytes());
            assert!(dedup.mark_seen(digest));
        }
        let first = [0; 32];
        assert!(dedup.contains(&first));

        let mut newest = [0; 32];
        newest[..4].copy_from_slice(&1_000u32.to_le_bytes());
        assert!(dedup.mark_seen(newest));

        assert!(!dedup.contains(&first));
        assert!(dedup.contains(&newest));
        assert!(!dedup.mark_seen(newest));
    }

    #[test]
    fn dedup_reservation_rolls_back_unless_committed() {
        let dedup = Arc::new(DedupWindow::new(1_000));
        let digest = [7; 32];

        let reservation = dedup.reserve(digest).expect("first reservation should win");
        assert_eq!(
            dedup.reserve(digest).unwrap_err(),
            DedupReservationError::InFlight
        );
        drop(reservation);
        let reservation = dedup
            .reserve(digest)
            .expect("dropped reservation should be retryable");
        reservation.commit();

        assert!(dedup.contains(&digest));
        assert_eq!(
            dedup.reserve(digest).unwrap_err(),
            DedupReservationError::AlreadyCommitted
        );
    }

    #[test]
    fn failed_peer_is_unavailable_until_cooldown_expires() {
        let target = peer(1, 1, 1, 1);
        let mut state = NodeState::new(10, &[target]);
        let now = Instant::now();

        state.record_peer_failure(target, Duration::from_secs(2), now);

        assert_eq!(
            state.choose_dial_targets_at(1, now + Duration::from_secs(1)),
            vec![]
        );
        assert_eq!(
            state.choose_dial_targets_at(1, now + Duration::from_secs(2)),
            vec![target]
        );
    }

    #[test]
    fn peer_cooldown_grows_to_maximum_and_success_resets_it() {
        let target = peer(1, 1, 1, 2);
        let mut state = NodeState::new(10, &[target]);
        let now = Instant::now();

        state.record_peer_failure(target, Duration::from_secs(200), now);
        state.record_peer_failure(
            target,
            Duration::from_secs(200),
            now + Duration::from_secs(200),
        );
        assert_eq!(state.pool_stats(now + Duration::from_secs(499)).dialable, 0);
        assert_eq!(state.pool_stats(now + Duration::from_secs(500)).dialable, 1);

        state.record_peer_failure(target, Duration::from_secs(2), now);
        state.record_peer_success(target);
        assert_eq!(state.pool_stats(now).dialable, 1);
    }

    #[test]
    fn short_sessions_preserve_failure_history_but_healthy_sessions_reset_it() {
        let target = peer(1, 2, 3, 4);
        let reconnect = Duration::from_secs(2);
        let start = Instant::now();
        let mut state = NodeState::new(10, &[target]);
        state.record_peer_failure(target, reconnect, start);

        let (tx, _rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        let peer_id = state
            .register_session(target, true, tx, disconnect_tx, target.port())
            .unwrap();
        state.unregister_session(peer_id);
        state.record_peer_failure(target, reconnect, start);
        assert!(state.is_cooling_down(target, start + Duration::from_secs(3)));

        let (tx, _rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        let peer_id = state
            .register_session(target, true, tx, disconnect_tx, target.port())
            .unwrap();
        state.set_connected_at(
            peer_id,
            Instant::now()
                .checked_sub(HEALTHY_SESSION_DURATION)
                .expect("test instant should support subtraction"),
        );
        state.unregister_session(peer_id);
        state.record_peer_failure(target, reconnect, start);
        assert!(!state.is_cooling_down(target, start + Duration::from_secs(3)));
    }

    #[test]
    fn custom_port_outbound_session_does_not_create_default_port_alias() {
        let target = SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 30_000);
        let mut state = NodeState::new(10, &[target]);
        let (tx, _rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        state
            .register_session(target, true, tx, disconnect_tx, 21_841)
            .unwrap();

        assert!(state.known_peers.contains(&target));
        assert!(
            !state
                .known_peers
                .contains(&SocketAddrV4::new(*target.ip(), 21_841))
        );
    }

    #[test]
    fn eviction_preserves_seed_active_and_pending_peers() {
        let seed = peer(1, 1, 1, 3);
        let active = peer(1, 1, 1, 4);
        let pending = peer(1, 1, 1, 5);
        let evictable = peer(1, 1, 1, 6);
        let replacement = peer(1, 1, 1, 7);
        let mut state = NodeState::new(4, &[seed]);
        let (tx, _rx) = mpsc::channel(1);
        let (disconnect_tx, _disconnect_rx) = watch::channel(false);
        state.add_discovered_peer(active);
        state
            .register_session(active, true, tx, disconnect_tx, DEFAULT_PORT)
            .expect("active peer should connect");
        state.add_discovered_peer(pending);
        state.pending_dials.insert(pending);
        state.add_discovered_peer(evictable);

        state.add_discovered_peer(replacement);

        assert!(state.known_peers.contains(&seed));
        assert!(state.known_peers.contains(&active));
        assert!(state.known_peers.contains(&pending));
        assert!(!state.known_peers.contains(&evictable));
        assert!(state.known_peers.contains(&replacement));
    }

    #[test]
    fn configured_known_peer_limit_is_enforced() {
        let mut state = NodeState::new(500, &[]);
        for index in 0..600u16 {
            let source = Ipv4Addr::new(9, 9, (index / 256) as u8, (index % 256) as u8);
            let _ = state.add_gossip_peer(
                peer(11, (index / 256) as u8, (index % 256) as u8, 1),
                source,
            );
        }

        assert_eq!(state.known_peers.len(), 500);
    }

    #[test]
    fn single_gossip_source_is_capped() {
        let mut state = NodeState::new(500, &[]);
        let source = Ipv4Addr::new(9, 9, 9, 9);
        for index in 0..100u8 {
            let _ = state.add_gossip_peer(peer(11, 0, index, 1), source);
        }

        assert_eq!(state.known_peers.len(), 32);
    }

    #[test]
    fn dns_discovery_promotes_gossip_without_later_downgrade() {
        let target = peer(11, 1, 1, 1);
        let source = Ipv4Addr::new(9, 9, 9, 9);
        let mut state = NodeState::new(100, &[]);
        assert!(state.add_gossip_peer(target, source).inserted);

        let admission = state.add_dns_peer(target);
        assert_eq!(
            admission,
            PeerAdmission {
                inserted: false,
                retained: true,
                promoted: true,
            }
        );
        assert_eq!(state.peer_origins.get(&target), Some(&PeerOrigin::Dns));

        assert!(
            !state
                .add_gossip_peer(target, Ipv4Addr::new(8, 8, 8, 8))
                .promoted
        );
        assert_eq!(state.peer_origins.get(&target), Some(&PeerOrigin::Dns));
    }

    #[test]
    fn trusted_dns_peers_are_not_limited_to_thirty_two() {
        let mut state = NodeState::new(100, &[]);
        for index in 1..=64u8 {
            let admission = state.add_dns_peer(peer(11, 2, index, 1));
            assert!(admission.retained);
        }

        assert_eq!(state.known_peers.len(), 64);
    }

    #[test]
    fn inactive_dns_peers_obey_the_overall_known_peer_limit() {
        let mut state = NodeState::new(40, &[]);
        for index in 1..=80u8 {
            let _ = state.add_dns_peer(peer(11, 3, index, 1));
        }

        assert_eq!(state.known_peers.len(), 40);
        assert!(
            state
                .peer_origins
                .values()
                .all(|origin| *origin == PeerOrigin::Dns)
        );
    }

    #[test]
    fn outbound_admission_distinguishes_peer_and_global_pressure() {
        let bytes = Bytes::from_static(&[1, 2]);
        let peer_error = OutboundFrame::try_new(
            bytes.clone(),
            Arc::new(Semaphore::new(1)),
            Arc::new(Semaphore::new(10)),
        )
        .unwrap_err();
        assert_eq!(peer_error, OutboundAdmissionError::PeerBudgetExhausted);

        let global_error = OutboundFrame::try_new(
            bytes,
            Arc::new(Semaphore::new(10)),
            Arc::new(Semaphore::new(1)),
        )
        .unwrap_err();
        assert_eq!(global_error, OutboundAdmissionError::GlobalBudgetExhausted);
    }
}
