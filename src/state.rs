use crate::frame::is_bogon;
use bytes::Bytes;
use rand::seq::{IteratorRandom, SliceRandom};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

const MAX_PEER_COOLDOWN: Duration = Duration::from_secs(5 * 60);

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
    pub(crate) incoming: usize,
    pub(crate) outgoing: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct RelayTarget {
    pub(crate) peer_id: u64,
    pub(crate) tx: mpsc::Sender<Bytes>,
}

#[derive(Clone, Debug)]
struct Session {
    remote: SocketAddrV4,
    outbound: bool,
    tx: mpsc::Sender<Bytes>,
    disconnect_tx: watch::Sender<bool>,
}

#[derive(Debug)]
pub(crate) struct NodeState {
    sessions: HashMap<u64, Session>,
    connected_ip_refcount: HashMap<Ipv4Addr, usize>,
    known_peers: HashSet<SocketAddrV4>,
    known_peers_order: VecDeque<SocketAddrV4>,
    seed_peers: HashSet<SocketAddrV4>,
    peer_health: HashMap<SocketAddrV4, PeerHealth>,
    pending_dials: HashSet<SocketAddrV4>,
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
            seed_peers: seed_peers.iter().copied().collect(),
            peer_health: HashMap::new(),
            pending_dials: HashSet::new(),
            max_known_peers,
            next_peer_id: 1,
        };
        for peer in seed_peers {
            let _ = node_state.add_discovered_peer(*peer);
        }
        node_state
    }

    pub(crate) fn outgoing_count(&self) -> usize {
        self.sessions.values().filter(|s| s.outbound).count()
    }

    pub(crate) fn incoming_count(&self) -> usize {
        self.sessions.values().filter(|s| !s.outbound).count()
    }

    fn is_ip_connected(&self, ip: Ipv4Addr) -> bool {
        self.connected_ip_refcount.contains_key(&ip)
    }

    pub(crate) fn register_session(
        &mut self,
        remote: SocketAddrV4,
        outbound: bool,
        tx: mpsc::Sender<Bytes>,
        disconnect_tx: watch::Sender<bool>,
        peer_port: u16,
    ) -> Option<u64> {
        let remote_ip = *remote.ip();
        if self.is_ip_connected(remote_ip) {
            return None;
        }

        let peer_id = self.next_peer_id;
        self.next_peer_id = self.next_peer_id.wrapping_add(1);

        self.sessions.insert(
            peer_id,
            Session {
                remote,
                outbound,
                tx,
                disconnect_tx,
            },
        );
        *self.connected_ip_refcount.entry(remote_ip).or_insert(0) += 1;
        let _ = self.add_discovered_peer(SocketAddrV4::new(remote_ip, peer_port));
        self.pending_dials.remove(&remote);
        self.pending_dials
            .remove(&SocketAddrV4::new(remote_ip, peer_port));
        self.record_peer_success(remote);
        self.record_peer_success(SocketAddrV4::new(remote_ip, peer_port));

        Some(peer_id)
    }

    pub(crate) fn unregister_session(&mut self, peer_id: u64) -> Option<SocketAddrV4> {
        let session = self.sessions.remove(&peer_id)?;
        let remote_ip = *session.remote.ip();
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
        Some(session.remote)
    }

    pub(crate) fn disconnect_session(&mut self, peer_id: u64) -> Option<SocketAddrV4> {
        let disconnect_tx = self.sessions.get(&peer_id)?.disconnect_tx.clone();
        let remote = self.unregister_session(peer_id)?;
        let _ = disconnect_tx.send(true);
        Some(remote)
    }

    pub(crate) fn collect_targets(&self, source_peer_id: u64, limit: usize) -> Vec<RelayTarget> {
        self.sessions
            .iter()
            .filter_map(|(peer_id, session)| {
                if *peer_id == source_peer_id {
                    None
                } else {
                    Some(RelayTarget {
                        peer_id: *peer_id,
                        tx: session.tx.clone(),
                    })
                }
            })
            .choose_multiple(&mut rand::rng(), limit)
    }

    pub(crate) fn collect_all_targets(&self, limit: usize) -> Vec<RelayTarget> {
        self.sessions
            .iter()
            .map(|(peer_id, session)| RelayTarget {
                peer_id: *peer_id,
                tx: session.tx.clone(),
            })
            .choose_multiple(&mut rand::rng(), limit)
    }

    pub(crate) fn add_discovered_peer(&mut self, peer: SocketAddrV4) -> bool {
        if is_bogon(peer.ip()) {
            return false;
        }
        if !self.known_peers.insert(peer) {
            self.touch_peer(peer);
            return false;
        }
        self.known_peers_order.push_back(peer);
        self.enforce_known_peer_limit();
        true
    }

    fn touch_peer(&mut self, peer: SocketAddrV4) {
        self.known_peers_order
            .retain(|candidate| *candidate != peer);
        self.known_peers_order.push_back(peer);
    }

    fn is_protected_peer(&self, peer: SocketAddrV4) -> bool {
        self.seed_peers.contains(&peer)
            || self.pending_dials.contains(&peer)
            || self.is_ip_connected(*peer.ip())
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
            self.known_peers.remove(&peer);
            self.peer_health.remove(&peer);
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

    pub(crate) fn choose_dial_targets(&mut self, needed: usize) -> Vec<SocketAddrV4> {
        self.choose_dial_targets_at(needed, Instant::now())
    }

    pub(crate) fn clear_pending_dial(&mut self, peer: SocketAddrV4) {
        self.pending_dials.remove(&peer);
        self.enforce_known_peer_limit();
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

    pub(crate) fn peer_candidates(&self, peer_port: u16) -> Vec<SocketAddrV4> {
        let now = Instant::now();
        let mut peers = HashSet::<SocketAddrV4>::new();

        for session in self.sessions.values() {
            if is_bogon(session.remote.ip()) {
                continue;
            }
            if session.outbound {
                peers.insert(session.remote);
            } else {
                peers.insert(SocketAddrV4::new(*session.remote.ip(), peer_port));
            }
        }

        for peer in &self.known_peers {
            if is_bogon(peer.ip()) {
                continue;
            }
            if !self.is_cooling_down(*peer, now) {
                peers.insert(*peer);
            }
        }

        peers
            .into_iter()
            .filter(|peer| !self.is_cooling_down(*peer, now))
            .collect()
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
            incoming: self.incoming_count(),
            outgoing: self.outgoing_count(),
        }
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
    max_seen: usize,
}

impl DedupWindow {
    pub(crate) fn new(max_seen: usize) -> Self {
        Self {
            inner: Mutex::new(DedupState {
                order: VecDeque::new(),
                set: HashSet::new(),
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
    fn relay_targets_exclude_source_peer() {
        let mut state = NodeState::new(1_000, &[]);
        let mut peer_ids = Vec::new();

        for last_octet in 1..=3 {
            let (tx, _rx) = mpsc::channel(1);
            let (disconnect_tx, _disconnect_rx) = watch::channel(false);
            let remote = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, last_octet), DEFAULT_PORT);
            let peer_id = state
                .register_session(remote, true, tx, disconnect_tx, DEFAULT_PORT)
                .expect("test peer should be registered");
            peer_ids.push(peer_id);
        }

        let source_peer_id = peer_ids[1];
        let mut target_peer_ids = state
            .collect_targets(source_peer_id, 6)
            .into_iter()
            .map(|target| target.peer_id)
            .collect::<Vec<_>>();
        target_peer_ids.sort_unstable();
        peer_ids.remove(1);

        assert_eq!(target_peer_ids, peer_ids);
    }

    #[test]
    fn relay_targets_are_limited_to_six() {
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
    fn relay_targets_include_every_available_peer_below_limit() {
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
            state.add_discovered_peer(peer(11, (index / 256) as u8, (index % 256) as u8, 1));
        }

        assert_eq!(state.known_peers.len(), 500);
    }
}
