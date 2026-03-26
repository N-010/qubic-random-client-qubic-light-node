use crate::frame::is_bogon;
use crate::types::TickStatus;
use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
struct Session {
    remote: SocketAddrV4,
    outbound: bool,
    tx: mpsc::Sender<Arc<[u8]>>,
}

#[derive(Debug)]
pub(crate) struct NodeState {
    sessions: HashMap<u64, Session>,
    connected_ip_refcount: HashMap<Ipv4Addr, usize>,
    known_peers: HashSet<SocketAddrV4>,
    known_peers_order: VecDeque<SocketAddrV4>,
    pending_dials: HashSet<SocketAddrV4>,
    seen_order: VecDeque<[u8; 32]>,
    seen_set: HashSet<[u8; 32]>,
    latest_tick: Option<TickStatus>,
    max_seen: usize,
    max_known_peers: usize,
    next_peer_id: u64,
}

impl NodeState {
    pub(crate) fn new(
        max_seen: usize,
        max_known_peers: usize,
        seed_peers: &[SocketAddrV4],
    ) -> Self {
        let mut node_state = Self {
            sessions: HashMap::new(),
            connected_ip_refcount: HashMap::new(),
            known_peers: HashSet::new(),
            known_peers_order: VecDeque::new(),
            pending_dials: HashSet::new(),
            seen_order: VecDeque::new(),
            seen_set: HashSet::new(),
            latest_tick: None,
            max_seen: max_seen.max(1_000),
            max_known_peers: max_known_peers.max(1_000),
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
        tx: mpsc::Sender<Arc<[u8]>>,
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
            },
        );
        *self.connected_ip_refcount.entry(remote_ip).or_insert(0) += 1;
        let _ = self.add_discovered_peer(SocketAddrV4::new(remote_ip, peer_port));
        self.pending_dials.remove(&remote);
        self.pending_dials
            .remove(&SocketAddrV4::new(remote_ip, peer_port));

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

    pub(crate) fn mark_seen(&mut self, digest: [u8; 32]) -> bool {
        if self.seen_set.contains(&digest) {
            return false;
        }
        self.seen_set.insert(digest);
        self.seen_order.push_back(digest);

        while self.seen_order.len() > self.max_seen {
            if let Some(old) = self.seen_order.pop_front() {
                self.seen_set.remove(&old);
            }
        }
        true
    }

    pub(crate) fn collect_targets(&self, source_peer_id: u64) -> Vec<mpsc::Sender<Arc<[u8]>>> {
        self.sessions
            .iter()
            .filter_map(|(peer_id, session)| {
                if *peer_id == source_peer_id {
                    None
                } else {
                    Some(session.tx.clone())
                }
            })
            .collect()
    }

    pub(crate) fn collect_all_targets(&self) -> Vec<mpsc::Sender<Arc<[u8]>>> {
        self.sessions
            .values()
            .map(|session| session.tx.clone())
            .collect()
    }

    pub(crate) fn add_discovered_peer(&mut self, peer: SocketAddrV4) -> bool {
        if is_bogon(peer.ip()) {
            return false;
        }
        if !self.known_peers.insert(peer) {
            return false;
        }
        self.known_peers_order.push_back(peer);

        while self.known_peers.len() > self.max_known_peers {
            if let Some(oldest_peer) = self.known_peers_order.pop_front() {
                self.known_peers.remove(&oldest_peer);
            }
        }
        true
    }

    pub(crate) fn choose_dial_targets(&mut self, needed: usize) -> Vec<SocketAddrV4> {
        let mut candidates: Vec<SocketAddrV4> = self
            .known_peers
            .iter()
            .copied()
            .filter(|peer| {
                !self.pending_dials.contains(peer)
                    && !self.is_ip_connected(*peer.ip())
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

    pub(crate) fn update_latest_tick(&mut self, tick_status: TickStatus) {
        if let Some(current) = self.latest_tick {
            let is_newer_epoch = tick_status.epoch > current.epoch;
            let is_same_epoch_newer_tick =
                tick_status.epoch == current.epoch && tick_status.tick >= current.tick;
            let enrich_same_tick = tick_status.epoch == current.epoch
                && tick_status.tick == current.tick
                && tick_status.initial_tick != 0
                && current.initial_tick == 0;

            if is_newer_epoch || is_same_epoch_newer_tick || enrich_same_tick {
                self.latest_tick = Some(tick_status);
            }
        } else {
            self.latest_tick = Some(tick_status);
        }
    }

    pub(crate) fn latest_tick(&self) -> Option<TickStatus> {
        self.latest_tick
    }

    pub(crate) fn peer_candidates(&self, peer_port: u16) -> Vec<SocketAddrV4> {
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
            peers.insert(*peer);
        }

        peers.into_iter().collect()
    }
}
