use bytes::Bytes;
use rand::Rng;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const COMPLETED_DEJAVU_TTL: Duration = Duration::from_secs(60);
const MAX_COMPLETED_DEJAVUS: usize = 4_096;

#[derive(Debug)]
pub(crate) enum PendingEvent {
    Frame(Bytes),
    PeerDisconnected,
}

#[derive(Debug, Default)]
pub(crate) struct PendingRequests {
    inner: Mutex<PendingState>,
}

#[derive(Debug, Default)]
struct PendingState {
    requests: HashMap<u32, HashMap<u64, mpsc::UnboundedSender<PendingEvent>>>,
    completed: HashMap<u32, Instant>,
    completed_order: VecDeque<(u32, Instant)>,
}

pub(crate) struct PendingRegistration {
    dejavu: u32,
    dispatcher: Weak<PendingRequests>,
}

impl PendingRequests {
    pub(crate) fn register(
        self: &Arc<Self>,
        peer_ids: impl IntoIterator<Item = u64>,
    ) -> (
        PendingRegistration,
        Vec<(u64, mpsc::UnboundedReceiver<PendingEvent>)>,
    ) {
        let mut inner = self
            .inner
            .lock()
            .expect("pending mutex should not be poisoned");
        inner.prune_completed(Instant::now());

        let dejavu = loop {
            let candidate = rand::rng().random::<u32>();
            if candidate != 0
                && !inner.requests.contains_key(&candidate)
                && !inner.completed.contains_key(&candidate)
            {
                break candidate;
            }
        };

        let mut senders = HashMap::new();
        let mut receivers = Vec::new();
        for peer_id in peer_ids {
            let (tx, rx) = mpsc::unbounded_channel();
            senders.insert(peer_id, tx);
            receivers.push((peer_id, rx));
        }
        inner.requests.insert(dejavu, senders);

        (
            PendingRegistration {
                dejavu,
                dispatcher: Arc::downgrade(self),
            },
            receivers,
        )
    }

    pub(crate) fn deliver(&self, peer_id: u64, dejavu: u32, frame: Bytes) -> bool {
        let mut inner = self
            .inner
            .lock()
            .expect("pending mutex should not be poisoned");
        inner.prune_completed(Instant::now());
        if inner.completed.contains_key(&dejavu) {
            return true;
        }
        let Some(peers) = inner.requests.get_mut(&dejavu) else {
            return false;
        };
        let Some(tx) = peers.get(&peer_id) else {
            return true;
        };
        let _ = tx.send(PendingEvent::Frame(frame));
        true
    }

    pub(crate) fn peer_disconnected(&self, peer_id: u64) {
        let mut inner = self
            .inner
            .lock()
            .expect("pending mutex should not be poisoned");
        for peers in inner.requests.values_mut() {
            if let Some(tx) = peers.remove(&peer_id) {
                let _ = tx.send(PendingEvent::PeerDisconnected);
            }
        }
    }

    fn complete(&self, dejavu: u32) {
        let mut inner = self
            .inner
            .lock()
            .expect("pending mutex should not be poisoned");
        inner.requests.remove(&dejavu);
        let expires_at = Instant::now() + COMPLETED_DEJAVU_TTL;
        inner.completed.insert(dejavu, expires_at);
        inner.completed_order.push_back((dejavu, expires_at));
        inner.prune_completed(Instant::now());
    }

    #[cfg(test)]
    pub(crate) fn active_count(&self) -> usize {
        self.inner
            .lock()
            .expect("pending mutex should not be poisoned")
            .requests
            .len()
    }
}

impl PendingState {
    fn prune_completed(&mut self, now: Instant) {
        while self.completed_order.len() > MAX_COMPLETED_DEJAVUS
            || self
                .completed_order
                .front()
                .is_some_and(|(_, expires_at)| *expires_at <= now)
        {
            let Some((dejavu, expires_at)) = self.completed_order.pop_front() else {
                break;
            };
            if self.completed.get(&dejavu) == Some(&expires_at) {
                self.completed.remove(&dejavu);
            }
        }
    }
}

impl PendingRegistration {
    pub(crate) fn dejavu(&self) -> u32 {
        self.dejavu
    }
}

impl Drop for PendingRegistration {
    fn drop(&mut self) {
        if let Some(dispatcher) = self.dispatcher.upgrade() {
            dispatcher.complete(self.dejavu);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn routes_by_peer_and_dejavu_and_suppresses_late_frames() {
        let pending = Arc::new(PendingRequests::default());
        let (registration, mut receivers) = pending.register([10, 20]);
        let dejavu = registration.dejavu();
        let (_, mut peer_10_rx) = receivers.remove(0);
        let (_, mut peer_20_rx) = receivers.remove(0);

        assert!(!pending.deliver(10, dejavu.wrapping_add(1), Bytes::from_static(&[1])));
        assert!(pending.deliver(30, dejavu, Bytes::from_static(&[2])));
        assert!(pending.deliver(20, dejavu, Bytes::from_static(&[3])));
        assert!(peer_10_rx.try_recv().is_err());
        let PendingEvent::Frame(frame) = peer_20_rx.recv().await.unwrap() else {
            panic!("expected a frame");
        };
        assert_eq!(frame, Bytes::from_static(&[3]));

        drop(registration);
        assert_eq!(pending.active_count(), 0);
        assert!(pending.deliver(20, dejavu, Bytes::from_static(&[4])));
    }

    #[tokio::test]
    async fn disconnect_notifies_only_that_peer() {
        let pending = Arc::new(PendingRequests::default());
        let (_registration, mut receivers) = pending.register([10, 20]);
        let (_, mut peer_10_rx) = receivers.remove(0);
        let (_, mut peer_20_rx) = receivers.remove(0);

        pending.peer_disconnected(10);

        assert!(matches!(
            peer_10_rx.recv().await,
            Some(PendingEvent::PeerDisconnected)
        ));
        assert!(peer_20_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn interleaved_same_type_frames_reach_their_own_requests() {
        let pending = Arc::new(PendingRequests::default());
        let (first_registration, mut first_receivers) = pending.register([10]);
        let (second_registration, mut second_receivers) = pending.register([10]);
        let mut first_rx = first_receivers.remove(0).1;
        let mut second_rx = second_receivers.remove(0).1;

        assert!(pending.deliver(10, second_registration.dejavu(), Bytes::from_static(&[2]),));
        assert!(pending.deliver(10, first_registration.dejavu(), Bytes::from_static(&[1]),));

        let Some(PendingEvent::Frame(first)) = first_rx.recv().await else {
            panic!("first request should receive a frame");
        };
        let Some(PendingEvent::Frame(second)) = second_rx.recv().await else {
            panic!("second request should receive a frame");
        };
        assert_eq!(first, Bytes::from_static(&[1]));
        assert_eq!(second, Bytes::from_static(&[2]));
    }

    #[tokio::test]
    async fn aborted_waiter_cleans_registration() {
        let pending = Arc::new(PendingRequests::default());
        let pending_for_task = Arc::clone(&pending);
        let (dejavu_tx, dejavu_rx) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            let (registration, _receivers) = pending_for_task.register([10]);
            dejavu_tx.send(registration.dejavu()).unwrap();
            std::future::pending::<()>().await;
        });
        let dejavu = dejavu_rx.await.unwrap();

        waiter.abort();
        let _ = waiter.await;

        assert_eq!(pending.active_count(), 0);
        assert!(pending.deliver(10, dejavu, Bytes::new()));
    }
}
