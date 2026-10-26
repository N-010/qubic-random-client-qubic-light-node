use bytes::Bytes;
use rand::Rng;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

const COMPLETED_DEJAVU_TTL: Duration = Duration::from_secs(60);
const MAX_COMPLETED_DEJAVUS: usize = 4_096;
#[cfg(test)]
const TEST_PENDING_CHANNEL_CAPACITY: usize = 256;
#[cfg(test)]
const TEST_PENDING_CHANNEL_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct PendingSpec {
    pub(crate) response_rules: &'static [ResponseRule],
    pub(crate) max_response_frames: usize,
    pub(crate) max_response_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ResponseRule {
    pub(crate) message_type: u8,
    pub(crate) min_frame_bytes: usize,
    pub(crate) max_frame_bytes: usize,
    pub(crate) max_frames: usize,
    pub(crate) terminal: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeliveryOutcome {
    NotPending,
    Delivered,
    PassThrough,
    Ignored,
    ReceiverClosed,
    LocalSaturation,
    ProtocolViolation,
}

pub(crate) struct PendingFrame {
    bytes: Bytes,
    _byte_permit: OwnedSemaphorePermit,
}

impl std::fmt::Debug for PendingFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingFrame")
            .field("len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

impl std::ops::Deref for PendingFrame {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

#[cfg(test)]
impl PendingFrame {
    pub(crate) fn for_test(bytes: Bytes) -> Self {
        let budget = Arc::new(Semaphore::new(TEST_PENDING_CHANNEL_BYTES));
        let permits = u32::try_from(bytes.len()).expect("test frame length fits into u32");
        let byte_permit = budget
            .try_acquire_many_owned(permits)
            .expect("test pending budget should have room");
        Self {
            bytes,
            _byte_permit: byte_permit,
        }
    }
}

#[cfg(test)]
impl PartialEq<Bytes> for PendingFrame {
    fn eq(&self, other: &Bytes) -> bool {
        self.bytes == *other
    }
}

#[cfg(test)]
impl std::ops::Not for DeliveryOutcome {
    type Output = bool;

    fn not(self) -> Self::Output {
        self == DeliveryOutcome::NotPending
    }
}

#[derive(Debug)]
pub(crate) enum PendingEvent {
    Frame(PendingFrame),
    PeerDisconnected,
}

#[derive(Debug, Default)]
pub(crate) struct PendingRequests {
    inner: Mutex<PendingState>,
}

#[derive(Debug, Default)]
struct PendingState {
    requests: HashMap<u32, PendingRequest>,
    completed: HashMap<u32, CompletedRequest>,
    completed_order: VecDeque<(u32, Instant)>,
}

#[derive(Clone, Debug)]
struct CompletedRequest {
    expires_at: Instant,
    duplicate_peers: HashSet<u64>,
}

#[derive(Debug)]
struct PendingRequest {
    spec: PendingSpec,
    peers: HashMap<u64, PendingPeer>,
    pass_through: bool,
}

#[derive(Debug)]
struct PendingPeer {
    tx: mpsc::Sender<PendingEvent>,
    byte_budget: Arc<Semaphore>,
    received_frames: usize,
    received_bytes: usize,
    received_by_type: HashMap<u8, usize>,
    received_terminal_frames: usize,
}

pub(crate) struct PendingRegistration {
    dejavu: u32,
    dispatcher: Weak<PendingRequests>,
    repeated_is_violation: bool,
}

impl PendingRegistration {
    pub(crate) fn retain_peers(&self, peer_ids: &HashSet<u64>) {
        if let Some(dispatcher) = self.dispatcher.upgrade() {
            dispatcher.retain_peers(self.dejavu, peer_ids);
        }
    }
}

impl PendingRequests {
    pub(crate) fn register_with_spec(
        self: &Arc<Self>,
        peer_ids: impl IntoIterator<Item = u64>,
        spec: PendingSpec,
    ) -> (
        PendingRegistration,
        Vec<(u64, mpsc::Receiver<PendingEvent>)>,
    ) {
        self.register_internal(peer_ids, spec, false)
    }

    pub(crate) fn register_control(
        self: &Arc<Self>,
        peer_id: u64,
        spec: PendingSpec,
    ) -> (PendingRegistration, mpsc::Receiver<PendingEvent>) {
        let (registration, mut receivers) = self.register_internal([peer_id], spec, true);
        (
            registration,
            receivers
                .pop()
                .expect("one control peer has one receiver")
                .1,
        )
    }

    fn register_internal(
        self: &Arc<Self>,
        peer_ids: impl IntoIterator<Item = u64>,
        spec: PendingSpec,
        pass_through: bool,
    ) -> (
        PendingRegistration,
        Vec<(u64, mpsc::Receiver<PendingEvent>)>,
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
            let (tx, rx) = mpsc::channel(spec.max_response_frames.max(1));
            senders.insert(
                peer_id,
                PendingPeer {
                    tx,
                    byte_budget: Arc::new(Semaphore::new(spec.max_response_bytes)),
                    received_frames: 0,
                    received_bytes: 0,
                    received_by_type: HashMap::new(),
                    received_terminal_frames: 0,
                },
            );
            receivers.push((peer_id, rx));
        }
        inner.requests.insert(
            dejavu,
            PendingRequest {
                spec,
                peers: senders,
                pass_through,
            },
        );

        (
            PendingRegistration {
                dejavu,
                dispatcher: Arc::downgrade(self),
                repeated_is_violation: pass_through,
            },
            receivers,
        )
    }

    #[cfg(test)]
    pub(crate) fn register(
        self: &Arc<Self>,
        peer_ids: impl IntoIterator<Item = u64>,
    ) -> (
        PendingRegistration,
        Vec<(u64, mpsc::Receiver<PendingEvent>)>,
    ) {
        self.register_with_spec(
            peer_ids,
            PendingSpec {
                response_rules: &[
                    ResponseRule {
                        message_type: 0,
                        min_frame_bytes: 0,
                        max_frame_bytes: TEST_PENDING_CHANNEL_BYTES,
                        max_frames: TEST_PENDING_CHANNEL_CAPACITY,
                        terminal: false,
                    },
                    ResponseRule {
                        message_type: 24,
                        min_frame_bytes: 0,
                        max_frame_bytes: TEST_PENDING_CHANNEL_BYTES,
                        max_frames: TEST_PENDING_CHANNEL_CAPACITY,
                        terminal: false,
                    },
                    ResponseRule {
                        message_type: 32,
                        min_frame_bytes: 0,
                        max_frame_bytes: TEST_PENDING_CHANNEL_BYTES,
                        max_frames: TEST_PENDING_CHANNEL_CAPACITY,
                        terminal: false,
                    },
                    ResponseRule {
                        message_type: 35,
                        min_frame_bytes: 0,
                        max_frame_bytes: TEST_PENDING_CHANNEL_BYTES,
                        max_frames: TEST_PENDING_CHANNEL_CAPACITY,
                        terminal: false,
                    },
                    ResponseRule {
                        message_type: 43,
                        min_frame_bytes: 0,
                        max_frame_bytes: TEST_PENDING_CHANNEL_BYTES,
                        max_frames: TEST_PENDING_CHANNEL_CAPACITY,
                        terminal: false,
                    },
                    ResponseRule {
                        message_type: 54,
                        min_frame_bytes: 0,
                        max_frame_bytes: TEST_PENDING_CHANNEL_BYTES,
                        max_frames: TEST_PENDING_CHANNEL_CAPACITY,
                        terminal: false,
                    },
                ],
                max_response_frames: TEST_PENDING_CHANNEL_CAPACITY,
                max_response_bytes: TEST_PENDING_CHANNEL_BYTES,
            },
        )
    }

    pub(crate) fn deliver(&self, peer_id: u64, dejavu: u32, frame: Bytes) -> DeliveryOutcome {
        let mut inner = self
            .inner
            .lock()
            .expect("pending mutex should not be poisoned");
        inner.prune_completed(Instant::now());
        if let Some(completed) = inner.completed.get(&dejavu) {
            return if completed.duplicate_peers.contains(&peer_id) {
                DeliveryOutcome::ProtocolViolation
            } else {
                DeliveryOutcome::Ignored
            };
        }
        let Some(request) = inner.requests.get_mut(&dejavu) else {
            return DeliveryOutcome::NotPending;
        };
        if !request.peers.contains_key(&peer_id) {
            return DeliveryOutcome::Ignored;
        }
        let Some(message_type) = frame.get(3).copied() else {
            request.peers.remove(&peer_id);
            return DeliveryOutcome::ProtocolViolation;
        };
        let Some(rule) = request
            .spec
            .response_rules
            .iter()
            .find(|rule| rule.message_type == message_type)
            .copied()
        else {
            request.peers.remove(&peer_id);
            return DeliveryOutcome::ProtocolViolation;
        };
        if !(rule.min_frame_bytes..=rule.max_frame_bytes).contains(&frame.len()) {
            request.peers.remove(&peer_id);
            return DeliveryOutcome::ProtocolViolation;
        }
        let frame_len = frame.len();
        let violates_response_limit = request.peers.get(&peer_id).is_some_and(|peer| {
            peer.received_frames >= request.spec.max_response_frames
                || peer
                    .received_bytes
                    .checked_add(frame_len)
                    .is_none_or(|bytes| bytes > request.spec.max_response_bytes)
                || peer
                    .received_by_type
                    .get(&message_type)
                    .copied()
                    .unwrap_or(0)
                    >= rule.max_frames
                || (rule.terminal && peer.received_terminal_frames >= 1)
        });
        if violates_response_limit {
            request.peers.remove(&peer_id);
            return DeliveryOutcome::ProtocolViolation;
        };
        let Ok(permits) = u32::try_from(frame_len) else {
            request.peers.remove(&peer_id);
            return DeliveryOutcome::ProtocolViolation;
        };
        let peer = request
            .peers
            .get_mut(&peer_id)
            .expect("pending peer was checked above");
        let Ok(byte_permit) = Arc::clone(&peer.byte_budget).try_acquire_many_owned(permits) else {
            request.peers.remove(&peer_id);
            return DeliveryOutcome::LocalSaturation;
        };
        peer.received_frames += 1;
        peer.received_bytes += frame_len;
        *peer.received_by_type.entry(message_type).or_insert(0) += 1;
        if rule.terminal {
            peer.received_terminal_frames += 1;
        }
        let outcome = match peer.tx.try_send(PendingEvent::Frame(PendingFrame {
            // A slice can retain a much larger TCP receive allocation than its length.
            bytes: Bytes::copy_from_slice(&frame),
            _byte_permit: byte_permit,
        })) {
            Ok(()) if request.pass_through => DeliveryOutcome::PassThrough,
            Ok(()) => DeliveryOutcome::Delivered,
            Err(mpsc::error::TrySendError::Closed(_)) => DeliveryOutcome::ReceiverClosed,
            Err(mpsc::error::TrySendError::Full(_)) => DeliveryOutcome::LocalSaturation,
        };
        if matches!(
            outcome,
            DeliveryOutcome::ReceiverClosed | DeliveryOutcome::LocalSaturation
        ) {
            request.peers.remove(&peer_id);
        }
        outcome
    }

    pub(crate) fn peer_disconnected(&self, peer_id: u64) {
        let mut inner = self
            .inner
            .lock()
            .expect("pending mutex should not be poisoned");
        for request in inner.requests.values_mut() {
            if let Some(peer) = request.peers.remove(&peer_id) {
                let _ = peer.tx.try_send(PendingEvent::PeerDisconnected);
            }
        }
    }

    fn retain_peers(&self, dejavu: u32, peer_ids: &HashSet<u64>) {
        let mut inner = self
            .inner
            .lock()
            .expect("pending mutex should not be poisoned");
        if let Some(request) = inner.requests.get_mut(&dejavu) {
            request
                .peers
                .retain(|peer_id, _| peer_ids.contains(peer_id));
        }
    }

    fn complete(&self, dejavu: u32, repeated_is_violation: bool) {
        let mut inner = self
            .inner
            .lock()
            .expect("pending mutex should not be poisoned");
        let request = inner.requests.remove(&dejavu);
        // Cancellation or timeout before any response is not evidence of a duplicate.
        let duplicate_peers = request
            .into_iter()
            .flat_map(|request| request.peers)
            .filter_map(|(peer_id, peer)| {
                (repeated_is_violation && peer.received_frames > 0).then_some(peer_id)
            })
            .collect();
        let expires_at = Instant::now() + COMPLETED_DEJAVU_TTL;
        inner.completed.insert(
            dejavu,
            CompletedRequest {
                expires_at,
                duplicate_peers,
            },
        );
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
            if self
                .completed
                .get(&dejavu)
                .is_some_and(|completed| completed.expires_at == expires_at)
            {
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
            dispatcher.complete(self.dejavu, self.repeated_is_violation);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pending_frame_debug_omits_payload() {
        let bytes = Bytes::from_static(b"secret-preimage");
        let frame = PendingFrame::for_test(bytes.clone());
        assert_eq!(format!("{frame:?}"), "PendingFrame { len: 15, .. }");
        assert_eq!(&*frame, bytes.as_ref());
        let event = PendingEvent::Frame(frame);
        assert!(!format!("{event:#?}").contains("secret-preimage"));
    }

    const CORE_MAX_RESPONSE_FRAMES: usize = 4096;

    #[test]
    fn completed_control_duplicate_is_bound_to_responding_peer() {
        let pending = Arc::new(PendingRequests::default());
        let (registration, _receiver) = pending.register_control(
            10,
            PendingSpec {
                response_rules: &[ResponseRule {
                    message_type: 35,
                    min_frame_bytes: 8,
                    max_frame_bytes: 8,
                    max_frames: 1,
                    terminal: true,
                }],
                max_response_frames: 1,
                max_response_bytes: 8,
            },
        );
        let dejavu = registration.dejavu();
        let frame = Bytes::from(crate::frame::build_request_frame(35, dejavu, &[]).unwrap());
        assert_eq!(
            pending.deliver(10, dejavu, frame.clone()),
            DeliveryOutcome::PassThrough
        );
        drop(registration);
        assert_eq!(
            pending.deliver(20, dejavu, frame.clone()),
            DeliveryOutcome::Ignored
        );
        assert_eq!(
            pending.deliver(10, dejavu, frame),
            DeliveryOutcome::ProtocolViolation
        );
    }

    #[test]
    fn completed_cache_churn_and_expiry_preserve_active_request() {
        let pending = Arc::new(PendingRequests::default());
        let (active, mut receivers) = pending.register([10]);
        for _ in 0..MAX_COMPLETED_DEJAVUS + 128 {
            let (finished, _) = pending.register([10]);
            assert_ne!(finished.dejavu(), active.dejavu());
            drop(finished);
        }
        {
            let mut state = pending.inner.lock().unwrap();
            assert_eq!(state.completed.len(), MAX_COMPLETED_DEJAVUS);
            assert_eq!(state.completed_order.len(), MAX_COMPLETED_DEJAVUS);
            assert!(state.requests.contains_key(&active.dejavu()));
            let expiry = state.completed_order.back().unwrap().1;
            state.prune_completed(expiry);
            assert!(state.completed.is_empty());
            assert!(state.completed_order.is_empty());
            assert!(state.requests.contains_key(&active.dejavu()));
        }
        let response =
            Bytes::from(crate::frame::build_request_frame(35, active.dejavu(), &[]).unwrap());
        assert_eq!(
            pending.deliver(10, active.dejavu(), response.clone()),
            DeliveryOutcome::Delivered
        );
        let PendingEvent::Frame(received) = receivers[0].1.try_recv().unwrap() else {
            panic!("expected active response");
        };
        assert_eq!(received, response);
        drop(active);
        assert_eq!(pending.active_count(), 0);
    }

    #[test]
    fn queued_response_does_not_retain_the_shared_receive_allocation() {
        struct SharedBuffer(Arc<Vec<u8>>);
        impl AsRef<[u8]> for SharedBuffer {
            fn as_ref(&self) -> &[u8] {
                self.0.as_slice()
            }
        }
        let pending = Arc::new(PendingRequests::default());
        let (registration, mut receivers) = pending.register([10]);
        let expected = crate::frame::build_request_frame(35, registration.dejavu(), &[]).unwrap();
        let mut storage = vec![0; 1024 * 1024];
        storage[..expected.len()].copy_from_slice(&expected);
        let owner = Arc::new(storage);
        let allocation = Arc::downgrade(&owner);
        let frame = Bytes::from_owner(SharedBuffer(owner)).slice(..expected.len());
        assert_eq!(
            pending.deliver(10, registration.dejavu(), frame),
            DeliveryOutcome::Delivered
        );
        assert!(
            allocation.upgrade().is_none(),
            "small queued frame retained the entire receive allocation"
        );
        let PendingEvent::Frame(queued) = receivers[0].1.try_recv().unwrap() else {
            panic!("expected queued response");
        };
        assert_eq!(&*queued, expected.as_slice());
    }

    #[tokio::test]
    async fn routes_by_peer_and_dejavu_and_suppresses_late_frames() {
        let pending = Arc::new(PendingRequests::default());
        let (registration, mut receivers) = pending.register([10, 20]);
        let dejavu = registration.dejavu();
        let (_, mut peer_10_rx) = receivers.remove(0);
        let (_, mut peer_20_rx) = receivers.remove(0);

        assert!(!pending.deliver(
            10,
            dejavu.wrapping_add(1),
            Bytes::from_static(&[4, 0, 0, 0])
        ));
        assert!(pending.deliver(30, dejavu, Bytes::from_static(&[4, 0, 0, 0])));
        assert!(pending.deliver(20, dejavu, Bytes::from_static(&[4, 0, 0, 0])));
        assert!(peer_10_rx.try_recv().is_err());
        let PendingEvent::Frame(frame) = peer_20_rx.recv().await.unwrap() else {
            panic!("expected a frame");
        };
        assert_eq!(frame, Bytes::from_static(&[4, 0, 0, 0]));

        drop(registration);
        assert_eq!(pending.active_count(), 0);
        assert!(pending.deliver(20, dejavu, Bytes::from_static(&[4, 0, 0, 0])));
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

        assert!(pending.deliver(
            10,
            second_registration.dejavu(),
            Bytes::from_static(&[4, 0, 0, 0]),
        ));
        assert!(pending.deliver(
            10,
            first_registration.dejavu(),
            Bytes::from_static(&[4, 0, 0, 0]),
        ));

        let Some(PendingEvent::Frame(first)) = first_rx.recv().await else {
            panic!("first request should receive a frame");
        };
        let Some(PendingEvent::Frame(second)) = second_rx.recv().await else {
            panic!("second request should receive a frame");
        };
        assert_eq!(first, Bytes::from_static(&[4, 0, 0, 0]));
        assert_eq!(second, Bytes::from_static(&[4, 0, 0, 0]));
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

    #[tokio::test]
    async fn protocol_limits_are_distinct_from_a_closed_receiver() {
        let pending = Arc::new(PendingRequests::default());
        let (registration, mut receivers) = pending.register_with_spec(
            [10],
            PendingSpec {
                response_rules: &[ResponseRule {
                    message_type: 32,
                    min_frame_bytes: 4,
                    max_frame_bytes: 64,
                    max_frames: 2,
                    terminal: false,
                }],
                max_response_frames: 2,
                max_response_bytes: 128,
            },
        );
        let dejavu = registration.dejavu();
        let (_, receiver) = receivers.remove(0);

        assert_eq!(
            pending.deliver(10, dejavu, Bytes::from_static(&[0, 0, 0, 24])),
            DeliveryOutcome::ProtocolViolation
        );
        drop(receiver);

        let (registration, mut receivers) = pending.register_with_spec(
            [10],
            PendingSpec {
                response_rules: &[ResponseRule {
                    message_type: 32,
                    min_frame_bytes: 4,
                    max_frame_bytes: 64,
                    max_frames: 2,
                    terminal: false,
                }],
                max_response_frames: 2,
                max_response_bytes: 128,
            },
        );
        let dejavu = registration.dejavu();
        drop(receivers.remove(0).1);
        assert_eq!(
            pending.deliver(10, dejavu, Bytes::from_static(&[0, 0, 0, 32])),
            DeliveryOutcome::ReceiverClosed
        );
    }

    #[tokio::test]
    async fn core_sized_tick_burst_fits_and_the_next_frame_is_a_protocol_violation() {
        let pending = Arc::new(PendingRequests::default());
        let frame_count = CORE_MAX_RESPONSE_FRAMES + 1;
        let (registration, mut receivers) = pending.register_with_spec(
            [10],
            PendingSpec {
                response_rules: &[
                    ResponseRule {
                        message_type: 24,
                        min_frame_bytes: 8,
                        max_frame_bytes: 8,
                        max_frames: CORE_MAX_RESPONSE_FRAMES,
                        terminal: false,
                    },
                    ResponseRule {
                        message_type: 35,
                        min_frame_bytes: 8,
                        max_frame_bytes: 8,
                        max_frames: 1,
                        terminal: true,
                    },
                ],
                max_response_frames: frame_count,
                max_response_bytes: frame_count * 8,
            },
        );
        let dejavu = registration.dejavu();
        let (_, _receiver) = receivers.remove(0);

        for _ in 0..CORE_MAX_RESPONSE_FRAMES {
            assert_eq!(
                pending.deliver(10, dejavu, Bytes::from_static(&[8, 0, 0, 24, 0, 0, 0, 0]),),
                DeliveryOutcome::Delivered
            );
        }
        assert_eq!(
            pending.deliver(10, dejavu, Bytes::from_static(&[8, 0, 0, 24, 0, 0, 0, 0]),),
            DeliveryOutcome::ProtocolViolation
        );

        let (registration, mut receivers) = pending.register_with_spec(
            [10],
            PendingSpec {
                response_rules: &[
                    ResponseRule {
                        message_type: 24,
                        min_frame_bytes: 8,
                        max_frame_bytes: 8,
                        max_frames: CORE_MAX_RESPONSE_FRAMES,
                        terminal: false,
                    },
                    ResponseRule {
                        message_type: 35,
                        min_frame_bytes: 8,
                        max_frame_bytes: 8,
                        max_frames: 1,
                        terminal: true,
                    },
                ],
                max_response_frames: frame_count,
                max_response_bytes: frame_count * 8,
            },
        );
        let dejavu = registration.dejavu();
        let (_, _receiver) = receivers.remove(0);
        for _ in 0..CORE_MAX_RESPONSE_FRAMES {
            assert_eq!(
                pending.deliver(10, dejavu, Bytes::from_static(&[8, 0, 0, 24, 0, 0, 0, 0]),),
                DeliveryOutcome::Delivered
            );
        }
        assert_eq!(
            pending.deliver(10, dejavu, Bytes::from_static(&[8, 0, 0, 35, 0, 0, 0, 0]),),
            DeliveryOutcome::Delivered
        );
        assert_eq!(
            pending.deliver(10, dejavu, Bytes::from_static(&[8, 0, 0, 24, 0, 0, 0, 0]),),
            DeliveryOutcome::ProtocolViolation
        );
    }

    #[test]
    fn unanswered_control_request_ignores_late_first_response() {
        let pending = Arc::new(PendingRequests::default());
        let spec = PendingSpec {
            response_rules: &[ResponseRule {
                message_type: 35,
                min_frame_bytes: 8,
                max_frame_bytes: 8,
                max_frames: 1,
                terminal: true,
            }],
            max_response_frames: 1,
            max_response_bytes: 8,
        };
        let (expired, expired_receiver) = pending.register_control(10, spec);
        let old_nonce = expired.dejavu();
        drop(expired_receiver);
        drop(expired);
        let (retry, mut receiver) = pending.register_control(10, spec);
        let late = Bytes::from(crate::frame::build_request_frame(35, old_nonce, &[]).unwrap());
        assert_eq!(
            pending.deliver(10, old_nonce, late),
            DeliveryOutcome::Ignored
        );
        assert!(receiver.try_recv().is_err());
        let response =
            Bytes::from(crate::frame::build_request_frame(35, retry.dejavu(), &[]).unwrap());
        assert_eq!(
            pending.deliver(10, retry.dejavu(), response),
            DeliveryOutcome::PassThrough
        );
        assert!(receiver.try_recv().is_ok());
    }

    #[tokio::test]
    async fn control_reservations_share_allocator_and_require_exact_terminal_size() {
        let pending = Arc::new(PendingRequests::default());
        let (api_registration, _api_receivers) = pending.register([10]);
        let (control_registration, _control_receiver) = pending.register_control(
            10,
            PendingSpec {
                response_rules: &[ResponseRule {
                    message_type: 35,
                    min_frame_bytes: 8,
                    max_frame_bytes: 8,
                    max_frames: 1,
                    terminal: true,
                }],
                max_response_frames: 1,
                max_response_bytes: 8,
            },
        );
        assert_ne!(api_registration.dejavu(), control_registration.dejavu());

        let padded = Bytes::from_static(&[9, 0, 0, 35, 1, 0, 0, 0, 1]);
        assert_eq!(
            pending.deliver(10, control_registration.dejavu(), padded),
            DeliveryOutcome::ProtocolViolation
        );

        let (terminal_registration, _receiver) = pending.register_control(
            10,
            PendingSpec {
                response_rules: &[ResponseRule {
                    message_type: 35,
                    min_frame_bytes: 8,
                    max_frame_bytes: 8,
                    max_frames: 1,
                    terminal: true,
                }],
                max_response_frames: 1,
                max_response_bytes: 8,
            },
        );
        let dejavu = terminal_registration.dejavu();
        let terminal = Bytes::from(crate::frame::build_request_frame(35, dejavu, &[]).unwrap());
        assert_eq!(
            pending.deliver(10, dejavu, terminal.clone()),
            DeliveryOutcome::PassThrough
        );
        drop(terminal_registration);
        assert_eq!(
            pending.deliver(10, dejavu, terminal),
            DeliveryOutcome::ProtocolViolation
        );
    }
}
