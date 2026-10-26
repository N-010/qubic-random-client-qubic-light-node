//! Local availability policy; observations are not consensus or peer authentication.
use super::*;
use crate::frame::{REQUEST_CURRENT_TICK_INFO_TYPE, RESPOND_CURRENT_TICK_INFO_TYPE};
use crate::peer_api::{PeerQueryError, probe_current_tick};
use std::collections::HashMap;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const ROTATION_INTERVAL: Duration = Duration::from_secs(30);
const RETIRED_COOLDOWN: Duration = Duration::from_secs(120);
const SAMPLE_MAX_AGE: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
struct Quality {
    rtt: Duration,
    good: u32,
    bad: u32,
    updated: Instant,
}

impl Quality {
    fn observe(&mut self, sample: Option<Duration>, now: Instant) {
        if now.saturating_duration_since(self.updated) > SAMPLE_MAX_AGE {
            self.rtt = Duration::ZERO;
            self.good = 0;
            self.bad = 0;
        }
        self.updated = now;
        match sample {
            Some(rtt) => {
                self.rtt = if self.good == 0 {
                    rtt
                } else {
                    self.rtt * 3 / 4 + rtt / 4
                };
                self.good = self.good.saturating_add(1);
                self.bad = 0;
            }
            None => {
                self.bad = self.bad.saturating_add(1);
                self.good = 0;
            }
        }
    }

    fn improves(&self, old: &Self, now: Instant) -> bool {
        self.good >= 3
            && self.bad == 0
            && now.saturating_duration_since(self.updated) <= SAMPLE_MAX_AGE
            && now.saturating_duration_since(old.updated) <= SAMPLE_MAX_AGE
            && (old.bad >= 3
                || (old.good >= 3
                    && self.rtt + Duration::from_millis(20) < old.rtt
                    && self.rtt < old.rtt * 3 / 4))
    }
}

fn fresh(status: crate::types::TickStatus, reference: Option<(u16, u32)>) -> bool {
    reference.is_some_and(|(epoch, tick)| {
        epoch != 0
            && tick != 0
            && status.epoch == epoch
            && status.tick != 0
            && status.tick >= tick.saturating_sub(3)
    })
}

fn replacement(
    state: &NodeState,
    quality: &HashMap<u64, Quality>,
    candidate: &Quality,
    now: Instant,
) -> Option<u64> {
    quality
        .iter()
        .filter(|(id, old)| state.selection_retirement_ready(**id) && candidate.improves(old, now))
        .max_by_key(|(_, old)| (old.bad >= 3, old.bad, old.rtt))
        .map(|(id, _)| *id)
}

pub(super) async fn run(resources: NetworkResources) {
    let mut quality = HashMap::<u64, Quality>::new();
    let mut next_rotation = Instant::now() + ROTATION_INTERVAL;
    let mut next_dns = Instant::now() + Duration::from_secs(300);
    loop {
        sleep(SAMPLE_INTERVAL).await;
        let targets = resources.state.lock().await.collect_all_targets(usize::MAX);
        quality.retain(|id, _| targets.iter().any(|target| target.peer_id == *id));
        let mut probes = tokio::task::JoinSet::new();
        let mut targets = targets.into_iter();
        loop {
            while probes.len() < 3 {
                let Some(target) = targets.next() else { break };
                let r = resources.clone();
                probes.spawn(async move {
                    let reference = unpack_epoch_tick(r.latest_epoch_tick.load(Ordering::Relaxed));
                    let started = Instant::now();
                    let result = probe_current_tick(
                        r.state,
                        r.pending,
                        r.outbound_budget,
                        target.clone(),
                        PROBE_TIMEOUT,
                    )
                    .await;
                    (target.peer_id, reference, started.elapsed(), result)
                });
            }
            let Some(result) = probes.join_next().await else {
                break;
            };
            if let Ok((id, reference, elapsed, result)) = result {
                // Local pressure is not evidence against a peer.
                if reference.is_none()
                    || matches!(
                        result,
                        Err(PeerQueryError::LocalOverload(_) | PeerQueryError::Internal(_))
                    )
                {
                    continue;
                }
                let sample = result
                    .ok()
                    .filter(|status| fresh(*status, reference))
                    .map(|_| elapsed);
                let now = Instant::now();
                quality
                    .entry(id)
                    .or_insert(Quality {
                        rtt: Duration::ZERO,
                        good: 0,
                        bad: 0,
                        updated: now,
                    })
                    .observe(sample, now);
            }
        }
        let now = Instant::now();
        if resources.config.dns_bootstrap && now >= next_dns {
            next_dns = now + Duration::from_secs(300);
            if let Ok(peers) = crate::dns::fetch_seed_peers_from_dns(
                resources.config.peer_port,
                resources.config.bootstrap_lite_peers(),
                resources.config.dns_timeout,
            )
            .await
            {
                let mut state = resources.state.lock().await;
                for peer in peers {
                    state.add_dns_peer(peer);
                }
            }
        }
        if now < next_rotation || !quality.values().any(|q| q.good >= 3 || q.bad >= 3) {
            continue;
        }
        next_rotation = now + ROTATION_INTERVAL;
        let candidate = {
            let mut state = resources.state.lock().await;
            if state.outgoing_count() < resources.config.target_outbound
                || state.pending_dial_count() != 0
            {
                None
            } else {
                state.choose_dial_targets_at(1, now).pop()
            }
        };
        let Some(remote) = candidate else { continue };
        println!("PEER_SELECTION probing candidate={remote}");
        match prepare_candidate(remote, &resources).await {
            Ok((stream, handshake, measured)) => {
                let now = Instant::now();
                let replace = {
                    let state = resources.state.lock().await;
                    quality.retain(|id, _| state.target(*id).is_some());
                    replacement(&state, &quality, &measured, now)
                };
                let vacancy = resources.state.lock().await.outgoing_count()
                    < resources.config.target_outbound;
                if replace.is_some() || vacancy {
                    println!(
                        "PEER_SELECTION qualified candidate={remote} rtt_us={} replacement={replace:?}",
                        measured.rtt.as_micros()
                    );
                    tokio::spawn(activate_connection(
                        stream,
                        remote,
                        resources.clone(),
                        handshake,
                        replace,
                    ));
                } else {
                    resources.state.lock().await.defer_selection_candidate(
                        remote,
                        now,
                        RETIRED_COOLDOWN,
                    );
                    println!(
                        "PEER_SELECTION retained current pool; candidate={remote} rtt_us={}",
                        measured.rtt.as_micros()
                    );
                }
            }
            Err(error) => {
                resources.state.lock().await.defer_selection_candidate(
                    remote,
                    Instant::now(),
                    RETIRED_COOLDOWN,
                );
                println!("PEER_SELECTION candidate={remote} unavailable: {error}");
            }
        }
    }
}

async fn prepare_candidate(
    remote: SocketAddrV4,
    resources: &NetworkResources,
) -> Result<(TcpStream, Bytes, Quality), String> {
    // One spare socket, never a transaction recipient until it passes all probes.
    let deadline = TokioInstant::now() + Duration::from_secs(10);
    timeout_at(deadline, async {
        let mut stream = timeout(
            resources.config.peer_connect_timeout,
            TcpStream::connect(remote),
        )
        .await
        .map_err(|_| "candidate connect timeout".to_string())?
        .map_err(|e| e.to_string())?;
        stream.set_nodelay(true).map_err(|e| e.to_string())?;
        configure_tcp_keepalive(&stream).map_err(|e| e.to_string())?;
        let handshake = build_exchange_public_peers_frame(
            resources.state.lock().await.choose_handshake_peers(),
        );
        let handshake = perform_handshake(
            &mut stream,
            &handshake,
            resources.config.peer_handshake_timeout,
            resources.config.max_frame_bytes,
        )
        .await?;
        let mut quality = Quality {
            rtt: Duration::ZERO,
            good: 0,
            bad: 0,
            updated: Instant::now(),
        };
        for attempt in 0..3 {
            if attempt != 0 {
                sleep(Duration::from_millis(250)).await;
            }
            let reference = unpack_epoch_tick(resources.latest_epoch_tick.load(Ordering::Relaxed));
            let started = Instant::now();
            let status = timeout(
                PROBE_TIMEOUT,
                probe_candidate(&mut stream, resources.config.max_frame_bytes),
            )
            .await
            .map_err(|_| "candidate status timeout".to_string())??;
            if !fresh(status, reference) {
                return Err("candidate tick is stale or unavailable".to_string());
            }
            quality.observe(Some(started.elapsed()), Instant::now());
        }
        Ok((stream, handshake, quality))
    })
    .await
    .map_err(|_| "candidate preparation timeout".to_string())?
}

async fn probe_candidate(
    stream: &mut TcpStream,
    max_frame_bytes: usize,
) -> Result<crate::types::TickStatus, String> {
    let nonce = rand::random::<u32>().max(1);
    stream
        .write_all(&build_request_frame(
            REQUEST_CURRENT_TICK_INFO_TYPE,
            nonce,
            &[],
        )?)
        .await
        .map_err(|e| e.to_string())?;
    let mut consumed = 0usize;
    for _ in 0..4096 {
        let mut header = [0; HEADER_SIZE];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|e| e.to_string())?;
        let size = decode_frame_size(&header);
        consumed = consumed.saturating_add(size);
        if size < HEADER_SIZE || size > max_frame_bytes || consumed > 8 * 1024 * 1024 {
            return Err("candidate response budget exceeded".to_string());
        }
        let mut frame = vec![0; size];
        frame[..HEADER_SIZE].copy_from_slice(&header);
        stream
            .read_exact(&mut frame[HEADER_SIZE..])
            .await
            .map_err(|e| e.to_string())?;
        if header[4..8] != nonce.to_le_bytes() {
            continue;
        }
        if header[3] != RESPOND_CURRENT_TICK_INFO_TYPE {
            return Err("candidate refused status request".to_string());
        }
        return parse_tick_status_from_frame(&frame)
            .ok_or_else(|| "malformed candidate status".to_string());
    }
    Err("candidate frame budget exceeded".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn measured(ms: u64, good: u32, bad: u32, now: Instant) -> Quality {
        Quality {
            rtt: Duration::from_millis(ms),
            good,
            bad,
            updated: now,
        }
    }
    #[test]
    fn replacement_skips_busy_peers_without_losing_their_quality() {
        let now = Instant::now();
        let mut state = NodeState::new(10, &[]);
        let mut receivers = Vec::new();
        let mut ids = Vec::new();
        for octet in 1..=3 {
            let remote = SocketAddrV4::new(std::net::Ipv4Addr::new(8, 8, 8, octet), 21841);
            let (tx, rx) = mpsc::channel(4);
            let (disconnect, _watch) = watch::channel(false);
            let id = state
                .register_session(remote, true, tx, disconnect, 21841)
                .unwrap();
            state.set_connected_at(id, now - Duration::from_secs(31));
            ids.push(id);
            receivers.push(rx);
        }
        let quality = HashMap::from([
            (ids[0], measured(500, 0, 3, now)),
            (ids[1], measured(300, 3, 0, now)),
            (ids[2], measured(200, 3, 0, now)),
        ]);
        let candidate = measured(100, 3, 0, now);
        assert_eq!(replacement(&state, &quality, &candidate, now), Some(ids[0]));
        let target = state.target(ids[0]).unwrap();
        let frame = OutboundFrame::try_new(
            Bytes::from_static(b"pending transaction"),
            target.byte_budget,
            Arc::new(Semaphore::new(100)),
        )
        .unwrap();
        target.tx.try_send(frame).unwrap();
        assert_eq!(replacement(&state, &quality, &candidate, now), Some(ids[1]));
        let in_flight = receivers[0].try_recv().unwrap();
        assert_eq!(replacement(&state, &quality, &candidate, now), Some(ids[1]));
        state.set_connected_at(ids[1], now);
        assert_eq!(replacement(&state, &quality, &candidate, now), Some(ids[2]));
        state.set_connected_at(ids[2], now);
        assert_eq!(replacement(&state, &quality, &candidate, now), None);
        drop(in_flight);
        assert_eq!(replacement(&state, &quality, &candidate, now), Some(ids[0]));
    }

    #[test]
    fn rotation_requires_repeated_evidence_and_material_improvement() {
        let now = Instant::now();
        let old = measured(200, 3, 0, now);
        assert!(!measured(100, 2, 0, now).improves(&old, now));
        assert!(!measured(160, 3, 0, now).improves(&old, now));
        assert!(measured(140, 3, 0, now).improves(&old, now));
        assert!(!measured(10, 3, 0, now).improves(&measured(25, 3, 0, now), now));
        assert!(!measured(100, 3, 0, now).improves(&old, now + Duration::from_secs(61)));
        assert!(!measured(300, 3, 0, now).improves(&measured(200, 0, 2, now), now));
        assert!(measured(300, 3, 0, now).improves(&measured(200, 0, 3, now), now));
    }
    #[test]
    fn one_bad_sample_cannot_trigger_rotation_and_recovery_resets_it() {
        let now = Instant::now();
        let mut q = measured(200, 8, 0, now);
        q.observe(None, now);
        assert_eq!((q.good, q.bad), (0, 1));
        q.observe(Some(Duration::from_millis(100)), now);
        assert_eq!((q.rtt, q.good, q.bad), (Duration::from_millis(100), 1, 0));
    }
    #[test]
    fn expired_history_cannot_be_revived_by_one_sample() {
        let start = Instant::now();
        let now = start + SAMPLE_MAX_AGE + Duration::from_nanos(1);
        let candidate = measured(100, 3, 0, now);
        for (mut old, sample, expected) in [
            (
                measured(400, 3, 0, start),
                Some(Duration::from_millis(200)),
                (Duration::from_millis(200), 1, 0),
            ),
            (measured(400, 0, 3, start), None, (Duration::ZERO, 0, 1)),
        ] {
            old.observe(sample, now);
            assert!(!candidate.improves(&old, now));
            assert_eq!(
                (old.rtt, old.good, old.bad, old.updated),
                (expected.0, expected.1, expected.2, now)
            );
            old.observe(sample, now + SAMPLE_INTERVAL);
            assert!(!candidate.improves(&old, now + SAMPLE_INTERVAL));
            old.observe(sample, now + SAMPLE_INTERVAL * 2);
            assert!(candidate.improves(&old, now + SAMPLE_INTERVAL * 2));
        }
    }

    #[test]
    fn history_remains_valid_at_exact_expiry_boundary() {
        let start = Instant::now();
        let now = start + SAMPLE_MAX_AGE;
        let mut old = measured(400, 3, 0, start);
        old.observe(Some(Duration::from_millis(200)), now);
        assert_eq!(
            (old.rtt, old.good, old.bad, old.updated),
            (Duration::from_millis(350), 4, 0, now)
        );
    }

    #[test]
    fn freshness_is_same_epoch_and_never_accepts_missing_reference() {
        let s = crate::types::TickStatus {
            epoch: 2,
            tick: 97,
            initial_tick: 0,
            tick_duration_ms: 0,
            aligned_votes: 0,
            misaligned_votes: 0,
        };
        assert!(fresh(s, Some((2, 100))));
        assert!(!fresh(s, Some((2, 101))));
        assert!(!fresh(s, Some((3, 100))));
        assert!(!fresh(s, None));
    }
    #[tokio::test]
    async fn candidate_correlates_nonce_and_checks_exact_core_payload() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 8];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(&request[..4], &[8, 0, 0, REQUEST_CURRENT_TICK_INFO_TYPE]);
            let nonce = u32::from_le_bytes(request[4..].try_into().unwrap());
            assert_ne!(nonce, 0);
            let mut payload = [0; 16];
            payload[2..4].copy_from_slice(&229u16.to_le_bytes());
            payload[4..8].copy_from_slice(&100u32.to_le_bytes());
            for _ in 0..100 {
                stream
                    .write_all(
                        &build_request_frame(
                            RESPOND_CURRENT_TICK_INFO_TYPE,
                            nonce.wrapping_add(1),
                            &payload,
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
            }
            stream
                .write_all(
                    &build_request_frame(RESPOND_CURRENT_TICK_INFO_TYPE, nonce, &payload).unwrap(),
                )
                .await
                .unwrap();
        });
        let mut stream = TcpStream::connect(endpoint).await.unwrap();
        let status = timeout(PROBE_TIMEOUT, probe_candidate(&mut stream, 1024))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((status.epoch, status.tick), (229, 100));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn candidate_rejects_oversized_frame_before_payload_allocation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 8];
            stream.read_exact(&mut request).await.unwrap();
            request[..3].copy_from_slice(&[255, 255, 255]);
            stream.write_all(&request).await.unwrap();
        });
        let mut stream = TcpStream::connect(endpoint).await.unwrap();
        assert_eq!(
            probe_candidate(&mut stream, 1024).await.unwrap_err(),
            "candidate response budget exceeded"
        );
        server.await.unwrap();
    }
}
