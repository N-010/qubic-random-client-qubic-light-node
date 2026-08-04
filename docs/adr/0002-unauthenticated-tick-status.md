# ADR-0002: Restore non-blocking single-peer tick status

- Status: Accepted
- Date: 2026-08-04
- Decision owners: QubicLightNode and RandomClient owner

## Context

RandomClient's `QlnBackend::tick_info` needs a continuously advancing epoch and
tick for scheduling. The previous QubicLightNode implementation exposed status
only after collecting 451 aligned, FourQ-authenticated `BroadcastTick` votes.
On the observed public network that cache remained unchanged for minutes even
while eight peer sessions stayed connected. RandomClient therefore froze at an
old tick and could not complete its terminal reveal chain.

The sources reviewed for this decision were:

- QubicLightNode `8f035b4171103647c3cdaeb8cf8c48036a8f15c8`, with the current dirty
  implementation worktree preserved;
- RandomClient `ea02475916536036fa69ce8c89354568c4c42c1b`, with its current
  README, architecture-document, and `src/engine.rs` modifications preserved;
- QThirtyFour Core `f55b46126c99a1c3f3266164c744b3d0cd694d9c`, with unrelated
  tracked and untracked changes preserved.

Core defines both exact wire layouts, fully verifies `BroadcastTick`, and
performs current-epoch range checks when handling `RespondCurrentTickInfo`.
Using either message as QubicLightNode's reduced status source is the explicit
adaptation in this record. The 451-vote publication rule was a locally
introduced trust policy rather than a RandomClient requirement.

## Decision

Update a lock-free monotonic epoch/tick cache immediately after receiving
either:

- an exact-size Core `BroadcastTick` frame whose literal computor index is in
  range; or
- an exact-size Core `RespondCurrentTickInfo` frame.

The update occurs before pending-response routing, state-lock acquisition, and
cryptographic worker admission. `GetStatus` reads this cache directly. Runtime
`BroadcastTick` signature verification, vote aggregation, and quorum waiting
are removed from this status path. Arbitrator-authenticated computor lists and
FourQ verification remain required for TickData and submitted transactions.

The cache accepts only a greater packed `(epoch, tick)` value, preventing
delayed messages from moving status backwards.

## Consequences

Tick publication is no longer stalled by verification semaphore saturation or
the absence of a 451-vote quorum. RandomClient can resume planning as soon as a
connected peer sends one structurally valid current-tick message.

Status is unauthenticated. A malicious or faulty public peer can report a false
future epoch/tick and advance the monotonic cache, potentially delaying or
mis-scheduling RandomClient work until restart or until the network surpasses
that value. Structural validation prevents malformed memory or framing input,
but it does not prove consensus truth. QubicLightNode prints this warning at
startup, and `GetStatus` identifies the source and leaves unavailable quorum
metadata at zero.

## Alternatives considered

- Keep the 451-vote authenticated quorum. Rejected because it reproduced the
  availability failure and is not required by the production consumer.
- Verify one `BroadcastTick` signature before updating. Rejected by explicit
  owner decision because cryptographic worker admission can still throttle the
  time-critical update and does not authenticate `RespondCurrentTickInfo`.
- Query a matching-response peer quorum. Rejected because it would introduce a
  new local protocol/trust rule and retain quorum-induced latency.

## Revisit criteria

Revisit this decision if RandomClient begins treating status as financial or
consensus proof, if a proof-bearing current-tick protocol becomes available,
or if false-future tick poisoning is observed operationally.
