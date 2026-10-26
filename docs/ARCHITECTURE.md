# QubicLightNode Architecture

## Purpose and authority

This document is the canonical description of QubicLightNode's component
boundaries, runtime flows, resource rules, protocol adaptations, and security
properties. `README.md` is the canonical user-facing operation guide, and
`proto/lightnode.proto` is the public gRPC schema.

Sources have the following precedence:

1. Current production RandomClient code and architecture define the required
   backend capabilities and observable caller behavior.
2. Qubic Core defines wire layouts, message identifiers, validation rules,
   cryptography, and peer request/response semantics.
3. This repository adapts that required subset to Rust, Tokio, and tonic
   without creating an independent Qubic protocol.
4. Explicit trust exceptions recorded in this document govern only their
   named operations.

A known conflict between these sources must not remain unresolved. A behavior
change that alters a trust decision requires explicit approval and an update
to this document instead of silently editing its meaning.

## Traceability baseline

This document was established on 2026-08-06 against:

- QubicLightNode base revision
  `0a01e935226ab5a3f0d1e7f0e4cb0504c05683b8`;
- RandomClient revision
  `d840000a449cf8f8cae6658bb778762e87b108ca` and its current
  `QlnBackend` implementation;
- the unversioned local `QThirtyFour/core` worktree inspected on that date.

The Core protocol baseline covers `common_def.h`, `network_message_type.h`,
`public_peers.h`, `contract.h`, `computors.h`, `tick.h`, `transactions.h`,
`qubic.cpp`, and `four_q.h`. The SHA-256 of the sorted manifest containing
`path + space + SHA-256(file)` is
`e03e3285d4e8605b17d93d916be027eda7e9a02a5871572edb614e4566538556`.
This fingerprint is used because the Core worktree has no repository revision.

## System context and product boundary

QubicLightNode is a single-process adapter between one trusted RandomClient
deployment and public Qubic peers:

```text
RandomClient
    |  local/plaintext gRPC
    v
QubicLightNode
    |  outbound Core TCP sessions
    v
Public Qubic peers
```

DNS bootstrap at `api.qubic.global`, manual seed peers, and peer-exchange gossip
provide connection candidates. The process owns no wallet seed, creates no
transactions, and persists no peer or consensus state.

The product boundary contains exactly four RandomClient operations:

- observe current epoch/tick status;
- report transaction presence and the complete authenticated digest set for
  one current-epoch historical tick;
- execute a read-only contract-function query with raw input/output bytes;
- validate and broadcast exact signed transaction bytes.

QubicLightNode does not provide balance lookup, inbound Qubic connectivity,
general relay behavior, an HTTP API, gRPC reflection, durable chain storage, or
a locally invented peer-consensus algorithm.

## Component map

| Component | Responsibility |
| --- | --- |
| `src/main.rs` / `src/app.rs` | Enter Tokio, build shared state, start networking and gRPC, emit trust warnings, and handle `Ctrl+C`. |
| `src/config.rs` / `src/dns.rs` | Parse and validate CLI configuration, manual peers, bootstrap peers, timeouts, and resource limits. |
| `src/network.rs` | Maintain outbound sessions, perform the Core handshake, read/write frames, bootstrap computors, dispatch broadcasts, and enforce connection policy. |
| `src/peer_selection.rs` | Measure active peers and qualify one replacement independently of broadcasts. |
| `src/state.rs` | Own the bounded peer pool, session registry, cooldowns, queue admissions, and signed-message deduplication windows. |
| `src/frame.rs` / `src/codec.rs` | Encode and decode the required Core layouts, validate transactions, compute K12 digests and Qubic identifiers, and reject bogon gossip. |
| `src/pending.rs` / `src/peer_api.rs` | Correlate responses by peer and `dejavu`, enforce response contracts, race bounded peer queries, and apply deadlines. |
| `src/verified.rs` | Authenticate computor sets and verify exact Core `TickData` using the current computor key. |
| `crates/qubic-fourq-verifier` | Provide the local SchnorrQ/FourQ verifier aligned with Core. |
| `src/grpc_api.rs` / `proto/lightnode.proto` | Expose the four RandomClient RPCs, validate requests, apply API admission limits, and map results. |
| `src/types.rs` / `src/logging.rs` | Hold shared API state and produce bounded operational diagnostics. |

The networking and gRPC tasks share `NodeState`, `PendingRequests`, the
monotonic epoch/tick cache, authenticated computor state, one global outbound
byte budget, and immutable configuration through `Arc` ownership.

## Startup and shutdown

1. Clap parses configuration and clamps documented minimum timeouts.
2. When DNS bootstrap is enabled and no manual peer was supplied, the process
   requests an initial peer set from `api.qubic.global`. Failure is logged and
   startup continues.
3. The app constructs bounded peer, pending-request, authenticated-computor,
   epoch/tick, verification, and outbound-budget state.
4. Startup logs the operating configuration and prints explicit warnings for
   unauthenticated status and contract-query results.
5. The dial loop runs in a Tokio task while tonic serves gRPC on the configured
   address.
6. A gRPC server failure returns a process error. `Ctrl+C` ends the app and
   dropping the Tokio runtime cancels owned background work.

There is no durable recovery state or shutdown drain. Restart begins with no
connected peers, no authenticated computor list, and no tick observation.

## Peer lifecycle and discovery

- HTTPS bootstrap response bodies are limited to 1 MiB before JSON parsing.
  Both declared Content-Length and streamed chunks are checked; an oversized
  response fails bootstrap under the existing retry policy.
- Initial, emergency, and periodic DNS requests share the same count policy:
  a non-zero `--dns-lite-peers` is used exactly; zero selects three times the
  outbound target (saturating), with a minimum of eight.
- Manual, DNS, and gossip peers enter one bounded pool with that priority
  order. Manual peers are not displaced by lower-priority discoveries.
- Bogon, multicast, documentation, loopback, and private addresses received
  through public-peer gossip are rejected. A peer never gossips itself back
  into the pool.
- The dial loop randomly selects eligible addresses until connected plus
  pending sessions reaches `target_outbound`. One active connection per IPv4
  address is permitted.
- TCP connect, handshake, frame completion, and writes have separate deadlines.
  TCP_NODELAY and platform keepalive are enabled when supported.
- Every connection must exchange an exact 24-byte Core
  `EXCHANGE_PUBLIC_PEERS` frame: 8-byte header plus four IPv4 addresses. The
  session is registered only after this exchange succeeds.
- Each registered session owns a bounded writer queue, reader task, disconnect
  signal, and computor-bootstrap task. I/O failure, malformed protocol input,
  or peer-specific queue failure removes and cools down the peer.
- If a connection worker panics or is cancelled, its supervisor unregisters
  the session, removes any saved disconnect reason, and notifies pending requests.
  This local task failure does not add a peer penalty; an already applied
  peer-specific penalty is retained.
- If the established count remains below the configured critical threshold,
  emergency DNS bootstrap retries with bounded exponential backoff.

QubicLightNode accepts no inbound Qubic sessions and does not advertise itself
as a public peer.

Initial and emergency DNS bootstrap share the automatic lite-peer count:
`max(8, target_outbound * 3)`, using saturating multiplication so accepted CLI
values cannot panic or wrap the count. A non-zero explicit `dns_lite_peers`
overrides this automatic count. Response-body and known-peer bounds still apply.

### Background peer selection (approved 2026-10-25)

RandomClient needs ready connections at its T-4 reveal gate. A separate Tokio
task evaluates the active pool without probing or dialing on the broadcast
path. A cycle starts after a ten-second delay and probes at most three sessions
concurrently, each with a one-second deadline, using Core RequestCurrentTickInfo
(27) and its exact 16-byte response (28). Existing pending correlation, queues
and byte budgets apply; local overload is not scored as peer failure. A good
sample has the snapshot epoch and a tick no more than three behind the snapshot
taken before the probe. This uses the existing unauthenticated cache, not a
consensus rule. RTT uses a 3/4 previous + 1/4 current EWMA.

At most once per 30 seconds, when the working pool is full and no ordinary dial
is pending, one eligible address is reserved from the bounded discovery pool.
There may be one additional TCP socket beyond target_outbound during qualification.
It completes the Core handshake and three correlated probes, 250 ms apart,
within ten seconds overall. Each probe has a one-second deadline and budgets
of 4,096 frames and 8 MiB, plus the configured per-frame maximum. Unrelated frames
are discarded during qualification and do not update trusted state or the tick
cache. The candidate is not an API or broadcast recipient before activation.

Replacement requires three good candidate samples and either three consecutive
bad old-peer samples, or three good old-peer samples with an RTT improvement
greater than both 25% and 20 ms. Comparison observations expire after 60 seconds.
A gap longer than 60 seconds between scored samples resets both streaks and
the RTT estimate before recording the next sample. Skipped local-overload samples
do not renew evidence; one new observation cannot revive an expired history.
Recovery resets the bad streak. A single failure cannot trigger replacement.
The old session must be at least 30 seconds old with no queued or in-flight
outbound bytes. Ranking excludes sessions that cannot currently be retired,
so a busy worst peer cannot hide another eligible improvement. Quality history
is retained for busy peers. Activation rechecks eligibility and registers the new connection
before administratively removing the old one in the same state-lock section.
Before retirement, it atomically acquires the entire old peer byte budget and
closes that semaphore while holding all permits. Every outbound frame holds its
permits from admission through write completion, so a concurrent admission
either prevents retirement or fails against the closed budget, including through
a previously copied RelayTarget. If admission wins after the initial idle check,
the unexposed candidate registration is rolled back under the same state lock.
No queued or in-flight frame is cancelled by quality-based retirement.
If a competing ordinary dial filled a vacancy, the candidate is discarded;
another peer is not evicted. A vacancy can accept the qualified candidate without
retiring a connection. Existing queue-acceptance semantics still apply: this
does not prove remote receipt of a previously accepted frame.

Rejected candidates and retired addresses have a 120-second selection cooldown,
not a protocol-violation penalty. Candidate failure leaves the active pool intact.
One-connection-per-IP checks and connected-plus-pending accounting still apply.
DNS candidates refresh every five minutes only when DNS bootstrap is enabled.
Origin priorities, pool bounds and ordinary reconnect backoff remain unchanged.
With no spare eligible address (for example max_known_peers equals target_outbound),
measurement continues but replacement is impossible. PEER_SELECTION logs report
candidate checks, qualification, replacement and rejection without payloads.

This user-approved availability adaptation supports the existing four client
operations. Core supplies the messages and handshake, not this RTT policy.
Peers can lie about ticks or prioritize probes over broadcasts; a false-future
cache can make healthy peers appear stale. No authentication, consensus or
inclusion guarantee is added. Wire formats, RPCs, signatures, fanout, client
scheduling and VPN routes are unchanged. The <=1% goal needs live verification.

## Framing, correlation, and resource limits

Core frames use an 8-byte header: 24-bit little-endian frame size, one-byte
message type, and 32-bit `dejavu`. Announced sizes below the header are protocol
violations; sizes above the configured local maximum end the connection as a
local-policy refusal. Incomplete frames must finish before the peer-frame
deadline.

Peer-backed requests allocate a non-zero random `dejavu` and register an exact
response contract before dispatch. The contract specifies permitted message
types, sizes, counts, terminal frames, total frames, and total bytes for each
peer. Responses from a wrong peer, repeated terminal response, incompatible
type, or incompatible size cannot satisfy the request. Completed identifiers
remain in a bounded 60-second cache so late frames are ignored or treated as a
protocol violation according to request type. A control request cancelled or
timed out before receiving any response ignores late frames; it must not penalize
a peer for its first delayed response. A completed control request that received
a response retains duplicate-response rejection only for the session that
actually responded. Another peer with the same identifier is ignored, as it is
while the request is active; identifier equality alone cannot justify a penalty.

Resource invariants are:

- at most 64 concurrent peer-backed gRPC queries;
- at most 32 concurrent broadcasts;
- at most three peer sessions raced per query;
- a configured 1–32 peer sessions selected for local transaction dissemination
  (default six);
- 1,024 queued frames and 32 MiB of queued bytes per peer;
- 256 MiB shared outbound-byte budget;
- bounded per-request response channels and byte permits;
- at most eight queued or executing TickData verification jobs process-wide;
  each blocking job owns its permit through completion, even when its query is
  cancelled or times out. Admission precedes copying the verification payload;
  exhaustion returns local overload without penalizing the responding peer;
- bounded signed-message verification concurrency and deduplication caches.

After response validation and byte-budget admission, each queued pending
response owns a compact copy of its frame. It must not retain a shared TCP
receive allocation larger than the charged frame length. Copies are bounded by
the existing response contracts and permits; correlation, ordering, deadlines,
and overload results are unchanged.

Peer-specific queue exhaustion disconnects and penalizes that peer. Global
local-memory pressure rejects new work without attributing the condition to a
healthy peer.

## Observed and authenticated network state

### Structural epoch/tick cache

`BroadcastTick` contributes status only when its frame and 352-byte payload
have exact sizes and its literal computor index is below 676.
`RespondCurrentTickInfo` contributes status only with its exact 16-byte Core
payload. Either message can update the lock-free cache before pending-response
routing or cryptographic work.

Only a greater packed `(epoch, tick)` replaces the current value. This prevents
delayed lower values from moving status backwards, but a malicious peer can
advance the cache to a false future value. No signature, epoch-range check, or
quorum authenticates this fast path. This architecture explicitly accepts
that availability and integrity tradeoff.

Revisit this exception if RandomClient starts treating status as consensus or
financial proof, Core provides an authenticated current-tick proof, or
false-future cache poisoning is observed in practice.

### Authenticated computor set

Each session maintains computor retrieval until disconnect. At the configured
reconnect interval it checks whether the applied authenticated set matches the
observed epoch (or any authenticated set before the first tick observation).
Missing or mismatched keys trigger Core `RequestComputors`, with exponential
retry delays capped at 30 seconds. Success pauses requests rather than ending
the task, so an epoch change refreshes keys on existing sessions. Receipt is
not readiness: signature verification and application must finish first.
Disconnect cancels both response waits and retry waits. The payload must contain an epoch, 676 non-zero public keys, a
64-byte signature, and only Core's permitted zero-to-four trailing struct
padding. The signature covers the canonical prefix with KangarooTwelve and is
verified against the built-in arbitrator identity using FourQ.

Authenticated older epochs are ignored, identical data is a duplicate, and a
different list for the same authenticated epoch is a conflict. Only the newest
accepted set supplies TickData verification keys.

### Authenticated TickData

`TickData` is accepted only when all of the following hold:

- payload size exactly matches the Core structure;
- response tick equals the requested tick;
- computor index is in range and equals `tick % 676`;
- calendar fields are valid and the timestamp is not more than five seconds in
  the future;
- non-zero transaction digests are unique;
- an authenticated computor list exists for the same epoch;
- K12 over the Core signed body, with the message-type transform, verifies
  under the designated computor's FourQ key.

Calendar field bounds follow Core's wire guard (February uses wire year modulo
four). Timestamp conversion separately follows Core `platform/time.h::dayIndex`,
including Gregorian century exceptions, shifted to the Unix epoch for comparison
with the host clock. The field guard and timestamp conversion are not interchangeable.

Missing current-epoch keys produces an unavailable observation, never a false
empty result. `has_transactions` is true if at least one verified digest is
non-zero. All non-zero 32-byte digests are returned over gRPC in Core slot
order as `transaction_digests`, with `digests_complete=true` only for a
successfully authenticated response (including an empty set). Raw transaction
bytes and the full TickData structure are not returned. These digests identify
K12 of complete signed transactions, not successful contract execution or
consensus finality. The local gRPC consumer trusts this node to authenticate
the leader signature; the response is not a portable cryptographic proof.

## gRPC operation flows

### GetStatus

`GetStatus` reads the monotonic cache without contacting a peer. Before the
first valid observation it returns `ok=false`. Afterwards it returns the
greatest observed epoch/tick and identifies the structural cache as its source.
`initial_tick`, `tick_duration_ms`, and vote counts are unavailable and remain
zero; RandomClient conservatively derives its epoch initial tick and uses a
1,000 ms duration fallback.

The internal `RespondCurrentTickInfo` decoder converts Core's unsigned 16-bit
elapsed whole seconds in the current tick to unsigned 32-bit milliseconds.
This is elapsed time, not an average tick-duration estimate. It is not published
by the atomic status cache or used for peer RTT ranking. The maximum wire value
converts to 65,535,000 ms without truncation.

### GetTickTransactions

The service admits the request through the shared peer-query semaphore, sends
Core `RequestTickData` to up to three established sessions, and waits within
the configured end-to-end deadline. The first fully authenticated TickData
returns its transaction-presence boolean and complete digest list. Errors leave
`digests_complete=false` and the list empty. The additive protobuf fields 5/6
preserve the existing fields; new clients distinguish legacy presence-only
responses from authenticated empty lists. `END_RESPONSE`, `TRY_AGAIN`, missing
computor keys, timeout, and disconnection are unavailable outcomes. A malformed
or bad-signature peer response disconnects and penalizes that peer.

The query deadline also bounds waiting for the state mutex when applying a
protocol penalty. If that wait exceeds the deadline, the query returns timeout
and releases its registrations and owned tasks; the pending penalty is cancelled,
as it is on external request cancellation. No detached penalty task is spawned.
The same rule applies to contract queries. With an available mutex, malformed
responses retain the existing disconnect and cooldown behavior.

### QueryContractFunction

The gRPC boundary requires contract index `1..=1023`, an input type fitting
`u16`, and at most 65,535 input bytes. The service encodes the exact Core
request and races up to three sessions under one end-to-end deadline. At query
admission it snapshots the structural epoch/tick cache. Each selected session
first answers a separately correlated Core `RequestCurrentTickInfo`; only a
non-zero tick in the snapshot epoch, at most one tick behind its tick, permits
the contract request on that same session. No reference or no eligible reply
returns unavailable, never an empty provider status. Lag does not penalize or
disconnect the peer. Both stages share the original API deadline and resource
limits; completion or cancellation releases both registrations. The first
structurally valid non-empty contract response from an eligible peer wins. Empty output indicates invocation
failure; `TRY_AGAIN` and `END_RESPONSE` without data are errors.

Core provides no proof for this response. Racing peers improves availability
but not authenticity, and no agreement is required. The freshness filter was
explicitly approved on 2026-09-08 after a production peer was observed about
157,000 ticks behind the other sampled peers. It is a local availability
adaptation using Core messages, not a Core consensus rule. A lying peer or
false-future cache observation can still defeat availability or integrity. This architecture
explicitly accepts this HIGH integrity risk for the current RandomClient
caller.

Revisit this exception if Core adds a proof or authenticated response,
RandomClient uses the result for an irreversible or security-sensitive
decision, equivalent local execution becomes available, or malicious
inconsistency is observed in practice.

### BroadcastTransaction

Before network fanout, the service requires the exact Core transaction layout:
80-byte fixed fields, no more than 1,024 input bytes, one 64-byte signature,
non-negative amount within Core's maximum, exact total length, and a valid K12
plus FourQ signature under the source public key.

Before signature verification, admission applies a global token bucket of 100 requests/second with burst
200 and a per-client bucket of 10 requests/second with burst 20. The broadcast
concurrency permit is also acquired before verification. Non-empty malformed or
bad-signature submissions consume admission capacity; they cannot bypass the
CPU-work limits by failing validation. No unverified bytes reach fanout. Simultaneous
identical byte sequences are coalesced behind one leader; completed submissions
are not permanently deduplicated. Registering a follower removes closed follower
channels first, so repeated cancellation cannot grow the waiter list beyond the
broadcast concurrency bound while the leader is pending. Leader completion or
cancellation removes the entry and releases all remaining followers. The transaction frame is offered to up to
`broadcast_fanout` established peer queues (1–32, default six).

On 2026-10-25 the user explicitly approved an experiment up to 32 direct
recipients. Core `src/network_core/peers.h` defines
`DISSEMINATION_MULTIPLIER = 6` and random recipient selection. The default
preserves that count and selection; `--broadcast-fanout` is an explicit local
submission adaptation, not a change to Core wire format or a general relay.
The production caller remains RandomClient `QlnBackend::broadcast_transaction`.
It addresses measured short dissemination windows after the T-4 reveal gate.
Recipients are distinct established sessions, limited by the configured count
and available sessions. Raising fanout increases traffic and queue pressure;
all signature checks, rate limits, FIFO queues, byte budgets, per-peer failure
handling, immutable bytes, and queue-acceptance semantics remain unchanged.
No new peer trust or inclusion guarantee is introduced.

`ok=true` means at least one peer queue accepted the validated frame. It does
not mean a peer transmitted it, a computor received it, or the contract
executed it. The returned transaction ID is the canonical lowercase Qubic
identity derived from K12 over the complete signed bytes.

## Trust boundaries and failure model

| Boundary | Security property | Explicit limitation |
| --- | --- | --- |
| RandomClient → gRPC | Local request validation and bounded admission | No TLS or application authentication; default loopback is the deployment boundary. |
| QubicLightNode → public peers | Exact framing, deadlines, correlation, and peer penalties | Public peers are untrusted and can withhold, delay, or selectively answer. |
| Status cache | Exact structural parsing and monotonicity | One peer can report an unauthenticated false future epoch/tick. |
| Contract query | Same-session current-tick preflight, exact Core request, bounded non-empty response | First eligible success and the reference cache are unauthenticated, not consensus state. |
| Computors/TickData | Arbitrator and designated-computor FourQ verification | Availability depends on obtaining a current authenticated computor list. |
| Transaction broadcast | Local layout and signature verification | Queue acceptance is not delivery or execution confirmation. |
| DNS bootstrap | HTTPS bootstrap and bounded peer admission | DNS provides candidates, not trusted consensus data. |

Malformed framing, invalid pending responses, malformed computor data, invalid
computor signatures, malformed tick messages, and invalid API responses are
connection-fatal protocol violations. Ordinary network errors, peer refusal,
timeout, no available peer, and local overload return unavailable/error results
without inventing data.

The service must remain safe under repeated retries. RandomClient may resubmit
identical signed bytes until an immutable target tick; QubicLightNode therefore
must not mutate, re-sign, or retarget transactions and must treat later retries
as independent delivery attempts.

## Source-to-implementation traceability

| RandomClient requirement | Core source and invariant | QubicLightNode implementation | Mapping |
| --- | --- | --- | --- |
| `QlnBackend::tick_info` | `BroadcastTick`, `RespondCurrentTickInfo`, and their Core handlers define layouts; Core has no authenticated current-info proof. | `parse_tick_status_from_frame`, monotonic `latest_epoch_tick`, `get_status`. | Explicit unauthenticated-status adaptation. |
| `QlnBackend::tick_has_transactions` | `RequestTickData`, `TickData`, `processRequestTickData`, and `processBroadcastFutureTickData` define response and verification semantics. | `query_tick_data`, `TrustedNetworkState::verify_tick_data`, `get_tick_transactions`. | Exact wire mapping with bounded async adaptation. |
| `QlnBackend::query_contract_function` | `RequestCurrentTickInfo`, `RespondCurrentTickInfo`, `processRequestCurrentTickInfo`, `RequestContractFunction`, `RespondContractFunction`, and `processRequestContractFunction` define current status, raw input/output and empty failure. | `query_contract_function`, `query_contract_on_peer`, pending response rules, tonic method. | Exact wire mapping plus approved freshness preflight and first-eligible-success trust exception. |
| Current-epoch authenticated TickData keys | `Computors`, `processRequestComputors`, `processBroadcastComputors` define layout, retrieval and arbitrator authentication. | `computor_bootstrap`, `has_computors`, `apply_computors`. | Exact wire/authentication mapping; persistent bounded session retries are an async adaptation. |
| `QlnBackend::broadcast_transaction` | `Transaction::checkValidity`, `processBroadcastTransaction`, K12, and FourQ define accepted bytes. | `validate_transaction`, `broadcast_transaction_to_network`, tonic method. | Exact validation with bounded outbound fanout adaptation. |

The 2026-09-08 repair was traced against RandomClient `d840000a449cf8f8cae6658bb778762e87b108ca`
plus its queue repair, QubicLightNode `8fa1b9cdd7d7f581341070ea05c6102a902d514f`
plus this repair, and Core `f55b46126c99a1c3f3266164c744b3d0cd694d9c`.
The Core worktree also contained unrelated test-project changes and untracked
operator files; the relevant production handlers/layouts were unmodified.
Regression tests cover epoch refresh on a live session, 0/1/2-tick freshness
boundaries, wrong epochs, stale/current peer races, shared deadlines, and
pending-registration cleanup. Client tests cover fair retry scheduling,
deduplication including active checks, capped backoff and epoch cancellation.

The local verifier is covered by Core-derived vectors and boundary tests. Frame,
configuration, pending-route, peer-state, gRPC, query, verification, overload,
retry, and protocol-violation behavior are tested in their owning Rust modules.

## Architectural invariants

- The public schema contains exactly the four current RandomClient methods.
- There is no balance operation and no balance-gated broadcast path.
- Qubic connectivity is outbound-only.
- Status and contract output never claim cryptographic or quorum authenticity.
- TickData and submitted transaction signatures never bypass FourQ validation.
- Raw transaction bytes are never altered, regenerated, signed, or retargeted.
- Peer work, response work, verification work, and queued bytes remain bounded.
- Peer-caused protocol failure may penalize a peer; global local pressure must
  not.
- No persistent state is treated as Qubic consensus state.
- New protocol behavior requires a demonstrated RandomClient production caller
  and a current Core parity review.

The 2026-10-25 digest API extension is traced in
`docs/ARCHITECTURE_COMPLIANCE_AUDIT.md` against RandomClient
`ab98d55b9bd10f61690f4fa017dcc13ab50faef2` plus the existing T-4 worktree change,
this node's `b959418775c1f1d0da833a04c596b87719e9d479`, and Core
`98102e74d50db4c21473ef3e9ad21cdca6fd69aa`. It exposes Core digests for the
client's delayed full-hash comparison without changing validation or trust.

## Change consistency checklist

Before changing behavior, record current RandomClient and Core baselines, trace
the production caller, inspect the corresponding Core source and tests, and run
GitNexus impact analysis for affected symbols. Protocol changes require an
updated source-to-implementation mapping and parity tests.

Before completion, compare the final behavior with RandomClient and Core,
update this document, README, protobuf comments, and trust decisions as
applicable, run
GitNexus change detection, and execute formatting, Clippy, and the complete
workspace test suite. A known discrepancy between code, schema, README,
architecture, trust decisions, and the traced upstream behavior must not
remain.


With `--traffic-log` or `--transaction-log`, transaction writes also emit `TX_RESULT` with the full
public transaction ID, target tick, peer, queue/write durations in microseconds,
observed ticks before/after the write, and written/failed result. A completed
write means delivery to the local TCP stack, not peer or leader acceptance.
No transaction bytes or preimages are logged.
Debug formatting of `ValidatedTransaction`, `OutboundFrame`, and `PendingFrame`
includes only the type and byte length, never the payload. This also protects
nested queue/event diagnostics. Queue timestamps do not change
FIFO ordering, byte budgets, deadlines, or broadcast acceptance semantics.

`--transaction-log` enables outbound frame and transaction diagnostics without
verbose inbound traffic. Both diagnostics switches default to off. The bounded
logger can drop records under overload; missing log entries are not evidence of
a missing network write.

## Build and release boundary

The workflow is supporting deployment infrastructure for the existing four
RandomClient operations. It uses `rust-toolchain.toml`, checks formatting,
Clippy and all workspace test targets on Linux and Windows, and runs dependency
security checks. Push, pull-request and manual events run these checks; a
weekly schedule runs security only. Jobs have bounded durations. Concurrency
is separated by workflow, event and ref; tag runs are not automatically cancelled.

Only pushed `v*` tags release, and the tag must equal the root package version.
Read-only build jobs produce both platform binaries, verify their version and
help commands, and upload artifacts with SHA256 sidecars. One publication job
waits for the complete matrix, requires both artifacts and matching checksums,
and publishes both binaries plus `SHA256SUMS`. Only that job has contents-write
permission. Remote publication is not atomic. No smoke command starts the node
or connects to Qubic peers. Protocol, API and trust mappings remain unchanged.

This workflow update was checked against RandomClient
`7ffec2a5e3fddbf77ed4b59469a02e93163a12f6` and Core
`98102e74d50db4c21473ef3e9ad21cdca6fd69aa` (unrelated NFT/asset worktree edits).
It preserves the existing node worktree changes based on `b959418775c1f1d0da833a04c596b87719e9d479`.
