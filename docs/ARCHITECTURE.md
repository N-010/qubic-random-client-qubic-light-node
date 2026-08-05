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
- report whether one current-epoch historical tick has transactions;
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
- If the established count remains below the configured critical threshold,
  emergency DNS bootstrap retries with bounded exponential backoff.

QubicLightNode accepts no inbound Qubic sessions and does not advertise itself
as a public peer.

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
protocol violation according to request type.

Resource invariants are:

- at most 64 concurrent peer-backed gRPC queries;
- at most 32 concurrent broadcasts;
- at most three peer sessions raced per query;
- at most six peer sessions selected for transaction dissemination;
- 1,024 queued frames and 32 MiB of queued bytes per peer;
- 256 MiB shared outbound-byte budget;
- bounded per-request response channels and byte permits;
- bounded signed-message verification concurrency and deduplication caches.

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

Each new session may request `BroadcastComputors` until trusted keys are
available. The payload must contain an epoch, 676 non-zero public keys, a
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

Missing current-epoch keys produces an unavailable observation, never a false
empty result. `has_transactions` is true if at least one verified digest is
non-zero; raw transactions and TickData are not returned over gRPC.

## gRPC operation flows

### GetStatus

`GetStatus` reads the monotonic cache without contacting a peer. Before the
first valid observation it returns `ok=false`. Afterwards it returns the
greatest observed epoch/tick and identifies the structural cache as its source.
`initial_tick`, `tick_duration_ms`, and vote counts are unavailable and remain
zero; RandomClient conservatively derives its epoch initial tick and uses a
1,000 ms duration fallback.

### GetTickTransactions

The service admits the request through the shared peer-query semaphore, sends
Core `RequestTickData` to up to three established sessions, and waits within
the configured end-to-end deadline. The first fully authenticated TickData
returns its transaction-presence boolean. `END_RESPONSE`, `TRY_AGAIN`, missing
computor keys, timeout, and disconnection are unavailable outcomes. A malformed
or bad-signature peer response disconnects and penalizes that peer.

### QueryContractFunction

The gRPC boundary requires contract index `1..=1023`, an input type fitting
`u16`, and at most 65,535 input bytes. The service encodes the exact Core
request, races up to three sessions, and returns the first structurally valid,
non-empty response before the deadline. Empty output indicates invocation
failure; `TRY_AGAIN` and `END_RESPONSE` without data are errors.

Core provides no proof for this response. Racing peers improves availability
but not authenticity, and no agreement is required. This architecture
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

Admission then applies a global token bucket of 100 requests/second with burst
200 and a per-client bucket of 10 requests/second with burst 20. Simultaneous
identical byte sequences are coalesced behind one leader; completed submissions
are not permanently deduplicated. The transaction frame is offered to up to
six established peer queues.

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
| Contract query | Exact Core request and bounded non-empty response | First-success output is unauthenticated and not consensus state. |
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
| `QlnBackend::query_contract_function` | `RequestContractFunction`, `RespondContractFunction`, and `processRequestContractFunction` define raw input/output and empty failure. | `query_contract_function`, pending response rules, tonic method. | Exact wire mapping plus explicit first-success trust exception. |
| `QlnBackend::broadcast_transaction` | `Transaction::checkValidity`, `processBroadcastTransaction`, K12, and FourQ define accepted bytes. | `validate_transaction`, `broadcast_transaction_to_network`, tonic method. | Exact validation with bounded outbound fanout adaptation. |

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
