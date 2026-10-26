# Core develop compatibility review — 2026-09-08

## Conclusion and scope

No implementation or protobuf update is required by the reviewed Core
`develop` changes for the four current RandomClient operations. This is a
point-in-time source comparison of the relevant upstream delta, not a complete
security audit or a claim of live-network interoperability testing.

The four RPCs are required product behavior. Framing, peer sessions, request
correlation, computor authentication, and FourQ verification are supporting
dependencies. Mining, vote generation, unrelated contracts, and their new RPCs
are outside this repository's product boundary.

## Exact baselines

- QubicLightNode: `b959418775c1f1d0da833a04c596b87719e9d479` (`main`), clean
  before this report; package version `2.0.2`. No Rust or schema edits.
- RandomClient: `8fac3c50efe5b8aa16e9dbfafb936ce5e9ebf041`, with existing
  changes in `Cargo.toml`, `README.md`, `docs/ARCHITECTURE.md`,
  `docs/ARCHITECTURE.ru.md`, and `src/engine.rs`. Current files were inspected;
  `src/backend.rs` and `proto/lightnode.proto` were unchanged from HEAD.
- Core local `develop`: `f55b46126c99a1c3f3266164c744b3d0cd694d9c`.
  Existing tracked modification: `test/test.vcxproj`; untracked operator,
  documentation, test, IDE, and build files were not protocol authority.
  Relevant production files were unchanged in the working tree.
- Core upstream `develop`, verified with `git ls-remote` and fetched from
  `https://github.com/qubic/core.git`:
  `9b430304b5de3e9c539704d30eaf5c38b5f4512a`, committed
  `2026-09-08T20:45:30+07:00`. It is 72 commits ahead of local `develop`.
  The review read these Git objects directly. Fetch updated `upstream/develop`;
  it did not switch the checked-out branch or change working files.

SHA-256 fingerprints of the RandomClient files used:

| File | SHA-256 |
| --- | --- |
| `docs/ARCHITECTURE.md` | `03ae3cd1699d72cb8035e969cdd5d29e76a2e654f803ee88bbf306177c36b87f` |
| `src/backend.rs` | `2f645a7952d3bff8824ba808d343259babf9d917e94987ea7567ad8da128a492` |
| `src/engine.rs` | `c58cb4c1ba4238fa71369f784dd6678dcbf9467000e37d6d067e4e1346a63961` |

## Requirement traceability

Core references below refer to the upstream revision above, not the older
checked-out files. The current RandomClient architecture and production
`QlnBackend` adapter define the required observable results.

| RandomClient caller | Core source and invariant | QubicLightNode mapping | Result |
| --- | --- | --- | --- |
| `QlnBackend::tick_info`, `src/backend.rs:384` | `network_messages/tick.h`; `processRequestCurrentTickInfo` and `processResponseCurrentTickInfo` in `src/qubic.cpp`: 352-byte BroadcastTick and 16-byte current-info payloads. | `src/frame.rs:223`, `parse_tick_status_from_frame`; `network::update_latest_epoch_tick`; `GrpcService::get_status`. | Layouts and current-info handlers unchanged. Existing structural single-peer status exception remains applicable. |
| `QlnBackend::tick_has_transactions`, `src/backend.rs:407` | `src/qubic.cpp:1221`, `processBroadcastFutureTickData`; `:1556`, `processRequestTickData`; `network_messages/tick.h`: exact TickData, designated leader, calendar bounds, unique nonzero digests, type-transformed signed body. | `peer_api::query_tick_data` / `receive_tick_data`; `src/verified.rs:61`, `verify_tick_data`; `GrpcService::get_tick_transactions`. | Wire and authentication unchanged; historical query uses the existing bounded async adaptation. Unavailable authentication never becomes an empty result. |
| `QlnBackend::query_contract_function`, `src/backend.rs:419` | `network_messages/contract.h`; `processRequestContractFunction`: eight-byte request prefix, raw input/output, empty response on invocation failure. | `src/peer_api.rs:139`, `query_contract_function`; `:217`, `query_contract_on_peer`; pending response contracts and gRPC validation. | Request/response layouts and handler unchanged. Approved same-session freshness preflight and first-success trust exception remain applicable. |
| `QlnBackend::broadcast_transaction`, `src/backend.rs:443` | `network_messages/transactions.h:14`; `src/qubic.cpp:1351`: exact transaction size, amount/input limits, K12 of unsigned body and source-key signature; K12 of complete signed bytes for ID. | `src/frame.rs:328`, `validate_transaction`; `src/network.rs:233`, `broadcast_transaction_to_network`; `src/codec.rs:74`, identity encoding. | Generic validation and ID rules unchanged. Exact signed bytes are retained; bounded queue acceptance remains the documented delivery result. |

## Relevant upstream changes

- `src/qubic.cpp:1104`: computor keys and signature are now copied before
  publishing the epoch. QubicLightNode already reads and applies the set under
  one mutex (`src/verified.rs:137`, `:222`), preserving atomic visibility.
  Computor wire layout, arbitrator identity, authentication, and request
  semantics are unchanged.
- `src/four_q.h:1879`: incremental vote signing and parallel signing in
  `src/qubic.cpp:6216` optimize vote generation. The existing `verify()`
  implementation and curve arithmetic are unchanged. QubicLightNode generates
  no votes; its transaction and TickData verifier requires no port.
- The special empty-vote rule for tick `77259626` belongs to Core consensus
  vote acceptance. QubicLightNode exposes structural status and authenticated
  TickData presence, and does not implement that consensus decision.
- Ant Colony adds message IDs `72..77`, broadcast-message subtype `3`, and
  mining transaction type `12`. The four required message families retain
  their IDs and layouts. No RandomClient production caller needs these new
  handlers. Well-framed, uncorrelated unrelated messages are ignored by the existing
  receive path; they do not require a new subsystem.
- Peer transport/handshake files, header layout, dissemination multiplier
  `6`, K12, transaction constants, Random contract source, and the transaction
  publication offset `2` are unchanged across the reviewed delta. Startup
  version/epoch/tick changes in Core do not require hardcoded updates in QLN.

## Verification and limits

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`:
  passed.
- `cargo test --workspace --all-targets --all-features --offline`: passed,
  116 application tests and 5 verifier tests.
- Current RandomClient and QLN `proto/lightnode.proto` files are identical.
  RandomClient was not rebuilt because no protocol or schema changes were made.
- Existing tests cover wire sizes, transaction validity and signature
  mutations, canonical IDs/scalars, low-order keys, authenticated TickData
  rejection cases, computor epoch refresh, query freshness, deadlines, and
  correlation cleanup. Core tests and changed sources were inspected, but
  the Core C++ suite was not compiled or run.
- TickData tests use synthetic payloads, local signing, and injected computor
  keys. A complete Core-produced TickData plus arbitrator-signed Computors
  fixture is a preexisting coverage gap. There is also no fixture from the
  new incremental vote signer. Neither gap demonstrates incompatibility.

GitNexus query/context was used for both Rust repositories. The QLN index
matches the reviewed HEAD. RandomClient's index names the same HEAD but its
architecture and engine content hashes differ from the inspected dirty files;
direct source inspection takes precedence. Core is not among the available
indexed repositories and was reviewed directly through Git.

An upstream impact query for `network::process_incoming_frame` reported HIGH
blast radius (15 symbols, 4 process groups); no edit to that symbol is needed
or was performed. GitNexus change detection found no implementation changes.
This report records the reviewed baseline without changing runtime behavior.


## TickData digest API extension — 2026-10-25

This addendum records the later implementation change; the review above is
historical and its no-change conclusion applies only to its original scope.

Baselines: RandomClient `ab98d55b9bd10f61690f4fa017dcc13ab50faef2` plus existing
T-4 engine/README/English and Russian architecture edits and local diagnostics;
QubicLightNode `b959418775c1f1d0da833a04c596b87719e9d479` plus this previously
untracked report; Core `98102e74d50db4c21473ef3e9ad21cdca6fd69aa` with unrelated
asset/NFT contract, QPI, test and operator worktree changes. The relevant Core
production TickData and transaction hashing sources were unmodified.

| Requirement / production caller | Core invariant | Implementation / mapping | Verification |
| --- | --- | --- | --- |
| RandomClient `ensure_tick_check` / `harvest_tick_check`, via `QlnBackend::tick_has_transactions`, must compare the complete local signed-transaction hash | `src/network_messages/tick.h::TickData.transactionDigests` holds 4096 32-byte slots; `src/qubic.cpp::processBroadcastFutureTickData` checks unique non-zero digests and leader signature; transaction lookup compares K12 over `Transaction::totalSize()` (including signature) with these slots | `verify_tick_data` retains the verified non-zero digests in slot order; `receive_tick_data` and `query_tick_data` carry the list to `get_tick_transactions`. Additive protobuf fields expose it with an explicit completeness marker. Exact Core digest mapping; gRPC representation is a local API adaptation. | Signed empty/non-empty gRPC fixtures; sparse-slot and last-slot preservation; corrupted signature rejection; existing wrong leader/tick/epoch, duplicate digest and missing-key rejection tests. Client tests reject malformed/contradictory replies and compare all 32 bytes, distinguish legacy unknown, and bound digest retention. |

This extension serves the existing historical-tick operation and adds no RPC,
network message, persistence, dependency, quorum rule, or broadcast behavior.
Only authenticated data can set completeness. Absence from the leader's set is
reported separately from empty ticks and contract execution. Client scheduling,
financial counters, and status-driven restart remain unchanged by this feature.
Graph analysis reported HIGH impact for verification/receive and CRITICAL
impact in the client's shared cycle/monitoring lifecycle. Generated tonic
dispatch and trait-object calls require direct source checks where graph impact
is UNKNOWN. Existing cryptographic fixture limitations above still apply.


### Delivery measurement (2026-10-25)

Same baselines as the digest API extension above. User requested iterative
delivery improvement toward at most 1% missing on non-empty ticks. The
RandomClient broadcast/check path needs per-transaction socket-write evidence.
`OutboundFrame::try_new` records a monotonic queue timestamp; `write_peer_frames`
reports queue/write durations and full K12-derived transaction ID under existing
traffic logging. Core's frame and transaction layouts and hash semantics are
unchanged; local telemetry is supporting infrastructure, not a protocol change.
Impact: CRITICAL for queue construction, HIGH for writer; existing backpressure,
timeout and session lifecycle tests cover the unchanged transport behavior.


## 2026-10-25 approved local broadcast fanout experiment

User decision: allow an experiment up to 32 peers, preserving T-4 disclosure,
identical signed bytes, and default fanout six. Baselines: RandomClient
`ab98d55b9bd10f61690f4fa017dcc13ab50faef2` plus current TickData/T-4/T-3/20ms
worktree; QubicLightNode `b959418775c1f1d0da833a04c596b87719e9d479` plus current
worktree; Core `98102e74d50db4c21473ef3e9ad21cdca6fd69aa`.

Caller: QlnBackend::broadcast_transaction. Core mapping: validated transaction
bytes and processBroadcastTransaction remain exact; network_core/peers.h
push / DISSEMINATION_MULTIPLIER retains the default count six. Configurable
1–32 fanout is an approved availability adaptation at local gRPC submission,
implemented by Config, GrpcService::broadcast_transaction and
broadcast_transaction_to_network using existing random distinct selection.
It does not add a relay, RPC, signature exception, or new trust decision.

Tests cover CLI bounds/default, exact full frame delivery to six/32/one
recipients, fewer available sessions, and existing queue-pressure, rate-limit,
signature, cancellation, and retry behavior. A wider fanout is an experiment;
the <=1% missing target still requires live full-coverage verification.


## 2026-10-25 background peer selection

User decision: independent background measurement and gradual replacement of
unstable/lagging/slow peers, keeping broadcasting off that path. This supports
RandomClient QlnBackend's four existing operations, particularly broadcasts at T-4.
Baselines: client ab98d55b9bd10f61690f4fa017dcc13ab50faef2 plus current worktree;
node b959418775c1f1d0da833a04c596b87719e9d479 plus current worktree; Core
98102e74d50db4c21473ef3e9ad21cdca6fd69aa with unrelated local contract changes.
Core network_messages/tick.h RequestCurrentTickInfo/RespondCurrentTickInfo and
qubic.cpp processRequestCurrentTickInfo define exact request/response layouts.
network_core/peers.h supplies the existing handshake. The existing parser and
handshake are reused, with nonce/size/budget parity tests. RTT ranking, freshness,
hysteresis, spare-socket qualification and cooldown are explicitly approved local
adaptations, not consensus. Implementation: network::selection (peer_selection.rs),
probe_current_tick, activate_connection, and NodeState selection helpers.
No RPC or dependency is added. Tests cover correlation, budgets, freshness,
hysteresis, recovery, local overload, pending cleanup, timeout, cooldown,
queue/in-flight retirement protection, and actual connection handoff.
The graph index predates new functions; LOW impact on existing startup/connection
paths was supplemented by direct source inspection. Existing parity tests pass.


## TickData calendar conversion repair (2026-10-25)

Against Core `98102e74d50db4c21473ef3e9ad21cdca6fd69aa`,
`processBroadcastFutureTickData` uses a wire-year modulo-four February guard but
`platform/time.h::ms` uses Gregorian `dayIndex`. LightNode previously reused the
modulo-four rule for timestamp accumulation, diverging from March 2100 onward.
`utc_millis` now ports `dayIndex` with a Unix-epoch offset; the packet guard and
five-second skew limit are unchanged. This supports RandomClient's existing
`QlnBackend::tick_has_transactions` requirement; no new API or trust policy.
A regression covers 2000, current dates, 2100, 2200, and maximum wire year 2255,
including the Core-accepted February 29, 2100 mapping. No current-date delivery
incident is attributed to this future-date defect.


## Current tick time units repair (2026-10-25)

Core 98102e74d50db4c21473ef3e9ad21cdca6fd69aa, qubic.cpp
`processRequestCurrentTickInfo`, divides elapsed TSC cycles by `frequency`
(cycles per second, platform/time_stamp_counter.h). The wire u16 tickDuration
is elapsed whole seconds, not milliseconds or an average tick duration.
LightNode's internal TickStatus.tick_duration_ms now converts to u32 milliseconds;
previously it stored the seconds unchanged in that field. Boundary tests cover
0, 1, 65, 66 and 65535 seconds. This is supporting parser correctness for the
existing freshness/selection probes. GetStatus still reports zero unavailable
duration metadata; RandomClient QlnBackend still uses its 1000 ms fallback.
No RPC, trust policy or scheduling change; no Missing incident is attributed
to this currently unexposed internal field. Client baseline ab98d55b9bd10f61690f4fa017dcc13ab50faef2
and node b959418775c1f1d0da833a04c596b87719e9d479 plus current worktrees.


## Pending response allocation repair (2026-10-25)

RandomClient's contract and TickData queries depend on bounded LightNode pending
responses. `extract_frame` returns shared `Bytes` slices from the TCP receive
buffer. Previously `PendingRequests::deliver` charged only the slice length but
queued that shared owner, allowing a small response to retain a much larger
allocation. It now copies only the accepted frame after validation and permit
acquisition. This is a Rust ownership adaptation, with no change to Core frame
bytes, request correlation, ordering, queue limits or failure semantics.

Regression `queued_response_does_not_retain_the_shared_receive_allocation`
demonstrates an eight-byte response retaining a 1 MiB backing owner before the
fix; after the fix the owner is released while queued response bytes survive.
This is a retention/byte-accounting defect, not a claim of an unbounded leak or
a measured remote denial of service. The client ABI and scheduling are unchanged.

### Connection worker failure cleanup (2026-10-25)

The Tokio supervisor consumes a saved disconnect reason even when a worker
panics before normal cleanup. It still unregisters the session and notifies
pending requests. This is supporting resource cleanup for all four client
operations; no wire, trust, queue admission, or peer penalty policy changes.
The injected-panic regression covers both an active session and a session
already removed administratively, and asserts no leftover reason or new cooldown.
It failed with the original fallback and passes after the fix. This does not
establish a remotely triggerable panic. Baselines: RandomClient
`ab98d55b9bd10f61690f4fa017dcc13ab50faef2`, LightNode
`b959418775c1f1d0da833a04c596b87719e9d479`, Core
`98102e74d50db4c21473ef3e9ad21cdca6fd69aa`, with existing worktree changes.
