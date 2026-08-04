# QubicLightNode Architecture Compliance Audit

Audit date: 2026-08-04
Status: **PARTIALLY COMPLIANT: TWO EXPLICITLY ACCEPTED UNAUTHENTICATED PUBLIC-PEER TRUST EXCEPTIONS**
Latest implementation re-verified: 2026-08-04

---

## Non-blocking Tick Status and Legacy RPC Removal (2026-08-04)

This change used the following point-in-time sources before implementation:

| Repository | Revision / worktree state used |
| --- | --- |
| QubicLightNode | `8f035b4171103647c3cdaeb8cf8c48036a8f15c8`; the existing TickData/product-boundary work and unrelated dirty files were preserved |
| RandomClient | `ea02475916536036fa69ce8c89354568c4c42c1b`; existing modifications in `README.md`, both architecture documents, and `src/engine.rs` were preserved |
| QThirtyFour Core | `f55b46126c99a1c3f3266164c744b3d0cd694d9c`; relevant production sources were unchanged while unrelated tracked and untracked changes were preserved |

Traceability for the changed status boundary:

| RandomClient requirement and caller | Core source and invariant | QubicLightNode implementation | Status and parity evidence |
| --- | --- | --- | --- |
| Continuously advancing scheduling input through `src/backend.rs::QlnBackend::tick_info` and tonic `GetStatus` | `core/src/network_messages/tick.h::{BroadcastTick,RespondCurrentTickInfo}` defines the exact layouts; `core/src/qubic.cpp::{processBroadcastTick,processResponseCurrentTickInfo}` rejects wrong sizes and constrains the literal computor index or current-epoch range before any use | `src/frame.rs::parse_tick_status_from_frame`, `src/network.rs::process_incoming_frame`, `src/types.rs::ApiState::latest_epoch_tick`, and `src/grpc_api::GrpcService::get_status` | Deliberate trust adaptation governed by ADR-0002: the greatest structurally valid epoch/tick from one peer is published before routing, locks, or crypto admission. Unit tests cover both message types, malformed BroadcastTick rejection, and lock-free cache publication. This restores availability but is not Core-equivalent authentication. |
| Keep malformed framing from reaching the status cache | Core request/response headers carry the exact 24-bit frame size and message type | `src/frame::{extract_frames,parse_tick_status_from_frame}` | Exact-size structural parity is retained; delayed lower packed values cannot regress the atomic cache. |

The previous 451-vote `BroadcastTick` signature/quorum state was supporting an
older locally strengthened status policy, not a current RandomClient
requirement. It has been removed from the runtime status path. Signed computor
lists remain required to authenticate current-epoch TickData, and transaction
signatures remain verified before broadcast. QubicLightNode emits a startup
warning and `GetStatus` explicitly reports zero for unavailable initial-tick,
duration, and vote metadata.

The separately authorized breaking change in ADR-0003 removes legacy
`GetBalance` from the protobuf, service, peer query path, proof state, and
tests. Current RandomClient already exposes and calls exactly the remaining
four RPCs, so its production call paths are unchanged. Older audit sections
below are retained as historical evidence and are superseded where they claim
authenticated quorum status or a retained balance RPC.

Verification completed for this implementation:

- `cargo fmt --all`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed.
- `cargo test --workspace --all-targets --all-features`: passed, with 110
  QubicLightNode tests and 5 FourQ verifier tests.
- Current RandomClient
  `cargo test --all-targets --all-features`: passed, with 47 library tests; its
  generated client compiled against the same four-RPC boundary.
- `git diff --check`: passed for both worktrees.

GitNexus `detect_changes` reported CRITICAL impact across 32 indexed processes,
centered on `process_incoming_frame`, service startup, and removed balance/tick
verification paths. Every live affected production path was reviewed directly
and is covered by the checks above. The index is stale relative to this
worktree: its context still names removed symbols such as
`tick_verification_digest`, `query_balance`, and the old
`unverified_tick_does_not_update_atomic_cache` test. Those graph-only references
were treated as historical rather than authoritative.

---

## Authenticated Tick-Transaction Presence API (2026-08-03)

This task used the following point-in-time sources before implementation:

| Repository | Revision / worktree state used |
| --- | --- |
| QubicLightNode | `8f035b4171103647c3cdaeb8cf8c48036a8f15c8` on `main`; clean before this task |
| RandomClient | `0b180b6deaec7a491c3cba1e58fe4337fbe2043e` on `main`; the current counter/empty-check changes in `README.md`, both architecture documents, `proto/lightnode.proto`, and `src/{app,backend,config,console,engine}.rs` are the production-consumer worktree |
| QThirtyFour Core | `f55b46126c99a1c3f3266164c744b3d0cd694d9c`; relevant `src/` files were unchanged, while the unrelated tracked `test/test.vcxproj` modification and untracked local artifacts were left untouched |

Traceability for the new required operation:

| RandomClient requirement and caller | Core source and invariant | QubicLightNode implementation | Status and parity evidence |
| --- | --- | --- | --- |
| Delayed empty-tick monitoring through `src/backend.rs::NetworkBackend::tick_has_transactions` and `QlnBackend::tick_has_transactions` | `core/src/network_messages/tick.h::{RequestTickData, TickData}` and message IDs 16/8; `core/src/qubic.cpp::{processRequestTickData, processBroadcastFutureTickData}` require exact layout, requested-tick response correlation, designated leader, calendar bounds, unique non-zero transaction digests, K12 over the unsigned body with type XOR, and a valid FourQ signature | `src/frame.rs::{build_request_tick_data_frame,TICK_DATA_PAYLOAD_SIZE}`, `src/peer_api::{query_tick_data,receive_tick_data}`, `src/verified.rs::TrustedNetworkState::verify_tick_data`, and `src/grpc_api::GrpcService::get_tick_transactions` | Core-compatible adaptation: the gRPC response exposes only whether the authenticated digest array contains a non-zero entry. A `HashSet` implements Core's duplicate-digest rejection with the same result and bounded memory. Unit and peer-routing tests cover exact wire bytes, empty/non-empty results, wrong tick, duplicate digest, bad signature, cleanup, and peer penalty. |

Only current-epoch TickData can be authenticated because this reduced node keeps
the active arbitrator-authenticated computor list. Calendar validation includes
Core's five-second future-time ceiling, and the minimum configurable frame size
is now the 139,384-byte TickData response frame. Missing prior-epoch keys,
`END_RESPONSE`, `TRY_AGAIN`, timeout, and local overload fail closed and let
RandomClient retry without changing counters. FourQ/K12 verification runs on a
blocking worker, outside Tokio state locks.

The service schema now contains the four current RandomClient RPCs plus legacy
`GetBalance`. Removing that existing public method was intentionally not folded
into this counter task because it is a separately authorized breaking change;
the older audit sections below remain point-in-time evidence for its proof
path.

Verification completed for the implementation before the final documentation
pass:

- `cargo fmt --all`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed.
- `cargo test --workspace --all-targets --all-features`: QubicLightNode **134
  passed, 0 failed**; FourQ verifier **5 passed, 0 failed**.
- GitNexus `detect_changes` against `main`: **HIGH**, with 57 mapped changed
  symbols and 7 affected indexed flows. The flows are the expected frame-size,
  pending-response, and adjacent network/balance tests; the committed index
  attributes new inserted ranges to neighboring old symbols and an older ADR
  heading, so the reviewed source diff and full test suite are authoritative
  for the new TickData symbols.

---

## Balance Spectrum-Root Tick-Window Correction (2026-08-02)

This task used the following point-in-time sources before implementation:

| Repository | Revision / worktree state used |
| --- | --- |
| QubicLightNode | `d92d821f960c2ef0afd178a0d49a456963092b9e` on `main`; pre-existing modifications in `Dockerfile`, this audit, `src/network.rs`, `src/peer_api.rs`, `src/state.rs`, and `src/verified.rs` were preserved |
| RandomClient | `33d217eab16da287b6b9ce5389be009227492abf`; dirty `AGENTS.md`, `README.md`, `compose.yaml`, `docs/ARCHITECTURE.md`, `docs/ARCHITECTURE.ru.md`, and `proto/lightnode.proto` were inspected as the current production-consumer worktree |
| QThirtyFour Core | `f55b46126c99a1c3f3266164c744b3d0cd694d9c` on `develop`; relevant `src/` files were clean, while the unrelated tracked `test/test.vcxproj` modification and untracked artifacts were left untouched |

The balance failure was a tick-to-state association defect. Core writes
`RespondEntity.tick` from `system.tick`, while request processors run
concurrently with tick processing. `Tick T.prevSpectrumDigest` authenticates
the state before tick T; after processing T, Core recomputes the spectrum root,
which becomes `Tick T+1.prevSpectrumDigest`. An entity response reporting T
can therefore contain a proof for the verified root committed by either T or
T+1. QubicLightNode previously checked only T, producing the observed failure
when an entity response at `71510919` arrived alongside verified tick
`71510920`.

Traceability for the correction:

| RandomClient requirement and caller | Core source and invariant | QubicLightNode implementation | Status and parity evidence |
| --- | --- | --- | --- |
| Authenticated balance consumed by `src/backend.rs::QlnBackend::balance` and used by `src/engine.rs::ensure_balance_query` before enrollment | `core/src/qubic.cpp::processRequestEntity`, `processTick`, and `tickProcessor`: the reported system tick may straddle the transition from the same-tick previous root to the successor-tick previous root | `src/verified.rs::verify_entity_spectrum_root` and `src/peer_api.rs::receive_balance` | Core-compatible runtime adaptation: accept only a K12 proof matching a verified root for T or T+1; no arbitrary cached-root or peer-quorum fallback |
| Invalid peer data must fail closed without penalizing a peer merely because authenticated state is not available yet | Core supplies proof material but no reduced-node availability policy; the Tokio adapter must distinguish unavailable trust data from a definitive mismatch | `SpectrumProofVerification::{Verified, Unavailable, Mismatch}` | A mismatch is a protocol fault only when the complete candidate window is known; an incomplete window remains retryable and does not disconnect the peer |

The gRPC field numbers and response shape are unchanged. README and protobuf
comments now document the Core-compatible two-tick verification window.

Correction verification:

- `cargo fmt --all`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed.
- QubicLightNode: **129 passed, 0 failed**; FourQ verifier: **5 passed,
  0 failed**.
- Current RandomClient worktree against the unchanged wire schema: **47 passed,
  0 failed**.
- Regression coverage includes same-tick and successor-tick acceptance,
  incomplete-root availability, definitive proof mismatch with peer penalty,
  and `u32::MAX` without successor wraparound.
- GitNexus `detect_changes`: **CRITICAL**, with 22 affected indexed processes.
  The balance-related processes are the expected response-success,
  invalid-response, cooldown, pending-request, and resource-limit test flows;
  the remaining reported processes belong to the pre-existing tick-parity and
  protocol-disconnect changes. The index follows committed `d92d821` and could
  not resolve the new `verify_entity_spectrum_root` symbol, so direct source
  review and the complete test results above are authoritative for this helper.

## Runtime Tick-Parity Correction (2026-08-02)

This task used the following point-in-time sources before the implementation
was changed:

| Repository | Revision / worktree state used |
| --- | --- |
| QubicLightNode | `d92d821f960c2ef0afd178a0d49a456963092b9e` on `main`; the pre-existing `Dockerfile` modification was outside this task and was preserved |
| RandomClient | `33d217eab16da287b6b9ce5389be009227492abf`; dirty `AGENTS.md`, `README.md`, `compose.yaml`, `docs/ARCHITECTURE.md`, `docs/ARCHITECTURE.ru.md`, and `proto/lightnode.proto` were inspected as the current consumer worktree |
| QThirtyFour Core | `f55b46126c99a1c3f3266164c744b3d0cd694d9c` on `develop`; no tracked `src/` changes, with the unrelated tracked `test/test.vcxproj` modification and local untracked artifacts left untouched |

The failure was a protocol-parity defect in tick authentication. Core keeps
`Tick::computorIndex` literal on the wire, temporarily XORs that field with
`BroadcastTick::type()` only while computing the KangarooTwelve digest, then
restores the field before selecting the computor public key. QubicLightNode
previously applied the XOR while parsing and therefore verified a valid vote
against a different key. It rejected live votes as protocol violations, never
formed a 451-computor quorum, and consequently left epoch and tick unknown.

Traceability for the correction:

| RandomClient requirement and caller | Core source and invariant | QubicLightNode implementation | Status and parity evidence |
| --- | --- | --- | --- |
| Authenticated current epoch/tick consumed by `src/backend.rs::QlnBackend::tick_info` through `GetStatus` | `core/src/qubic.cpp::processBroadcastTick`: validate the literal wire index; XOR it only for the signed-body digest; restore it before `publicKeys[computorIndex]` lookup | `src/verified.rs::ParsedTick::parse`, `tick_signature_digest`, and `TrustedNetworkState::tick_context` | Exact protocol meaning restored. A property test covers every valid computor index, and a Core-compatible signature vector proves that the parsed wire index selects the authenticating public key. |
| Reject and penalize invalid peer traffic without leaking packet data | Core handlers reject malformed or unauthenticated messages; the reduced Tokio runtime additionally terminates the faulty peer session | `src/state.rs::ProtocolViolationReason`, `DisconnectReason`, `src/network.rs::process_incoming_frame`, and `src/peer_api.rs::penalize_protocol_peer` | Runtime adaptation only. Disconnect logs now identify the validation class while omitting payloads, keys, signatures, and request data. Tests assert the pending-response reason and the complete stable reason vocabulary. |

This correction changes neither the public gRPC schema nor the four-operation
product boundary, so `README.md` and protobuf documentation require no change.

Correction verification:

- `cargo fmt --all`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed.
- QubicLightNode: **125 passed, 0 failed**; FourQ verifier: **5 passed,
  0 failed**.
- Current RandomClient worktree against the generated schema: **47 passed,
  0 failed**.
- Live public-peer smoke test: `GetStatus` returned epoch `224`, tick
  `71509542`, `452` aligned votes, and `0` misaligned votes. The same verified
  epoch/tick remained visible while outbound peers were replaced.
- GitNexus `detect_changes`: **HIGH**, with 10 affected indexed processes. All
  reported processes are expected inbound-frame, session-replacement,
  deduplication, or their direct network tests; no additional product RPC or
  relay flow was introduced. The index was stale enough to report the removed
  `decodes_wire_computor_index_xor` test and pre-change line numbers, so direct
  source inspection and the live/test evidence above are authoritative.

## Remediation Verification

The findings below preserve the original pre-remediation evidence. The current
worktree implements the approved correction plan:

| Finding | Result | Verification |
| --- | --- | --- |
| FIND-01 | Partially resolved; remaining risk explicitly accepted | Balance now fails closed unless the exact entity proof matches a spectrum root from verified quorum tick state. Absent entities are not reported as zero balances. First-success contract output remains unauthenticated by owner decision and is governed by ADR-0001 plus a startup warning. |
| FIND-02 | Resolved | FourQ now enforces Core canonical encodings, `S < r`, Core's identity guard, and low-order public-key rejection, with Core regression vectors. |
| FIND-03 | Resolved | Broadcast validates the exact transaction layout, K12 digest, and canonical FourQ signature before rate accounting or fanout. |
| FIND-04 | Resolved | The schema exposes exactly four RandomClient RPCs. Tick streaming, inbound peer listening, general relay, reflection, and their CLI/config surface were removed. |
| FIND-05 | Resolved | The independent general-frame BLAKE3 dedup path was removed. Bounded digest caches remain only as a local optimization for already authenticated signed computor/tick verification results. |
| FIND-06 | Resolved | `relay-all` and general relay were removed, so no alternate non-zero-`dejavu` dissemination mode exists. |
| FIND-07 | Resolved | Computor payload parsing accepts Core's canonical bytes plus zero to four trailing struct-padding bytes while authenticating only the canonical signed prefix. |
| FIND-08 | Resolved | Tick calendar validation uses Core's one-byte wire-year leap rule, including year 2100. |

Public API and deployment synchronization:

- QubicLightNode and RandomClient protobuf files are semantically identical and
  enumerate only `GetStatus`, `GetBalance`, `QueryContractFunction`, and
  `BroadcastTransaction`.
- Both integration documents identify QubicLightNode version `0.3.0` and the
  unauthenticated contract-query boundary.
- RandomClient compose no longer publishes inbound Qubic port `21841`.

Post-remediation verification:

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- QubicLightNode: **123 passed, 0 failed**
- FourQ verifier: **5 passed, 0 failed**
- RandomClient against the synchronized schema: **47 passed, 0 failed**
- `docker compose config --quiet` through WSL: passed
- GitNexus `detect_changes` classified the QubicLightNode change set as
  CRITICAL because it spans startup, network, verification, balance, and
  broadcast flows; RandomClient integration changes were LOW. The QLN index
  was stale and still referenced removed stream/relay symbols and old line
  numbers, so its graph was used for blast-radius enumeration only. Direct
  source review and the tests above verified the current flows.

The accepted exception is material: RandomClient uses contract output for
provider-state reconciliation, while the output is controlled by the first
successful public peer. Compliance here means this deviation is explicit and
approved, not that the response has gained cryptographic integrity.

---

## 1. Original Executive Summary (Pre-remediation)

QubicLightNode implements all four operations used by RandomClient and its
protobuf is wire-compatible with RandomClient after normalizing line endings.
The authenticated tick-status path, Core frame layouts, message IDs, request
correlation, transaction ID generation, peer exchange, and six-peer
dissemination multiplier are substantially aligned with the inspected Core
worktree.

The project is not yet compliant with the stricter architecture contract in
AGENTS.md. This audit found:

- **2 HIGH** findings: unauthenticated peer-backed contract/balance results can
  drive RandomClient state, and the Rust FourQ verifier is behind current Core
  signature hardening.
- **4 MEDIUM** findings: transaction signatures are not verified before
  successful broadcast/relay, the product exposes functionality outside the
  four-operation boundary, deduplication is an independent algorithm despite a
  Core implementation, and relay-all permits propagation Core intentionally
  gates on zero dejavu.
- **2 LOW** findings: computor-list padding policy differs from Core, and the
  calendar rule diverges for century years such as 2100.

No direct exploit was executed during this review. The HIGH rating for
peer-backed results reflects a trust-boundary mismatch: RandomClient treats
GetProviderStatus.lastUpdateTick as contract-execution confirmation, while
QubicLightNode accepts the first non-empty response from up to three ordinary
peers. A malicious fast peer can therefore supply a syntactically valid
680-byte result that controls reconciliation. At the largest supported tier,
three managed slots represent an upper-bound exposure of
3 × 1,000,000,000 = 3,000,000,000 qu in locked collateral; this is exposure,
not a demonstrated guaranteed loss.

Verification completed successfully:

- cargo fmt --all -- --check
- cargo clippy --workspace --all-targets --all-features -- -D warnings
- QubicLightNode tests: **134 passed, 0 failed**
- RandomClient tests against its current worktree/schema: **47 passed, 0 failed**

---

## 2. Documentation Sources Identified

### Audited baselines

| Repository | Revision / worktree | Notes |
| --- | --- | --- |
| QubicLightNode | main, 99d17ddc008e05d094a16a09c93f7f779d012116 | Clean production code before this documentation-only change |
| RandomClient | main, 051742b4d65e606b247c02621ba0f1f8ce7db966 plus current dirty worktree | Current uncommitted architecture/backend rewrite is the consumer baseline |
| QThirtyFour/core | develop, f55b46126c99a1c3f3266164c744b3d0cd694d9c plus current worktree | No tracked changes under src; test/test.vcxproj is modified |

### Reproducibility hashes

| Source | SHA-256 |
| --- | --- |
| RandomClient/docs/ARCHITECTURE.md | 0C5D18F019EB6CCC8EB0AA3BF085A9C6FA203A17B893D8AA4C1E1D3A26B02659 |
| RandomClient/src/backend.rs | 845D279D09D3A4E83EC3A76188362FE92FEFD862357EC069EEF807B5DB8DFE15 |
| RandomClient/proto/lightnode.proto | B12DC125B44BB55BEA34F0FAAEB9633B6C826E90414C494341F6D5F4DA8304FB |
| Core/src/qubic.cpp | 1449142B86899A812BF04E21DB3658B406CEE4488EE1DA8295E2763E481DB77A |
| Core/src/four_q.h | 1EFD535739EF8BAA3FF09011FA8587BCB2C88B111BAE3E084738097D3EDE39CF |
| Core/src/network_messages/network_message_type.h | FFAC0C436338AF5F6258D5497C98376EF8E9EDC24B5FD1C70D260A8D00A0A6B5 |

### Normative corpus

- AGENTS.md lines 3-18 defines this project as a reduced implementation whose
  only product consumer is RandomClient.
- AGENTS.md lines 20-38 defines source precedence.
- AGENTS.md lines 40-60 defines the strict four-operation product boundary.
- AGENTS.md lines 62-87 prohibits independent protocol designs when Core has
  an implementation.
- RandomClient docs/ARCHITECTURE.md lines 249-269 defines the four backend
  operations and exact-byte requirement.
- RandomClient docs/ARCHITECTURE.md lines 140-154 defines broadcast,
  GetProviderStatus, balance, and collateral semantics.
- Core production headers and handlers define protocol behavior; the principal
  evidence is cited per alignment record below.

The English RandomClient architecture document is authoritative. The copied
protobuf contains StreamTickTransactions, but no RandomClient production
NetworkBackend method calls it. Under the user-selected strict rule, a schema
declaration without a production call path does not establish product need.

---

## 3. Spec Intent Breakdown (Spec-IR)

~~~yaml
- id: SPEC-01
  spec_excerpt: "Its only product consumer is RandomClient"
  source_section: "AGENTS.md § Architectural Role, lines 10-16"
  source_document: "AGENTS.md"
  semantic_type: "scope"
  normalized_form: "Only behavior required by RandomClient plus necessary support infrastructure is allowed."
  confidence: 1.0

- id: SPEC-02
  spec_excerpt: "All transports implement the same four operations"
  source_section: "RandomClient Architecture § Backend boundary, lines 249-254"
  source_document: "RandomClient/docs/ARCHITECTURE.md"
  semantic_type: "interface"
  normalized_form: "Tick status, balance, raw contract query, and exact transaction broadcast are required."
  confidence: 1.0

- id: SPEC-03
  spec_excerpt: "reports epoch and tick only after a FourQ-verified quorum of 451 computors"
  source_section: "RandomClient Architecture § Backend boundary, lines 261-266"
  source_document: "RandomClient/docs/ARCHITECTURE.md"
  semantic_type: "security invariant"
  normalized_form: "GetStatus must not succeed before 451 distinct authenticated aligned votes."
  confidence: 1.0

- id: SPEC-04
  spec_excerpt: "initial_tick ... first verified tick ... tick_duration_ms ... 1,000 ms"
  source_section: "RandomClient Architecture § Backend boundary, lines 261-267"
  source_document: "RandomClient/docs/ARCHITECTURE.md"
  semantic_type: "fallback"
  normalized_form: "QLN may return zero metadata; RandomClient normalizes it at the backend boundary."
  confidence: 1.0

- id: SPEC-05
  spec_excerpt: "GetProviderStatus.lastUpdateTick is the contract-execution confirmation"
  source_section: "RandomClient Architecture § Contract and scheduling invariants, lines 140-145"
  source_document: "RandomClient/docs/ARCHITECTURE.md"
  semantic_type: "trust boundary"
  normalized_form: "Contract-query integrity directly controls commit/reveal reconciliation."
  confidence: 1.0

- id: SPEC-06
  spec_excerpt: "Balance is consulted only before enrollment"
  source_section: "RandomClient Architecture § Contract and scheduling invariants, lines 147-154"
  source_document: "RandomClient/docs/ARCHITECTURE.md"
  semantic_type: "flow"
  normalized_form: "Balance integrity gates opening up to three collateralized slots."
  confidence: 1.0

- id: SPEC-07
  spec_excerpt: "broadcast exact signed transaction bytes and return a transaction id"
  source_section: "RandomClient Architecture § Backend boundary, line 254"
  source_document: "RandomClient/docs/ARCHITECTURE.md"
  semantic_type: "postcondition"
  normalized_form: "The backend must preserve bytes and return the canonical ID for the same bytes."
  confidence: 1.0

- id: SPEC-08
  spec_excerpt: "all backends must preserve exact transaction bytes"
  source_section: "RandomClient Architecture § Backend boundary, lines 268-269"
  source_document: "RandomClient/docs/ARCHITECTURE.md"
  semantic_type: "invariant"
  normalized_form: "Retries and fanout must not rebuild or mutate the signed transaction."
  confidence: 1.0

- id: SPEC-09
  spec_excerpt: "size is three bytes; type is one byte; dejavu is four bytes"
  source_section: "Core RequestResponseHeader, header.h lines 6-18"
  source_document: "Core/src/network_messages/header.h"
  semantic_type: "wire layout"
  normalized_form: "Every Qubic frame uses the exact eight-byte little-endian Core header."
  confidence: 1.0

- id: SPEC-10
  spec_excerpt: "REQUEST_ENTITY=31, RESPOND_ENTITY=32, END_RESPONSE=35, REQUEST_CONTRACT_FUNCTION=42, RESPOND_CONTRACT_FUNCTION=43"
  source_section: "Core NetworkMessageType, lines 18-30"
  source_document: "Core/src/network_messages/network_message_type.h"
  semantic_type: "wire constants"
  normalized_form: "The reduced node must use Core message IDs without local renumbering."
  confidence: 1.0

- id: SPEC-11
  spec_excerpt: "checkValidity ... amount range and inputSize"
  source_section: "Core Transaction, transactions.h lines 14-32"
  source_document: "Core/src/network_messages/transactions.h"
  semantic_type: "validation"
  normalized_form: "Transaction structure, amount, input size, total size, K12 digest, and SchnorrQ signature govern acceptance."
  confidence: 1.0

- id: SPEC-12
  spec_excerpt: "Reject non-canonical signature scalars ... Reject low-order public keys"
  source_section: "Core FourQ verify, four_q.h lines 1892-1953"
  source_document: "Core/src/four_q.h"
  semantic_type: "security requirement"
  normalized_form: "S must be below the curve order and cofactor-subgroup public keys must be rejected."
  confidence: 1.0

- id: SPEC-13
  spec_excerpt: "A zero dejavu is used to signal that a message should be distributed"
  source_section: "Core RequestResponseHeader, header.h lines 48-56"
  source_document: "Core/src/network_messages/header.h"
  semantic_type: "routing invariant"
  normalized_form: "Core dissemination occurs only for the message classes and zero-dejavu condition used by Core handlers."
  confidence: 1.0

- id: SPEC-14
  spec_excerpt: "DISSEMINATION_MULTIPLIER 6"
  source_section: "Core peers.h lines 24 and 456-477"
  source_document: "Core/src/network_core/peers.h"
  semantic_type: "cardinality"
  normalized_form: "A broadcast is sent to no more than six randomly selected peers."
  confidence: 1.0

- id: SPEC-15
  spec_excerpt: "NUMBER_OF_EXCHANGED_PEERS 4"
  source_section: "Core common_def.h line 8 and public_peers.h lines 7-15"
  source_document: "Core network message headers"
  semantic_type: "wire layout"
  normalized_form: "The handshake payload carries exactly four IPv4 addresses."
  confidence: 1.0

- id: SPEC-16
  spec_excerpt: "Do not create a local alternative when Core already contains a solution"
  source_section: "AGENTS.md § No Independent Protocol Designs, lines 62-81"
  source_document: "AGENTS.md"
  semantic_type: "architecture invariant"
  normalized_form: "Local algorithms require parity with Core or an explicit architectural decision."
  confidence: 1.0
~~~

---

## 4. Code Behavior Summary (Code-IR)

All tracked production modules and externally reachable flows were inspected
at subsystem level, with relevant private helpers followed where needed to
establish behavior. This is an architecture-compliance audit, not a complete
line-by-line security audit. Tests were reviewed as evidence but are not
treated as specification.

| Area | Primary code | Observed behavior | Classification |
| --- | --- | --- | --- |
| Startup/config | src/app.rs, src/config.rs | Starts DNS, listener, dialer, and optional gRPC; exposes relay and resource policy | required + extra |
| Wallet/IDs | src/codec.rs | Validates identity/hex key and derives K12 transaction ID | required |
| Frames | src/frame.rs | Encodes/decodes Core header, message IDs, contract/entity/transaction layouts | required + tick-stream extra |
| gRPC | src/grpc_api.rs lines 136-421 | Implements five RPCs, concurrency/rate limiting, reflection | four required + one extra |
| Peer queries | src/peer_api.rs lines 131-478 | Races three peers for balance/contract; streams tick transactions from one peer | required + extra |
| Pending routing | src/pending.rs | Routes bounded responses by peer and dejavu, enforces terminal/frame limits | required |
| Network | src/network.rs | Incoming/outgoing sessions, bootstrap, verified broadcasts, general relay, fanout | required + extra |
| Peer state | src/state.rs | Peer pool, random targets, cooldown, queues, BLAKE3 dedup window | supporting + divergent |
| Trusted status | src/verified.rs | Authenticates computor list and 451 aligned tick votes | required |
| FourQ | crates/qubic-fourq-verifier/src/lib.rs | Verifies older SCAPI-style SchnorrQ signatures | required + weaker than current Core |
| Packaging | Dockerfile, RandomClient/compose.yaml | Builds and connects RandomClient to light-node:50051 | required |

Representative Code-IR records:

~~~yaml
- id: CODE-STATUS
  file: "src/grpc_api.rs"
  function: "GrpcService::get_status"
  lines: "137-160"
  visibility: "gRPC public"
  modifiers: []
  behavior:
    preconditions: ["TrustedNetworkState has a cached quorum status at lines 141-142"]
    state_reads: ["trusted_network.status at line 141"]
    state_writes: []
    computations: ["maps verified status at lines 143-150"]
    external_calls: []
    events: []
    postconditions: ["ok=false until quorum; ok=true with zero unavailable metadata"]
  invariants_enforced:
    - "Unverified current-tick responses are never exposed as status."
    - "The source is identified as verified_tick_quorum."
    - "Unavailable initial tick and duration remain zero for client normalization."

- id: CODE-PEER-QUERY
  file: "src/peer_api.rs"
  function: "query_balance / query_contract_function"
  lines: "131-216, 311-478"
  visibility: "crate"
  modifiers: ["async"]
  behavior:
    preconditions: ["At least one connected peer", "bounded deadline and response shape"]
    state_reads: ["up to three random sessions at lines 141-145 and 322-326"]
    state_writes: ["pending request registration", "peer cooldown on malformed response"]
    computations: ["first structurally successful response wins"]
    external_calls: ["ordinary Qubic peers"]
    events: []
    postconditions: ["returns unproven peer data or an error"]
  invariants_enforced:
    - "Responses are bound to peer and dejavu."
    - "Balance public key and fixed layout are validated."
    - "Contract output must be non-empty and at most 65535 bytes."

- id: CODE-BROADCAST
  file: "src/grpc_api.rs, src/network.rs"
  function: "broadcast_transaction / broadcast_transaction_to_network"
  lines: "grpc_api.rs 357-421; network.rs 275-338"
  visibility: "gRPC public / crate"
  modifiers: ["async", "rate limited", "concurrency limited"]
  behavior:
    preconditions: ["non-empty structurally valid Core transaction"]
    state_reads: ["connected peers", "dedup window"]
    state_writes: ["in-flight registry", "dedup reservation"]
    computations: ["K12 transaction ID; BLAKE3 internal dedup digest"]
    external_calls: ["fanout to at most six Qubic peers"]
    events: []
    postconditions: ["ok=true when at least one peer queue accepts the unmodified bytes"]
  invariants_enforced:
    - "Input bytes are not rebuilt."
    - "The wire frame uses type 24 and dejavu zero."
    - "Local acceptance does not imply Core signature acceptance."

- id: CODE-FOURQ
  file: "crates/qubic-fourq-verifier/src/lib.rs"
  function: "verify_digest"
  lines: "16-57"
  visibility: "public"
  modifiers: ["must_use"]
  behavior:
    preconditions: ["fixed 32/32/64-byte inputs"]
    state_reads: []
    state_writes: []
    computations: ["decode key, K12 challenge, double-scalar multiplication, encoded-point equality"]
    external_calls: []
    events: []
    postconditions: ["returns bool"]
  invariants_enforced:
    - "Top encoding bits are constrained at lines 21-24."
    - "The point must decode at lines 29-32."
    - "The signature equation must match at lines 46-56."
  missing_current_core_invariants:
    - "No exact S < curve_order check."
    - "No identity public-key rejection."
    - "No cofactor-subgroup public-key rejection."

- id: CODE-DEDUP
  file: "src/state.rs"
  function: "DedupWindow"
  lines: "569-688"
  visibility: "crate"
  modifiers: ["mutex protected", "reservation/commit"]
  behavior:
    preconditions: ["32-byte BLAKE3 digest"]
    state_reads: ["set and reserved hashes"]
    state_writes: ["FIFO order, committed set, in-flight set"]
    computations: ["exact hash membership and FIFO eviction"]
    external_calls: []
    events: []
    postconditions: ["failed dispatch rolls reservation back; successful dispatch commits"]
  invariants_enforced:
    - "At most max_seen committed entries."
    - "Concurrent duplicate work is coalesced/rejected."
    - "Unsent frames remain retryable."
~~~

---

## 5. Full Alignment Matrix

| ID | Spec | QubicLightNode evidence | Match | Confidence | Finding |
| --- | --- | --- | --- | --- | --- |
| ALIGN-01 | Four backend operations | proto lines 6-10; handlers lines 137-421 | full_match for required four | 1.00 | — |
| ALIGN-02 | Strict RandomClient-only scope | StreamTickTransactions plus inbound/general relay | code_stronger_than_spec | 1.00 | FIND-04 |
| ALIGN-03 | 451 authenticated aligned votes | verified.rs lines 14-18, 181-227, 274-321 | full_match | 0.96 | — |
| ALIGN-04 | Zero unavailable tick metadata | grpc_api.rs lines 143-149 | full_match | 1.00 | — |
| ALIGN-05 | Balance operation | peer_api.rs lines 131-216, 568-615 | partial_match | 0.97 | FIND-01 |
| ALIGN-06 | Contract-query confirmation | peer_api.rs lines 311-478 | code_weaker_than_spec | 0.98 | FIND-01 |
| ALIGN-07 | Exact-byte broadcast and ID | grpc_api.rs lines 384-413; codec.rs lines 74-110 | full_match | 0.99 | — |
| ALIGN-08 | Core transaction acceptance | frame.rs lines 334-370; Core qubic.cpp 958-975 | code_weaker_than_spec | 1.00 | FIND-03 |
| ALIGN-09 | Eight-byte Core frame | frame.rs lines 7-12, 164-181 | full_match | 1.00 | — |
| ALIGN-10 | Core message IDs | frame.rs lines 14-29 | full_match | 1.00 | — |
| ALIGN-11 | Request/response dejavu correlation | pending.rs registrations/delivery; peer_api request builders | full_match | 0.98 | — |
| ALIGN-12 | Zero-dejavu dissemination | network.rs lines 1289-1290, configurable bypass | partial_match | 1.00 | FIND-06 |
| ALIGN-13 | Six-peer dissemination | network.rs line 35 and lines 296-306; state.rs 273-299 | full_match | 1.00 | — |
| ALIGN-14 | Four-peer handshake | frame.rs lines 9-12, 128-161 | full_match | 1.00 | — |
| ALIGN-15 | Core dedup semantics/provenance | state.rs lines 569-688 vs Core peers.h 982-1020 | mismatch | 1.00 | FIND-05 |
| ALIGN-16 | Current Core FourQ verification | verifier lib.rs lines 16-57 vs Core four_q.h 1892-1953 | code_weaker_than_spec | 1.00 | FIND-02 |
| ALIGN-17 | Computor packet acceptance | verified.rs lines 107-129 vs Core qubic.cpp 684-713 | code_stronger_than_spec | 0.99 | FIND-07 |
| ALIGN-18 | Core calendar bounds | verified.rs lines 408-465 vs Core qubic.cpp 763-768 | mismatch | 0.99 | FIND-08 |
| ALIGN-19 | END_RESPONSE and TRY_AGAIN | peer_api.rs lines 398-478; Core qubic.cpp 1339-1357 | full_match for required calls | 0.97 | — |
| ALIGN-20 | Resource/backpressure safety | bounded channels, semaphores, timeouts, rate limits | code_stronger_than_spec; justified runtime adaptation | 0.92 | — |
| ALIGN-21 | Container deployment | RandomClient compose.yaml lines 2-34 | full_match | 1.00 | — |

---

## 6. Divergence Findings

### FIND-01 — HIGH — Peer-backed contract and balance results are not authenticated

~~~yaml
id: FIND-01
severity: HIGH
title: "First ordinary peer response controls RandomClient contract reconciliation"
spec_claim: "GetProviderStatus.lastUpdateTick is the contract-execution confirmation; balance gates enrollment."
code_finding: "Balance and contract queries race up to three sessions and return the first structurally successful response without proof or quorum."
match_type: code_weaker_than_spec
confidence: 0.98
reasoning: "A malicious connected peer can provide a matching RespondEntity or non-empty 680-byte GetProviderStatus payload. Dejavu binding authenticates correlation, not content."
evidence:
  spec_quote: "RandomClient Architecture lines 140-154"
  code_quote: "peer_api.rs lines 141-200 and 322-379"
  consumer_quote: "RandomClient engine.rs lines 392-412 decodes status and obtains balance"
exploitability:
  prerequisites: "Attacker becomes one connected session and wins response latency."
  sequence: "Observe request dejavu; send valid-shaped false data; QLN returns it; RandomClient reconciles or enrolls from the false state."
  impact: "DoS, incorrect reveal-chain ownership decisions, or collateral exposure. Maximum configured three-slot exposure is 3,000,000,000 qu at the 1,000,000,000 tier; guaranteed loss was not demonstrated."
remediation: "For balance, verify the Merkle sibling path against a spectrum digest established by signed quorum data. Generic Core contract responses carry no proof, so choose and document an explicit architecture: trusted configured full node, matching-response quorum from independent peers, or a proof-bearing protocol extension. Do not silently invent the policy."
testing: "Adversarial fast-peer tests, conflicting-response tests, proof vectors from Core, and end-to-end RandomClient reconciliation tests."
~~~

### FIND-02 — HIGH — Rust FourQ verification is behind current Core

~~~yaml
id: FIND-02
severity: HIGH
title: "Missing canonical-scalar and low-order-key rejection"
spec_claim: "Current Core requires S < curve_order and rejects identity/cofactor-subgroup public keys."
code_finding: "verify_digest checks encoding bits and the signature equation but has none of the current Core guards."
match_type: code_weaker_than_spec
confidence: 1.0
reasoning: "The local crate is attributed to older SCAPI commit 3403107..., while the audited Core has explicit security hardening and regression vectors."
evidence:
  spec_quote: "Core four_q.h lines 1892-1953"
  code_quote: "crates/qubic-fourq-verifier/src/lib.rs lines 16-57"
  test_quote: "Core test/fourq.cpp lines 299-413 and 413-691"
exploitability:
  prerequisites: "A non-canonical signature under a trusted key, or a trusted key list containing a low-order key."
  sequence: "Present input accepted by the older equation-only verifier but rejected by current Core."
  impact: "Verification semantics differ at a cryptographic trust boundary. Distinct-computor quorum counting limits immediate replay amplification; no direct financial loss was demonstrated."
remediation: "Port the current Core scalar comparison, identity guard, cofactor clearing, and all associated Core vectors into qubic-fourq-verifier before using it as a trust anchor."
testing: "S+r malleability vectors, S==r boundary, every Core weak-key vector, real forged-transaction vectors, and existing positive SCAPI vectors."
~~~

### FIND-03 — MEDIUM — Broadcast reports success without Core signature validation

~~~yaml
id: FIND-03
severity: MEDIUM
title: "Structurally valid but cryptographically invalid transactions are relayed"
spec_claim: "Core processBroadcastTransaction requires checkValidity, exact size, K12 digest, and FourQ signature verification before dissemination."
code_finding: "QLN checks amount/input/size only, queues the frame, and returns ok=true when one peer queue accepts it."
match_type: code_weaker_than_spec
confidence: 1.0
reasoning: "Core validation exists and is not faithfully ported."
evidence:
  spec_quote: "Core qubic.cpp lines 946-975"
  code_quote: "frame.rs lines 334-370; grpc_api.rs lines 357-421; network.rs lines 275-338 and 1280-1287"
exploitability:
  prerequisites: "Access to the gRPC endpoint or ability to send frames to an incoming session."
  sequence: "Submit a correct-length transaction with an invalid signature; QLN accepts and fans it out; Core peers later reject it."
  impact: "False-positive API acceptance and network resource amplification. Configured sustained global ingress is 100 broadcasts/s with fanout up to six, or up to 600 invalid peer-frame sends/s before downstream rejection."
remediation: "Compute K12 over transaction bytes excluding the 64-byte signature and verify with the updated Core-parity FourQ verifier before dedup commit, fanout, or ok=true."
testing: "Core valid/invalid transaction vectors, malformed source keys, non-canonical signatures, and assertions that rejected data reaches zero peer queues."
~~~

### FIND-04 — MEDIUM — Functionality exceeds the strict RandomClient boundary

~~~yaml
id: FIND-04
severity: MEDIUM
title: "Unused tick-transaction API and general relay remain product features"
spec_claim: "Only four RandomClient production operations plus demonstrated dependencies are allowed."
code_finding: "The service exposes StreamTickTransactions, listens for incoming peers, relays general traffic, enables reflection, and provides relay-specific CLI modes."
match_type: code_stronger_than_spec
confidence: 1.0
reasoning: "RandomClient NetworkBackend has exactly four methods and never invokes the stream RPC. The copied client proto is not a production call path."
evidence:
  spec_quote: "RandomClient backend.rs lines 59-65; AGENTS.md lines 40-60"
  code_quote: "proto lines 6-10; grpc_api.rs lines 202-306; peer_api.rs lines 218-309; app.rs lines 66-123"
exploitability:
  prerequisites: "Network or gRPC access."
  sequence: "Exercise an unused public surface."
  impact: "One unused RPC, one public TCP listener, general relay paths, and more than 220 dedicated production lines increase maintenance and attack surface."
remediation: "Remove StreamTickTransactions and its types/builders/tests from both schemas. Decide whether inbound relay is a proven dependency; otherwise remove listener/general relay and relay-only options while retaining outbound sessions needed by the four operations."
testing: "Schema compatibility test that enumerates exactly four RPCs and startup tests for the reduced outbound-only topology."
~~~

### FIND-05 — MEDIUM — Deduplication is an independent design

~~~yaml
id: FIND-05
severity: MEDIUM
title: "BLAKE3 FIFO dedup does not implement Core K12/dejavu filtering"
spec_claim: "Do not create a local alternative when Core already contains deduplication semantics."
code_finding: "QLN keeps 65,536 exact BLAKE3 hashes by default with reservation/commit rollback. Core uses a salted K12 32-bit identifier, two 512 MiB bitsets, and a 1,000,000-message swap interval."
match_type: mismatch
confidence: 1.0
reasoning: "The algorithms, window sizes, collision properties, and retry semantics differ."
evidence:
  spec_quote: "AGENTS.md lines 62-81"
  code_quote: "state.rs lines 569-688; config.rs line 467"
  core_quote: "Core peers.h lines 23 and 982-1020"
exploitability:
  prerequisites: "Sustained distinct traffic or hash/window manipulation."
  sequence: "Evict an older exact digest after 65,536 commits and replay it, or trigger behavior that differs from Core's probabilistic two-generation filter."
  impact: "Earlier replay eligibility and protocol behavior drift; no financial exploit was demonstrated."
remediation: "Port Core semantics or obtain an explicit reduced-node architecture decision that defines the exact-hash deviation, memory budget, replay window, and compatibility requirements."
testing: "Cross-implementation replay traces around 65,536, 1,000,000, and 2,000,000 messages plus collision/retry tests."
~~~

### FIND-06 — MEDIUM — relay-all bypasses Core zero-dejavu dissemination

~~~yaml
id: FIND-06
severity: MEDIUM
title: "Optional mode propagates non-zero-dejavu traffic"
spec_claim: "Core broadcast handlers call dissemination only when header.isDejavuZero()."
code_finding: "QLN defaults to the Core gate but --relay-all bypasses it for non-internal, non-pending traffic."
match_type: mismatch
confidence: 1.0
reasoning: "This is an intentional alternate protocol mode absent from Core and unnecessary for RandomClient."
evidence:
  spec_quote: "Core qubic.cpp lines 709, 777, 970 and header.h lines 48-56"
  code_quote: "network.rs lines 1289-1317; config.rs lines 191-202"
exploitability:
  prerequisites: "Operator enables --relay-all and attacker sends correlatable non-zero-dejavu frames."
  sequence: "Frame bypasses the default gate and is disseminated to up to six peers."
  impact: "Request/response traffic amplification and behavior inconsistent with Core; default configuration is not affected."
remediation: "Remove relay-all under the strict boundary. If retained by explicit decision, enumerate exactly which message types may propagate and prove no correlated response leaks."
testing: "Property test that every non-zero-dejavu frame produces zero relay sends."
~~~

### FIND-07 — LOW — Computor payload size is stricter than current Core

~~~yaml
id: FIND-07
severity: LOW
title: "QLN rejects the four-byte compatibility padding accepted by Core"
spec_claim: "Current Core temporarily accepts sizeof(BroadcastComputors) through +4 bytes."
code_finding: "QLN requires the exact canonical payload size."
match_type: code_stronger_than_spec
confidence: 0.99
reasoning: "Canonical Core responses remain accepted, but compatibility with external tools differs."
evidence:
  spec_quote: "Core qubic.cpp lines 684-688"
  code_quote: "verified.rs lines 107-113"
exploitability:
  prerequisites: "A source sends a Core-accepted padded packet."
  sequence: "Core accepts the size range; QLN rejects before verification."
  impact: "Availability/compatibility only; no security loss identified."
remediation: "Either mirror the temporary Core size range while signing only canonical bytes, or document exact-size rejection as an explicit approved hardening deviation."
testing: "Canonical, +1 through +4, and +5-byte packet cases."
~~~

### FIND-08 — LOW — Calendar validation differs for century years

~~~yaml
id: FIND-08
severity: LOW
title: "Gregorian leap-year rule differs from Core's wire-year rule"
spec_claim: "Core treats February as 29 days whenever the one-byte year is divisible by four."
code_finding: "Rust applies the full Gregorian century exception, making 2100 non-leap."
match_type: mismatch
confidence: 0.99
reasoning: "The implementations first diverge for wire year 100 (calendar year 2100)."
evidence:
  spec_quote: "Core qubic.cpp lines 763-768"
  code_quote: "verified.rs lines 453-465"
exploitability:
  prerequisites: "A valid tick dated 2100-02-29 under Core rules."
  sequence: "Core accepts the date bound; QLN rejects it as malformed."
  impact: "No impact before 2100 and no current exploit."
remediation: "Mirror Core's wire-year divisibility rule unless Core itself changes."
testing: "Years 2096, 2100, 2104, and February boundary vectors."
~~~

---

## 7. Missing Invariants

- No enforced policy establishes integrity for generic contract-query results
  before RandomClient uses them as execution confirmation.
- No balance Merkle-proof check binds RespondEntity to a quorum-backed spectrum
  digest.
- No automated parity gate imports current Core FourQ negative vectors.
- No CI rule proves that every public RPC has a RandomClient production caller.
- Until this change, no versioned repository instruction required Core
  provenance for protocol algorithms.

---

## 8. Incorrect Logic

- FourQ rejection behavior differs from current Core (FIND-02).
- Transaction acceptance stops at structure instead of Core signature
  verification (FIND-03).
- Deduplication is behaviorally different from Core (FIND-05).
- relay-all violates Core's zero-dejavu dissemination gate (FIND-06).
- The calendar rule differs for year 2100 and later century boundaries
  (FIND-08).

The four required gRPC calls otherwise preserve request fields and response
shapes expected by RandomClient.

---

## 9. Math Inconsistencies

Aligned constants:

- NUMBER_OF_COMPUTORS = 676.
- QUORUM = 676 × 2 / 3 + 1 = 451.
- DISSEMINATION_MULTIPLIER = 6.
- NUMBER_OF_EXCHANGED_PEERS = 4.
- MAX_NUMBER_OF_CONTRACTS = 1024.
- MAX_INPUT_SIZE = 1024.
- MAX_AMOUNT = 1,000,000,000,000,000.
- SIGNATURE_SIZE = 64.

The material arithmetic mismatch is FourQ scalar canonicality: Core compares
the 256-bit little-endian S value with the exact curve order; the Rust entry
point only applies upper-bit constraints. Calendar leap-year arithmetic also
differs as described in FIND-08.

---

## 10. Flow and State-Machine Mismatches

- Required flow is present:
  RandomClient → gRPC → persistent peer request/broadcast → bounded response →
  RandomClient.
- Tick status is stronger than an unsigned current-tick response: it is
  established only from authenticated computor/tick traffic.
- Balance and contract status cross an untrusted peer boundary without content
  authentication, then influence RandomClient enrollment/reconciliation.
- StreamTickTransactions is a complete independent stateful flow not present
  in RandomClient's production backend interface.
- Incoming general relay and relay-all create runtime flows unrelated to the
  required four operations.

---

## 11. Access-Control Drift

Core P2P has no gRPC boundary. QubicLightNode adds one as a necessary adapter:

- Default bind is loopback, which limits exposure in the default deployment.
- Operators may bind gRPC publicly.
- Broadcast has global/per-client token buckets and a 32-call concurrency
  limit.
- Read operations have concurrency limits but no authentication.
- Incoming P2P accepts ordinary network peers, which are then candidates for
  first-response balance/contract races.

RandomClient documents the backend as trusted with early reveal material, but
the local QLN backend does not turn ordinary peers into trusted contract-data
sources. This is part of FIND-01 rather than a missing user login mechanism.

---

## 12. Undocumented or Out-of-Scope Behavior

Under the strict boundary, the following are out of scope until a production
dependency is demonstrated:

- StreamTickTransactions and its transaction-decoding types.
- General inbound relay service.
- relay-all.
- Traffic-log as a relay diagnostic.
- gRPC reflection.
- Balance public-key hex formats not used by RandomClient's identity caller.

Bounded queues, timeouts, cooldown, DNS recovery, rate limiting, and
coalescing are accepted supporting adaptations because they protect the four
required operations in the reduced asynchronous runtime. They still require
documented invariants and must not change Core wire semantics.

---

## 13. Ambiguity Hotspots

- RandomClient's copied protobuf declares StreamTickTransactions while its
  canonical architecture and production trait define four operations. This
  audit resolves the conflict in favor of the production call graph, per the
  user-selected strict rule.
- Core supplies a Merkle path in RespondEntity but generic contract-function
  responses have no proof-bearing wire format. A secure reduced-node policy for
  generic contract state therefore needs an explicit architectural decision.
- QLN's 451-vote consensus reconstruction is necessary because the reduced
  node does not execute Core consensus locally. Core has no drop-in light-node
  implementation for this adapter; the selected consensus fields were checked
  against Core's current aligned-vote comparisons.
- RandomClient's audited architecture/backend files are uncommitted. Their
  hashes, not only HEAD, are required to reproduce this report.

---

## 14. Recommended Remediations

Priority order:

1. Define and implement an integrity model for GetProviderStatus and balance
   before treating the gRPC backend as an execution-confirmation source.
2. Port current Core FourQ verification and all negative vectors.
3. Apply full Core transaction signature validation before relay and API
   success.
4. Remove StreamTickTransactions and other unneeded product surfaces from both
   QLN and RandomClient schemas.
5. Remove relay-all and decide whether inbound general relay is necessary.
6. Replace the independent dedup algorithm with Core parity or approve and
   document a reduced-node exception.
7. Resolve the two LOW compatibility differences.

Each remediation must be a separate implementation task with RandomClient/Core
traceability and regression tests. This audit intentionally applies none of
them.

---

## 15. Documentation Update Suggestions

- Update QubicLightNode README after scope remediation so its stated role and
  RPC list match the strict RandomClient boundary.
- Remove the unused stream RPC and messages from RandomClient's copied proto at
  the same time as QLN to preserve generated-client compatibility.
- Document the selected trust model for generic contract queries in both
  repositories.
- Record the exact Core revision used by the FourQ crate and update attribution
  whenever parity changes.
- Keep this report point-in-time; create a new dated audit or update baselines
  after material protocol/source changes.

---

## 16. Final Risk Assessment

**Overall architectural risk: HIGH until FIND-01 and FIND-02 are resolved.**

The required API is present and tests are healthy, but passing tests currently
encode several local behaviors rather than proving current Core parity.
RandomClient can compile and run against the service, yet compatibility alone
does not establish trustworthy contract state. The largest risk is therefore
not missing functionality; it is treating unverified ordinary-peer data as the
authoritative input to a collateralized commit/reveal state machine.

The project should remain operational only under its documented trusted-backend
assumption and loopback/private deployment while remediation is planned. No
claim is made that the findings guarantee financial loss or that the audited
network is currently under attack.
