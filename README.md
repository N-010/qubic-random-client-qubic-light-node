# QubicLightNode

`QubicLightNode` is the outbound-only Qubic backend for
`D:\Work\MySelf\Qubic\RandomCient`. It is a deliberately reduced port of the
required behavior from `D:\Work\MySelf\Qubic\QThirtyFour\core`; it is not a
general relay node.

The current service exposes the four operations used by RandomClient:

- current epoch/tick status from one structurally valid public-peer message;
- authenticated current-epoch tick transaction-presence lookup;
- read-only contract-function query with raw input and output bytes;
- validation and broadcast of an already signed transaction.

The architecture contract is in `AGENTS.md`. The point-in-time compliance
report is in `docs/ARCHITECTURE_COMPLIANCE_AUDIT.md`.

## Build and run

```bash
cargo build --release
cargo run --release
```

Manual seed peers can be supplied more than once:

```bash
cargo run --release -- --peer 1.2.3.4:21841 --peer 5.6.7.8:21841
```

By default the backend:

- maintains eight outbound Qubic TCP sessions;
- uses remote Qubic port `21841`;
- fetches bootstrap peers from `api.qubic.global` when no manual peer is set;
- serves gRPC on `127.0.0.1:50051`.

It does not listen for inbound Qubic connections, relay arbitrary peer
traffic, expose an HTTP API, or enable gRPC reflection. To expose gRPC on a
different address, use `--grpc-listen`, for example:

```bash
cargo run --release -- --grpc-listen 0.0.0.0:50051
```

Run `cargo run --release -- --help` for the complete generated option list.
The principal options are `--peer`, `--peer-port`, `--target-outbound`,
`--max-known-peers`, peer timeout controls, DNS bootstrap controls,
`--api-timeout-ms`, `--grpc-listen`, and `--traffic-log`.

## gRPC boundary

Service: `lightnode.LightNode`

- `GetStatus` returns the greatest epoch/tick observed in one exact-size,
  structurally valid Core `BroadcastTick` or `RespondCurrentTickInfo` message.
  This fast path deliberately performs no signature or quorum authentication;
  the accepted availability/security tradeoff is documented in
  `docs/adr/0002-unauthenticated-tick-status.md` and warned about at startup.
- `GetTickTransactions` sends Core `RequestTickData` to up to three existing
  peer sessions and accepts only an exact-size `TickData` for the requested
  tick whose leader index, calendar fields, unique non-zero transaction
  digests, current-epoch computor key, K12 digest, and FourQ signature are
  valid. The response exposes only `has_transactions`; it never returns raw
  transactions or trusts an unsigned peer claim. `--max-frame-bytes` cannot be
  configured below the 139,384-byte Core TickData response frame.
- `QueryContractFunction` races up to three existing public-peer sessions and
  returns the first valid non-empty Core response. This response is not
  cryptographically authenticated. The accepted risk and revisit criteria are
  documented in `docs/adr/0001-unauthenticated-contract-query.md` and a warning
  is printed at startup.
- `BroadcastTransaction` enforces the current Core transaction layout and
  limits, computes the K12 digest of the unsigned bytes, and verifies the
  canonical FourQ signature before any network fanout. `ok=true` means the
  validated bytes were queued to at least one peer; it is not confirmation.

The canonical server schema is `proto/lightnode.proto` and contains exactly
the four current RandomClient operations.

## Operational behavior

- Peer handshake and discovery use Core message type `0` and the exact
  24-byte exchange frame.
- Computor broadcasts accept the canonical Core payload plus its permitted
  zero-to-four bytes of C++ struct padding; only the signed canonical prefix is
  authenticated.
- Tick status accepts only the exact Core 352-byte `BroadcastTick` or 16-byte
  `RespondCurrentTickInfo` payload layout. The cached packed epoch/tick value is
  monotonic, so delayed lower values do not move status backwards.
- Request/response correlation uses `dejavu`; bounded pending routes enforce
  response types, frame counts, total bytes, and deadlines.
- Peer queues and the shared byte budget are bounded. A peer-specific queue
  failure disconnects that peer; global local-memory pressure does not punish
  healthy peers.
- Repeated valid transaction submissions are independent broadcasts. The
  service coalesces only simultaneous submissions of identical bytes.

## Trust boundary

Computor lists, TickData, and submitted transaction signatures are locally
verified. Tick status and contract-function output are explicit unauthenticated
public-peer trust exceptions governed by ADR-0002 and ADR-0001 respectively.
Do not use either result as authenticated consensus state.

## Verification

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```
