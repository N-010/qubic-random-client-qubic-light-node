# QubicLightNode

QubicLightNode is the outbound-only Qubic backend for RandomClient. It keeps a
small pool of public-peer connections and exposes only the four gRPC operations
that RandomClient uses. It is a deliberately reduced port of required Qubic
Core behavior, not a general relay node or a complete Qubic implementation.

## Release status

Build from the tagged checkout; successful tag workflows also publish Linux
and Windows binaries to GitHub Releases. The crate version matches the tag.

The gRPC contract is defined by the matching `proto/lightnode.proto` files
in these compatible checkouts.

## Requirements

The repository pins Rust 1.93.0, including `rustfmt` and `clippy`, in
`rust-toolchain.toml`. A compatible Rust installation automatically selects
that toolchain when commands are run from the repository.

## Build and run

Build and verify the workspace:

```bash
cargo build --release --locked
cargo test --workspace --all-targets --all-features --locked
```

Start the node with DNS bootstrap and the default local gRPC endpoint:

```bash
cargo run --release --locked
```

Manual seed peers can be supplied more than once. A plain IP uses the current
`--peer-port` value:

```bash
cargo run --release --locked -- \
  --peer 1.2.3.4:21841 \
  --peer 5.6.7.8:21841
```

Then run the matching RandomClient checkout against it:

```bash
cargo run --release --locked -- \
  --backend grpc \
  --endpoint http://127.0.0.1:50051
```

Run `cargo run --release --locked -- --help` for the generated CLI reference.

## Configuration

| Option | Default | Purpose |
| --- | --- | --- |
| `--peer <IP[:PORT]>` | None | Add a manual seed peer; may be repeated. |
| `--peer-port <PORT>` | `21841` | Set the remote port used for discovery and peer values without a port. |
| `--target-outbound <N>` | `8` | Set the desired number of outbound Qubic sessions. |
| `--broadcast-fanout <N>` | `6` | Offer each local transaction to up to N established peers; range 1–32. |
| `--max-known-peers <N>` | `500` | Bound the in-memory peer set. |
| `--reconnect-ms <MS>` | `2000` | Set the delay between dial cycles. |
| `--peer-write-timeout-ms <MS>` | `5000` | Bound one peer-frame write. |
| `--peer-connect-timeout-ms <MS>` | `5000` | Bound one outbound TCP connection attempt. |
| `--peer-handshake-timeout-ms <MS>` | `5000` | Bound the mandatory Qubic peer exchange. |
| `--peer-frame-timeout-ms <MS>` | `30000` | Bound completion of an announced peer frame. |
| `--max-frame-bytes <BYTES>` | `1048576` | Bound accepted Qubic frames; allowed range 139384–16777215 bytes, including a complete Core TickData frame. |
| `--no-dns-bootstrap` | Disabled | Disable bootstrap requests to `api.qubic.global`. |
| `--dns-lite-peers <N>` | `0` | Request this peer count for initial, emergency, and periodic DNS; zero selects automatic sizing. |
| `--dns-timeout-ms <MS>` | `5000` | Bound one DNS bootstrap request. |
| `--critical-peer-threshold <N>` | `0` | Trigger emergency DNS below this count; zero means half the target, at least one. |
| `--no-emergency-dns` | Disabled | Disable emergency DNS when the connected pool is critically low. |
| `--emergency-dns-backoff-initial-ms <MS>` | `10000` | Set the initial emergency-DNS retry backoff. |
| `--emergency-dns-backoff-max-ms <MS>` | `300000` | Cap emergency-DNS retry backoff. |
| `--api-timeout-ms <MS>` | `6000` | Set the end-to-end deadline for a peer-backed gRPC query. |
| `--grpc-listen <IP:PORT>` | `127.0.0.1:50051` | Set the gRPC bind address. |
| `--traffic-log` | Disabled | Log Qubic frame metadata for diagnostics. |
| `--transaction-log` | Disabled | Log outbound frames and transaction write timing without inbound traffic. |

For an explicitly wider direct broadcast, combine `--target-outbound 32` with
`--broadcast-fanout 32`. Fanout alone does not create additional connections.
The default remains Core's six recipients; values above six are an opt-in
availability adaptation for RandomClient's short reveal window, not a delivery
guarantee. More recipients increase network traffic and queue admissions while
the existing global/per-peer budgets and RPC rate limits still apply.

The configured outbound target must not exceed the known-peer limit. The frame
limit must be large enough for the exact Core `TickData` response used by
`GetTickTransactions`.

## gRPC boundary

Service: `lightnode.LightNode`

| Method | Result | Trust level |
| --- | --- | --- |
| `GetStatus` | Greatest observed epoch/tick | Structurally validated, but unauthenticated single-peer observation. |
| `GetTickTransactions` | Transaction presence and the complete non-zero digest list for one requested tick | Authenticated with the active arbitrator-signed computor set and FourQ-verified Core `TickData`. |
| `QueryContractFunction` | Raw non-empty contract output | First eligible response from up to three peers after a same-session current-tick preflight; unauthenticated. |
| `BroadcastTransaction` | Canonical transaction ID after queueing to at least one peer | Transaction layout and FourQ signature are verified locally; queueing is not execution confirmation. |

The schema contains exactly these four methods. There is no balance RPC.

`GetTickTransactions` includes `repeated bytes transaction_digests = 5` (32
bytes per digest, Core slot order) and `bool digests_complete = 6`. Successful
authenticated responses set completeness even for an empty list; errors leave
it false. Older clients ignore these additive fields; updated RandomClient
compares the full K12 hash of its signed bytes and reports included/missing.
Old servers provide only presence, so updated clients report inclusion as
unknown. Neither inclusion nor the leader signature proves contract success
or consensus finality. No CLI option is required.

## Operating model

- The node opens outbound Qubic TCP connections only. It does not accept
  inbound Qubic sessions, relay arbitrary traffic, expose HTTP, or enable gRPC
  reflection.
- DNS, manual seeds, and peer-exchange gossip feed a bounded peer pool. Failed
  peers enter cooldown; emergency DNS can repopulate a critically small pool.
- Each session completes the exact 24-byte Core peer exchange before it becomes
  usable. Reader and writer tasks enforce framing, deadlines, queue budgets,
  request correlation, and protocol-specific response limits.
- Computor retrieval stays active for each session and refreshes authenticated
  keys when the observed epoch changes; temporary failures retry up to every
  30 seconds without bypassing signature verification.
- Contract queries snapshot the cached epoch/tick and first request current
  tick info from each selected peer. Only the same epoch and at most one tick
  of lag qualify. Missing status or no eligible result is a temporary error;
  ordinary lag does not disconnect the peer. Preflight and contract response
  share one API deadline.
- Peer-backed queries race at most three established sessions. The service
  limits concurrent queries and broadcasts instead of allowing unbounded work.
  Broadcast rate/concurrency admission precedes signature verification, including
  for invalid non-empty submissions.
- TickData verification admits at most eight queued or running jobs process-wide.
  Cancelled queries retain their verification slot until blocking work finishes;
  overload returns an error without penalizing the peer.
- Per-peer and global outbound byte budgets are bounded. Peer-specific queue
  failure disconnects that peer; global local-memory pressure returns an
  overload error without penalizing healthy peers.
- Simultaneous submissions of identical transaction bytes share one network
  broadcast. A later retry is an independent broadcast.
- `Ctrl+C` stops the process. Peer, tick, computor, and pending-request state is
  memory-only and is rebuilt after restart.

## Trust and exposure

`GetStatus` and `QueryContractFunction` are explicit public-peer trust
exceptions. They are not cryptographic authentication, quorum agreement, or a
Qubic consensus proof. Their rationale, limits, and revisit criteria are in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

The gRPC server currently has no application authentication or TLS. Its default
loopback bind is the safe deployment baseline. If `--grpc-listen` exposes it to
another host, use a trusted private network, firewall, or authenticated proxy.
In particular, do not expose transaction broadcast access directly to the
public internet.

QubicLightNode does not hold a wallet seed or sign transactions. It validates
already signed bytes supplied by RandomClient and does not persist peer-backed
responses as durable consensus state.

The complete component boundaries, runtime flows, resource invariants, and
source traceability are specified in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Development and security

Run the primary checks used by CI:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
```

CI also runs `cargo-audit` and enforces the dependency policies in
[`deny.toml`](deny.toml). See [`SECURITY.md`](SECURITY.md) for private
vulnerability reporting, [`CHANGELOG.md`](CHANGELOG.md) for release changes,
and [`LICENSE`](LICENSE) for the MIT license.


With `--traffic-log` or `--transaction-log`, transaction writes also emit `TX_RESULT` with the full
public transaction ID, target tick, peer, queue/write durations in microseconds,
observed ticks before/after the write, and written/failed result. A completed
write means delivery to the local TCP stack, not peer or leader acceptance.
No transaction bytes or preimages are logged. Queue timestamps do not change
FIFO ordering, byte budgets, deadlines, or broadcast acceptance semantics.

`--transaction-log` enables outbound frame and transaction diagnostics without
verbose inbound traffic. Both diagnostics switches default to off. The bounded
logger can drop records under overload; missing log entries are not evidence of
a missing network write.


## Background peer selection

Peer quality is monitored automatically in a separate Tokio task. RTT and tick
freshness are measured repeatedly. A replacement is qualified before an idle
old connection is retired; broadcasts never wait for probing. One temporary
candidate socket beyond `--target-outbound` is permitted. Keep `--max-known-peers`
greater than the outbound target to provide spare candidates (default pool: 500).

Selection attempts occur at most every 30 seconds; samples run every ten seconds
plus bounded probe time. Replacement requires three good candidate samples and
three bad old-peer samples, or an RTT improvement greater than 25% and 20 ms.
Retired/rejected addresses have a two-minute cooldown. `PEER_SELECTION` logs
explain decisions. `--no-dns-bootstrap` also disables periodic DNS refresh.
Tick observations remain peer-trusted; low RTT does not guarantee inclusion.

The HTTPS peer-bootstrap response body is limited to 1 MiB, including chunked
responses. Larger responses fail the bootstrap attempt and use the existing
retry policy.

## CI and releases

Pushes, pull requests, and manual workflow runs check formatting, Clippy, and
the complete workspace test targets on Linux and Windows, plus dependency
security. A weekly schedule runs dependency security checks. Rust is selected
from `rust-toolchain.toml`; jobs have explicit time limits.

Push a `v<version>` tag matching `Cargo.toml` to release. Both platform builds
must pass `--version` and `--help` checks before the publication job downloads
and verifies their SHA256 checksums. It publishes `QubicLightNode`,
`QubicLightNode.exe`, and `SHA256SUMS` together after both builds succeed.
Remote uploads are not atomic; resolve any publication failure before treating
the release as complete. Manual runs do not publish releases.
