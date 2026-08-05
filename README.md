# QubicLightNode

QubicLightNode is the outbound-only Qubic backend for RandomClient. It keeps a
small pool of public-peer connections and exposes only the four gRPC operations
that RandomClient uses. It is a deliberately reduced port of required Qubic
Core behavior, not a general relay node or a complete Qubic implementation.

## Release status

This repository is being prepared as the compatible QubicLightNode v2.0.0
release that must precede RandomClient v2.0.0. The crate currently reports
version `0.3.0`; no v2.0.0 tag, packaged binaries, or checksums are published
yet. Build and run the matching source checkout for now.

The gRPC contract is versioned by the matching `proto/lightnode.proto` files in
QubicLightNode and RandomClient. Do not mix revisions until a compatibility
point is published.

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
| `--max-known-peers <N>` | `500` | Bound the in-memory peer set. |
| `--reconnect-ms <MS>` | `2000` | Set the delay between dial cycles. |
| `--peer-write-timeout-ms <MS>` | `5000` | Bound one peer-frame write. |
| `--peer-connect-timeout-ms <MS>` | `5000` | Bound one outbound TCP connection attempt. |
| `--peer-handshake-timeout-ms <MS>` | `5000` | Bound the mandatory Qubic peer exchange. |
| `--peer-frame-timeout-ms <MS>` | `30000` | Bound completion of an announced peer frame. |
| `--max-frame-bytes <BYTES>` | `1048576` | Bound accepted Qubic frames. |
| `--no-dns-bootstrap` | Disabled | Disable bootstrap requests to `api.qubic.global`. |
| `--dns-lite-peers <N>` | `0` | Request a DNS peer count; zero selects automatic sizing. |
| `--dns-timeout-ms <MS>` | `5000` | Bound one DNS bootstrap request. |
| `--critical-peer-threshold <N>` | `0` | Trigger emergency DNS below this count; zero means half the target, at least one. |
| `--no-emergency-dns` | Disabled | Disable emergency DNS when the connected pool is critically low. |
| `--emergency-dns-backoff-initial-ms <MS>` | `10000` | Set the initial emergency-DNS retry backoff. |
| `--emergency-dns-backoff-max-ms <MS>` | `300000` | Cap emergency-DNS retry backoff. |
| `--api-timeout-ms <MS>` | `6000` | Set the end-to-end deadline for a peer-backed gRPC query. |
| `--grpc-listen <IP:PORT>` | `127.0.0.1:50051` | Set the gRPC bind address. |
| `--traffic-log` | Disabled | Log Qubic frame metadata for diagnostics. |

The configured outbound target must not exceed the known-peer limit. The frame
limit must be large enough for the exact Core `TickData` response used by
`GetTickTransactions`.

## gRPC boundary

Service: `lightnode.LightNode`

| Method | Result | Trust level |
| --- | --- | --- |
| `GetStatus` | Greatest observed epoch/tick | Structurally validated, but unauthenticated single-peer observation. |
| `GetTickTransactions` | Whether one requested tick contains transaction digests | Authenticated with the active arbitrator-signed computor set and FourQ-verified Core `TickData`. |
| `QueryContractFunction` | Raw non-empty contract output | First structurally valid response from up to three peers; unauthenticated. |
| `BroadcastTransaction` | Canonical transaction ID after queueing to at least one peer | Transaction layout and FourQ signature are verified locally; queueing is not execution confirmation. |

The schema contains exactly these four methods. There is no balance RPC.

## Operating model

- The node opens outbound Qubic TCP connections only. It does not accept
  inbound Qubic sessions, relay arbitrary traffic, expose HTTP, or enable gRPC
  reflection.
- DNS, manual seeds, and peer-exchange gossip feed a bounded peer pool. Failed
  peers enter cooldown; emergency DNS can repopulate a critically small pool.
- Each session completes the exact 24-byte Core peer exchange before it becomes
  usable. Reader and writer tasks enforce framing, deadlines, queue budgets,
  request correlation, and protocol-specific response limits.
- Peer-backed queries race at most three established sessions. The service
  limits concurrent queries and broadcasts instead of allowing unbounded work.
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
