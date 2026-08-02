# QubicLightNode

`QubicLightNode` is a small Qubic relay node with a local `gRPC` API.

The wire protocol is compatible with current Qubic Core builds configured for 4096 transactions per tick.

It currently does four things:

- connects to the public Qubic network over TCP
- relays Qubic frames between peers
- keeps the latest tick confirmed by a signed computor quorum in local memory
- exposes a local `gRPC` API for status, balance, tick transactions, contract function queries, and transaction broadcast

There is no web UI and no HTTP REST API in the current codebase.

## Defaults

- Qubic peer listener: `0.0.0.0:21841`
- Default remote peer port: `21841`
- Local gRPC API: `127.0.0.1:50051`
- DNS bootstrap endpoint: `https://api.qubic.global/random-peers?service=bobNode&litePeers=...`

If you do not pass any `--peer` values, the node tries to fetch bootstrap peers from `api.qubic.global`.

## Build

```bash
cargo build --release
```

Binary path:

- Windows: `target\release\QubicLightNode.exe`
- Linux/macOS: `target/release/QubicLightNode`

## Start

The simplest start:

```bash
cargo run --release
```

Start with manual seed peers:

```bash
cargo run --release -- --peer 1.2.3.4:21841 --peer 5.6.7.8:21841
```

Use a custom default port for `--peer` values that do not include a port:

```bash
cargo run --release -- --peer-port 31841 --peer 1.2.3.4 --peer 5.6.7.8
```

Run without the local gRPC API:

```bash
cargo run --release -- --no-grpc
```

Run a second instance on the same machine:

```bash
cargo run --release -- --port 21842 --grpc-listen 127.0.0.1:50052
```

Expose the gRPC API to other machines:

```bash
cargo run --release -- --grpc-listen 0.0.0.0:50051
```

## Startup Behavior

After launch, the node:

1. optionally fetches bootstrap peers from `api.qubic.global` if `--peer` was not used and DNS bootstrap is enabled
2. binds the local TCP listener for incoming Qubic peers
3. starts the outbound reconnect loop and tries to maintain the configured number of outbound peer sessions
4. starts the local gRPC API unless you used `--no-grpc`
5. requests the signed computor list and updates the in-memory tick cache only after a valid computor quorum

Important notes:

- right after start, `GetStatus` can return no local tick data yet; this is normal until the node receives and verifies a signed computor list and 451 matching tick votes
- the process can still start even if DNS bootstrap fails; in that case you can wait for incoming peers or pass `--peer` manually
- by default, relay is limited to frames with `dejavu == 0`; use `--relay-all` to also relay frames with non-zero `dejavu`
- peer exchange frames are consumed locally and are never relayed; Oracle Machine and Outsourced Computation channel-only message types (`190`-`192`) are also never forwarded to ordinary Qubic peers
- computor lists are authenticated with the Qubic arbitrator identity and FourQ and must use the exact canonical payload size without unsigned padding; tick votes are checked for the exact 352-byte Core layout, Gregorian UTC calendar bounds, a timestamp no older than two minutes and no more than 30 seconds in the future, score threshold, computor signature, and a quorum of 451 distinct computors
- signed-frame verification uses a small non-queueing global CPU gate; when it is saturated the frame remains retryable, while exact valid and invalid cryptographic replays reuse a bounded domain-separated result cache
- each registered session runs at most one computor bootstrap attempt at a time; an exact empty eight-byte `END_RESPONSE` means that the list is temporarily unavailable and is retried with exponential delay up to 30 seconds
- broadcast transactions are structurally checked against current Core limits before relay fanout; balance Merkle proofs and transaction membership in signed tick data are not verified
- each relayed frame is queued to at most six randomly selected peers, matching the Qubic Core dissemination multiplier
- TCP input is accumulated in reusable buffers and complete frames are split into immutable, reference-counted byte views; relay fanout shares the same frame storage across all selected peer queues without copying the payload per peer
- peer/session state and the rolling deduplication window use separate lock domains, while the latest epoch/tick is kept in an atomic cache for lock-free status reads
- a peer is disconnected when its own bounded outbound queue or byte budget is full, allowing the reconnect loop to replace a slow session; exhaustion of the shared global outbound byte budget is treated as local overload and does not disconnect healthy peers
- incoming sessions, incomplete handshakes, frame assembly, pending API responses, and gRPC streams all have explicit count, byte, and/or time limits
- every peer write has a deadline controlled by `--peer-write-timeout-ms`; a peer that stops reading is disconnected and replaced instead of retaining a stalled writer
- console logs use a bounded non-blocking queue and a dedicated writer thread, so a slow Docker log consumer cannot block the Tokio runtime; log messages are dropped if that queue is full
- failed dial attempts, peer-specific queue failures, and protocol violations put the canonical dial address into an exponential cooldown, starting at `--reconnect-ms` and capped at five minutes; administrative shutdown and local global-memory/frame policy limits do not penalize peers
- completing a handshake does not erase failure history: sessions shorter than 60 seconds continue the previous cooldown exponent, while a session lasting at least 60 seconds resets old history before the next failure
- configured `--peer` addresses are retained even when the discovered-peer cache is full; active and pending connection addresses are protected from eviction as well
- emergency DNS recovery is based only on the outbound connection count, so incoming connections cannot hide a depleted outbound pool
- peer-pool changes are logged as `known`, `dialable`, `cooldown`, `pending`, `incoming`, `outgoing`, and `target` counts

## CLI Options

### P2P

- `--peer <ip[:port]>`
  Add a seed peer manually. You can repeat this option more than once.
- `--port <port>`
  Local TCP listen port for incoming Qubic peers. Default: `21841`.
- `--peer-port <port>`
  Default remote port for discovery and for `--peer` values without an explicit port. Default: `21841`.
- `--listen-ip <ipv4>`
  IPv4 address for the peer listener. Default: `0.0.0.0`.
- `--target-outbound <n>`
  Desired number of outbound peer connections to keep. Default: `8`. It must not exceed `--max-known-peers`.
- `--max-incoming <n>`
  Maximum number of incoming peer sessions. Default: `32`; values above Tokio's semaphore capacity are rejected during startup.
- `--max-known-peers <n>`
  Maximum number of discovered peers kept in memory. Default: `500`. Evictable addresses use LRU order; configured seeds, active peers, and pending dials are never evicted, so protected entries can temporarily keep the pool above the limit.
- `--reconnect-ms <ms>`
  Delay between outbound reconnect attempts. Default: `2000`. Values below `200` are currently clamped to `200` internally.
- `--peer-write-timeout-ms <ms>`
  Maximum time allowed for one TCP frame write before the peer is disconnected. Default: `5000`.
- `--peer-connect-timeout-ms <ms>`
  Maximum duration of an outbound TCP connect attempt. Default: `5000`; values below `500` are clamped to `500`.
- `--peer-handshake-timeout-ms <ms>`
  Time allowed for the first exact 24-byte `EXCHANGE_PUBLIC_PEERS` frame before registration as a session. Default: `5000`; values below `500` are clamped to `500`.
- `--peer-frame-timeout-ms <ms>`
  Time allowed to finish an announced frame after its header arrives. Default: `30000`; values below `1000` are clamped to `1000`.
- `--max-frame-bytes <bytes>`
  Maximum accepted frame size, including its eight-byte header. Default: `1048576`; valid range: `65551..16777215`. The minimum guarantees that a contract request or response carrying the full 65535-byte Core payload fits. A larger wire-valid frame is closed as a local policy limit without peer cooldown.

### Relay

- `--relay-all`
  Relay frames even when `dejavu` is non-zero.
- `--traffic-log`
  Log RX, TX, and relay activity for network frames.
- `--max-seen <n>`
  Deduplication window size for seen frame hashes. Default: `65536`. Values below `1000` are currently clamped to `1000` internally.

### Bootstrap

- `--no-dns-bootstrap`
  Disable bootstrap peer fetches from `api.qubic.global`.
- `--dns-lite-peers <n>`
  Requested `litePeers` count for DNS bootstrap. Default: `0`, which means auto mode.
- `--dns-timeout-ms <ms>`
  Timeout for the DNS bootstrap HTTP request. Default: `5000`. Values below `500` are currently clamped to `500` internally.

Auto mode for `--dns-lite-peers` currently requests `max(target_outbound * 3, 8)`.

Emergency DNS bootstrap runs when outbound connections fall below `--critical-peer-threshold`. The automatic threshold is half of `--target-outbound`, with a minimum of `1` whenever the target is positive. Empty responses and responses containing only already-known addresses count as failures and increase the DNS retry backoff. A newly discovered address or retained promotion of a gossip address to DNS provenance resets the backoff. DNS provenance is exempt from the per-gossip-source quota, but an inactive DNS peer remains subject to the overall `--max-known-peers` LRU limit. Only manual, active, and pending endpoints are protected from overall eviction.

An explicit `--critical-peer-threshold` must not exceed `--target-outbound`; when the outbound target is zero, the threshold must also be zero. Emergency DNS backoff starts after the DNS request completes, so request latency never consumes the configured retry delay.

### API

- `--grpc-listen <ip:port>`
  Bind address for the gRPC server. Default: `127.0.0.1:50051`.
- `--no-grpc`
  Disable the gRPC server.
- `--api-timeout-ms <ms>`
  End-to-end deadline for a balance or tick-transactions query over existing peer sessions, including delivery to a slow gRPC client. Default: `6000`. Values below `1000` are currently clamped to `1000` internally.

To see the parser-generated help text:

```bash
cargo run --release -- --help
```

## gRPC API

Service name: `lightnode.LightNode`

Methods:

- `GetStatus`
  Returns the latest tick confirmed by at least 451 valid signatures from distinct members of an arbitrator-signed computor list.
- `GetBalance`
  Queries peers for wallet balance data and returns the first structurally valid response whose public key matches the request.
- `StreamTickTransactions`
  Selects one random healthy peer, matching Qubic Core request semantics, and streams that peer's transactions in wire order. Only `END_RESPONSE` completes the RPC successfully, including an empty response. If the peer disconnects or sends malformed data after partial output, the stream ends with an error and the client must start a new RPC to select another peer. Transactions must satisfy the Core wire-size, tick, amount, input-size, and per-tick count limits.
- `QueryContractFunction`
  Calls a read-only smart-contract function and returns its raw output bytes. An empty Core response is treated as invocation failure, while `TRY_AGAIN` lets another queried peer win the race.
- `BroadcastTransaction`
  Structurally validates and broadcasts raw transaction bytes to currently connected peers. A successful response contains the canonical lowercase 60-character Qubic transaction ID derived from the KangarooTwelve digest of the complete transaction.

Protocol file: `proto/lightnode.proto`

The gRPC server also enables reflection, so tools like `grpcurl` can inspect the service without a separate generated client.

At most 64 small peer-backed gRPC calls, 8 tick transaction streams, and 32 transaction broadcasts run at once. Balance and contract queries race at most three existing persistent peer sessions; a tick transaction stream uses exactly one. No temporary query connections are opened. The API also applies global and per-client token buckets to broadcasts and coalesces concurrent submissions of the same transaction into one network fanout. Sequential duplicate broadcasts are idempotent while their digest remains in the deduplication window.

### Migration from 0.1.x

Version `0.2.0` intentionally removes the unary `GetTickTransactions` RPC. Regenerate clients from `proto/lightnode.proto` and call the server-streaming `StreamTickTransactions` method instead. For example:

```bash
grpcurl -plaintext -d '{"tick":123456}' 127.0.0.1:50051 lightnode.LightNode/StreamTickTransactions
```

The stream can yield transactions before its final status is known. `END_RESPONSE` is the only successful completion marker; after a partial-result error, discard or retain those partial results according to your application policy and issue a new RPC if you want to retry with another peer.

Tick-stream terminal failures are delivered after any already-buffered partial transactions. Deadlines use `DEADLINE_EXCEEDED`, missing or disconnected peers use `UNAVAILABLE`, local outbound pressure uses `RESOURCE_EXHAUSTED`, malformed peer responses use `DATA_LOSS`, and internal task failures use `INTERNAL`.

`QueryContractFunction` accepts a contract index, a function input type, and up to 65535 raw input bytes, matching the Core `u16` input-size field. Contract-specific encoding and output decoding remain the caller's responsibility.

Tick status is based on FourQ-verified quorum votes. Other peer-backed responses do not yet provide cryptographic trust: the node does not verify the `RespondEntity` Merkle proof or transaction membership in signed `TickData`. Balance and contract queries race up to three peers and accept the first response that passes strict structural and request-binding validation.

`BroadcastTransaction.ok=true` means that the transaction passed the locally available subset of Core's `Transaction::checkValidity()` and was queued to at least one connected peer. It does not mean that a Core node accepted the signature or that the network confirmed the transaction.

## Wallet Format For GetBalance

`GetBalance` accepts either:

- a Qubic identity of 60 letters `A-Z`; lowercase input is normalized automatically and the four-character K12 checksum is validated
- a public key in hex form: either `0x` + 64 hex characters or plain 64 hex characters

## Troubleshooting

- Startup fails with `Address already in use`:
  another process is already using either the peer listener port from `--port` or the gRPC address from `--grpc-listen`. By default those are `0.0.0.0:21841` and `127.0.0.1:50051`.
- You want to run another instance without stopping the first one:
  start it with a different `--port`, and usually a different `--grpc-listen`, for example `--port 21842 --grpc-listen 127.0.0.1:50052`.
- The node says there are no peers:
  add one or more `--peer` values, or keep DNS bootstrap enabled.
- DNS bootstrap fails:
  the node can still run, but it may not find peers until you add `--peer` values or receive inbound connections.
- The pool reports many peers but makes no immediate dial attempts:
  inspect the `Peer pool` log. `cooldown` addresses are temporarily suppressed after failures, while `dialable` shows addresses eligible for a new outbound attempt now.
- Peers repeatedly disconnect with `write timed out`:
  those peers accepted a connection but did not consume outbound traffic within `--peer-write-timeout-ms`. The reconnect loop replaces them automatically; increase the timeout only when slow links are expected.
- `GetStatus` says there is no tick data yet:
  wait until the node receives traffic from the network.
- Balance or tick transaction requests fail:
  the node may not have enough working peers yet, or the peer-backed API query timed out. Wait a bit, add manual peers, or increase `--api-timeout-ms`.
- Broadcasting a transaction fails:
  there may be no connected peers, or every available peer queue may currently be full or closed. Full peer queues are disconnected automatically, so an outbound replacement can be established before the client retries.
- You want less console noise:
  do not use `--traffic-log`.
- You want the API reachable from another machine:
  change `--grpc-listen`, for example to `0.0.0.0:50051`, and protect that port with firewall or other network rules.

## Notes For Advanced Users

- Peer connections use TCP.
- The peer listener is IPv4-based.
- The node exchanges peer lists through the Qubic handshake and keeps a bounded in-memory peer cache.
- A peer is registered only after completing the Qubic peer-exchange handshake. Gossip discoveries are tagged with their source and capped per source; DNS peers retain provenance but, unlike configured manual peers, are still evictable from the overall LRU pool while inactive.
- Duplicate frames are filtered in memory with a rolling deduplication window.
- Deduplication reservations are committed only after at least one peer accepts a frame, so failed relay and local submissions can be retried.
- Relay and locally submitted transactions share the same FIFO peer queues; neither traffic class has priority.
- Pending response routing is bounded by per-frame size and by protocol-specific total frame/byte limits. A valid tick response can contain 4096 maximum-size transaction frames plus `END_RESPONSE`; exceeding that limit is a peer protocol violation, while a closed or locally saturated receiver does not disconnect the peer.
- Small peer-backed API requests race several peers in parallel and return the first successful answer; tick transactions use one peer and bounded end-to-end streaming.
