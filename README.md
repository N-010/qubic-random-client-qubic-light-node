# QubicLightNode

`QubicLightNode` is a small Qubic relay node with a local `gRPC` API.

It currently does four things:

- connects to the public Qubic network over TCP
- relays Qubic frames between peers
- keeps the latest known tick in local memory
- exposes a local `gRPC` API for status, balance, tick transactions, and transaction broadcast

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
5. updates the in-memory latest tick cache from network traffic

Important notes:

- right after start, `GetStatus` can return no local tick data yet; this is normal until the node receives network frames
- the process can still start even if DNS bootstrap fails; in that case you can wait for incoming peers or pass `--peer` manually
- by default, relay fanout is limited to frames with `dejavu == 0`; use `--relay-all` to also relay frames with non-zero `dejavu`

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
  Desired number of outbound peer connections to keep. Default: `8`.
- `--max-incoming <n>`
  Maximum number of incoming peer sessions. Default: `32`.
- `--max-known-peers <n>`
  Maximum number of discovered peers kept in memory. Default: `500`. Values below `1000` are currently clamped to `1000` internally.
- `--reconnect-ms <ms>`
  Delay between outbound reconnect attempts. Default: `2000`. Values below `200` are currently clamped to `200` internally.

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

### API

- `--grpc-listen <ip:port>`
  Bind address for the gRPC server. Default: `127.0.0.1:50051`.
- `--no-grpc`
  Disable the gRPC server.
- `--api-timeout-ms <ms>`
  Timeout used by peer-backed API queries such as balance and tick transactions. Default: `6000`. Values below `1000` are currently clamped to `1000` internally.

To see the parser-generated help text:

```bash
cargo run --release -- --help
```

## gRPC API

Service name: `lightnode.LightNode`

Methods:

- `GetStatus`
  Returns the latest tick known to the node's local cache.
- `GetBalance`
  Queries peers for wallet balance data and returns the first successful response.
- `GetTickTransactions`
  Queries peers for transactions from the requested tick and returns the first successful response.
- `BroadcastTransaction`
  Broadcasts raw transaction bytes to currently connected peers.

Protocol file: `proto/lightnode.proto`

The gRPC server also enables reflection, so tools like `grpcurl` can inspect the service without a separate generated client.

## Wallet Format For GetBalance

`GetBalance` accepts either:

- a Qubic identity of 60 letters `A-Z`; lowercase input is normalized automatically
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
- `GetStatus` says there is no tick data yet:
  wait until the node receives traffic from the network.
- Balance or tick transaction requests fail:
  the node may not have enough working peers yet, or the peer-backed API query timed out. Wait a bit, add manual peers, or increase `--api-timeout-ms`.
- Broadcasting a transaction fails:
  there may be no connected peers, or all peer outbound queues may currently be full or closed.
- You want less console noise:
  do not use `--traffic-log`.
- You want the API reachable from another machine:
  change `--grpc-listen`, for example to `0.0.0.0:50051`, and protect that port with firewall or other network rules.

## Notes For Advanced Users

- Peer connections use TCP.
- The peer listener is IPv4-based.
- The node exchanges peer lists through the Qubic handshake and keeps a bounded in-memory peer cache.
- Duplicate frames are filtered in memory with a rolling deduplication window.
- Peer-backed API requests race several peers in parallel and return the first successful answer.
