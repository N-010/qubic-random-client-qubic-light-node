# QubicLightNode

`QubicLightNode` is a small Qubic node that connects to the public network and gives you a simple local API.

In plain words, it can:

- connect to other Qubic nodes
- keep track of the latest network tick
- show a wallet balance
- show transactions from a chosen tick
- send a prepared transaction to the network

This project currently has a `gRPC` API only. There is no web page and no HTTP REST API in the current code.

## What You Need To Know

- Default Qubic peer port: `21841`
- Default local gRPC address: `127.0.0.1:50051`
- If you do not add peers manually, the node tries to find peers automatically through `api.qubic.global`

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

If automatic peer search is not enough, add peers manually:

```bash
cargo run --release -- --peer 1.2.3.4:21841 --peer 5.6.7.8:21841
```

If you only want the node itself and do not need the local API:

```bash
cargo run --release -- --no-grpc
```

## What The Program Does After Start

After launch, the node:

1. opens a local TCP port for Qubic peers
2. tries to connect to other peers
3. remembers the latest network tick it sees
4. starts the local gRPC API unless you used `--no-grpc`

Important: right after start, `GetStatus` can temporarily return "no tick data yet". This is normal until the node receives network traffic.

## Main Options

You do not need most settings at first. Usually these are enough:

- `--peer <ip[:port]>`
  Add a known peer manually. You can repeat this option more than once.
- `--traffic-log`
  Show detailed network activity in the console. Useful for debugging.
- `--no-dns-bootstrap`
  Disable automatic peer search through `api.qubic.global`.
- `--grpc-listen <ip:port>`
  Change where the local gRPC API listens.
- `--no-grpc`
  Turn off the local gRPC API.

To see the full list of settings:

```bash
cargo run --release -- --help
```

## What You Can Ask Through The API

The current gRPC service is `lightnode.LightNode`.

Methods:

- `GetStatus`
  Shows the latest tick known to this node.
- `GetBalance`
  Shows wallet balance information.
- `GetTickTransactions`
  Shows transactions for one tick.
- `BroadcastTransaction`
  Sends raw transaction bytes to connected peers.

Protocol file: `proto/lightnode.proto`

## Wallet Format For GetBalance

`GetBalance` accepts either:

- a Qubic identity of 60 letters
- a public key in hex form: `0x` + 64 hex characters

## Simple Troubleshooting

- The node says there are no peers:
  add one or more `--peer` values, or do not disable DNS bootstrap.
- Balance or tick transaction requests fail:
  the node may not have enough working peers yet. Wait a bit or add manual peers.
- You want less console noise:
  do not use `--traffic-log`.
- You want the API reachable from another machine:
  change `--grpc-listen`, for example to `0.0.0.0:50051`, and protect that port with firewall or other network rules.

## Notes For Advanced Users

- Peer connections use TCP.
- The node relays Qubic frames between peers.
- Duplicate frames are filtered in memory.
- The gRPC server has reflection enabled.
- Peer-backed API requests use parallel queries and return the first successful answer.
