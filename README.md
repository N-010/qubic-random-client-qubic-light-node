# QubicLightNode

QubicLightNode is a lightweight Qubic relay node with a local gRPC API.

It connects to public Qubic peers over TCP, relays network frames, tracks the latest tick, and exposes read-only methods for status, wallet balance, and tick transactions.

## Features

- P2P relay over TCP (default port `21841`)
- Automatic outbound connection management
- Peer discovery from:
  - manually provided peers (`--peer`)
  - DNS bootstrap via `api.qubic.global` (enabled by default)
  - peer exchange frames (`EXCHANGE_PUBLIC_PEERS`)
- Frame deduplication by BLAKE3 digest with a sliding window (`--max-seen`)
- Local gRPC API (default `127.0.0.1:50051`) with reflection enabled

## Build

```bash
cargo build --release
```

Binary path:

- Windows: `target\\release\\QubicLightNode.exe`
- Linux/macOS: `target/release/QubicLightNode`

## Quick Start

Run with defaults:

```bash
cargo run --release
```

Run with explicit peers and verbose traffic logs:

```bash
cargo run --release -- \
  --peer 1.2.3.4:21841 \
  --peer 5.6.7.8 \
  --traffic-log
```

## Command-Line Parameters

### Overview

```text
QubicLightNode [options]
```

All available options:

| Option | Type |           Default | What it does | When to use |
|---|---|------------------:|---|---|
| `--peer <ip[:port]>` | repeatable |              none | Adds a manual peer. If port is omitted, uses `--port`. | Use when you have trusted/stable peers, want faster startup, or run with `--no-dns-bootstrap`. |
| `--port <u16>` | integer |           `21841` | Sets both P2P listening port and default port for peers provided without `:port`. | Change when running multiple nodes on one host, or when firewall/NAT requires a different port. |
| `--listen-ip <ipv4>` | IPv4 |         `0.0.0.0` | Sets the P2P bind IP address. | Use `127.0.0.1` for local-only testing; use a specific interface on multi-NIC hosts. |
| `--target-outbound <n>` | integer |               `8` | Desired number of outbound peer connections maintained by the reconnect loop. | Increase for better resilience/availability; decrease on low-resource or restricted environments. |
| `--max-incoming <n>` | integer |              `32` | Maximum simultaneous inbound connections. New inbound sessions are rejected after the limit. | Decrease to protect CPU/RAM or home network uplink; increase for public relay servers. |
| `--max-seen <n>` | integer |           `65536` | Size of dedup window for recently seen frame hashes (BLAKE3). | Increase under heavy traffic to reduce duplicate relays; decrease to reduce memory usage. |
| `--max-known-peers <n>` | integer |             `500` | Maximum number of discovered peers stored in memory. Oldest entries are evicted when the limit is reached. | Decrease to reduce long-run RAM growth on noisy public networks; increase if you need a larger dialing pool. |
| `--reconnect-ms <n>` | milliseconds |            `2000` | Interval between outbound reconnect rounds. | Lower for faster recovery after disconnects; higher to reduce dial pressure/noise. |
| `--relay-all` | flag |               off | Relays frames regardless of `dejavu`. Without this flag, only `dejavu == 0` frames are relayed. | Enable if you want full relay behavior. Keep off for a lighter/default relay profile. |
| `--no-dns-bootstrap` | flag |               off | Disables bootstrap request to `api.qubic.global`. | Use in isolated/private environments or if external bootstrap must be avoided. |
| `--dns-lite-peers <n>` | integer |              auto | Requested `litePeers` count in DNS bootstrap API call. Auto mode uses `max(target_outbound * 3, 8)`. | Increase when initial peer pool is often too small; keep auto for normal operation. |
| `--dns-timeout-ms <n>` | milliseconds |            `5000` | Timeout for DNS bootstrap HTTP request. | Increase on slow/unreliable internet; decrease for faster failover to manual peers. |
| `--traffic-log` | flag |               off | Prints detailed RX/TX/relay/dedup events for frames. | Enable for debugging/troubleshooting; disable in production for lower log volume. |
| `--api-timeout-ms <n>` | milliseconds |            `6000` | Timeout for outbound peer queries used by gRPC methods (`GetBalance`, `GetTickTransactions`). | Increase when peers are slow; decrease for more responsive API failures. |
| `--grpc-listen <ip:port>` | socket address | `127.0.0.1:50051` | gRPC bind address. | Use `0.0.0.0:50051` if API must be reachable from other machines (secure externally). |
| `--no-grpc` | flag |               off | Disables gRPC server startup. | Use when you only need relay functionality and no local API. |
| `-h`, `--help` | flag |               n/a | Prints command usage and exits. | Use to verify supported options quickly. |

### Built-in Safety Floors

Some timeouts/intervals are clamped to minimum values even if smaller numbers are passed:

- `--api-timeout-ms`: minimum `1000 ms`
- `--reconnect-ms`: minimum `200 ms`
- `--dns-timeout-ms`: minimum `500 ms`
- `--max-seen`: minimum effective value is `1000`
- `--max-known-peers`: minimum effective value is `1000`

## Practical Configuration Profiles

### 1) Default public relay + local API

```bash
cargo run --release
```

Use when you want a simple setup with DNS bootstrap and local gRPC access.

### 2) Stable peers, no external bootstrap

```bash
cargo run --release -- \
  --peer 203.0.113.10:21841 \
  --peer 198.51.100.55:21841 \
  --no-dns-bootstrap
```

Use when operating in controlled infrastructure and you already know good peers.

### 3) High-observability debug mode

```bash
cargo run --release -- \
  --traffic-log \
  --api-timeout-ms 12000 \
  --reconnect-ms 1000
```

Use when investigating dropped sessions, frame flow, or slow peer responses.

### 4) Resource-constrained node

```bash
cargo run --release -- \
  --target-outbound 4 \
  --max-incoming 8 \
  --max-seen 20000
```

Use on low-memory or low-bandwidth hosts.

## gRPC API

gRPC service: `lightnode.LightNode`

Methods:

- `GetStatus`
- `GetBalance`
- `GetTickTransactions`

Protocol definition: `proto/lightnode.proto`

### Notes

- gRPC reflection is enabled, so tools like `grpcurl` can discover schema without manually passing `.proto` files.
- `GetBalance` accepts either:
  - Qubic identity: 60 uppercase letters (`A-Z`)
  - public key hex: `0x` + 64 hex chars (or 64 hex chars without prefix)
- API data is fetched from peers with parallel race queries (up to 3 peers in parallel), returning the first successful response.

## Network Protocol Notes

Core Qubic frame format used by this node:

- `size`: 3 bytes (little-endian)
- `type`: 1 byte
- `dejavu`: 4 bytes (little-endian)
- `payload`: variable length

Important message types used by this node:

- `0` `EXCHANGE_PUBLIC_PEERS`
- `27` `REQUEST_CURRENT_TICK_INFO`
- `28` `RESPOND_CURRENT_TICK_INFO`
- `29` `REQUEST_TICK_TRANSACTIONS`
- `31` `REQUEST_ENTITY`
- `32` `RESPOND_ENTITY`
- `24` `BROADCAST_TRANSACTION`
- `35` `END_RESPONSE`

## Operational Notes

- This node stores peer/discovery/dedup state in memory.
- Logs include connection lifecycle and optional frame-level events (`--traffic-log`).
- By default, gRPC listens on loopback only (`127.0.0.1`). If exposed externally, place it behind proper network security controls.

## Troubleshooting

- "No peers configured" on startup:
  - Provide `--peer` values, or keep DNS bootstrap enabled.
- Frequent dial failures:
  - Check firewall/NAT, adjust `--reconnect-ms`, and verify peer reachability.
- gRPC API returns slow/failure responses:
  - Increase `--api-timeout-ms`, ensure node has enough connected peers (`--target-outbound`).
- Too many logs:
  - Disable `--traffic-log`.
