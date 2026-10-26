# Changelog

All notable changes to this project will be documented in this file.

## v2.1.0 - 2026-10-26

### Added

- Return all authenticated non-zero TickData transaction digests in Core slot
  order, with a completeness marker for client-side inclusion checks.
- Evaluate peer RTT and tick freshness in a bounded background task, qualifying
  a replacement before retiring an idle connection.
- Add configurable broadcast fanout from 1 to 32 peers (default six).
- Add transaction-only diagnostics with queue/write timings and public
  transaction IDs, without logging signed payloads or reveal material.

### Fixed

- Rank replacement candidates only against peers eligible for retirement;
  synchronize retirement with queue admission so rotation cannot discard
  a concurrently accepted transaction.
- Expire stale peer-quality observations and clean up sessions and pending
  requests after connection-worker failure.
- Bound protocol-penalty handling by query deadlines and correctly handle
  late responses after control-request cancellation.
- Convert Core current-tick elapsed seconds to milliseconds without truncation
  and align the minimum frame limit with the complete TickData message.

### Security

- Keep blocking signature-verification work bounded even after query cancellation.
- Copy pending response payloads into compact buffers to avoid retaining larger
  receive allocations; cap HTTPS peer-bootstrap responses at 1 MiB.
- Redact payloads from queue and frame debug output and update dependencies.

### Changed

- Refresh discovery candidates periodically when DNS bootstrap is enabled and
  apply the configured peer-count policy consistently.
- Gate release publication on successful Linux and Windows builds, matching
  package/tag versions, binary version/help checks, and SHA256 verification.
- Publish SHA256SUMS; add manual CI, weekly dependency checks, job timeouts,
  and write permission restricted to the publication job.
- Expand regression coverage and document Core compatibility, trust boundaries,
  resource limits, and queue-acceptance semantics. Queue acceptance and completed
  TCP writes remain distinct from network inclusion or contract execution.

## v2.0.2 - 2026-09-08

### Security

- Update h2 to 0.4.16 to fix RUSTSEC-2026-0258.
- Replace the yanked chacha20 0.10.1 with 0.10.2.

## v2.0.1 - 2026-09-08

### Fixed

- Refresh authenticated computor keys across epoch changes on existing
  peer sessions, with bounded retry delays.
- Check each peer's current epoch/tick on the same session before contract
  queries, sharing one deadline and rejecting stale observations.
- Align the crate version with the release tag and restore trust-decision
  documentation links.

## v2.0.0

### Added

- Added the minimal RandomClient gRPC boundary for status, authenticated tick
  transaction presence, contract queries, and signed transaction broadcast.
- Added local FourQ verification for computor data, tick data, and submitted
  transactions.

### Changed

- Hardened peer framing, bounded queues, request correlation, timeouts, and
  protocol-violation handling.
- Removed the legacy balance RPC from the RandomClient-facing API.

### Security

- Added dependency advisory, license, and source-policy checks for release
  preparation.
