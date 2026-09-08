# Changelog

All notable changes to this project will be documented in this file.

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
