# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

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
