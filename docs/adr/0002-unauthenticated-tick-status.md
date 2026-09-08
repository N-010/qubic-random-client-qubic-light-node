# Tick status trust decision

Status: accepted, unchanged by the 2026-09-08 repair.

The authoritative decision, constraints and revisit criteria are in
[Structural epoch/tick cache](../ARCHITECTURE.md#structural-epochtick-cache).
The greatest structurally valid epoch/tick is a single-peer observation without
signature or quorum authentication. Freshness preflight does not authenticate
this reference or protect against a false-future observation.
