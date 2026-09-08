# Contract query trust decision

Status: accepted; freshness amendment approved on 2026-09-08.

The authoritative decision, constraints and revisit criteria are in
[QueryContractFunction](../ARCHITECTURE.md#querycontractfunction).
The first eligible peer response remains unauthenticated. A same-session
current-tick preflight excludes a different epoch or more than one tick of lag
against the query snapshot. This is an availability filter, not consensus.
