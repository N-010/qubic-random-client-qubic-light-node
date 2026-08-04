# ADR-0003: Remove legacy GetBalance RPC

- Status: Accepted
- Date: 2026-08-04
- Decision owners: QubicLightNode and RandomClient owner

## Context

The production RandomClient `NetworkBackend` no longer has a balance operation
and its current protobuf schema contains only `GetStatus`,
`GetTickTransactions`, `QueryContractFunction`, and `BroadcastTransaction`.
QubicLightNode retained `GetBalance` only for legacy compatibility. Its proof
path depended on spectrum roots collected from the removed verified tick-vote
state and had no current production consumer.

The reviewed repository revisions and worktree states are the same as those
recorded in ADR-0002.

## Decision

Remove `GetBalance` from `proto/lightnode.proto`, the tonic service
implementation, peer request/response routing, proof verification state, and
tests. The public QubicLightNode service now contains exactly the four methods
used by current RandomClient production code.

## Consequences

This is a breaking schema change for any unrecorded legacy consumer. Current
RandomClient is unaffected because its schema and backend do not declare or
call the RPC. Removing the unused path also allows tick status to be separated
from spectrum-root collection without retaining dead verification state.

Restoring balance lookup requires a new demonstrated production call path, a
current Core parity review, and a new architectural decision.
