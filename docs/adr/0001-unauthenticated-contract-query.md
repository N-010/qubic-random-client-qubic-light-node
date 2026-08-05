# ADR-0001: Retain first-success public-peer contract queries

## Status

Accepted

## Date

2026-08-02

## Context

RandomClient requires `QueryContractFunction` to obtain raw output bytes from
a read-only Qubic contract invocation. Qubic Core provides the request and
response wire behavior, but the returned `RESPOND_CONTRACT_FUNCTION` payload
does not carry a proof that QubicLightNode can authenticate locally.

QubicLightNode currently sends the request over up to three established public
peer sessions and accepts the first structurally valid, non-empty response.
The architecture compliance audit classifies this as a HIGH integrity risk:
one malicious or stale peer can win the race and control the result.

The decision was made against QubicLightNode worktree base
`99d17ddc008e05d094a16a09c93f7f779d012116`, RandomClient revision
`33d217eab16da287b6b9ce5389be009227492abf`, and the unversioned
`QThirtyFour/core` parent worktree inspected on 2026-08-02.

## Decision drivers

- Preserve the current RandomClient behavior requested by the owner.
- Avoid inventing a consensus algorithm that does not exist in Core.
- Keep the risk visible rather than implying cryptographic authentication.
- Make later replacement possible without changing the other three RPCs.

## Considered options

### Retain first-success

Small, low-latency, and compatible with the current caller, but trusts one
public peer and offers no integrity guarantee.

### Require matching responses from multiple peers

Reduces single-peer influence, but would be a locally invented quorum policy
with unresolved handling for dynamic or time-dependent contract output.

### Disable the RPC

Removes the integrity risk, but breaks a required RandomClient operation.

## Decision

Retain the existing first-success query over at most three established public
peers. Do not describe the output as verified, authenticated, quorum-backed,
or consensus state.

The service must print a startup warning and user-facing documentation must
identify the trust boundary. Structural validation, request correlation,
bounded response sizes, timeouts, and peer-failure handling remain mandatory.

## Consequences

### Positive

- RandomClient retains its required contract query.
- No non-Core consensus semantics are introduced.
- The decision is isolated and reversible.

### Negative

- A malicious, compromised, stale, or forked peer can return attacker-chosen
  bytes if it responds first.
- Racing three peers improves availability, not authenticity.
- Callers must not use the result as authenticated financial or consensus
  state without an independent validation mechanism.

### Risk acceptance

Severity: **HIGH**. The repository owner explicitly accepted this risk by
choosing to leave the current behavior unchanged. This acceptance applies only
to `QueryContractFunction`; it does not relax verification for status,
balances, or transaction signatures.

## Revisit criteria

Supersede this ADR if any of the following occurs:

- Core adds an authenticated contract-response mechanism;
- RandomClient uses contract output for an irreversible or security-sensitive
  decision;
- a trustworthy local execution path becomes available;
- an incident demonstrates malicious or inconsistent public-peer output;
- the owner elects to disable the RPC or approves a documented non-Core policy.

## Related records

- `docs/ARCHITECTURE_COMPLIANCE_AUDIT.md`, finding FIND-01.
- `README.md`, the strict four-operation product boundary.
