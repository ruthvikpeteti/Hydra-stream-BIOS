# ADR-0005: Defence-in-depth policy enforcement, no override path

Status: Accepted

## Context
Residency is a legal property. One enforcement point is one bug away from a violation.

## Decision
INV-1 and INV-2 are enforced separately at the control plane (validation and placement), the regional agent (replica topic creation), the source replicator, the destination sink, the gateway (processing region), and the network egress policy. Bundles are signed and short-lived. Missing, expired or unverifiable bundles stop the affected action. No operator action, flag or break-glass scope can waive policy (INV-7).

## Consequences
- Some legitimate operations (adding a region) need a registry change, not a flag, and are slower.
- Stale bundles reduce availability of replication, by design.
- Enforcement points must be tested individually in later phases (AS-1).

## Alternatives
- Single central enforcement: rejected (single point of failure for residency).
- Allow break-glass policy override: rejected (cannot be made safe in a legal context).
