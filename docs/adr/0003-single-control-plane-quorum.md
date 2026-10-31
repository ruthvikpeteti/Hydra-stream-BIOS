# ADR-0003: Single global control-plane quorum as the only authority

Status: Accepted

## Context
Split-brain needs two entities each believing they decide. The design needs one linearizable source of truth for active region, epoch, policy and registry, which tolerates the loss of one region.

## Decision
One consensus group (Raft or equivalent, linearizable, quorum-committed). MVP: 3 members, one per region. Production: 5 members, placed so that loss of any single region keeps quorum (for example A×2, B×2, C×1). Only records committed in its log are authoritative (INV-8). Phase 1 chooses the implementation (for example etcd or an embedded Raft library) subject to: linearizable writes, membership changes through the log, and a monotonic term. No second quorum exists except through audited break-glass reconfiguration (SB-5), which also bumps `cluster_incarnation`.

## Consequences
- Loss of two of three regions (MVP and production) halts epoch grants. Automatic promotion is impossible. Break-glass is the manual path (FAILURE_MATRIX F5).
- Cross-region latency affects control operations only (lease renewal, promotion), not the produce hot path, except via lease validity.
- Escaping that limit requires an external witness member outside A/B/C. Left as a production option.

## Alternatives
- Per-region leader election with gossip: rejected (cannot prevent two leaders under partition).
- Kafka's own controller as authority: rejected (it is intra-cluster by ADR-0001).
