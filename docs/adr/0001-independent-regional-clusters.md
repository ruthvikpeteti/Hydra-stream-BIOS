# ADR-0001: Independent regional Kafka clusters, no stretched KRaft

Status: Accepted

## Context
Regions A, B, C are separate failure and legal domains. A single Kafka cluster or KRaft quorum spanning regions would make cross-region latency part of every produce and metadata operation, and would put controller state, and therefore leadership decisions, outside the policy layer.

## Decision
Each region runs its own Kafka cluster with its own KRaft controller quorum (MVP: A has 3 combined nodes, B and C have 1 each; production: ≥3 brokers and 3 dedicated controllers per region). No cluster, controller quorum or consumer group spans regions. Cross-region data movement happens only in the replication plane.

## Consequences
- A regional failure cannot corrupt another region's cluster metadata.
- Data movement is explicit, so policy is enforceable at a single seam (ADR-0005).
- Replication is asynchronous at the Kafka level. Synchronous behaviour is provided by the gateway's `SYNC_REGIONAL` mode, not by Kafka.
- Consumer offsets are per region and need translation (`_ss.offset_map`).
- MVP regions B and C have no local fault tolerance.

## Alternatives
- Stretched cluster with rack awareness: rejected (latency, shared fate, no policy seam).
- Active/active multi-master: rejected (conflicts with INV-3 single writer).
