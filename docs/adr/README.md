# Architecture Decision Records

Format: Context, Decision, Consequences, Alternatives. Status values: Proposed, Accepted, Superseded. All records below are Accepted as part of Phase 0. Changing one requires a new ADR that supersedes it.

| ADR | Title |
|---|---|
| [0001](0001-independent-regional-clusters.md) | Independent regional Kafka clusters, no stretched KRaft |
| [0002](0002-custom-replication-plane.md) | Custom policy-enforcing replication plane instead of stock MirrorMaker 2 |
| [0003](0003-single-control-plane-quorum.md) | Single global control-plane quorum as the only authority |
| [0004](0004-lease-epoch-fencing-fail-closed.md) | Lease-based epoch fencing, fail closed, with quiet periods |
| [0005](0005-defense-in-depth-policy-enforcement.md) | Defence-in-depth policy enforcement, no override path |
| [0006](0006-gateway-sole-entry-point.md) | Application gateway as the sole client entry point |
| [0007](0007-promotion-eligibility-and-break-glass.md) | Promotion eligibility from committed attestations, break-glass for lag only |
| [0008](0008-envelope-and-topic-naming.md) | Two-form event envelope and identical topic names across regions |
