# ADR-0002: Custom policy-enforcing replication plane instead of stock MirrorMaker 2

Status: Accepted

## Context
Replication must (a) verify INV-1 and INV-2 before each stream, (b) fence records by epoch at the sink, (c) stamp `source_*` coordinates, (d) emit signed attestations used for promotion, (e) refuse unapproved paths. MirrorMaker 2 replicates by config and topic patterns, and offers none of these hooks.

## Decision
Build replicator services (source reader + transactional sink writer) on Kafka client libraries. They pull the signed bundle, enforce policy, dedup by `event_id`, commit source offsets in the sink transaction, write `_ss.offset_map`, and send attestations. MM2 or Connect can be reconsidered later only if it can satisfy all five requirements, and that would need a new ADR.

## Consequences
- More code to own and test. Exactly-once semantics and offset translation are our responsibility.
- Enforcement logic is in one place that we control and can fail closed.
- The replicator has no destination-add endpoint (API_SPEC §5), so streams come only from the bundle.

## Alternatives
- Stock MM2 plus a policy sidecar: rejected (cannot intercept per-record epoch or path decisions).
- Cluster Linking (vendor feature): rejected (vendor lock-in, no policy hooks).
