# ADR-0008: Two-form event envelope and identical topic names across regions

Status: Accepted

## Context
Kafka assigns offsets after append, so a producer cannot embed its own record's offset. MM2-style prefixing of topic names (for example `A.orders`) forces consumers to change topics on failover.

## Decision
- Ingest form: the value holds `event_id`, `topic`, `partition_key`, `payload`, `origin_region`, `leadership_epoch`, `replication_generation`, `event_timestamp`, `envelope_version`, `policy_revision`. `source_cluster`, `source_partition` and `source_offset` are the record's own coordinates.
- Replicated form: value bytes unchanged. The replicator adds `source_*` as headers (`ss-src-*`) and appends to `ss-hops`.
- The topic has the same name in every region holding it. Writability depends on gateway state and ACLs.

## Consequences
- Consumers keep the same topic name after failover. Offsets still differ per region and need translation.
- Value-level integrity (hashes, signatures) survives replication.
- Replica topics are writable only by sinks, enforced by ACLs rather than naming.
- A topic must never exist in a region outside its allowed set, so naming cannot hide a violation.

## Alternatives
- Prefixed names (MM2): rejected (client-visible failover changes).
- Rewrite value on replication to embed source coordinates: rejected (breaks value integrity).
