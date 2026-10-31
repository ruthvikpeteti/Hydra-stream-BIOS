# SovereignStream — Data Model

Defines the control-plane records, the topic policy, the event envelope, and the epoch and lag semantics. Invariants (INV-n) are defined in [ARCHITECTURE.md §3](ARCHITECTURE.md).

## 1. Region and link registry

Maintained by the control plane. Policies are validated against it.

```yaml
regions:
  - id: region-a
    cluster_id: kafka-a
    legal_zones: [EU, DE, GLOBAL]
    capacity: { max_local_rf: 3 }       # MVP: A=3, B=1, C=1
  - id: region-b
    cluster_id: kafka-b
    legal_zones: [EU, FR, GLOBAL]
    capacity: { max_local_rf: 1 }
  - id: region-c
    cluster_id: kafka-c
    legal_zones: [US, GLOBAL]
    capacity: { max_local_rf: 1 }
links:                                   # directed; one record per direction
  - { from: region-a, to: region-b, transit_zones: [EU] }
  - { from: region-a, to: region-c, transit_zones: [EU, ATLANTIC, US] }
  - { from: region-b, to: region-a, transit_zones: [EU] }
  - { from: region-b, to: region-c, transit_zones: [EU, ATLANTIC, US] }
  - { from: region-c, to: region-a, transit_zones: [US, ATLANTIC, EU] }
  - { from: region-c, to: region-b, transit_zones: [US, ATLANTIC, EU] }
```

A link is *approved for a topic* iff `link.transit_zones ⊆ topic.allowed_transit_zones` (INV-2). A path through an intermediate region R (for example A→B→C) is approved only if both links are approved and R ∈ `allowed_processing_regions`. A link that is missing from the registry is not approved.

## 2. Topic policy

### 2.1 Fields

| Field | Type | Meaning |
|---|---|---|
| `classification` | enum `PUBLIC`, `INTERNAL`, `CONFIDENTIAL`, `RESTRICTED` | Sensitivity class. Selects the objectives scope, default `lag_breach_action`, and audit level. |
| `legal_zone` | string | Jurisdiction label the topic is bound to (for example `EU`). A region is eligible only if it appears in the region's `legal_zones`. |
| `allowed_storage_regions` | list of region ids | Regions where a replica (including the active copy) may be stored. |
| `allowed_processing_regions` | list of region ids | Regions where gateways may produce or consume the topic, and where a promoted active region may run. |
| `allowed_transit_zones` | list of zone labels | Zones the replication traffic may traverse. |
| `local_replication_factor` | int ≥ 1 | Kafka RF inside each region holding the topic. |
| `regional_replica_count` | int ≥ 0 | Number of *remote* regions holding a replica, in addition to the home (active) region. |
| `max_tolerated_replication_lag_ms` | int ≥ 0, or null | Maximum lag a replica may have and still be eligible for automatic promotion, and the lag-breach threshold. Null only for `LOCAL_DURABLE`. |
| `failover_priority` | ordered list of region ids | Index 0 is the preferred active region. Entries 1..`regional_replica_count` are the replica regions in promotion order. |
| `durability_mode` | enum `LOCAL_DURABLE`, `ASYNC_REGIONAL`, `SYNC_REGIONAL` | Acknowledgement semantics (§2.2). |

Supplementary fields (needed to make the above enforceable; all have defaults):

| Field | Default | Meaning |
|---|---|---|
| `local_min_insync_replicas` | `floor(local_replication_factor/2)+1` | Kafka `min.insync.replicas` in each region. |
| `sync_remote_acks` | 1 (only valid with `SYNC_REGIONAL`) | Replica regions that must confirm before ack. Must be ≤ `regional_replica_count`. |
| `lag_breach_action` | `REJECT_WRITES` for `CONFIDENTIAL`/`RESTRICTED`; `CONTINUE_ALERT` otherwise | What the active gateway does when lag exceeds the maximum (`ASYNC_REGIONAL` only). |

System-managed (read-only to clients): `revision` (int, compare-and-set on update), `created_at`, `updated_at`, `policy_hash`.

### 2.2 Durability modes

| Mode | Ack when | Regional copies | Region-loss behaviour |
|---|---|---|---|
| `LOCAL_DURABLE` | Local `acks=all` with `local_min_insync_replicas` | None (`regional_replica_count = 0`) | Topic is unavailable until the home region returns. No failover. Data never leaves the region. |
| `ASYNC_REGIONAL` | Local ack, then asynchronous replication | ≥ 1 | Automatic promotion only if lag bound ≤ `max_tolerated_replication_lag_ms`. Loss bounded by attested lag. |
| `SYNC_REGIONAL` | Local ack plus `sync_remote_acks` remote durable acks | ≥ `sync_remote_acks` | Acked writes survive region loss. Writes are rejected when remote acks cannot be obtained. |

### 2.3 Validation rules

A policy is rejected at write time (`POLICY_INVALID`) unless all hold:

| Rule | Condition |
|---|---|
| V1 | Every region id exists in the registry. |
| V2 | Every region in `allowed_storage_regions` and `allowed_processing_regions` lists `legal_zone` in its `legal_zones`. |
| V3 | `failover_priority` has no duplicates and `len = 1 + regional_replica_count`. |
| V4 | Every region in `failover_priority` is in `allowed_storage_regions ∩ allowed_processing_regions`. |
| V5 | `local_replication_factor ≤ capacity.max_local_rf` for every region in `failover_priority`. |
| V6 | For each replica region R (`failover_priority[1..]`), at least one approved path exists from the active region to R (INV-2), under the registry. |
| V7 | `LOCAL_DURABLE` ⇒ `regional_replica_count = 0` and `max_tolerated_replication_lag_ms = null`. Other modes ⇒ `regional_replica_count ≥ 1` and lag value set. |
| V8 | `sync_remote_acks` set only with `SYNC_REGIONAL`, and `1 ≤ sync_remote_acks ≤ regional_replica_count`. |
| V9 | `local_min_insync_replicas ≤ local_replication_factor`. |
| V10 | `classification` is one of the enum values. For `RESTRICTED`, `lag_breach_action = REJECT_WRITES`. |

V6 is checked on every promotion path too: after a promotion, the *new* active region must have approved paths to the remaining replica regions. If not, those replicas stay unreachable and the condition alarms (INV-2 is never relaxed to restore replication).

### 2.4 Example

```json
{
  "topic": "payments.eu.v1",
  "classification": "RESTRICTED",
  "legal_zone": "EU",
  "allowed_storage_regions": ["region-a", "region-b"],
  "allowed_processing_regions": ["region-a", "region-b"],
  "allowed_transit_zones": ["EU"],
  "local_replication_factor": 1,
  "regional_replica_count": 1,
  "max_tolerated_replication_lag_ms": 0,
  "failover_priority": ["region-a", "region-b"],
  "durability_mode": "SYNC_REGIONAL",
  "sync_remote_acks": 1,
  "lag_breach_action": "REJECT_WRITES"
}
```

Note `local_replication_factor: 1` because MVP region B has `max_local_rf = 1` (V5). In the production design this would be 3.

## 3. Event envelope

Events travel in two forms of one logical envelope. The distinction exists because Kafka assigns offsets only after an append, so a producer cannot embed the offset of its own record.

### 3.1 Fields

| Field | Type | Set by | Notes |
|---|---|---|---|
| `event_id` | UUIDv7 (string) | Gateway (or client-supplied idempotency key, validated as UUID) | Global identity and dedup key (INV-10). |
| `topic` | string | Gateway | Same topic name in every region. |
| `partition_key` | bytes (nullable) | Client | Determines partition. Null means gateway-chosen sticky partition. |
| `payload` | bytes | Client | Opaque. Encrypted under a zone key if classification ≥ `CONFIDENTIAL` (SECURITY §6). |
| `source_cluster` | string | Replicator at each hop; in the origin cluster, derived from record metadata | Cluster the record was read from at this hop. |
| `source_partition` | int | As above | Partition in `source_cluster`. |
| `source_offset` | int64 | As above | Offset in `source_cluster`. |
| `origin_region` | string | Gateway | Region where the gateway first accepted the event. Never changed by replicators. |
| `leadership_epoch` | uint64 | Gateway, from epoch token | Epoch under which the event was accepted. |
| `replication_generation` | uint64 | Gateway, from epoch token | Generation under which the event was accepted. |
| `event_timestamp` | int64 (epoch ms, UTC) | Gateway (client timestamp kept in header `ss-client-ts`) | Used for lag estimation, never for ordering or authority. |

Additional envelope metadata: `envelope_version` (int, for evolution) and `policy_revision` (int, the policy revision at accept time).

### 3.2 Two forms

- **Ingest form** (in the origin cluster). The Kafka record value carries `event_id`, `topic`, `partition_key`, `payload`, `origin_region`, `leadership_epoch`, `replication_generation`, `event_timestamp`, `envelope_version`, `policy_revision`. `source_cluster`, `source_partition` and `source_offset` are *not* in the value. They are the record's own cluster/partition/offset, exposed to readers as metadata.
- **Replicated form** (in a destination cluster). The replicator copies the value unchanged and adds `source_cluster`, `source_partition`, `source_offset` as record headers (`ss-src-cluster`, `ss-src-partition`, `ss-src-offset`). The value bytes are unchanged, so signatures or hashes over the value stay valid. `origin_*` and epoch fields are never rewritten.

After a promotion, the new active region's own records use the ingest form again. A replica fed by it gets the new region as `source_cluster`. Hops are appended to the `ss-hops` header for audit.

### 3.3 Encoding

Value: Avro or Protobuf (schema chosen in a later phase) with the fields above in a fixed envelope message and `payload` as bytes. Headers: `ss-envelope-version`, `ss-src-*`, `ss-hops`, `ss-client-ts`.

### 3.4 Ordering and dedup

- Order is defined per partition within one epoch/generation by source offset. Across an epoch change, order is `(leadership_epoch, source_offset)`.
- Sinks dedup by `event_id` in a bounded window plus the transactional source-offset commit. Dedup state is rebuildable from `_ss.offset_map`.

## 4. Epochs and generations

- **Granularity:** per topic. Policy is per topic and so is the active region.
- **`leadership_epoch` (E):** uint64, assigned only by the control plane via a Raft-committed `EpochGrant`. Starts at 1 on topic creation. Increases by exactly 1 on every change of active region (including planned switchover). Never decreases or repeats.
- **`replication_generation` (G):** uint64, increases by 1 on every `EpochGrant` and on every replica reseed or truncation that rebuilds a replica's lineage. So `G ≥ E` always holds.
- **Epoch start offsets:** at each grant the new active region's sink records, per partition, the highest source offset it holds from the previous epoch (`epoch_start_offsets`). Anything the previous epoch produced beyond that offset is the *divergent suffix*.
- **Sink rule:** for each topic a sink keeps `max_epoch_seen`. A record with `leadership_epoch < max_epoch_seen` is rejected into `_ss.quarantine` (INV-4, INV-9).

## 5. Control-plane records

All are Raft-committed (INV-8).

| Record | Key fields |
|---|---|
| `TopicPolicy` | See §2. |
| `TopicState` | `topic`, `active_region`, `leadership_epoch`, `replication_generation`, `state` (`ACTIVE`, `FENCING`, `PROMOTING`, `UNAVAILABLE`), `epoch_start_offsets`. |
| `WriteLease` | `topic`, `region`, `epoch`, `granted_at_cp_ms`, `ttl_ms`, `expiry_upper_bound_cp_ms`, `released` (bool). |
| `Attestation` | `topic`, `region`, `partition`, `replicated_source_offset`, `source_head_offset_at_report`, `source_head_ts`, `replicated_event_ts`, `reported_at_cp_ms`, `generation`, `signature`. |
| `PromotionIntent` | `topic`, `from_epoch`, `candidate_region`, `reason`, `eligibility_snapshot`, `override` (null or break-glass ref), `state`. |
| `BreakGlassGrant` | `topic`, `scope` (`WAIVE_LAG` only), `requested_by`, `approved_by` (≥ 2 distinct, none equal to requester), `expires_at`, `reason`. |
| `Objectives` | See ARCHITECTURE §8.1. |
| `AuditEntry` | `seq`, `prev_hash`, `hash`, `actor`, `action`, `target`, `before`, `after`, `ts`. |

The epoch token (the credential a gateway holds) is a signed projection of `WriteLease` + `TopicState`: `{topic, region, epoch, generation, policy_revision, cluster_incarnation, ttl_ms, nonce, kid}` with an Ed25519 signature. Time is a TTL measured on the gateway's monotonic clock from the moment it sent the renewal request (see SECURITY §4). `cluster_incarnation` increases only on a break-glass forced quorum reconfiguration.

## 6. Replication lag and promotion eligibility

The old active region may be unreachable at failover, so eligibility uses only data the control plane already committed.

- **Attested lag:** `attested_lag_ms = source_head_ts − replicated_event_ts` at the time of the attestation. Each attestation carries the source head offset and timestamp the replicator last read from the source cluster, so no separate heartbeat from the active region is needed.
- **Pessimistic lag bound at decision time `t`:**
  `lag_upper_bound_ms = attested_lag_ms + (t − reported_at_cp_ms)`
  This assumes the source kept producing at full speed since the last attestation, and therefore over-estimates the lag when the source was idle. It never under-estimates it.
- **Eligible for automatic promotion** iff all hold:
  1. `t − reported_at_cp_ms ≤ attestation_ttl_ms` for every partition (else the lag is *unknown* and the candidate is ineligible).
  2. `lag_upper_bound_ms ≤ max_tolerated_replication_lag_ms`.
  3. Policy checks of INV-5 pass.
- If the old active region *voluntarily released* its lease (it is write-unavailable but alive), it may add a final `source_head` into the release. The bound then becomes exact for the partitions covered.
- **Break-glass** (`WAIVE_LAG`) can promote a candidate failing check 1 or 2. It cannot waive check 3. The `AuditEntry` records the estimated loss window.
- **Under `SYNC_REGIONAL`:** every acked write is already in ≥ `sync_remote_acks` replica regions, so a candidate that is one of those regions has no acked-write loss. The candidate selection still requires a fresh attestation, since a replica may lag behind others.

## 7. Internal Kafka topics (per region)

| Topic | Purpose | Producers |
|---|---|---|
| `_ss.offset_map` | Source-offset → destination-offset mapping, per (topic, partition, source cluster). | Replicator sink |
| `_ss.quarantine` | Records rejected for stale epoch or other integrity failures. Policy-compliant location only (same region as the rejecting sink, which is by construction an allowed storage region). | Replicator sink, gateway |
| `_ss.epoch` | Local mirror of `TopicState` and bundle revision for consumers within the region. Compacted. Not authoritative. | Control-plane agent (regional) |

Internal topics are compacted or time-retained as configured. They hold only metadata and quarantined records, so quarantine inherits the retention and encryption of the originating topic's classification.

## 8. Phase 3 refinement of the topic policy

The authoritative policy model is now in [POLICY.md](POLICY.md) §2. Differences from §2 above: `replication_factor` replaces `local_replication_factor`; `maximum_replication_lag_ms` replaces `max_tolerated_replication_lag_ms`; `preferred_regions` and `excluded_regions` and `home_region` are added; `failover_priority` is computed by the placement ranking instead of being supplied. A region has a single `legal_zone` (the multi-zone list in §1 was a simplification that Phase 3 replaced). The rules V1 to V10 of §2.3 map onto the engine's validation and hard constraints.
