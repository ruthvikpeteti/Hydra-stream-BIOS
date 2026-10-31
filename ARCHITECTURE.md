# SovereignStream — Architecture Specification

Status: Phase 0 (design). No application code exists yet.
Companion documents: [FAILURE_MATRIX.md](FAILURE_MATRIX.md) · [DATA_MODEL.md](DATA_MODEL.md) · [API_SPEC.md](API_SPEC.md) · [SECURITY.md](SECURITY.md) · [ACCEPTANCE_SCENARIOS.md](ACCEPTANCE_SCENARIOS.md) · [docs/adr/](docs/adr/README.md)

## 1. Purpose

SovereignStream is a policy-governed, multi-region Kafka platform. Each topic carries a **policy** that says where its data may be stored, processed and transported, how many regional copies exist, how far a copy may lag, and which region may take over if the active region fails. The system must never trade a policy or single-writer guarantee for availability.

Design stance, applied everywhere:

1. **Fail closed.** If a component cannot prove an action is permitted (valid lease, valid policy, quorum), it refuses the action.
2. **One authority.** Only the control-plane quorum decides who is active for a topic and what policy applies. Everything else obeys or enforces.
3. **Objectives are configuration, not promises.** RPO and RTO are configured targets that the system measures and reports on. Section 8 states the exact behaviour that is guaranteed regardless of configuration.

## 2. Terminology

| Term | Meaning |
|---|---|
| Region | An independent failure and legal domain hosting one Kafka cluster (A, B, C). |
| Active region (of a topic) | The single region currently allowed to accept writes for that topic. |
| Passive / replica region | A region holding a replica of the topic that accepts no application writes. |
| Policy | The per-topic governance record in [DATA_MODEL.md §2](DATA_MODEL.md). |
| Policy bundle | A control-plane-signed, versioned snapshot of policies and the region registry that data-plane components consume. |
| Leadership epoch | Monotonic per-topic integer, bumped on every change of active region. Defined in DATA_MODEL.md §4. |
| Replication generation | Monotonic per-topic integer, bumped on every change of replication lineage (promotion or reseed). |
| Write lease | A short, time-bounded permission for a region's gateway to write a topic under a given epoch. |
| Epoch token | The control-plane-signed credential that proves a write lease. |
| Attestation | A signed report from a replicator stating how far a replica region has replicated. |
| Fence wait | The time the control plane waits after the last possible lease validity before granting a new epoch. |
| Break-glass | Audited dual-approval operator override. It can never override policy (INV-7). |

## 3. Invariants

These identifiers are referenced by every other document and by the acceptance scenarios.

| ID | Invariant |
|---|---|
| INV-1 | **Placement.** A replica of a topic exists only in a region listed in the topic's `allowed_storage_regions` whose legal zones include the topic's `legal_zone`. |
| INV-2 | **Transit.** Topic data crosses only links whose transit zones are a subset of the topic's `allowed_transit_zones`. Relaying via an intermediate region additionally requires that region to be in `allowed_processing_regions`. |
| INV-3 | **Single writer.** For any topic at any instant, at most one region holds a valid (unexpired) write lease. |
| INV-4 | **Epoch monotonicity.** Epochs and generations only increase. A lower epoch never overrides a higher one. |
| INV-5 | **Promotion eligibility.** A region is promoted only if it is in `allowed_storage_regions ∩ allowed_processing_regions`, is in `failover_priority`, holds a replica, and has a fresh attestation with lag ≤ `max_tolerated_replication_lag_ms`. Lag may be waived only by break-glass. Policy may never be waived. |
| INV-6 | **Fail closed.** Without quorum, a valid lease, or a valid policy bundle, the action is refused. Reads of already-stored data stay allowed where policy permits. |
| INV-7 | **Policy is non-overridable.** No operator, break-glass path, or configuration flag can place, process, transit or promote outside the policy. |
| INV-8 | **Authority.** Epoch grants, promotions, policy changes and region-registry changes are valid only if committed in the control-plane Raft log. |
| INV-9 | **No silent merge.** Divergent histories (writes from a fenced epoch) are quarantined. They are never auto-merged into the live log. |
| INV-10 | **Idempotent replication.** Replicating an event twice, or after a restart, does not create duplicates in the destination (dedup by `event_id`, transactional offset commit). |

## 4. Planes and boundaries

```
                         ┌────────────────────── Monitoring & Dashboard (read-only) ─────────────────────┐
                         │ Prometheus scrape · control-plane watch stream · no write path to any plane   │
                         └───────────────▲───────────────────▲────────────────────▲──────────────────────┘
                                         │                   │                    │
 ┌──────────────── CONTROL PLANE (global Raft quorum, 1 member per region in MVP) ───────────────────────┐
 │  Region registry · Topic policy store · Epoch/lease authority · Promotion engine · Audit log           │
 └───────▲──────────────────────────▲───────────────────────────▲────────────────────────────────────────┘
         │ lease/epoch token        │ policy bundle, attestations │ bundle
 ┌───────┴────────┐        ┌────────┴────────────┐       ┌────────┴────────────┐
 │ Region A       │        │ Region B            │       │ Region C            │
 │ ┌────────────┐ │        │ ┌────────────┐      │       │ ┌────────────┐      │
 │ │ App Gateway│ │        │ │ App Gateway│      │       │ │ App Gateway│      │
 │ └─────┬──────┘ │        │ └─────┬──────┘      │       │ └─────┬──────┘      │
 │  DATA PLANE    │        │  DATA PLANE         │       │  DATA PLANE         │
 │ ┌────────────┐ │        │ ┌────────────┐      │       │ ┌────────────┐      │
 │ │Kafka (KRaft)│ │       │ │Kafka (KRaft)│     │       │ │Kafka (KRaft)│     │
 │ └─────▲──────┘ │        │ └─────▲──────┘      │       │ └─────▲──────┘      │
 │ ┌─────┴──────┐ │  REGIONAL REPLICATION PLANE (replicator source-reader / sink-writer pairs)           │
 │ │ Replicator │◄┼────────┼──►Replicator  ◄─────┼───────┼──►Replicator        │
 │ └────────────┘ │        │ └────────────┘      │       │ └────────────┘      │
 └────────────────┘        └─────────────────────┘       └─────────────────────┘
```

### 4.1 Kafka data plane

- **What:** One Kafka cluster per region with its own KRaft controller quorum. No cluster, controller quorum, or consumer group spans regions (ADR-0001).
- **Owns:** Storage of records, partition leadership, ISR, and offsets *within one cluster*.
- **Authoritative for:** Intra-cluster partition leadership and offset assignment only.
- **Does not:** Know about epochs, policy, or other regions. It trusts only the ACLs that restrict who can produce (gateway principal for application topics, replicator-sink principal for replica topics).
- **Topic naming:** The same topic name in every region (no `A.` prefix). Whether a topic is writable in a region depends on gateway state and ACLs, not on naming (ADR-0008).

### 4.2 Regional replication plane

- **What:** Per-region replicator services. A *source reader* consumes from the source cluster. A *sink writer* produces to the destination cluster using a Kafka transaction that includes the offset commit.
- **Owns:** Moving records, stamping source coordinates into the replicated envelope, offset mapping (`_ss.offset_map`), reporting attestations to the control plane.
- **Authoritative for:** Nothing. It is an enforcement point. Before opening any (topic, destination) stream it verifies, using the current signed policy bundle, INV-1 and INV-2. The destination sink verifies INV-1 again independently. Each sink rejects any record whose epoch is below the highest epoch it has accepted for that topic (INV-4, INV-9).
- **Fail-closed behaviour:** No valid bundle, an unknown destination, or a path that is not approved means the stream does not start, or stops. Lag grows and alarms fire. The replicator never reroutes through an unapproved path (INV-2).

### 4.3 Control plane

- **What:** A small replicated state machine on a consensus protocol (Raft or equivalent) with linearizable, quorum-committed writes. MVP: 3 members, one per region. Production: 5 members, each on independent infrastructure (ADR-0003).
- **Owns:** Region registry, link registry, topic policies, epoch/generation counters, write leases, promotion state machine, attestations, and an append-only, hash-chained audit log.
- **Authoritative for:** Everything in INV-8. It is the only authority.
- **Does not:** Carry application data. It also never writes to Kafka topics on a data path.
- **New-leader quiet period:** After any Raft leader election the new leader makes no epoch grant or promotion until `lease_ttl_ms + max_clock_drift_ms` has passed. It may not know the last lease-renewal time (ADR-0004).

### 4.4 Application gateway

- **What:** A stateless-ish regional service. It is the *only* way applications produce or consume (ADR-0006). Clients have no Kafka credentials.
- **Owns:** Authenticating clients, per-topic authorization, building the event envelope, validating the epoch token and lease on each write, enforcing durability mode and backpressure, and consume-side processing-region checks.
- **Authoritative for:** Only the local admit/reject decision on a request, and only while it holds an unexpired lease for the topic. It cannot create or extend a lease on its own.
- **Self-fencing:** When the lease is not renewed in time, the gateway rejects writes by itself with `LEASE_EXPIRED`. It does this on its own monotonic clock, so it still works when the control plane is unreachable.
- **Region-redirect:** A gateway that is not the active region for a topic answers `NOT_ACTIVE_REGION` with a redirect hint. A gateway in a region not in the topic's `allowed_processing_regions` answers `POLICY_VIOLATION`. The gateway is itself processing, so the processing rule applies to it.

### 4.5 Monitoring and dashboard

- **What:** Prometheus-style metrics, a control-plane watch stream, and a read-only dashboard.
- **Authoritative for:** Nothing. It has no credentials that can mutate any plane. Operator actions (promotion, break-glass) go through the control-plane API with RBAC, never through the dashboard backend.
- **Must show:** Active region and epoch per topic, lease time remaining, per-replica lag and attestation age, policy-violation rejections, RPO/RTO objective status (met/breached/unknown), quorum health, quarantine counts.

## 5. Clusters

| Region | Role (default) | Illustrative legal zones | MVP brokers | Production brokers |
|---|---|---|---|---|
| A | Primary | `EU`, `DE` | 3 | ≥3 (rack/AZ-aware) |
| B | Secondary | `EU`, `FR` | 1 | ≥3 |
| C | Tertiary | `US`, `GLOBAL` | 1 | ≥3 |

"Primary / secondary / tertiary" describe the *default* `failover_priority` of a topic. They are not properties of the cluster. Different topics can have different active regions at the same time. The legal-zone assignment is illustrative; the real values live in the region registry ([DATA_MODEL.md §1](DATA_MODEL.md)). The `GLOBAL` zone is included by every region, so `PUBLIC` topics may use all three.

## 6. Deployment profiles

### 6.1 MVP

| Item | Region A | Region B | Region C |
|---|---|---|---|
| Brokers | 3 | 1 | 1 |
| KRaft | 3 combined broker+controller nodes (own quorum) | 1 combined node (own quorum of 1, no tolerance) | 1 combined node (own quorum of 1, no tolerance) |
| Local RF / min ISR | 3 / 2 | 1 / 1 | 1 / 1 |
| Control-plane members | 1 | 1 | 1 (global quorum of 3, tolerates loss of 1 region) |
| Gateway / Replicator | 1+ each | 1 each | 1 each |

MVP consequences, stated plainly:

- Topics asking `local_replication_factor: 3` can only have A as an active region. B and C can hold a replica only with RF=1. The policy validator ([DATA_MODEL.md §2.3](DATA_MODEL.md)) takes per-region capacity from the region registry and rejects policies the registry cannot satisfy for the region's role.
- A broker failure in B or C makes that region unavailable. Its data is still on disk.
- The 3-member control plane cannot make progress after two regions are lost (see FAILURE_MATRIX F5).

### 6.2 Production design

- ≥3 brokers per region with RF=3, `min.insync.replicas=2`, rack/AZ-aware placement.
- Independent KRaft controller quorum per region: 3 dedicated controller nodes in separate failure domains. No shared controllers, no stretched cluster.
- Control plane: 5 members on independent infrastructure, placed so that loss of any single region keeps quorum (e.g. A×2, B×2, C×1). It still cannot survive loss of two regions. Surviving that needs an external witness outside A/B/C, or break-glass.
- Gateways and replicators: ≥2 instances per region, stateless, with lease state re-acquired from the control plane.
- Per-zone KMS keys and per-region secrets (SECURITY.md §6).

## 7. End-to-end flows

### 7.1 Write

1. Client calls `POST /v1/topics/{topic}/events` on its regional gateway (API_SPEC §3.1).
2. Gateway authenticates, authorizes, and checks that the region is in `allowed_processing_regions` (else `POLICY_VIOLATION`).
3. Gateway checks that it holds an unexpired lease for the topic (else `LEASE_EXPIRED`, or `NOT_ACTIVE_REGION` if another region is active). If the client sent `expected_epoch` and it differs, the gateway answers `STALE_EPOCH`.
4. Gateway builds the ingest envelope (DATA_MODEL §3), stamping `leadership_epoch` and `replication_generation` from the token.
5. Gateway produces to local Kafka with `acks=all` and `min.insync.replicas` from policy. A failure here gives `LOCAL_DURABILITY_UNAVAILABLE`.
6. By `durability_mode`:
   - `LOCAL_DURABLE`: acknowledge.
   - `ASYNC_REGIONAL`: acknowledge. The replicator ships it later. If lag exceeds `max_tolerated_replication_lag_ms`, `lag_breach_action` applies (default `REJECT_WRITES`, giving `REPLICATION_BACKPRESSURE`).
   - `SYNC_REGIONAL`: acknowledge only after `sync_remote_acks` replica regions confirm durable receipt. Otherwise `REPLICATION_BACKPRESSURE`.

### 7.2 Replicate

1. The source reader reads committed records from the source cluster (read-committed).
2. For each (topic, destination), the replicator checks INV-1 and INV-2 against the signed bundle. A violation means the stream never starts.
3. The replicator adds `source_cluster`, `source_partition` and `source_offset` to form the replicated envelope.
4. The sink writer verifies INV-1 for its own region and `epoch ≥ max_epoch_seen`, then produces in a transaction together with the source-offset commit. It dedups by `event_id`.
5. The replicator sends an attestation (replicated source offsets, source head offsets, timestamps) to the control plane every `attestation_interval_ms`.

### 7.3 Promotion (automatic)

Triggers: the active region's lease expires without renewal, the active gateway voluntarily releases its lease (it is write-unavailable), or an operator requests a planned switchover.

1. The control plane (quorum leader, past its quiet period) evaluates candidates in `failover_priority` order using INV-5. A candidate with a stale or missing attestation, or with lag above the maximum, is skipped.
2. If no candidate is eligible, no automatic promotion happens. The topic stays write-unavailable and an alert fires (F5/F7). Break-glass is the only way forward, and it can waive only the lag check.
3. The control plane commits `PromotionIntent` and waits out the fence: either the old region's explicit lease release is committed, or `last_granted_lease_expiry + max_clock_drift_ms` has passed.
4. The candidate's sink drains data and records `epoch_start_offsets` per partition.
5. The control plane commits `EpochGrant{epoch+1, generation+1, region, lease}`. The candidate gateway becomes active on receiving the token.
6. Replicators re-point to the new source. Any replica ahead of the divergence point is truncated or reseeded under the new generation. The old region's post-fence data is quarantined (INV-9).

## 8. RPO / RTO: configurable objectives, explicit guarantees

### 8.1 Configurable objectives

Objectives are values in a control-plane `objectives` record, scoped by classification and durability mode. They are **targets that the system measures and reports on**; no document, API or UI may describe them as guarantees. Example (illustrative values, not defaults to rely on):

```yaml
objectives:
  - scope: { classification: RESTRICTED, durability_mode: SYNC_REGIONAL }
    rpo_ms: 0
    rto_s: 120
  - scope: { classification: INTERNAL, durability_mode: ASYNC_REGIONAL }
    rpo_ms: 30000
    rto_s: 300
  - scope: { classification: PUBLIC, durability_mode: LOCAL_DURABLE }
    rpo_ms: null        # no regional copy: region loss means data unavailable
    rto_s: null
```

Status values per objective: `MET`, `BREACHED` (current or last measured behaviour exceeds target), `UNKNOWN` (no fresh measurement).

### 8.2 What is guaranteed regardless of configuration

- A write acknowledged under `SYNC_REGIONAL` is durable in the active region and in at least `sync_remote_acks` replica regions at ack time.
- Under `ASYNC_REGIONAL`, the acknowledged-but-unreplicated window at failover is bounded by the attested lag. Automatic promotion only happens when the *pessimistic lag bound* is ≤ `max_tolerated_replication_lag_ms` (DATA_MODEL §6). Larger loss needs break-glass.
- No automatic promotion can violate policy (INV-5, INV-7), and no two regions hold a valid lease at once (INV-3).

### 8.3 RTO composition

`RTO_auto ≈ T_detect + T_fence_wait + T_decision + T_propagate`

| Term | Meaning | Tunable |
|---|---|---|
| `T_detect` | Time to notice the active region lost its lease or went write-unavailable. | `lease_ttl_ms`, `write_unavailable_grace_s` |
| `T_fence_wait` | 0 if the old region released explicitly. Otherwise remaining lease validity plus drift margin. | `lease_ttl_ms`, `max_clock_drift_ms` |
| `T_decision` | Raft commits, the new leader's quiet period if an election happened, and candidate drain. | `lease_ttl_ms + max_clock_drift_ms` (quiet period) |
| `T_propagate` | Gateway receives token and starts serving. | `lease_renew_interval_ms` |

### 8.4 Tunables

All names appear in the control-plane and gateway config. Values are starting points, set per environment and not guarantees.

| Name | Starting value | Meaning |
|---|---|---|
| `lease_ttl_ms` | 10000 | Write-lease validity. |
| `lease_renew_interval_ms` | 3000 | Gateway renewal cadence (must be ≤ `lease_ttl_ms / 3`). |
| `max_clock_drift_ms` | 500 | Upper bound of monotonic-clock drift over one TTL, plus margin. |
| `attestation_interval_ms` | 1000 | How often replicators attest. |
| `attestation_ttl_ms` | 5000 | Max age of an attestation usable for automatic promotion. |
| `write_unavailable_grace_s` | 60 | How long a degraded active region may hold the lease before the control plane treats it as failed (it may release sooner). |
| `bundle_ttl_ms` | 300000 | Validity of a signed policy bundle. Expired means fail closed (SECURITY §5). |
| `auto_failover_enabled` | true | Global switch. When false, promotion is operator-initiated, with the same eligibility checks. |

## 9. Authority matrix

| Decision | Who may make it | Who must only enforce it |
|---|---|---|
| Which region is active for a topic, and epoch/generation values | Control-plane quorum (Raft commit) | Gateways, replicators, sinks |
| Grant, extend or revoke a write lease | Control-plane quorum | Gateways |
| Topic policy create/update, region and link registry | Control-plane quorum, after RBAC and validation | All |
| Promotion eligibility | Control-plane promotion engine, using committed attestations | Replicators (report only) |
| Promotion override of lag (not policy) | Control plane after dual-approval break-glass | — |
| Admit or reject one write | Gateway, only under a valid lease | Kafka (ACLs only) |
| Allow a replication stream to start | Replicator + destination sink, each verifying the signed bundle | — |
| Partition leader and offset inside one cluster | That cluster's KRaft quorum | Everything else |
| Display status | Dashboard (no authority) | — |

No other component may infer leadership from reachability, heartbeats seen by a single peer, or local config. A region that cannot reach the quorum does not become active; it self-fences.

## 10. Out of scope for Phase 0

Implementation, language and framework selection, schema registry choices, and capacity sizing. These belong to later phases and must not contradict this document. Phase 1 may choose the consensus implementation (for example etcd, or an embedded Raft library) subject to ADR-0003's requirements.

## 11. As built (Phase 8): where the implementation differs from this specification

This document was written in Phase 0, before any code. The implementation follows its intent and its invariants, but not every detail. The differences, so that nobody reads the design as a description of what runs:

| Specified here | As built | Consequence |
|---|---|---|
| Control plane = own Raft state machine, 3 members (MVP) / 5 (production), signed epoch tokens, quiet period after a new leader | **etcd 5 members** (a, b, c + two witnesses), state in one key changed by compare-and-swap; gateways renew by a linearizable read; no signed epoch tokens; the controller waits `lease TTL + drift` after fencing instead of a leader quiet period | Safety rests on etcd's quorum and the lease wait. The MVP's three-regions-only quorum was replaced by 3+2 because it cannot authorise Tier 3 after losing A and B (DR.md §4.2) |
| INV-8: authority = committed in the control-plane log | the active region, epoch and fence are authoritative in etcd; the policy engine's PostgreSQL topology is *made to agree* (and re-checked each cycle) | the database alone is never the authority for who may write |
| Replicator moves records with transactional offset commit into the destination (INV-10) and keeps `_ss.offset_map` | an **HTTP receiver** per region; checkpoints in PostgreSQL written only after the destination's acknowledgement; ordering gate by `prev_offset`; at-least-once with detectable duplicates; no `_ss.offset_map` (replicas are offset-aligned with their source, verified through the checkpoints) | duplicates can occur after retries; consumers de-duplicate on `event_id`. Exactly-once is not claimed |
| `SYNC_REGIONAL` durability | **not implemented** (the gateway rejects it as unsupported); only `LOCAL_DURABLE` and `ASYNC_REGIONAL` | no synchronous cross-region durability; RPO is not zero |
| Region B and C in production: 3 brokers each | MVP profile as specified: **one broker each, RF 1** | B and C have no broker-level redundancy |
| Network paths from a link registry, checked by every enforcement point | implemented, plus measured routing (Phase 5) with eligibility before score and a receiver-side path check | see ROUTING.md |
| Gateway with its own policy store | gateway asks the policy engine (authority) and caches; topics created in another region are adopted from the engine when that region becomes active | see GATEWAY.md §10 |
| Dashboard read-only via the watch stream | polling snapshot API with a small set of forwarded admin operations | see DASHBOARD.md |
| Break-glass dual approval, forced quorum reconfiguration (`cluster_incarnation`) | **not implemented** | losing the quorum for good needs an out-of-band procedure that does not exist yet |

The invariants INV-1 to INV-9 held in every automated test that exercises them; INV-10 holds as "at-least-once with idempotent consumers", not as written.
