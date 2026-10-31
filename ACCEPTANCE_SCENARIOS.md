# SovereignStream — Design-Level Acceptance Scenarios

These scenarios show that the documented design satisfies five required properties. They are design-level: each one traces the specific mechanisms in the documents that make the property hold, and states the observable outcome a later-phase automated test must assert. No code exists yet, so nothing here has been executed.

References: invariants in [ARCHITECTURE.md §3](ARCHITECTURE.md), failures in [FAILURE_MATRIX.md](FAILURE_MATRIX.md), records in [DATA_MODEL.md](DATA_MODEL.md), errors in [API_SPEC.md](API_SPEC.md).

## Shared fixture

Registry per [DATA_MODEL §1](DATA_MODEL.md): region-a `[EU,DE,GLOBAL]`, region-b `[EU,FR,GLOBAL]`, region-c `[US,GLOBAL]`. Link A→C transits `[EU, ATLANTIC, US]`.

Topic `payments.eu.v1`: `legal_zone: EU`, `allowed_storage_regions: [a, b]`, `allowed_processing_regions: [a, b]`, `allowed_transit_zones: [EU]`, `failover_priority: [a, b]`, `regional_replica_count: 1`, `durability_mode: ASYNC_REGIONAL`, `max_tolerated_replication_lag_ms: 30000`.

---

## AS-1 — No destination outside the allowed region set can receive a replica

**Property:** INV-1, INV-2, INV-7.

| Step | Given / When | Then |
|---|---|---|
| 1 | Create a topic with `failover_priority: [a, c]` and `legal_zone: EU`. | `422 POLICY_INVALID`, rule V2/V4 (C has no `EU` zone and is not in the allowed set). No topic, no `TopicState`, no replica assignment. |
| 2 | Update `payments.eu.v1` to add region-c to `allowed_storage_regions`. | `422 POLICY_INVALID`, rule V2. |
| 3 | An operator or script asks a replicator to start a stream A→C for `payments.eu.v1` (for example by editing local config). | The replicator has no endpoint to add streams (API_SPEC §5). Streams exist only when the signed bundle approves (topic, destination, path). No stream starts. |
| 4 | A bug or compromised source replicator attempts to send records to region-c's sink. | The sink independently checks region-c ∈ `allowed_storage_regions` of its bundle and rejects the batch. No replica topic exists in C, since the regional agent creates replica topics only for approved pairs, and the sink principal has no create right (SECURITY §5). The attempt is counted in `ss_rejections_total{code="POLICY_VIOLATION"}`. |
| 5 | Region-b is allowed, but the only available physical path from A to B transits a zone not in `[EU]`. | Link not approved (INV-2). Replication to B stalls, lag grows, alert fires (FAILURE_MATRIX F7). No fallback path is used. |
| 6 | Break-glass is requested with a scope other than `WAIVE_LAG`. | `403 POLICY_VIOLATION`. No scope waives policy (INV-7). |

**Observable assertions:** zero records for `payments.eu.v1` in cluster kafka-c at any time; zero topics named `payments.eu.v1` in kafka-c; rejection counters non-zero for steps 3–4 attempts that reach an enforcement point.

**Mechanisms:** DATA_MODEL V1–V6; SECURITY §5 (six independent enforcement points); API_SPEC §4.1, §4.5, §5.

## AS-2 — A stale active-region epoch cannot authorize a write

**Property:** INV-3, INV-4, INV-8.

Setup: A is active at epoch 7. A is isolated from the control plane. The quorum (B, C members) promotes B, granting epoch 8.

| Step | Given / When | Then |
|---|---|---|
| 1 | A's gateway cannot renew. | After `lease_ttl_ms` on its monotonic clock, it answers writes with `503 LEASE_EXPIRED`. Its token no longer authorizes anything. |
| 2 | The control plane considers promotion. | It waits `last_granted_lease_expiry_upper_bound + max_clock_drift_ms` (or an explicit release), so epoch 8 cannot be granted while A's epoch-7 lease can still be valid (ARCHITECTURE §7.3, ADR-0004). |
| 3 | A client calls B's gateway with `expected_epoch: 7`. | `409 STALE_EPOCH`, `details.current_epoch: 8`. |
| 4 | A stale gateway at A (clock fault, bug) writes anyway to A's Kafka. | The write lands only in A's cluster. A's gateway has no credentials for B's cluster (SECURITY B2). |
| 5 | A's replicator tries to ship that record to B. | B's sink has `max_epoch_seen = 8` and rejects `leadership_epoch = 7` into `_ss.quarantine` (DATA_MODEL §4). |
| 6 | A renews with the old token. | `409 STALE_EPOCH`. The control plane refuses a renewal below the current epoch (API_SPEC §4.2). |
| 7 | A forced reconfiguration happened while A was away. | A's tokens carry an older `cluster_incarnation` and are rejected (SECURITY §4). |

**Observable assertions:** no acknowledged write at epoch 7 after the time epoch 8 was granted; no record with `leadership_epoch = 7` visible in B's live log with a source offset above `epoch_start_offsets`; quarantine count equals A's divergent suffix.

**Mechanisms:** ADR-0004 (lease TTL < fence wait), monotonic-clock leases (SECURITY §4), sink fencing, Kafka ACL scoping.

## AS-3 — A region cannot be promoted if it violates the topic policy

**Property:** INV-5, INV-7.

| Step | Given / When | Then |
|---|---|---|
| 1 | Operator requests promotion of region-c for `payments.eu.v1` while A is down. | Engine evaluation: C ∉ `allowed_storage_regions ∩ allowed_processing_regions`, C ∉ `failover_priority`, no replica. `409 PROMOTION_INELIGIBLE_POLICY`. |
| 2 | Automatic promotion after A fails. | The engine only considers `failover_priority` entries: B only. C is never a candidate. |
| 3 | Break-glass `WAIVE_LAG` for C. | `execute` still runs the policy checks and returns `409 PROMOTION_INELIGIBLE_POLICY` (API_SPEC §4.5). |
| 4 | Both A and B are down (F5), operators force-reconfigure the quorum to C. | Quorum is restored, but promotion of C for `payments.eu.v1` is still refused. The topic stays `UNAVAILABLE`. Topics whose policy includes C (for example a `GLOBAL` topic) can be promoted (FAILURE_MATRIX F5). |
| 5 | Policy is edited between candidate evaluation and the grant. | `EpochGrant` commit re-validates `policy_revision` atomically (compare-and-set). A stale evaluation fails with `412 PRECONDITION_FAILED`. |
| 6 | The candidate's own region state is modified (for example the registry removes `EU` from region-b). | Policy evaluation at commit time uses the current registry. B becomes ineligible. |

**Observable assertions:** zero `EpochGrant` records naming a region outside `failover_priority`; `ss_promotions_total{result="rejected_policy"}` increments; no gateway in C becomes active for the topic.

**Mechanisms:** INV-5, DATA_MODEL V4/V6, API_SPEC §4.4 step 2 (shared by automatic and manual paths), CAS on policy revision.

## AS-4 — A stale replica cannot be automatically promoted when its lag exceeds the configured RPO

**Property:** INV-5 (lag, attestation freshness), INV-6.

Setup: `max_tolerated_replication_lag_ms = 30000`. B's last committed attestation shows `attested_lag_ms = 2000`.

| Step | Given / When | Then |
|---|---|---|
| 1 | The A→B link fails (F4/F7) for 120 s. Replication stalls. B stops attesting a fresh replicated offset. A then fails. | At decision time `t`: `lag_upper_bound_ms = 2000 + (t − reported_at_cp_ms)`. Once `t − reported_at_cp_ms > 28000` the bound exceeds 30000. (The attestation also expires at `attestation_ttl_ms = 5000`, so the earlier condition triggers first.) |
| 2 | Automatic promotion evaluates B. | `PROMOTION_INELIGIBLE_ATTESTATION_STALE` (age > 5000 ms), or `PROMOTION_INELIGIBLE_LAG` where attestations are fresh but the bound is above 30000. B is not promoted. |
| 3 | No other candidate exists. | Topic state `UNAVAILABLE`. Alert fires. Writes stay rejected. The system does not promote "best available". |
| 4 | The replica's own view says it is caught up (it cannot know the source head). | Ignored. Eligibility uses only control-plane-committed attestations that include the source head. |
| 5 | An operator requests a manual promotion without break-glass. | Same engine, same ineligible result. |
| 6 | Break-glass `WAIVE_LAG` with two approvers. | Allowed. The audit records the estimated loss window. Policy checks still apply (AS-3). |
| 7 | A replica region was idle (source produced nothing). | The bound over-estimates, so the replica is conservatively ineligible. The operator may use break-glass. The design accepts this false negative (DATA_MODEL §6). |

**Observable assertions:** no `EpochGrant` where the snapshot shows `lag_upper_bound_ms > max_tolerated_replication_lag_ms` and `override = null`; promotion attempts with a stale attestation rejected with the specific code.

**Mechanisms:** DATA_MODEL §6 pessimistic bound; `attestation_ttl_ms`; promotion engine is the only evaluator.

## AS-5 — Loss of the coordination quorum must not result in two active leaders

**Property:** INV-3, INV-6, INV-8.

| Step | Given / When | Then |
|---|---|---|
| 1 | Majority of control-plane members fail or are partitioned from each other. A holds epoch 7 with a lease. | No Raft commit is possible, so no lease renewal, epoch grant, or promotion can occur. |
| 2 | Time passes beyond `lease_ttl_ms`. | A's gateway self-fences (`LEASE_EXPIRED`). B and C have no lease and cannot get one. Zero valid leases exist for the topic, so zero active leaders. |
| 3 | B and C notice A's heartbeat is gone and want to promote. | They have no authority. A gateway cannot mint tokens (it has no signing key, and tokens need a quorum-committed lease record). |
| 4 | A minority of members is partitioned together with A. | The minority cannot commit. A cannot renew via it, and A self-fences after the TTL. The majority partition (if any) may grant epoch 8 only after the fence wait, so A's lease is expired by then (SB-1). |
| 5 | A stale Raft leader that has not noticed the lost quorum. | It cannot commit renewals, so it cannot extend a lease (SB-2). |
| 6 | Quorum returns with a newly elected leader. | The new leader waits the quiet period `lease_ttl_ms + max_clock_drift_ms` before any grant or promotion (SB-3). |
| 7 | Both A and B regions are lost (F5). | No automatic promotion of C. Break-glass force reconfiguration bumps `cluster_incarnation` and waits `lease_ttl_ms + max_clock_drift_ms` after isolation evidence (SB-5). |

**Observable assertions:** at every instant `count(valid leases per topic) ≤ 1`, evaluated across all gateways using ground-truth time in a simulation. With quorum lost, writes are rejected from every region within `lease_ttl_ms + max_clock_drift_ms`.

**Mechanisms:** ADR-0003 (single quorum authority), ADR-0004 (lease TTL, fail closed, quiet period), SECURITY §4 and §9.

---

## Traceability

| Invariant | Scenarios | Primary documents |
|---|---|---|
| INV-1 Placement | AS-1 | DATA_MODEL §2.3, SECURITY §5 |
| INV-2 Transit | AS-1 | DATA_MODEL §1, FAILURE_MATRIX F7 |
| INV-3 Single writer | AS-2, AS-5 | ARCHITECTURE §7.3, SECURITY §4, §9 |
| INV-4 Epoch monotonicity | AS-2 | DATA_MODEL §4 |
| INV-5 Promotion eligibility | AS-3, AS-4 | API_SPEC §4.4, DATA_MODEL §6 |
| INV-6 Fail closed | AS-4, AS-5 | ARCHITECTURE §1 |
| INV-7 Policy non-overridable | AS-1, AS-3 | API_SPEC §4.5 |
| INV-8 Single authority | AS-2, AS-5 | ARCHITECTURE §9 |
| INV-9 No silent merge | AS-2 | FAILURE_MATRIX §2, DATA_MODEL §4 |
| INV-10 Idempotent replication | (covered by FAILURE_MATRIX F4/F7 recovery; test in Phase on replication) | DATA_MODEL §3.4 |

## Mapping to later automated tests (naming only)

`test_as1_replica_placement_rejected`, `test_as2_stale_epoch_write_rejected`, `test_as3_policy_ineligible_promotion_rejected`, `test_as4_lag_over_rpo_not_promoted`, `test_as5_quorum_loss_no_dual_leader`. The AS-5 test must run in a deterministic simulation with injectable clocks and partitions, asserting the "≤ 1 valid lease" invariant at every step.
