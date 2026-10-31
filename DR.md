# SovereignStream — Three-Tier Disaster Recovery (Phase 6)

Related: [ARCHITECTURE.md](ARCHITECTURE.md) · [FAILURE_MATRIX.md](FAILURE_MATRIX.md) · [SECURITY.md](SECURITY.md) · [REPLICATION.md](REPLICATION.md) · [GATEWAY.md](GATEWAY.md) · [DEPLOYMENT.md](DEPLOYMENT.md)

**Read §9 first.** It separates what is tested on this single Docker host from what is simulated and what is not done. Nothing here is a claim about production availability, zero data loss, or exactly-once delivery.

## 1. The three tiers

| Tier | Failure | Who acts | What |
|---|---|---|---|
| 1 | One A broker | **Kafka** | Native leader election among in-sync replicas. RF 3, `min.insync.replicas=2`, producer `acks=all`. The DR controller only *observes* (health `DEGRADED`, state `LOCAL_DEGRADED_A`); it never touches partition leadership. With fewer than 2 replicas in sync the gateway refuses writes (`acks=all`); we do not claim writes continue. |
| 2 | Region A | DR controller + coordinator | Fence A, promote B, redirect gateway traffic, rebuild B→C replication. |
| 3 | Region B (after promotion) | same | Same procedure, C becomes active. Also used when A and B are lost together (A → C directly). |

Not implemented: our own Kafka leader election, our own consensus. Kafka and etcd do those.

## 2. Components

| Component | Where |
|---|---|
| Pure core: state machine, region health, eligibility | `policy/ss_dr/model.py` |
| etcd client (v3 JSON gateway), CAS, audit, controller election | `policy/ss_dr/coord.py` |
| Controller (health manager, eligibility, promotion, fencing, steady-state reconcile) | `policy/ss_dr/controller.py`, `adapters.py` |
| Failback | `policy/ss_dr/failback.py` |
| HTTP API of the controller | `policy/ss_dr/main.py` |
| Gateway lease from etcd | `gateway/app/etcd_lease.py` |
| Topic adoption by a passive-then-active gateway | `gateway/app/topics.py` (`_adopt`) |
| Policy engine support: explicit epoch, idempotent promote, dry run, reseed, topology list | `policy/engine_service/replication.py` |
| Compose overlay (5-member etcd, gateways B and C, two controllers) | `infrastructure/docker-compose.dr.yml`, `scripts/up.sh --dr` |

## 3. State machine

States: `HEALTHY_A`, `LOCAL_DEGRADED_A`, `REGION_A_UNAVAILABLE`, `PROMOTING_B`, `HEALTHY_B`, `REGION_B_UNAVAILABLE`, `PROMOTING_C`, `HEALTHY_C`, `NO_ELIGIBLE_TARGET`, `COORDINATION_UNAVAILABLE`, `FAILBACK_PENDING`.

Transitions are listed in `model.TRANSITIONS` and unit-tested (a promotion is never skipped or abandoned half-way; `A` never becomes active by itself; `COORDINATION_UNAVAILABLE` has no outgoing promotion).

* **Durable:** every transition is one compare-and-swap on the etcd key `/ss/dr/state`, written together with an audit record (`/ss/dr/audit/<seq>`) in the same etcd transaction. The same event is forwarded to the policy engine's hash-chained audit.
* **Idempotent:** a promotion carries a transition id and the `step` it has reached (`FENCED` → `TOPOLOGY` → activation). A restarted or different controller resumes from the persisted step. The policy engine's promote is idempotent for a given `(topic, region, epoch)`.
* **`COORDINATION_UNAVAILABLE` is the controller's local view**: it cannot be written while the coordinator is unavailable. It is shown by `/dr/v1/status` and the last persisted phase is kept as `last_known_phase`.

## 4. Coordination, leadership and fencing

### 4.1 What is authoritative
One etcd key holds `{phase, active_region, leadership_epoch, replication_generation, fenced, transition}`. A database row is **not** the authority: the policy engine's topology is *made to agree* with the coordinator (explicit epoch passed to the engine) and is checked on every steady-state cycle.

### 4.2 Quorum placement (the claim and what supports it)
Five etcd members: `a`, `b`, `c` (one per region's fault domain) and **two independent witnesses** `w1`, `w2`. Majority = 3.

| Lost | Alive | Quorum? |
|---|---|---|
| A | b, c, w1, w2 | yes |
| A + B | c, w1, w2 | **yes** (this is what authorises Tier 3) |
| A + B + both witnesses | c | no → fail closed |
| a 3-member cluster with one member per region, after A + B | c | **no**: cannot authorise Tier 3 (why witnesses exist) |

MVP: all five run as containers on one Docker host (`ss-coord-net`); the "fault domains" are simulated. **Production** must place `w1` and `w2` in fault domains that are independent of A, B and C (for example two different providers/zones), such that the loss of A and B (and of their shared dependencies) still leaves a majority. Coordination metadata carries only region ids, epochs and audit events: no payloads and no secrets. It needs its own residency and security policy (mTLS between members and clients, RBAC so that only the DR controllers can write `/ss/dr/*` and gateways have read-only access). **The MVP etcd has no authentication or TLS.** Anything on `ss-coord-net` could write the state; this is a known gap, not a feature.

If the quorum cannot be reached, nothing can be read or written, so **no automatic promotion can happen**. Reads are linearizable (etcd default).

### 4.3 The active-region lease, and how the old region is fenced
Each gateway renews its lease by a linearizable read of the state, measured on its **monotonic clock from before the read** (same rule as Phase 2). No quorum → no renewal → the lease expires after `SS_LEASE_TTL_MS` and writes are refused (`LEASE_EXPIRED`). A region listed in `fenced`, or not `active_region`, is passive (`421 NOT_ACTIVE_REGION`).

Promotion (the fencing instant is step 1):

1. **One CAS:** `epoch+1`, `active_region=""` (nobody may write), old region added to `fenced`, `transition` recorded.
2. **Wait `lease_ttl + drift`** (default 6 s + 1 s): any gateway that could not hear the fence has now expired its lease.
3. Policy engine topology is promoted to the new epoch (flows from the new active region to the others are created `PENDING_CUTOVER`).
4. **One CAS:** `active_region = target`.

Steps 3–4 are retried until they succeed; until step 4 nobody can write.

**Cluster-level fence** (so a restored old region cannot resume writes even if a producer bypasses the gateway): the controller sets the topic config `max.message.bytes=1` on every topic in a fenced region that is reachable, which makes the broker refuse every produce request. Re-applied every 5 s. (`min.insync.replicas` above the replication factor was tried first and is **not** effective on Kafka 3.9 for acks=all writes; it is not used.) In production, additionally use Kafka ACLs and network policy.

## 5. Health manager

Per region, every probe interval, three independent signals: Kafka cluster metadata (reachability, broker count, controller present, partition leaders, ISR), the region's gateway `/health`, and the region's agent `/health`.

* `DOWN` only if Kafka is unreachable **and** another independent signal also fails. Kafka alone failing is `SUSPECT` (our own path may be broken). A dead gateway alone, a lost broker, or a leaderless partition is `DEGRADED`.
* A **slow replication link or slow consumer is not an input at all**: lag and backlog never reach the classifier.
* `fail_after` consecutive DOWN samples (default 3) to declare failure, `recover_after` consecutive non-DOWN samples (default 3) to declare recovery. The failure boundary is the start of the first failing probe round.
* If the coordinator itself is the thing that is down, gateways turn `DEGRADED` (lease), not `DOWN` (Kafka is fine): coordination loss is never mistaken for region loss.

## 6. Eligibility (deterministic)

Evaluated per topic and per candidate (and once region-wide), by `model.evaluate_candidate`. Hard reasons can never be waived:

`POLICY:*` (engine dry-run: storage and processing legality, replica status, flow from the active region), `CANDIDATE_UNHEALTHY`, `CONTROLLER_QUORUM_NOT_OPERATIONAL`, `PARTITION_DATA_MISSING` (candidate end offsets below what the checkpoints claim), `NO_CHECKPOINT`, `INSUFFICIENT_CAPACITY`, `COORDINATION_QUORUM_UNAVAILABLE`, `LEASE_NOT_ACQUIRABLE`, `OLD_LEADERSHIP_NOT_FENCEABLE`, `CUTOVER_POSITION_INVALID`.

Waivable **only** by an authorised operator **and only if the deployment allows degraded-RPO recovery** (`SS_DR_ALLOW_DEGRADED_RPO=1`, default off): `LAG_EXCEEDS_RPO`, `LAG_UNKNOWN`. A waived promotion is flagged `degraded_rpo`, records the approver and the expected gap, and is audited.

A region is chosen only if it is eligible for **every** topic (first in priority order); otherwise the state is `NO_ELIGIBLE_TARGET` and nothing is promoted. A healthy but legally unauthorised region is never promoted.

**How lag is measured (and its limit).** Once the source is dead its true lag cannot be read. The bound used is the replicator's last backlog report before the failure boundary: with nothing unreplicated, the age of that report; with a backlog, the larger of the drain estimate and the time since the replica last acknowledged anything, plus the report age. No report, or a report already stale at the boundary, means *unknown* (refused). The actual gap is only known when the old region returns (quarantine, §8) or, in tests, by comparing what clients were acknowledged with what the new active region holds.

## 7. Tier 2 / Tier 3 procedure (as implemented)

detect (consecutive probes) → failure boundary → eligibility (read-only) → **fence + next epoch in one CAS** → wait out leases → read checkpoints and compute the consumer offset translation → topology promote (explicit epoch) → activate the new gateway → B→C (or A→C…) flows start via the existing cutover (`PENDING_CUTOVER` → `ENABLED`) → the controller records `replication_ready_ms` → audit at every step.

Consumers: replicas are offset-aligned with their source (checked against the checkpoints, recorded per partition in `last_failover.offsets` as `[source_acked, destination]`). A consumer that read the old region resumes at the same offset in the new one.

## 8. Failback (explicit, never automatic)

`POST /dr/v1/failback/start` (admin) → validates the restored region (health UP, **Kafka cluster id equals the expected id**, receiver healthy) → `FAILBACK_PENDING`:

1. **RESYNC:** the restored region's divergent suffix (records beyond what it ever acknowledged to the active region) is copied to `_ss.quarantine.<topic>` *in that region* (never merged); its copy of each topic is recreated empty and re-seeded from the active region (policy `reseed`).
2. **FRESHNESS:** wait until each re-seeded replica is within the lag budget → `READY`.
3. `POST /dr/v1/failback/approve {approved_by}` (admin, only accepted at `READY`, freshness re-checked).
4. **SWITCH:** fence the current active region (epoch+1, nobody writes), wait out leases, require the restored region's end offsets to equal the fenced region's, compare the last 50 event ids per partition (lineage), promote, activate.
5. **RE-PROTECT:** the other regions' copies are recreated and re-seeded from the new active region.

A switch that cannot finish (catch-up timeout, lineage mismatch, engine refusal) **stays fenced** (no writer) and is retried; there is no automatic rollback. `failback/abort` works until the switch begins.

Limitation: re-seeding recreates a replica's copy from offset 0, so during RE-PROTECT the topic has fewer replicas than its policy asks for.

## 9. Evidence: tested vs simulated vs not done

### 9.1 Test matrix (each row is an automated test; `policy/tests/dr_run.sh` runs them on fresh stacks)

| ID | Scenario | Test | Result |
|---|---|---|---|
| DR-01 / T1 | A broker dies | `test_dr.py::test_01` | Kafka elected new leaders (ids 102/103), 30 further writes acknowledged, state `LOCAL_DEGRADED_A`, epoch unchanged |
| DR-02 / T2 | Second A broker dies | `test_02` | 0 of 12 writes acknowledged while only 1 of 3 replicas was in sync (`acks=all`, min ISR 2); no failover; writes resumed after both brokers returned |
| DR-03 / T3 | Region A fails | `test_05` | B promoted at epoch 2; gateway B the only active writer; consumers resume at the same offsets (identity translation, verified from checkpoints) |
| DR-04 / T4 | New events written in B | `test_06` | 50 of 50 present in C; C's history is a subset of B's; B→C flow `ENABLED` at generation 2 with a cutover report |
| DR-05 / T5 | B fails after promotion | `test_07` | C promoted at epoch 3; fenced = `{A: 2, B: 3}`; each epoch increase is one audited CAS |
| DR-06 / T6 | A returns after B promotion | `test_08` | stays fenced; gateway write `421 NOT_ACTIVE_REGION`; a producer bypassing the gateway is refused by Kafka (positive control proves the method works) |
| DR-07 / T7 | B returns after C promotion | `test_08` | same |
| DR-08 / T8 | Two controllers race | `test_05` | exactly one `PROMOTION_STARTED` and one `PROMOTED` in the coordinator audit; the other requests answered `ALREADY_IN_PROGRESS`/`REFUSED` |
| DR-09 / T9 | Candidate exceeds RPO | `test_04`, `test_dr_failback::test_01` | automatic promotion refused (`NO_ELIGIBLE_TARGET`, `LAG_EXCEEDS_RPO`); degraded recovery refused when the deployment forbids it; allowed only with a named approver when enabled, and then flagged `degraded_rpo` with the expected gap; if A returns first the controller goes back to `HEALTHY_A` |
| DR-10 / T10 | Coordinator unavailable | `test_03` | 3 of 5 members down: no state change, the (healthy) active region's gateway stops writing when its lease runs out, writes resume after recovery |
| DR-11 / T11 | A and B lost, witnesses reachable | `test_07` | coordinator kept its quorum (c, w1, w2); C authorised only after the eligibility checks |
| DR-12 / T12 | Witnesses lost too | `test_09` | only a and b alive: no promotion, no active writer anywhere, even though A and B were healthy |
| DR-13 / T13 | Controller crashes mid-transition | `test_07` | the leading controller was killed right after the topology step was persisted (`os._exit`); promotion completed idempotently, topology equals the coordinator (epoch 3, region C) |
| DR-14 / T14 | Manual failback | `test_dr_failback::test_02-04` | validated, resynced, 94 divergent records quarantined, explicit approval, B fenced before A activated, lineage verified, replicas re-seeded |
| (split brain) | False-positive detection with the old region alive and cut off from the coordinator | `test_dr_failback::test_01` | A acknowledged nothing after its lease expired (last ack 4.8 s after the fence, lease 6 s); B acknowledged nothing before activation; no overlap of acknowledged writes |

Pure tests: `test_dr_core.py` (23: states, transitions, health classification, every hard eligibility check, waiver rules, determinism property).

### 9.2 Measured RPO and RTO (single runs, this machine, simulated failures)

These are what the tests observed, not guarantees. Failures are `docker stop -t 1` of whole regions on one host.

| Event | Measured RPO | Measured RTO |
|---|---|---|
| A→B, replication healthy, writes in flight | **0** of 182 acknowledged events missing from B | controller: 10.0 s from the first failed probe to activation (7.0 s of that is the deliberate lease wait); a client writing continuously saw 15.3 s without an accepted write |
| B→C with A already lost, controller crashed and restarted | **0** of 76 acknowledged B-origin events missing from C | 19.7 s including the crash and resume |
| A→B with replication paths cut before the failure, approved degraded RPO | **94** acknowledged events missing from the new active region (they exist only in A's quarantine); expected bound at decision time 6.4 s of lag | 7.7 s controller |
| Failback B→A | 0 events lost by the switch (A verified equal to the fenced B before activation) | 10.6 s from approval to activation (10.3 s of that is fence plus lease wait) |

What "RPO 0" means here: nothing the clients were acknowledged for was missing after the run. Replication is asynchronous, so a failure at a worse moment loses the unreplicated tail; the 94-event row is exactly that case. The 94 is measured after the fact from the old region's quarantine; at decision time the controller only has a bound.

### 9.3 Tested vs simulated vs not done

**Tested:** everything in 9.1.

**Simulated (be careful what you conclude):**
* Regions are Docker containers on one host and one Docker network per region; "region failure" is stopping containers. No real WAN, no real zone/provider independence, no clock skew between hosts.
* The coordinator's fault domains (3 regions + 2 witnesses) are containers. The independence claim for the witnesses is an assumption of the deployment, not something this test proves.
* Network partitions are emulated with Docker network disconnects and Toxiproxy.
* One Kafka node in B and C (replication factor 1): their local durability is not claimed (Phase 1).

**Not done / known limits:**
* etcd has no TLS or authentication in this dev stack (SECURITY.md §2 B9 describes the intent).
* Lag is a bound taken from the last backlog report. Its resolution is the report interval (2 s) plus the length of the first probe round that fails (up to about 8 s, because failing probes wait for their timeouts): measured bounds of 4 to 9 s were seen for fully replicated, idle topics. RPO budgets below roughly 15 s therefore cannot be certified and are refused as `LAG_EXCEEDS_RPO` even for a replica that is fully in sync.
* Eligibility is all-or-nothing per region. There is no per-topic promotion and no per-topic active region.
* Topics created while a non-home region is active are not supported (new replicated topics are created through the home region's gateway).
* Gateways' idempotency state is per region; a retry crossing a failover can duplicate (consumers de-duplicate on `event_id`).
* Failback re-seeds replicas (fewer replicas than policy during RE-PROTECT); a failback switch that cannot finish stays fenced and needs an operator, there is no automatic rollback; only the last 50 event ids per partition are compared (end offsets are compared in full).
* The cluster-level fence is `max.message.bytes=1` per topic, re-applied every 5 s: it covers topics the policy engine knows. In production add Kafka ACLs and network policy.
* Failing over a topic whose region lacks a replica (not replicated to B, say) blocks the whole region-level failover (`NO_ELIGIBLE_TARGET`).
* Leader-election across controllers narrows who acts; safety does not depend on it (CAS does).
* The controller does not itself restart or repair failed infrastructure.
* Phase 8's long chaos/soak runs are not part of this phase.

### 9.4 Defects the clean end-to-end runs found (all fixed)

* Phase 4 replicator: a partition pipeline that died (for example because its source Kafka was still restarting) stayed `STOPPED` forever, so replication silently stopped after a source-cluster restart. Pipelines are now supervised and restarted from their checkpoint.
* Idle partitions never reported `IN_SYNC`, so one empty partition made every topic non-promotable. A caught-up empty partition now reports `IN_SYNC`.
* Controller: a region that was coming back could be overtaken by a promotion because its replicator's fresh "backlog 0" report made the replica look current. The controller now waits for the recovery window and ignores reports made after the failure boundary.
* Phase 8 demonstration: after a failback, re-protection tried to rebuild a replica in a region that the topic's policy forbids (an EU-restricted topic and the US-only region). It created an empty copy of the topic in that region *before* the policy engine refused, the engine's refusal path left its earlier writes committed, and the controller retried forever. Now the policy engine is asked first (a dry run with no side effects), nothing is created where the policy forbids it, only regions that held a replica of the topic are rebuilt, and a refusal is reported instead of retried silently. `test_dr_failback.py` asserts that no copy exists in the forbidden region.
* A first cluster-level fence (`min.insync.replicas` above the replication factor) was shown not to block `acks=all` writes in Kafka 3.9 and was replaced.


## 10. Phase 6 checklist and gate

| Item | Status |
|---|---|
| Failover state machine implemented (11 states, durable, idempotent, audited) | done, unit-tested; transitions exercised end to end |
| Health detection distinguishes broker and region failures (and ignores a slow link/consumer) | done, unit-tested; broker loss observed as `LOCAL_DEGRADED_A` in the live test |
| Consensus-backed coordinator integrated | done (etcd, 5 members) |
| Independent witness / fault-domain assumptions documented | done (§4.2); the independence itself is simulated in the MVP |
| Leadership epochs implemented | done (each change is one CAS and one audit entry) |
| Gateway enforces the active lease | done (etcd lease source; tested with quorum loss) |
| Old-region fencing implemented | done: coordinator fence + lease wait + cluster-level `max.message.bytes=1` |
| Tier 1 Kafka-native recovery tested | done (DR-01, DR-02) |
| A→B promotion tested | done (DR-03) |
| B→C replication after promotion tested | done (DR-04) |
| B→C promotion tested | done (DR-05, DR-11, DR-13) |
| RPO threshold enforced | done (DR-09); lag is a bound with up to ~10 s resolution (§9.3) |
| Split-brain scenario tested | done (false-positive detection with the old region alive and isolated) |
| Coordinator quorum loss tested | done (DR-10, DR-12) |
| Concurrent promotion attempts tested | done (DR-08) |
| Restored old regions remain fenced | done (DR-06, DR-07) |
| Failback implemented and tested | done (DR-14); re-seeding and "stays fenced on failure" limits in §8 |
| RPO and RTO measured | done, single runs (§9.2) |

**Phase gate (A → B → C recovery, and a restored old region cannot continue to accept writes):** demonstrated by `test_dr.py` (A→B at epoch 2, B→C at epoch 3 with A also lost and a controller crash in the middle, then A and B restored and refused at the gateway and at Kafka), on one Docker host, with simulated failures. That is evidence that the mechanism works under those conditions. It is not evidence of production availability, and the system does not provide zero data loss: replication is asynchronous (the failback scenario loses 94 acknowledged events from the live history by design, preserved in quarantine).
