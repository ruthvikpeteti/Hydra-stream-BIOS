# SovereignStream — Operational runbook

Scope: the stack in this repository, as tested on one machine. Procedures marked **(tested)** were exercised by an automated test or a recorded run; those marked **(not tested)** follow from the design and have not been run. Nothing here is a production procedure for a production deployment (see [RELEASE.md](RELEASE.md)).

## 1. Where to look

| Question | Where |
|---|---|
| What is the system doing right now? | Dashboard http://localhost:8200 (operator/viewer token in `infrastructure/.env`); `GET :8101/dr/v1/status` (DR controller); `infrastructure/scripts/status.sh` (Kafka) |
| Who is allowed to write? | The coordinator, not a database: `GET :8200/api/v1/snapshot` → `dr.active_region`, `dr.phase`, `dr.fenced`; or the gateways' `GET /api/v1/active-region` (`role`, `lease.valid`) |
| Why was something refused? | policy engine audit: `GET :8090/api/v1/audit?topic=…&decision=REJECTED` (service key), dashboard → Audit logs / Residency policies; coordinator transitions: `GET :8101/dr/v1/audit` |
| Metrics | `GET :8200/metrics` (Prometheus job `sovereignstream-dashboard-api`), label policy in [DASHBOARD.md](DASHBOARD.md) |
| Logs | `docker logs ss-dr-1` (decisions: lines `PROMOTED`, `FENCED region …`, `COORDINATION_UNAVAILABLE`), `ss-a-replicator`, `ss-policy-engine`, gateways |

## 2. Reading the failover state (`dr.phase`)

| State | Meaning | Operator action |
|---|---|---|
| `HEALTHY_A` / `HEALTHY_B` / `HEALTHY_C` | the named region is active and healthy | none |
| `LOCAL_DEGRADED_A` | a broker, leader or ISR problem inside A; **not** a failover | see §4 (Tier 1); A still serves if ISR ≥ 2 |
| `REGION_*_UNAVAILABLE` | declared failed after N consecutive probes; the controller is evaluating candidates | wait; watch the next state |
| `NO_ELIGIBLE_TARGET` | no candidate passed every check; **nobody can write**; the reasons per candidate are in the dashboard (DR → Candidate eligibility) | §5 |
| `PROMOTING_B` / `PROMOTING_C` | the old region is fenced, the new one is being activated (≈ lease TTL + 1 s) | wait; if it lasts minutes see §6 |
| `FAILBACK_PENDING` | an operator-started failback is in progress (stage in the dashboard) | §8 |
| `COORDINATION_UNAVAILABLE` | the controller cannot reach a quorum of the coordinator; shown by the controller, not stored | §7 |

## 3. Start, stop, health (tested)

```
infrastructure/scripts/up.sh --dr [--monitoring]    # idempotent; builds images; waits for health
infrastructure/scripts/status.sh                    # clusters, ISR, controller quorum, exporters
infrastructure/scripts/down.sh                      # stop, keep data
infrastructure/scripts/down.sh --volumes            # stop and DELETE all Kafka, PostgreSQL and etcd data
infrastructure/scripts/dc.sh <compose args>         # docker compose with the DR overlay, e.g. dc.sh logs -f ss-dr-1
```
Start order matters only through health checks (Compose handles it): etcd and PostgreSQL → policy engine → brokers → gateways and agents → replicators → DR controllers → dashboard. The coordinator state is created by the first controller that starts (`HEALTHY_A`, epoch 1).

## 4. A broker fails (Tier 1) (tested: DR-01, DR-02, Phase 1 tests C–E)

Kafka elects another in-sync replica; the DR controller only observes (`LOCAL_DEGRADED_A`). With RF 3 / min ISR 2 topics, one broker down: writes continue. Two down: **writes are refused** (`acks=all`); this is the configured durability, not a fault. Restart the brokers (`docker start ss-a-broker-N`); ISR recovers by itself; the state returns to `HEALTHY_A` after the recovery window. Replicated topics have RF 1 in this deployment (B and C cannot hold more), so losing the one broker that leads a partition makes that partition unavailable until the broker returns; that is a property of the sizing, not of the DR logic.

B and C each have **one** node: there is no broker-level redundancy in B or C.

## 5. The active region failed (tested: DR-03, DR-05, DR-09, DR-11)

Automatic path: detection (3 probes, ≈3–8 s) → eligibility → fence and next epoch (one coordinator write) → lease wait (7 s) → topology → activation. Measured durations: [PERFORMANCE.md](PERFORMANCE.md).

If the state is `NO_ELIGIBLE_TARGET`:
1. Read the reasons (dashboard → DR → Candidate eligibility): `POLICY:*` (a legal block: never overridden), `CANDIDATE_UNHEALTHY`, `PARTITION_DATA_MISSING`, `LAG_EXCEEDS_RPO` / `LAG_UNKNOWN`, `CUTOVER_POSITION_INVALID`, coordination reasons.
2. If the failed region can be brought back, do that: when it answers probes for the recovery window the controller returns to `HEALTHY_x` by itself (tested). **This preserves the most data.**
3. If it cannot, and the data loss is acceptable and **permitted by the deployment** (`SS_DR_ALLOW_DEGRADED_RPO=1`), an operator may approve a degraded-RPO promotion: dashboard → "Request failover" with *accept degraded RPO* (or `POST /dr/v1/failover {"accept_degraded_rpo":true,"approved_by":"<name>"}`). The expected gap is shown and recorded. Policy, health, fencing and quorum reasons can **never** be waived. If the deployment forbids it, the request is refused (tested).
4. Never edit the coordinator or the policy database by hand to force a promotion: the fence and epoch are what keep two regions from writing.

## 6. Promotion seems stuck (`PROMOTING_*` for minutes) (partly tested)

Nobody can write while it lasts (fail closed). Check `docker logs ss-dr-1` / `ss-dr-2` for `promotion of … is incomplete; topics blocked by the policy engine: …`. Typical cause: the policy engine rejects the topology change for a topic (for example the replica's flow was never activated). Fix the named cause; the controller retries on its own and a restarted controller resumes from the persisted step (tested: controller crash mid-promotion). The state machine has no automatic rollback of a half-done promotion.

## 7. Coordination quorum lost (tested: DR-10, DR-12)

Five etcd members: a, b, c (one per region) and two witnesses; quorum is 3. Symptoms: dashboard DR section shows the coordinator unreachable, the controllers report `COORDINATION_UNAVAILABLE`, and **every gateway stops accepting writes within one lease TTL** (even in a healthy region). No promotion can happen. Do not try to promote around it. Restore members (`docker start ss-etcd-…`); the system returns to the state it had (tested). If a region's data is intact and only quorum is missing, nothing else needs doing.

Loss of regions A and B leaves c + both witnesses = quorum (tested). Loss of A, B and both witnesses leaves no quorum by design.
Forced quorum reconfiguration (break-glass) is **not implemented** (design only: SECURITY.md SB-5).

## 8. Old region restored → failback (tested: DR-06, DR-07, DR-14)

A restored old region **stays fenced**: its gateway answers `421 NOT_ACTIVE_REGION` and Kafka refuses writes to its topics (`max.message.bytes=1`). It does not become active by itself. To fail back:
1. Dashboard → DR → choose the fenced region → "Start failback" (or `POST /dr/v1/failback/start {"region":"region-a","requested_by":"<name>"}`). The controller checks health, **Kafka cluster identity**, and the receiver.
2. It sets aside the region's divergent records in `_ss.quarantine.<topic>` (in that region), recreates its copy empty and re-seeds it from the active region, then waits for freshness → stage `READY`. (Divergent records are acknowledged writes that never replicated: this is where the actual RPO gap becomes known.)
3. An operator approves ("Approve failback"). Only then: the current active region is fenced, leases are waited out, the restored region must equal it exactly (end offsets, last 50 event ids per partition), and it is activated at the next epoch.
4. The other regions are re-seeded as replicas (fewer replicas than policy asks for until that finishes).
`abort` works until the switch begins. A switch that cannot finish stays fenced (no writer) and is retried; it needs an operator to look at `docker logs ss-dr-N` (catch-up timeout, lineage mismatch). **Quarantined records are never merged automatically**; reconcile them by a business decision.

## 9. Replication problems

| Symptom | Check | Action |
|---|---|---|
| backlog growing, state `HARD`, writes rejected `REPLICATION_BACKPRESSURE` (REJECT_WRITES topics) | dashboard → Replication / Routing; `ss_path_health` | find the cause on the route (all paths down? receiver down?). Writes resume when the backlog drops below the limit (tested). Do not raise the hard limit to "make it go away" |
| route flapping or on a worse path | dashboard → Routing (scores, breaker, exclusions) | paths excluded by policy or security (unencrypted for RESTRICTED/CONFIDENTIAL) are never used regardless of score |
| replica not usable for promotion | `Candidate eligibility` reasons; engine `GET /placement/{topic}` replica status | wait for `IN_SYNC`; an idle partition reports `IN_SYNC` once caught up |
| a replication worker died | `docker logs ss-X-replicator` | it is restarted from its checkpoint automatically; `docker restart` is safe at any time (tested: no loss, at-least-once) |
| duplicates at the destination | consumers de-duplicate on `event_id` | expected after retries (at-least-once); not a fault |

## 10. Chaos and fault injection (development only)

`infrastructure/chaos/{latency,jitter,bandwidth,loss,reset,disconnect,recover,list}.sh` act on Toxiproxy paths; `recover.sh --all` removes every fault. The test endpoints `SS_FAULT_INJECTION`, `SS_DR_FAULTS` must be **0** anywhere that matters.

## 11. Secrets, backup, upgrade

* Secrets are generated into `infrastructure/.env` (git-ignored) and the bundle signing keys into `infrastructure/config/keys` (development permissions). **Rotation is not implemented.** Production needs a secrets manager, TLS and authentication on etcd/Kafka/PostgreSQL (not present).
* Backup/restore of Kafka, PostgreSQL and etcd **(not tested)**: PostgreSQL via `pg_dump` of `sspolicy`; etcd via `etcdctl snapshot save`; Kafka data is the replicated log itself. Restoring an old PostgreSQL snapshot **loses checkpoints and audit history** and can disagree with the coordinator: the controller's topology reconcile would then rewrite topology to the coordinator's epoch, but replication checkpoints would restart from the snapshot. Treat as a recovery of last resort.
* Schema changes: `policy/engine_service/migrations/` (idempotent; applied by the engine at start; `infrastructure/scripts/db-migrate.sh`).
* Upgrades of images: change the pinned digests together in `ci/images.lock`, the Dockerfiles and the Compose files (`ci/pin-images.sh`), then run the stages in `ci/run_stage.sh`.

## 12. Things not to do

* Do not start a second writer by pointing clients at a standby gateway: it refuses (and should).
* Do not delete the `/ss/dr/*` keys or restore etcd from an old snapshot while regions are live: epochs would regress and gateways ignore regressions.
* Do not run `down.sh --volumes` on a system you want to keep.
* Do not set `SS_DR_ALLOW_DEGRADED_RPO=1` as a default: it turns a refusal into a decision someone must own.
