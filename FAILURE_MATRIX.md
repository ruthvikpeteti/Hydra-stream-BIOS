# SovereignStream — Failure Matrix

Describes the behaviour the design requires for each failure. Terms and tunables are defined in [ARCHITECTURE.md](ARCHITECTURE.md) (§2, §8.4). Invariants INV-n are in ARCHITECTURE §3. Lag semantics are in [DATA_MODEL.md §6](DATA_MODEL.md).

Conventions:

- "Topic" below means a topic whose active region is A with replicas per its `failover_priority`, unless stated.
- RPO/RTO cells state *what bounds the value*, never a promised number. Objective status is reported as `MET`, `BREACHED` or `UNKNOWN` (ARCHITECTURE §8.1).
- Control-plane quorum: MVP 3 members (1 per region). Production 5 members (A×2, B×2, C×1).
- MVP A: RF=3, min ISR=2. MVP B and C: RF=1, min ISR=1.

## 1. Summary matrix

| # | Failure | Writes (active region) | Reads | Auto promotion? | Data-loss exposure | Split-brain guard |
|---|---|---|---|---|---|---|
| F1 | One broker fails | Continue | Continue | No | None for acked writes (RF=3, min ISR=2). MVP B/C: region unavailable (see F1 detail) | n/a |
| F2 | Two brokers fail | MVP A: rejected (`LOCAL_DURABILITY_UNAVAILABLE`). Production: continue if ISR ≥ 2 | Continue where leaders survive | Yes, after grace, if eligible | Bounded by attested lag; none for acked `SYNC_REGIONAL` | Gateway releases the lease before promotion |
| F3 | All primary-region brokers fail | Rejected | Rejected in A | Yes, if eligible | `ASYNC_REGIONAL`: bounded by attested lag. `SYNC_REGIONAL`: none. `LOCAL_DURABLE`: unavailable, no failover | Lease expiry + fence wait |
| F4 | Primary–secondary network fails | Continue in A if C (or another approved replica) can still satisfy the mode | Continue | No (A is healthy to the quorum) | Replica B lags, bounded by `lag_breach_action` | B alone is not a quorum and cannot decide |
| F5 | Primary and secondary regions fail | Rejected | C: stale reads only where policy allows | **No** (quorum lost in MVP) | Break-glass only; may be large | Quorum required for any grant (INV-8) |
| F6 | Control-plane coordinator fails | Continue until lease expiry | Continue | Not during election or quiet period | None added | Quiet period; leases expire |
| F7 | All approved replication paths unavailable | Depends on mode, §3 F7 | Continue | Not a trigger | Lag grows without bound until the path returns | Replicator never uses unapproved paths (INV-2) |

## 2. Rules shared by all scenarios

1. A promotion needs the quorum to commit an `EpochGrant`. Nothing else can create an active region (INV-8).
2. A region that cannot renew its lease stops writing on its own, before the control plane can grant a new epoch (lease TTL < fence wait; ADR-0004).
3. A candidate is eligible only under INV-5. A stale or missing attestation means *unknown lag*, which means ineligible.
4. Policy cannot be waived by any operator action (INV-7).
5. Data written by a fenced epoch after the divergence point is quarantined, never merged (INV-9).
6. Recovery of a failed region requires a new replication generation for its replicas before it rejoins (reseed or truncate to `epoch_start_offsets`).

## 3. Scenario details

### F1 — One broker fails

| | MVP | Production |
|---|---|---|
| Region A (RF=3, ISR≥2) | Leaders on the failed broker move (seconds). `acks=all` continues with ISR=2. No data loss for acked writes. Gateway sees brief retriable errors. | Same, plus rack-aware placement. |
| Region B or C (1 broker) | Region fully unavailable. As *replica*: lag grows (see F7 behaviour for that destination). As *active* (after a promotion): writes fail, region is write-unavailable. Data remains on disk and returns on restart. | n/a: ≥3 brokers, behaves like Region A. |

- **Detection:** Gateway produce errors, KRaft controller metrics, broker-up gauge.
- **Automatic response:** Kafka re-elects leaders. Under-replicated partitions alarm.
- **RPO/RTO:** Region A: no RPO impact. RTO = Kafka leader election time. MVP B/C: RTO = broker restart time, no failover (replicas elsewhere may be promoted only through the F3 route).
- **Operator action:** Restore the broker. Confirm ISR recovered.

### F2 — Two brokers fail

| | MVP Region A | Production |
|---|---|---|
| Effect | ISR falls below 2 for partitions whose replicas were on both failed brokers. With 3 brokers and RF=3 that is every partition: only one in-sync replica remains. Produces fail. | Remaining ≥1 broker of 3+ may still satisfy ISR ≥ 2 for partitions placed elsewhere; partitions with ISR < 2 reject writes. |
| Gateway | Returns `LOCAL_DURABILITY_UNAVAILABLE` for affected partitions and stops renewing the lease after `write_unavailable_grace_s`. If the region is judged write-unavailable (majority of partitions affected), the gateway **voluntarily releases** the lease. | Same rule, evaluated per topic. |
| Reads | Served from surviving replicas. | Same. |

- **Detection:** Gateway write-availability probe, `NOT_ENOUGH_REPLICAS` rate.
- **Automatic response:** After `write_unavailable_grace_s`, if the control plane has an eligible candidate it promotes it. A voluntary release removes the fence wait. If no candidate is eligible, writes stay unavailable (INV-6).
- **Risk:** Acked `ASYNC_REGIONAL` writes in A that had not replicated are not lost from A's disk, but they are fenced out of the live log and quarantined when A returns (INV-9). Under `SYNC_REGIONAL`, none are lost.
- **RPO:** Bounded by the pessimistic lag bound at promotion. **RTO:** `T_detect` (grace) + decision; `T_fence_wait ≈ 0` with release.
- **Operator action:** Restore brokers. Reseed A under the new generation before A can serve as replica or candidate.

### F3 — All primary-region brokers fail

- **Detection:** Gateway in A cannot reach Kafka, or the whole region is isolated. The control plane sees missed lease renewals.
- **Automatic response:** The gateway rejects writes and does not renew. If it cannot talk to the control plane, the lease expires by itself (≤ `lease_ttl_ms`). The control plane waits `lease_expiry_upper_bound + max_clock_drift_ms`, then promotes `failover_priority[1]` if eligible, else the next, else none.
- **By mode:**
  - `SYNC_REGIONAL`: acked writes already in replica regions. The candidate must still have a fresh attestation, so a break from the attestation chain means an unknown lag and no automatic promotion.
  - `ASYNC_REGIONAL`: promotion only if the lag bound is within `max_tolerated_replication_lag_ms`. Otherwise the topic stays unavailable until A returns or break-glass waives the lag.
  - `LOCAL_DURABLE`: no replica, no failover. Unavailable until A returns. This is intended; it keeps data in-region.
- **RPO:** Bounded by the lag bound (ASYNC), zero for acked writes (SYNC), not applicable (LOCAL). **RTO:** `T_detect + T_fence_wait + T_decision + T_propagate`, up to about `2×lease_ttl_ms + max_clock_drift_ms + election time` in the worst case; configured via the tunables.
- **Operator action:** After A returns, replicas reseed under the new generation. Review `_ss.quarantine` for A's divergent suffix.

### F4 — Primary–secondary network fails (A↔B cut; both still reach C)

- **Detection:** Replicator connection errors, lag growth from A→B, replicator inter-region health probes.
- **Automatic response:**
  - A stays active. A and B both reach the quorum through C's member, so A keeps renewing its lease. B is a replica and cannot decide anything alone.
  - Replication A→B stops. Replication A→C continues only if that path is approved for the topic (INV-2); otherwise there is only a stall (F7 behaviour for the missing replicas).
  - `SYNC_REGIONAL`, `sync_remote_acks=1`: writes continue if some other approved replica can ack. Otherwise `REPLICATION_BACKPRESSURE`.
  - `ASYNC_REGIONAL`: writes continue until lag exceeds the maximum, then `lag_breach_action` (default `REJECT_WRITES` for `CONFIDENTIAL`/`RESTRICTED`).
  - B must not promote: it is not the quorum, and A's lease is valid.
- **Variant — A isolated from both B and C:** A cannot renew, self-fences after the lease TTL. The quorum (B, C) may then promote an eligible candidate after the fence wait. This is F3 in effect, with A still holding data.
- **RPO:** B's replica is stale by the outage length, so B is likely ineligible for promotion once lag exceeds the maximum. **RTO:** n/a (no failover).
- **Operator action:** Restore the link. Replicators resume from committed offsets with no duplicates (INV-10).

### F5 — Primary and secondary regions fail (A and B down; C up)

- **MVP (quorum 3, one per region):** Quorum is lost (1 of 3). The control plane commits nothing. No epoch can be granted.
- **Production (quorum 5 as A×2, B×2, C×1):** Quorum is also lost (1 of 5).
- **Automatic response:** None. C's gateway does **not** become active, even when C is the next candidate in `failover_priority`. Within one lease TTL every remaining lease in A and B has expired or been abandoned, so no region holds a valid lease. C serves reads of its replica where policy allows, and rejects writes (`NO_QUORUM`/`NOT_ACTIVE_REGION`). Reads from C are stale and flagged `X-SS-Stale: true`.
- **Why not auto-promote C:** C cannot distinguish "A and B are down" from "C is partitioned away from live A and B". A promotion would risk two leaders (INV-3).
- **Break-glass path (explicit and manual):**
  1. Operators confirm out-of-band that A and B are down or isolated, and fenced (power-off or network isolation evidence recorded).
  2. Dual-approved `BreakGlassGrant` with scope `WAIVE_LAG` plus a **forced quorum reconfiguration** of the control plane to the surviving member(s). Reconfiguration requires signed approvals from at least 2 distinct operators, and writes a `QuorumReconfig` audit entry. The reconfiguration increases a `cluster_incarnation` counter that is part of every epoch token, so any token minted by the pre-failure quorum is rejected by gateways that have seen the new incarnation.
  3. The promotion proceeds only for topics where C is in the policy (INV-5, INV-7). Topics whose policy excludes C stay unavailable. This is intentional (legal zone `EU`, for example, cannot move to C).
- **RPO:** Not bounded by the system. The estimated loss window is recorded for the approvers. **RTO:** Human-time.
- **Return of A/B:** They must self-fence on seeing `cluster_incarnation` higher than their cached value, and may not rejoin as active. Their divergent suffix is quarantined (INV-9). Their former leases cannot revalidate because a minted token carries the old incarnation.
- **Known residual risk:** If an operator forces reconfiguration while A is alive and isolated, A's gateway self-fences only after its lease TTL. So break-glass requires waiting `lease_ttl_ms + max_clock_drift_ms` after isolation evidence, before activating C. The runbook enforces this wait in the tool (the control plane refuses the grant sooner).

### F6 — Control-plane coordinator (Raft leader) fails

| Sub-case | Effect |
|---|---|
| Leader fails, quorum intact | Election in about one election timeout. Gateways keep their leases. Renewals pause briefly and succeed once a new leader exists, as long as the election completes within `lease_ttl_ms − lease_renew_interval_ms`. New leader enters the **quiet period** of `lease_ttl_ms + max_clock_drift_ms`: no promotion, no new-epoch grant. Renewals continue. |
| Election takes longer than the lease | Some gateways self-fence and reject writes (`LEASE_EXPIRED`) until the leader returns. That is the intended trade (availability for safety). |
| Quorum lost (≥ majority of members down or partitioned) | No renewals. Every gateway self-fences after its TTL. No promotion is possible. **No two leaders can exist** because no new lease can be minted and old ones expire. Reads continue. When quorum returns, the new leader waits the quiet period, then resumes. |
| A minority partition of control-plane members | The minority cannot commit. Components that talk only to the minority self-fence. |

- **Detection:** Raft health, renewal error rate, `control_plane_has_quorum` gauge.
- **RPO:** None added. **RTO:** Election time. If the quorum was lost: time to regain quorum plus quiet period.
- **Operator action:** Restore members. Do not force-reconfigure unless F5 conditions apply.

### F7 — All approved replication paths become unavailable

An "approved path" is a path satisfying INV-2 for the topic. This includes the case where the only available physical path crosses a disallowed transit zone, or an unapproved relay region.

- **Detection:** Replicator stream errors, lag growth, and `approved_paths_available = 0` gauge per (topic, destination).
- **Automatic response:**
  - The replicator **does not fall back** to an unapproved path. It keeps retrying approved ones. This is fail-closed (INV-2, INV-7).
  - `LOCAL_DURABLE`: no effect.
  - `ASYNC_REGIONAL`: writes continue until lag exceeds `max_tolerated_replication_lag_ms`, then `lag_breach_action`: `REJECT_WRITES` gives `REPLICATION_BACKPRESSURE`; `CONTINUE_ALERT` continues and flags the objective `BREACHED`.
  - `SYNC_REGIONAL`: writes rejected with `REPLICATION_BACKPRESSURE` immediately once `sync_remote_acks` cannot be met.
- **Not a promotion trigger:** The active region is healthy.
- **If the active region also fails during the outage:** Replicas are stale, so lag bound > maximum (or attestation stale), so no automatic promotion. Break-glass may waive lag but never policy.
- **RPO:** Grows with outage time until writes are rejected (`REJECT_WRITES`) or the objective is breached (`CONTINUE_ALERT`). **RTO:** n/a.
- **Operator action:** Restore an approved link, or change the policy and the link registry through the control plane (which re-validates INV-1/INV-2). Do not route around the registry.

## 4. Combined-failure notes

- F3 during F4: A is down and B cut off. Quorum of C plus (production) A's or B's other members decides. A candidate whose attestations are stale is skipped (INV-5).
- F6 during F3: The quiet period delays promotion by up to `lease_ttl_ms + max_clock_drift_ms`. This is intentional.
- Any failure while `auto_failover_enabled=false`: the same eligibility checks run, but the promotion is operator-initiated through the API.

## 5. What each failure does to the acceptance scenarios

Cross-reference to [ACCEPTANCE_SCENARIOS.md](ACCEPTANCE_SCENARIOS.md): AS-1 (F7, F4), AS-2 (F3, F4 variant), AS-3 (F3, F5), AS-4 (F3, F4, F7), AS-5 (F5, F6).


## Phase 6 note: what the implemented DR plane covers

The detailed Phase 6 behaviour, the quorum placement that supports "A and B lost" and the evidence level of each claim are in [DR.md](DR.md). In short: a lost broker is Kafka's job (Tier 1); a lost region is fenced, then the next eligible region is promoted through a compare-and-swap on a 5-member etcd (3 regions + 2 witnesses); without a quorum nothing is promoted and the active region's own gateways stop writing when their lease runs out; a restored old region stays fenced until an explicit, approved failback.
