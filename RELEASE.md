# SovereignStream — Release statement

What this release is, and what it is not. Read it before quoting anything from the other documents.

## What was built and verified (tested)

Verified by the automated suite and the recorded campaign in [TEST_RESULTS.md](TEST_RESULTS.md), [SCENARIOS.md](SCENARIOS.md), [PERFORMANCE.md](PERFORMANCE.md) and [SOAK.md](SOAK.md), **on one machine**:

* Three independent Kafka clusters with distinct cluster ids; region A with 3 brokers + 3 controllers (RF 3 / min ISR 2 / `acks=all` verified for local topics: one broker down keeps writing, two down refuses writes).
* An HTTP gateway (not Kafka-compatible) with idempotency, write leases measured on a monotonic clock, epoch checks and backpressure.
* Deny-by-default residency policy with reasons, versions and a hash-chained audit log; independent re-checks at the receiving agent and a database guard.
* Checkpointed, at-least-once replication A→B and A→C with ordering gate, retry, and cutover after a promotion; adaptive routing with eligibility before score.
* A coordinator-backed failover: fencing (coordinator + lease wait + Kafka-level topic fence), deterministic eligibility, A→B and B→C promotion (also A and B lost together, authorised by the witness quorum), refusal without quorum, refusal for stale replicas, idempotent resume after a controller crash, controlled failback.
* A dashboard whose active region and epoch come from the coordinator, with stale-data indicators, operator-only administrative controls, and no payloads or credentials in its output.

## Claims this release does NOT make

| Not claimed | Why |
|---|---|
| **Zero data loss** | Replication is asynchronous. Writes acknowledged by the active region and not yet replicated are lost from the live history when that region is lost; measured gaps are reported (0 in the clean runs, 94 events when replication was cut first). Synchronous multi-region durability (`SYNC_REGIONAL`) is **not implemented** |
| **Exactly-once delivery** | Replication is at-least-once; the receiver and consumers rely on `event_id` de-duplication, and duplicates were observed and counted in tests |
| **Production availability** | Everything ran on one laptop-class machine in one Docker VM: no real WAN, no host or disk failures, no clock skew, no sustained load beyond minutes |
| **Three-tier broker redundancy** | Only region A has multiple brokers. **Regions B and C each have a single broker with replication factor 1.** Tier 1 (native broker failover) exists in A only; the multi-region tiers protect against losing a *region*, not a broker in B or C |
| **Safe automatic failover in general** | Automatic failover is safe *only while* the coordinator has quorum and fencing works, which was tested for the listed scenarios. The coordinator here has no TLS/authentication, and its independence (witnesses) is simulated by containers |
| That the policy engine decides what the law is | It enforces what the operator declared in the registry (POLICY.md) |

## Tested, simulated, future

| | Tested (automatic tests, this machine) | Simulated (behaves like the real thing only to a degree) | Future production work |
|---|---|---|---|
| Regions | failure of a whole region, of a broker, restart of every component | "regions" are containers on one host and one Docker network each; "fault domains" are containers | real sites, independent power/network/cloud accounts, distance |
| Network | latency, bandwidth, disconnects, resets (Toxiproxy), isolation of one gateway from the coordinator | packet loss is emulated with connection resets; relays are proxies to the same receiver | real WAN behaviour, MTU, BGP, partial partitions, asymmetric failure |
| Coordinator | quorum loss, witness-only quorum, leader election, CAS races, controller crash | five etcd containers; two "independent witnesses" | independent witness sites, mTLS and RBAC, break-glass reconfiguration, etcd backup/restore drills |
| Security | tokens/roles for admin operations, payload and credential scrubbing, gateway-only Kafka access, signed policy bundles | plain-text Kafka and etcd inside Docker networks | TLS everywhere, Kafka ACLs/SASL, KMS-held keys, key rotation, OIDC with MFA, envelope encryption |
| Durability | `acks=all` with RF 3 in A, checkpoints after destination acknowledgement | disk sync behaviour of Docker Desktop | `SYNC_REGIONAL`, fsync policy, capacity and disk-usage monitoring |
| Scale | 3 partitions, up to a few hundred events/s from one load generator, minutes to tens of minutes | | multi-tenant volumes, many topics/partitions, hours-to-days soak, capacity planning |
| Operations | the runbook's tested procedures | | alerting rules, dashboards beyond the provided one, on-call process, upgrade and rollback drills, backup/restore drills |

## Known limitations (consolidated)

* Replica topics use RF 1 in A as well, because B and C cannot hold more (policy rule V5); only local-only topics get RF 3.
* The failover is region-level and all-or-nothing: one topic that has no eligible target blocks the region's promotion. Topics created while a non-home region is active are not supported.
* The lag bound used for the RPO check has about 10 s resolution; budgets below ~15 s cannot be certified and are refused (DR.md §9.3).
* Failback re-seeds replicas (fewer replicas than policy for a while) and a switch that cannot finish stays fenced until an operator acts.
* Idempotency state is per region: a client retry crossing a failover can create a duplicate.
* The dashboard's authentication is static tokens in `sessionStorage`; no planned (healthy-region) switchover exists.
* See also ROUTING.md §5, REPLICATION.md, DR.md §9.3, DASHBOARD.md §8, OPERATIONS.md §11 for the untested procedures.
