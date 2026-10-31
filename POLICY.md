# SovereignStream — Policy Engine (Phase 3)

> **What this is, and is not.** The engine enforces the placement, processing and transit policies that *the organisation supplies and approves*. It does not interpret statutes or decide what the law requires. A wrong or incomplete policy is enforced faithfully. Reviewing policies for legal adequacy is outside the system.

Code: [policy/ss_policy/](policy/ss_policy/) (pure library), [policy/engine_service/](policy/engine_service/) (API + PostgreSQL), [policy/agent_service/](policy/agent_service/) (per-region agents). Tests: [policy/tests/](policy/tests/). Deployment: [DEPLOYMENT.md](DEPLOYMENT.md) §10.

## 1. Components

| Component | Role |
|---|---|
| `ss_policy.engine` | Pure functions, no I/O: deny-by-default evaluation, placement planning, ranking. Used by the engine and by every agent. |
| Engine service (`ss-policy-engine`, :8090) | Policy/registry CRUD, placement API, replica assignments, signed bundle, audit log. PostgreSQL is its only state. |
| Regional agent (`ss-{a,b,c}-agent`) | One per region. Holds its **own** verified copy of the policy state and re-validates every request that reaches its region. Creates replica topics in its own Kafka cluster. |
| Gateway | Asks the engine to approve every new topic and re-confirms RESTRICTED topics' policy on a short timer. |

## 2. Data model (Pydantic in `models.py`, PostgreSQL in `engine_service/schema.sql`)

| Table / model | Fields |
|---|---|
| `Region` | `region_id`, `legal_zone`, `availability_status` (`AVAILABLE`, `DEGRADED`, `UNAVAILABLE`, `DRAINING`), `storage_capacity {total_bytes, used_bytes}`, `processing_capabilities`, `supported_transit_zones`; plus `failure_domain`, `compute_headroom`, `max_replication_factor` (needed for hard technical limits and ranking). |
| `Link` | `from_region`, `to_region`, `transit_zones`, `latency_ms`, `status` (`UP`/`DOWN`). |
| `TopicPolicy` | `topic`, `classification`, `legal_zone`, `allowed_storage_regions`, `allowed_processing_regions`, `allowed_transit_zones`, `replication_factor`, `regional_replica_count`, `preferred_regions`, `excluded_regions`, `maximum_replication_lag_ms` (alias `maximum_replication_lag`), `durability_mode`, `policy_version`; plus `home_region` (where the topic is created and active, needed as the placement source). Every version ever in force is kept in `topic_policy_versions`. |
| `ReplicaAssignment` | `topic`, `partition`, `source_region`, `destination_region`, `replica_status` (`PENDING`, `PROVISIONED`, `SYNCING`, `IN_SYNC`, `FAILED`, `REVOKED`), `policy_version`, `assigned_at`. |
| `policy_decisions` | Audit log (§7). |

Field mapping to the Phase 0 design: `replication_factor` = `local_replication_factor`; `maximum_replication_lag_ms` = `max_tolerated_replication_lag_ms`; the ordered `failover_priority` is now the *output* of ranking (home first, then chosen replicas), steered by `preferred_regions`. `preferred_regions` never grants permission, and `excluded_regions` always vetoes.

## 3. Hard constraints first, optimisation second

Placement works in two stages and the second can only reorder what the first allowed.

**Stage 1: elimination (all must hold, otherwise the region is out and the reasons are recorded):**

| Category | Constraint (reason code) |
|---|---|
| Legal | Region is registered (`UNKNOWN_REGION`); not excluded (`REGION_EXCLUDED`); region's `legal_zone` equals the topic's, unless the topic is `GLOBAL` (`LEGAL_ZONE_MISMATCH`); in `allowed_storage_regions` (`REGION_NOT_ALLOWED_STORAGE`) or, for processing, `allowed_processing_regions` (`REGION_NOT_ALLOWED_PROCESSING`). |
| Legal (route) | A route exists whose every link's transit zones are within `allowed_transit_zones` (`NO_LINK`, `TRANSIT_ZONE_NOT_ALLOWED`, `NO_APPROVED_ROUTE`); any relay region may process the topic (`RELAY_REGION_NOT_ALLOWED`); at most 3 hops, no loops. |
| Technical | Not `UNAVAILABLE`/`DRAINING` (`REGION_UNAVAILABLE`); free capacity ≥ requested bytes (`INSUFFICIENT_CAPACITY`); `replication_factor` ≤ the region's limit (`RF_EXCEEDS_REGION_LIMIT`); link is `UP` and both ends support its zones (`LINK_DOWN`, `TRANSIT_ZONE_UNSUPPORTED`, `NO_AVAILABLE_ROUTE`); required capability present (`MISSING_CAPABILITY`). |

Network quality is **not** a constraint: a region 900 ms away that passes stage 1 stays eligible.

**Stage 2: ranking of the survivors** (weights are configurable; defaults): failure-domain independence 0.35, latency 0.25, storage pressure 0.15, configured preference 0.15, infrastructure headroom 0.10. Selection is greedy, so the second replica is penalised for sharing a failure domain with the source or the first. Ties break by region id, so results are deterministic. `ALLOWED` carries the chosen regions with their scores; if fewer than requested survive, the decision is `INSUFFICIENT_ELIGIBLE_REGIONS` and lists every eliminated region with reasons.

## 4. Evaluations

| Operation | Question answered | Notes |
|---|---|---|
| `STORE` | May this region hold the topic? | Legal + technical. |
| `PROCESS` | May this region run gateways/consumers for it? | Processing list + capability. |
| `ROUTE` | May this route carry it? | Per hop and relay. |
| `ASSIGN` | Is this replica assignment legal? | Source holds it legally, destination legal and usable, route approved. A stale `policy_version` is rejected. |
| `PROMOTE` | May this region become active? | Must be in the storage **and** processing lists, available, have the gateway capability, and hold a usable replica (`SYNCING`/`IN_SYNC`). This is the *policy* part of promotion. Replication-lag eligibility is judged separately, and neither can waive the other. |
| `BACKUP` | Is this backup location eligible? | Same as `ASSIGN` (storage rules, route), with the destination not required to be a replica region. |
| `PLAN` | Where should replicas go? | Stages 1 and 2. |
| `RECOVERY` | Where should replacement replicas go after a failure? | Same hard constraints, failed regions and existing replicas excluded. |
| `FAILBACK` | May the topic move back to its original region? | Judged against **today's** policy and registry, destination must be `AVAILABLE`. Only the destination and route are judged: failing back *out of* a region that has become illegal must be possible. |

Decisions: `ALLOWED`, `REJECTED`, `INSUFFICIENT_ELIGIBLE_REGIONS`, `POLICY_UNAVAILABLE`. Every non-`ALLOWED` decision has at least one reason (code, category, message, region) and a `policy_version`. For a create or update that fails validation, the version is the *proposed* one (`details.policy_version_kind: "proposed"`). A topic with no policy is `POLICY_UNAVAILABLE` (`POLICY_NOT_FOUND`), never "allowed".

## 5. API (engine, `/api/v1`, header `X-Service-Key`)

Roles: `service` (read, evaluate, bundle, audit), `writer` (also create policies and assignments), `admin` (also update policies, regions, links).

| Endpoint | Purpose |
|---|---|
| `POST /policies` | Create a policy. Validates, plans placement, stores v1 only if both succeed. |
| `GET /policies/{topic}` | Policy, version, history. |
| `PUT /policies/{topic}` | New version. `If-Match` must equal the current version (else 412). Live assignments that became **legally** illegal are `REVOKED` in the same transaction. |
| `POST /placement/evaluate` | Any operation above. Takes the caller's `policy_version`, enforcement point, requester and request id. |
| `GET /placement/{topic}` | Current plan and the current legality of every assignment. |
| `POST /placement/{topic}/assignments` | Plan and record assignments (`PENDING`). |
| `POST /placement/{topic}/provision` | Record assignments, then ask each destination's agent to create the replica topic. |
| `PUT /placement/{topic}/assignments/{partition}/{region}/status` | Advance a replica. Revoked or now-illegal replicas cannot advance. |
| `PUT /regions/{id}`, `PUT /links/{from}/{to}`, `GET /regions`, `GET /links` | Registry. A change that makes an assignment illegal revokes it. |
| `GET /bundle` | Ed25519-signed snapshot of registry and policies, with a monotonic `bundle_version`. |
| `GET /audit`, `GET /audit/verify`, `POST /audit/decisions` | Decision log, chain verification, agent submissions. |

Agent API (`/agent/v1`): `POST /authorize` (flow initialisation, backup, process, promotion, failback), `POST /batches` (per-batch admission), `POST /replicas` (validate, then create the topic), `GET /status`.

## 6. Enforcement points and versioning

| Point | What enforces it | Status |
|---|---|---|
| Topic creation | Gateway calls `POST /policies` **before** creating anything in Kafka. Engine unreachable means `503 POLICY_UNAVAILABLE`. | Implemented, tested |
| Regional replica creation | `provision` records assignments, then the destination's agent re-validates against its own bundle and only then creates the topic. | Implemented, tested (topic really created in cluster B, never in C) |
| Replication flow initialisation | Agent `POST /authorize` (`enforcement_point: FLOW_INIT`) registers the flow under the current policy version. | Endpoint implemented, tested. **No replicator calls it yet** (Phase 4) |
| Every replication batch | Agent `POST /batches` repeats the full assignment check per batch, and refuses a flow admitted under an older policy version (`STALE_FLOW`) or a sender on an older version (`STALE_POLICY_VERSION`). | Endpoint implemented, tested. Same caveat |
| Recovery destination selection | `RECOVERY` evaluation. | Implemented, tested. Nothing calls it automatically yet |
| Failback | `FAILBACK` evaluation, also exposed by the agent. | Implemented, tested. Same caveat |
| Promotion, backup, processing | `PROMOTE`, `BACKUP`, `PROCESS` on the engine and on the receiving agent. Gateways also check processing on every read and write. | Implemented, tested |

**Independent receiving-side validation.** An agent never trusts the sender's verdict: it checks its own verified bundle (signature, monotonic `bundle_version`). The code is shared with the engine, but the state is independent, so a stale or compromised sender cannot widen what the receiver accepts.

**Staleness (fail closed).** Each agent measures the age of its bundle on its own monotonic clock. Limits: `RESTRICTED` 30 s, `CONFIDENTIAL` 120 s, `INTERNAL` 600 s, `PUBLIC` 3600 s; past the limit, decisions are `POLICY_UNAVAILABLE`. A sender on a *newer* version than the agent also gets `POLICY_UNAVAILABLE` (the agent is behind), and on an *older* one `REJECTED`. The gateway confirms RESTRICTED topics with the engine every `SS_RESTRICTED_POLICY_TTL_S` (10 s in the dev stack), refreshing synchronously; failure means `503 POLICY_UNAVAILABLE`. Other classes use the cached policy and refresh opportunistically (every 5 min).

## 7. Audit trail

Every evaluation, policy create/update, assignment, revocation and registry change is written to `policy_decisions` (source, operation, enforcement point, topic, policy version, decision, every reason, requester, request id). Properties:

- **Metadata only.** Request models reject unknown fields, so a payload cannot be submitted, and `subject` is filtered through a key whitelist. Tests assert both.
- **Append-only** for the application: database triggers refuse UPDATE, DELETE and TRUNCATE.
- **Hash-chained:** each row's hash covers the previous one. `GET /audit/verify` recomputes the chain. A test shows that a superuser bypassing the trigger is detected.
- **Engine decisions are written in the same transaction** as the decision and fail closed (no audit row, no answer). **Agent decisions** are sent synchronously (flow, replica, promotion) or queued and flushed every second (batches, for volume). Agents also log every decision to stdout.

## 8. Limitations

- **No replicator exists yet.** Flow-initialisation and batch-admission endpoints are built and tested with simulated workers. Phase 4 connects the real ones.
- **Revocation does not delete data.** A revoked replica is marked and refused further use; physically purging it needs a deletion workflow that is not built.
- **Propagation window.** Agents refresh the bundle every 5 s, so after a policy change a stale worker may still be admitted for up to that long. Gateways confirming RESTRICTED topics: up to the TTL.
- **Single engine, single PostgreSQL.** No HA. When either is down, everything fails closed after the staleness limits.
- **Audit tamper-resistance:** the trigger stops the application, but the dev PostgreSQL role is a superuser who can bypass it; the hash chain *detects* that. External anchoring of the chain head and a least-privilege DB role are production work.
- **Agent state is in memory** (bundle, flows). After an agent restart, workers must re-initialise flows.
- **Two sources of truth in dev:** the gateway still validates against its own `registry.json`, and the engine against its database. Both must allow a topic. Aligning them is a later cleanup.
- **A topic created in the engine but failing in Kafka** leaves an orphan policy (the name stays blocked).
- **Integration tests create uniquely-named regions** in the dev database and do not remove them. `down.sh --volumes` resets.
- Engine and agent API keys are static; `chmod 644` dev keys for the bundle signing key are development-only.
