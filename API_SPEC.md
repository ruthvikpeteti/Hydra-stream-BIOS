# SovereignStream — API Specification

Design-level contract. Field names match [DATA_MODEL.md](DATA_MODEL.md). Invariants INV-n are in [ARCHITECTURE.md §3](ARCHITECTURE.md). Security is in [SECURITY.md](SECURITY.md). Transport shown as HTTP/JSON for readability; a gRPC mapping for the data and control APIs may be added in a later phase without changing semantics.

> **Phase 2 status.** The gateway implemented so far uses simpler paths than this design document: `POST /api/v1/topics`, `POST /api/v1/events` (topic in the body), `GET /api/v1/events/{topic}`, `GET /api/v1/topics/{topic}`, `GET /api/v1/active-region`, `GET /api/v1/health`, plus `PUT /api/v1/consumers/{id}/offsets`. Error codes, check order and headers follow this spec. See [GATEWAY.md](GATEWAY.md) for the implemented contract. The control-plane, replication and monitoring APIs below are not implemented yet.

## 1. Conventions

- Base paths: Gateway `/v1`, Control plane `/cp/v1`, Monitoring `/mon/v1`. All over TLS (mTLS for service-to-service).
- Auth: clients and operators use OIDC bearer tokens. Services use mTLS workload identity (SECURITY §3).
- Content type `application/json`. Binary fields (`payload`, `partition_key`) are base64.
- Mutations of policy/state require `If-Match: <revision>`. A mismatch gives `412 PRECONDITION_FAILED`.
- Idempotency: produce uses `event_id` (or `Idempotency-Key`) as the idempotency key. Duplicate submissions return the original result.
- Time: epoch milliseconds (UTC). Lease lifetimes are TTLs, never absolute times (SECURITY §4).

### 1.1 Response headers on gateway data endpoints

| Header | Values | Meaning |
|---|---|---|
| `X-SS-Region` | region id | Region that served the request. |
| `X-SS-Region-Role` | `active` / `passive` | Role of that region for the topic. |
| `X-SS-Epoch` | uint64 | Epoch known to the gateway for the topic. |
| `X-SS-Stale` | `true` | Reads served while quorum is unavailable or the replica lag is unknown. |

### 1.2 Error model

```json
{ "error": { "code": "STALE_EPOCH", "message": "…", "topic": "payments.eu.v1",
             "details": { "current_epoch": 7, "active_region": "region-b" }, "retryable": false } }
```

| HTTP | `code` | Meaning | Retryable |
|---|---|---|---|
| 400 | `ENVELOPE_INVALID` | Malformed request or envelope fields. | No |
| 401/403 | `UNAUTHENTICATED` / `FORBIDDEN` | Auth or RBAC failure. | No |
| 403 | `POLICY_VIOLATION` | Request would place, process, transit or promote outside policy (INV-1, 2, 5, 7). | No |
| 404 | `NOT_FOUND` | Unknown topic/region. | No |
| 409 | `STALE_EPOCH` | Supplied `expected_epoch` ≠ the current epoch, or a stale token or record was presented. | No |
| 409 | `PROMOTION_INELIGIBLE_POLICY` | Candidate violates INV-5 policy checks. Not waivable. | No |
| 409 | `PROMOTION_INELIGIBLE_LAG` | Candidate lag bound exceeds `max_tolerated_replication_lag_ms`. Waivable only by break-glass. | No |
| 409 | `PROMOTION_INELIGIBLE_ATTESTATION_STALE` | Attestation missing or older than `attestation_ttl_ms`. Waivable only by break-glass. | After fresh attestation |
| 409 | `PROMOTION_IN_PROGRESS` | Another promotion for the topic is active. | After completion |
| 412 | `PRECONDITION_FAILED` | `If-Match` or `from_epoch` compare-and-set failed. | After re-read |
| 421 | `NOT_ACTIVE_REGION` | This region is not active for the topic. `details.active_region` gives the hint. | Redirect |
| 422 | `POLICY_INVALID` | Policy fails validation V1–V10 (DATA_MODEL §2.3). `details.rule` names the failing rule. | No |
| 503 | `LEASE_EXPIRED` | The gateway's write lease is not valid. The region is self-fenced. | Yes, with backoff |
| 503 | `NO_QUORUM` | Control plane cannot commit (no quorum). | Yes |
| 503 | `LOCAL_DURABILITY_UNAVAILABLE` | Local Kafka cannot satisfy `local_min_insync_replicas`. | Yes |
| 503 | `REPLICATION_BACKPRESSURE` | Mode requires remote acks that cannot be obtained, or lag breach with `REJECT_WRITES`. | Yes |
| 503 | `FENCE_WAIT_REQUIRED` | Grant refused because the fence wait or quiet period has not elapsed. `details.retry_after_ms` is set. | After wait |
| 429 | `RATE_LIMITED` | | Yes |

Every rejection that stems from a policy or epoch check is counted in metrics by `code`.

## 2. Resource model

| Resource | Where | Notes |
|---|---|---|
| Topic policy | control plane | DATA_MODEL §2 |
| Topic state | control plane | `active_region`, `leadership_epoch`, `replication_generation`, `state` |
| Write lease / epoch token | control plane → gateway | Issued only by quorum. |
| Attestation | replicator → control plane | Signed, sequenced. |
| Promotion | control plane | `PromotionIntent` lifecycle |
| Break-glass grant | control plane | Waives lag only |
| Objectives | control plane | ARCHITECTURE §8.1 |
| Region/link registry | control plane | DATA_MODEL §1 |

## 3. Application gateway API (`/v1`)

### 3.1 Produce

`POST /v1/topics/{topic}/events`

Request:

```json
{
  "event_id": "018f…",              // optional UUIDv7; generated if absent
  "partition_key": "b3JkZXItMTIz",  // base64, optional
  "payload": "eyJhbW91bnQiOjEyfQ==",
  "client_timestamp": 1760000000000,
  "expected_epoch": 7               // optional fencing precondition
}
```

Response `200`:

```json
{ "event_id": "018f…", "topic": "payments.eu.v1", "origin_region": "region-a",
  "leadership_epoch": 7, "replication_generation": 9,
  "source_cluster": "kafka-a", "source_partition": 4, "source_offset": 18211,
  "durability_mode": "SYNC_REGIONAL", "durable_regions": ["region-a", "region-b"] }
```

`durable_regions` lists regions that confirmed durability at ack time. Under `ASYNC_REGIONAL` it lists only the active region.

Check order (first failure wins):

1. Authn/authz → `UNAUTHENTICATED`/`FORBIDDEN`.
2. Gateway region ∈ `allowed_processing_regions` → else `POLICY_VIOLATION`.
3. Bundle valid and unexpired → else `POLICY_VIOLATION` (fail closed, `details.reason = "bundle_expired"`).
4. This region is active for the topic → else `NOT_ACTIVE_REGION`.
5. Lease valid on the monotonic clock → else `LEASE_EXPIRED`.
6. `expected_epoch` (if given) equals the lease epoch → else `STALE_EPOCH`.
7. Local produce → else `LOCAL_DURABILITY_UNAVAILABLE`.
8. Durability mode requirements → else `REPLICATION_BACKPRESSURE`.

A client cannot supply `leadership_epoch`, `replication_generation`, or `origin_region`. Any such fields are ignored and logged. Only the gateway stamps them (INV-3, INV-4).

Batch: `POST /v1/topics/{topic}/events:batch` (same checks per request, all-or-nothing per partition).

### 3.2 Consume

`GET /v1/topics/{topic}/events?partition=&offset=&max=&wait_ms=`

- Allowed in a region only if the region ∈ `allowed_processing_regions` (else `POLICY_VIOLATION`).
- In a passive region, responses carry `X-SS-Region-Role: passive` and return data replicated so far. If the control plane is unreachable or the lag is unknown, `X-SS-Stale: true`.
- Returns envelopes in the replicated form (with `source_*`).
- Consumer groups are regional. Offsets are per region. Offset translation across regions uses `GET /v1/topics/{topic}/offset-translation?from_region=&partition=&offset=` backed by `_ss.offset_map`.

### 3.3 Topic info

`GET /v1/topics/{topic}` returns the policy summary the caller may see, active region, epoch, role of this region, and lease TTL remaining. Read-only mirror, not authoritative.

### 3.4 Health

`GET /v1/health` returns `lease_state` per topic (`VALID`/`EXPIRED`/`NOT_ACTIVE`), `bundle_expires_in_ms`, `local_kafka` status.

## 4. Control-plane API (`/cp/v1`)

All mutations are Raft-committed and audited (INV-8). RBAC roles from SECURITY §2 (B7).

### 4.1 Topics and policy

| Method / path | Purpose | Notes |
|---|---|---|
| `POST /cp/v1/topics` | Create topic with `TopicPolicy`. | Validates V1–V10. Creates `TopicState` with epoch 1, generation 1, `active_region = failover_priority[0]`, and the first lease on request by that region's gateway. |
| `GET /cp/v1/topics/{topic}` | Policy + state. | |
| `PUT /cp/v1/topics/{topic}/policy` | Update policy. `If-Match` required. | Must pass V1–V10. A change that would store data in a new region triggers a reseed plan. Placement never precedes the validation of INV-1 and INV-2. Narrowing a policy (removing a region) triggers deletion of the replica and a signed audit entry. |
| `GET /cp/v1/topics/{topic}/state` | `TopicState`. | |
| `GET /cp/v1/bundle` | Signed policy bundle. | Supports `If-None-Match` revision. Response signature `kid-bundle`. |
| `GET/PUT /cp/v1/objectives` | RPO/RTO objectives. | ARCHITECTURE §8.1. Status read from `/mon`. |
| `GET/PUT /cp/v1/registry/regions`, `/registry/links` | Region and link registry. | `policy-admin` + dual approval for changes that add legal zones or links. |

`PUT policy` error example for AS-1:

```json
{ "error": { "code": "POLICY_INVALID", "details": { "rule": "V4", "region": "region-c" } } }
```

### 4.2 Leases and epoch tokens (called by gateways)

`POST /cp/v1/leases:acquire`

```json
{ "topic": "payments.eu.v1", "region": "region-a", "instance": "gw-a-1", "nonce": "…", "expected_epoch": null }
```

- Succeeds only if `TopicState.active_region = region` and state is `ACTIVE`. Returns the signed epoch token with `ttl_ms`.
- If another region is active: `421 NOT_ACTIVE_REGION`. If a promotion is in progress: `409 PROMOTION_IN_PROGRESS`. If it is the new leader's quiet period or a fence wait: `503 FENCE_WAIT_REQUIRED`.

`POST /cp/v1/leases:renew` — same body plus the current token id. Renewal with a lower epoch than `TopicState.leadership_epoch` gives `409 STALE_EPOCH`. Renewals are batched per region (one request carries many topics) to limit Raft write volume.

`POST /cp/v1/leases:release` — voluntary release by an active region that is write-unavailable. The body may include `final_source_head` per partition (DATA_MODEL §6). Committed release removes the fence wait.

Token body (signed): `{topic, region, epoch, generation, policy_revision, cluster_incarnation, ttl_ms, nonce, kid}`.

### 4.3 Attestations (called by replicators)

`POST /cp/v1/attestations`

```json
{ "topic": "payments.eu.v1", "region": "region-b", "sequence": 88123, "generation": 9,
  "partitions": [ { "partition": 4, "replicated_source_offset": 18205,
                    "source_head_offset": 18211, "source_head_ts": 1760000000500,
                    "replicated_event_ts": 1760000000400 } ],
  "signature": "…" }
```

The control plane commits it with `reported_at_cp_ms` set by itself. A non-increasing `sequence` or a mismatching `generation` is rejected.

### 4.4 Promotion

| Method / path | Purpose |
|---|---|
| `POST /cp/v1/topics/{topic}/promotions` | Request promotion (planned switchover, or manual when auto-failover is off). Body: `{ "candidate_region": "region-b", "from_epoch": 7, "reason": "…" }`. Evaluated by the engine, never trusted. |
| `GET /cp/v1/topics/{topic}/promotions/{id}` | Intent state and the `eligibility_snapshot`. |
| `POST /cp/v1/topics/{topic}/promotions/{id}:abort` | Abort before the epoch grant is committed. |

Promotion evaluation (same code path as the automatic trigger):

1. `from_epoch` equals the current epoch, else `412 PRECONDITION_FAILED`.
2. Candidate satisfies INV-5 policy conditions, else `409 PROMOTION_INELIGIBLE_POLICY`.
3. Attestations fresh, else `409 PROMOTION_INELIGIBLE_ATTESTATION_STALE`.
4. `lag_upper_bound_ms ≤ max_tolerated_replication_lag_ms`, else `409 PROMOTION_INELIGIBLE_LAG`.
5. Fence satisfied (release committed, or expiry bound + drift passed), else `503 FENCE_WAIT_REQUIRED`.
6. Commit `EpochGrant` (epoch+1, generation+1).

Automatic promotion runs steps 2–6 for each candidate in `failover_priority` order. The first success wins; if none, the topic state becomes `UNAVAILABLE` and an alert fires.

### 4.5 Break-glass

| Method / path | Purpose |
|---|---|
| `POST /cp/v1/break-glass` | Request. Body: `{ "topic", "scope": "WAIVE_LAG", "candidate_region", "reason", "estimated_loss_window_ms" }`. Requester must hold `operator`. |
| `POST /cp/v1/break-glass/{id}:approve` | Approve. Role `break-glass-approver`. Approvers must be distinct and not the requester. At least 2 required. |
| `POST /cp/v1/break-glass/{id}:execute` | Executes the promotion with `override` set. Policy checks still run and `PROMOTION_INELIGIBLE_POLICY` still applies. |
| `POST /cp/v1/quorum:force-reconfigure` | F5 only. Requires 2 approvals and isolation evidence age ≥ `lease_ttl_ms + max_clock_drift_ms`, else `FENCE_WAIT_REQUIRED`. Bumps `cluster_incarnation`. |

There is no `scope` that waives policy. Requests naming one are rejected with `POLICY_VIOLATION`.

### 4.6 Audit

`GET /cp/v1/audit?from_seq=` returns the hash-chained entries. Role `auditor`.

## 5. Replication-plane internal interface

Not a public API. Replicators expose a local admin endpoint (`/repl/v1`) and talk to the control plane.

| Interaction | Direction | Purpose |
|---|---|---|
| `GET /cp/v1/bundle` | replicator → control plane | Policy bundle. |
| `POST /cp/v1/attestations` | replicator → control plane | Progress. |
| `GET /repl/v1/streams` | monitoring → replicator | Per (topic, destination): `path_approved`, `state`, `lag_ms`, `last_offset`. |
| `GET /repl/v1/streams/{topic}/{dest}` | | Details. |

Admin endpoints are read-only. There is no endpoint that can add a destination or path. Streams exist only because the bundle approves them.

## 6. Monitoring API (`/mon/v1`, read-only)

| Path | Content |
|---|---|
| `GET /mon/v1/topics` | Per topic: active region, epoch, generation, state, lease TTL remaining, replica lag and attestation age per region, objective status. |
| `GET /mon/v1/regions` | Region health, broker counts, ISR status, control-plane member status. |
| `GET /mon/v1/quorum` | Control-plane membership, leader, term, `cluster_incarnation`. |
| `GET /mon/v1/violations` | Counts and recent samples of `POLICY_VIOLATION`, `STALE_EPOCH` rejections and quarantine entries. |
| `GET /mon/v1/events` (SSE) | Live promotions, lease expirations, break-glass events. |
| `GET /metrics` | Prometheus metrics (§6.1). |

### 6.1 Core metrics

`ss_lease_ttl_remaining_ms{topic,region}`, `ss_active_epoch{topic}`, `ss_replication_lag_ms{topic,dest}`, `ss_attestation_age_ms{topic,region}`, `ss_lag_upper_bound_ms{topic,region}`, `ss_approved_paths_available{topic,dest}`, `ss_rejections_total{code}`, `ss_quarantined_records_total{topic}`, `ss_control_plane_has_quorum`, `ss_objective_status{scope,kind}` (0=MET, 1=BREACHED, 2=UNKNOWN), `ss_promotions_total{result}`.

## 7. Versioning

Path versioning (`/v1`). Envelope has `envelope_version`. Additive changes only within a version. Unknown fields in requests are rejected on control-plane APIs and ignored-and-logged on the produce API.
