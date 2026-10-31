# SovereignStream — Application Gateway (Phase 2)

An **HTTP/JSON gateway in front of Kafka**, built with FastAPI and `aiokafka`. It is the only way applications produce or consume ([ADR-0006](docs/adr/0006-gateway-sole-entry-point.md)).

> **Not Kafka-compatible.** The gateway does not speak the Kafka wire protocol. Kafka clients (`librdkafka`, the Java client, `kafka-console-*`) cannot connect to it, and it does not expose consumer groups, rebalancing, transactions, or the Admin API. Applications use the HTTP API below. Kafka itself is unreachable from clients by design.

Code: [gateway/app/](gateway/app/). Tests: [gateway/tests/](gateway/tests/). Run with the rest of the stack: [DEPLOYMENT.md](DEPLOYMENT.md) §9.

## 1. Components

| Component | File | Role |
|---|---|---|
| Producer manager | `producer.py` | One long-lived idempotent producer, `acks=all`, flushed on shutdown. Maps Kafka errors to API errors. |
| Consumer manager | `consumer.py` | Short-lived, always-stopped consumers per read (`assign` + `seek`, no rebalancing), concurrency-limited by a semaphore. |
| Topic metadata service | `topics.py` | Creates Kafka topics and stores each topic's policy in the compacted topic `_ss.topic-policy` (cached, reloaded on a miss). |
| Active-region resolver / lease | `leases.py` | Per-topic active region, epoch, generation, lease validity. Dev stand-in for the control plane (§5). |
| Event validation | `models.py`, `events.py` | Required stable event ID and partition key, size limits, server-stamped fields. |
| Idempotency | `idempotency.py`, `events.py` | §4. |
| Policy rules | `registry.py` | Rules V1–V10 from [DATA_MODEL §2.3](DATA_MODEL.md), identical to what the control plane will enforce. |

Everything runs on the asyncio loop. Kafka I/O is natively async, and the only blocking call (reading the control-state file) runs in a worker thread.

## 2. API

Base: `http://localhost:8080/api/v1`. All endpoints except `health` need `X-API-Key` (when `SS_API_KEYS` is set; with it empty, authentication is off and the gateway logs a warning). Errors use the shape in [API_SPEC §1.2](API_SPEC.md).

| Method / path | Purpose |
|---|---|
| `POST /topics` | Create a topic with a policy. Body: `name`, `partitions` (default 3), any policy field from DATA_MODEL §2.1. Defaults: INTERNAL, `LOCAL_DURABLE`, RF 3, min ISR 2, stored and processed in this region only. Returns `201`. |
| `GET /topics/{topic}` | Policy, partition start/end offsets, active region, epoch, generation. |
| `POST /events` | Publish one event. |
| `GET /events/{topic}` | Read events (offset-addressed, resumable). |
| `PUT /consumers/{consumer_id}/offsets` | Acknowledge: store next offsets per partition for this consumer ID. |
| `GET /active-region[?topic=]` | Active region, role of this region, epoch, generation, lease validity and remaining TTL. |
| `GET /health` | `200` only if Kafka is reachable, the producer runs, and the lease source is fresh; else `503`. No broker details are included. |

### Publish

```json
POST /api/v1/events
Idempotency-Key: 7c1f...            (optional)
{ "topic": "payments", "event_id": "018f2c1e-5b7a-7c3d-9e21-0a4b6c8d1e2f",
  "partition_key": "account-42", "payload": {"amount": 12},
  "timestamp": 1760000000000,        (optional epoch ms; default: gateway time)
  "expected_epoch": 3 }              (optional fencing precondition)
```

Response `200`:

```json
{ "event_id": "…", "topic": "payments", "origin_region": "region-a", "origin_cluster": "kafka-a",
  "leadership_epoch": 3, "replication_generation": 3, "event_timestamp": 1760000000000,
  "source_cluster": "kafka-a", "source_partition": 1, "source_offset": 18211,
  "durability_mode": "LOCAL_DURABLE", "durable_regions": ["region-a"] }
```

Required: `event_id` (a UUID, stable across retries) and a non-empty `partition_key` (≤ 256 bytes). `payload` is any non-null JSON value (≤ 1 MiB). `leadership_epoch`, `replication_generation`, `origin_region`, `origin_cluster` and `source_*` are stamped by the gateway; if a client sends them they are ignored and logged.

**Check order** (first failure wins): authentication (401) → topic exists (404) → this region may process the topic (403 `POLICY_VIOLATION`) → idempotent replay → this region is active (421 `NOT_ACTIVE_REGION`) → lease valid (503 `LEASE_EXPIRED`) → `expected_epoch` equals the current epoch (409 `STALE_EPOCH`) → durability mode supported (503 `REPLICATION_BACKPRESSURE`) → Kafka write (503 `LOCAL_DURABILITY_UNAVAILABLE`). Responses carry `X-SS-Region`, `X-SS-Region-Role`, `X-SS-Epoch`.

### Read and resume

`GET /events/{topic}?partition=&offset=&limit=&wait_ms=&consumer_id=&commit=`

- `partition` + `offset` read from that offset. `offset` without `partition` is a 400. An offset outside the retained range is `400 OFFSET_OUT_OF_RANGE` (no silent reset).
- The response has `events`, `next_offsets` (per partition), `end_offsets`, `has_more`. **Resume by passing a returned `next_offsets` value as the next `offset`.** Each event carries the stored envelope plus `source_cluster`, `source_partition`, `source_offset`.
- Without `partition`, all partitions are read from the start (or from `consumer_id`'s stored offsets) and results are grouped per partition.
- **Server-side tracking:** with `consumer_id`, and no `offset`, reads start from that ID's stored offsets (stored in Kafka as group `ss-gw.<consumer_id>`). Offsets move only when you call `PUT /consumers/{id}/offsets` (acknowledge after processing, giving at-least-once) or pass `commit=true` (stored when the response is built, so a client that loses the response skips events: at-most-once).
- Reads use `read_committed`. `wait_ms` (≤ 30 s) long-polls for new data. Reads in a passive region or without a fresh control state are allowed and marked (`X-SS-Region-Role: passive`, `X-SS-Stale: true`).

## 3. Partitioning, ordering, durability

- **Partitioning:** the Kafka record key is the `partition_key`, so Kafka's default murmur2 partitioner maps equal keys to the same partition. A test recomputes murmur2 and checks the partition of 1,000 events.
- **Ordering:** guaranteed per partition: the idempotent producer keeps send order and each request waits for its ack. A client that sends events with one key sequentially (waiting for each response) gets increasing offsets in that order. Concurrent requests with the same key have no defined relative order. There is no ordering across partitions.
- **Durability:** every write uses `acks=all`, and the topic's own `min.insync.replicas` (from `local_min_insync_replicas`) sets how many replicas must hold the record. The local primary cluster defaults to RF 3 / min ISR 2. If fewer replicas are in sync, the write fails with `503 LOCAL_DURABILITY_UNAVAILABLE` (retryable). Durability modes: `LOCAL_DURABLE` and `ASYNC_REGIONAL` are accepted. `SYNC_REGIONAL` writes are refused with `REPLICATION_BACKPRESSURE` because remote acks cannot exist yet. Even `ASYNC_REGIONAL` has no actual replication until the replication plane exists, so it is only a policy label today.
- **MVP consequence of one RF per policy:** B and C hold RF 1, so a topic with a replica in B or C must set `local_replication_factor: 1` (rule V5). Such a topic then has RF 1 in A too, which is a real cost of the MVP profile.

## 4. Idempotency contract

Two identities are deduplicated: the optional `Idempotency-Key` header (the request) and `event_id` (the event). Records are kept for 24 h (`SS_IDEMPOTENCY_TTL_S`) in memory and in the compacted Kafka topic `_ss.idempotency`, so they survive gateway restarts.

| Request | Result |
|---|---|
| New key / new `event_id` | Written once, `200`. |
| Same `Idempotency-Key`, same content | Original response returned again (`200`, `Idempotent-Replay: true`, `"idempotent_replay": true`). Nothing written. |
| Same `Idempotency-Key`, different content | `409 IDEMPOTENCY_KEY_REUSED`. Nothing written. |
| Same `event_id`, same content (any or no key) | Replay of the original, as above. |
| Same `event_id`, different content | `409 EVENT_ID_CONFLICT`. Nothing written. |

"Content" is `topic`, `event_id`, `partition_key`, `payload`, and `timestamp` if supplied. `expected_epoch` is a precondition and is not part of it. Concurrent duplicates are serialized in-process, so exactly one write happens.

Honest limits: (1) if the gateway crashes after Kafka acknowledged an event but before its idempotency record was stored, a retry writes a second copy; consumers should deduplicate by `event_id`. (2) A `503` with `"outcome": "unknown"` may or may not have written the event; retrying with the same `event_id` can duplicate it for the same reason. (3) Deduplication is per gateway process, so run one instance until a shared store exists.

## 5. Active region, lease and epoch (Phase 2 stand-in)

The real control plane does not exist yet. A JSON file, `infrastructure/config/control/dev-control-state.json`, stands in for it (`{"default": {...}, "topics": {"<topic>": {...}}}` with `active_region`, `leadership_epoch`, `replication_generation`). The gateway behaves as the design requires, so the control-plane client can replace the file reader without changing the write path:

- **Lease:** the gateway re-reads the file every `SS_LEASE_RENEW_MS` (3 s). A successful read grants a lease of `SS_LEASE_TTL_MS` (10 s), measured on the monotonic clock from before the read ([SECURITY §4](SECURITY.md)). If reads fail, the lease expires and the gateway self-fences: writes return `503 LEASE_EXPIRED`, reads continue flagged stale.
- **Active region:** a topic is writable only if its `active_region` is this gateway's region; otherwise `421 NOT_ACTIVE_REGION` with the active region as a hint.
- **Stale epochs:** a write carrying `expected_epoch` that differs from the current epoch gets `409 STALE_EPOCH`, and the gateway stamps every stored event with the current epoch and generation, never a client value. Epochs and generations never go backwards in memory: a file that lowers them, or drops a topic override, is ignored as a failed renewal (fail closed, INV-4). Restart the gateway to reset during development.
- To exercise it: edit the file, e.g. set a topic's `leadership_epoch` to 2 and `replication_generation` to 2, and wait about 3 s.

Not yet present: signed epoch tokens, a quorum, fence waits, promotion. The file is trusted completely.

## 6. Security model in this phase

- **Gateway-only access:** the brokers are only on `ss-region-a-net`. The gateway is on `ss-client-net` and `ss-region-a-net`. No Kafka port is published to the host (the `--expose-kafka` debug override aside). Tests prove that from the host and from a container on the client network the brokers cannot be resolved or connected to, by name or by IP, and that the gateway cannot reach other regions' brokers.
- **No credentials or topology leaked:** the API never returns bootstrap servers, broker addresses or credentials (a test scans responses). Kafka credentials, when used, are read only by the gateway from `SS_KAFKA_*` environment variables or `*_FILE` secret files. The local dev cluster is PLAINTEXT, so none exist yet.
- **Client auth:** static API keys (`SS_API_KEYS`, constant-time comparison). The production design is OIDC with per-topic scopes (SECURITY §3).
- The container runs as a non-root user.

## 7. Limitations

- One gateway instance and one region (A). No gateways for B or C yet.
- No replication plane, so no regional replica topics, lag, or attestations. `SYNC_REGIONAL` writes are refused.
- Dev control-plane stand-in only (§5). No TLS between client and gateway.
- API keys are not scoped: any client can read any topic and use any `consumer_id`.
- Each read creates and stops a Kafka consumer. Locally a small read takes about 10 ms and a write about 20 ms (single requests, not a load test), but per-read consumer setup will cost more under load. A consumer pool is a later optimization.
- A topic created through Kafka tooling outside the gateway has no policy and is not served. A crash between topic creation and policy storage leaves a Kafka topic without policy; the name stays blocked until it is removed manually.
- Exporter metrics ports stay published on the host. They carry metrics only, no Kafka protocol.

## 8. Tests

`gateway/tests/run.sh` (stack must be up; about 80 s; creates `gateway/tests/.venv`). 32 tests: 17 integration tests against the real stack and 15 unit tests of lease and policy rules.

| Required test | Where |
|---|---|
| Publish one event | `test_01_publish_one_event` |
| Publish 1,000 events; same-key partitioning and ordering | `test_02_*`, `test_03_*` |
| Consume from offset 0; resume from offset 500 (and paging, server-side offsets) | `test_04_*`, `test_04b_*` |
| Restart the gateway; stream and idempotency persist | `test_06_*` |
| Restart a Kafka broker; stream persists; acknowledged writes not lost; no duplicates | `test_07_*` |
| Duplicate request ID: defined behaviour | `test_05_*` |
| Stale leadership epoch rejected | `test_08_*` (also not-active region, lease expiry) |
| Direct broker access from client side blocked | `test_10_*` (and `test_11_*` for leakage) |

## 9. Phase 3: policy engine integration

When `SS_POLICY_URL` is set (it is in the dev stack) the gateway consults the policy engine ([POLICY.md](POLICY.md)):

- **Topic creation:** after the local rules, the gateway sends the policy to the engine **before** creating anything in Kafka. `REJECTED` becomes `403`/`422 POLICY_VIOLATION`, `INSUFFICIENT_ELIGIBLE_REGIONS` becomes `409`, an unreachable or erroring engine becomes `503 POLICY_UNAVAILABLE`. All carry `details.policy_version` and `details.reasons`. The response of a successful create includes `policy_version` and `replica_plan`.
- **RESTRICTED topics** are re-confirmed with the engine at most every `SS_RESTRICTED_POLICY_TTL_S` (10 s in the dev stack). If it cannot be confirmed, reads and writes get `503 POLICY_UNAVAILABLE`. If the policy changed, the new version applies at once, so a topic moved out of this region is refused (`403 POLICY_VIOLATION`, new `policy_version`). Other classes use the cached policy and refresh every 5 minutes, ignoring failures.
- The gateway attaches to `ss-control-net` and uses the engine's writer key.

## 10. Phase 6 additions

* **Lease source.** With `SS_COORD_ENDPOINTS` set (the `--dr` overlay) the lease comes from the etcd coordinator (`app/etcd_lease.py`): a renewal is a linearizable read of `/ss/dr/state`, timed on the monotonic clock. No quorum means no renewal, so the lease expires and writes are refused. A region in the state's `fenced` set is passive.
* **Adopted topics.** A topic created through another region's gateway has its gateway record in that region's `_ss.topic-policy`. A gateway that does not find a topic asks the policy engine and, if this region may store and process it and the replica topic exists locally, builds the record from the engine's policy (`TopicService._adopt`). This is what lets the gateway of B or C serve a replicated topic once it becomes active. Idempotency state is per region, so a client retry that crosses a failover is not de-duplicated by the gateway (at-least-once; consumers de-duplicate on `event_id`).
