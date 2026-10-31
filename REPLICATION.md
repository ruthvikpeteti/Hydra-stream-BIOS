# SovereignStream — Cross-Region Replication (Phase 4)

At-least-once, partition-aware replication between regional Kafka clusters. **It is not exactly-once**, and nothing here claims zero data loss: a retry after a lost response *does* create duplicates, which are made identifiable and handled by idempotent consumers (§5).

Code: [policy/ss_replication/](policy/ss_replication/) (agent, checkpoints, cutover), [policy/agent_service/receiver.py](policy/agent_service/receiver.py) (receiver), [policy/engine_service/replication.py](policy/engine_service/replication.py) (topology API). Tests: [policy/tests/test_replication.py](policy/tests/test_replication.py), [test_replication_core.py](policy/tests/test_replication_core.py). Run: `infrastructure/tests/test_f_replication_e2e.sh`.

## 1. Components

| Component | Where | Role |
|---|---|---|
| Source agent (`ss-{a,b,c}-replicator`) | one per region, reads that region's Kafka | Asks the engine which flows it may run, reads partition by partition, batches, sends to the target's receiver, checkpoints **after** the acknowledgement. |
| Receiver (`POST /internal/v1/replication/batches`) | inside each region's agent (`ss-{a,b,c}-agent`) | Authenticates the sender, validates, writes to its own Kafka, waits for the producer ack, then answers with the offset mapping. |
| Checkpoint manager | PostgreSQL table `replication_checkpoints` | Durable progress per (source cluster, target cluster, topic, partition). |
| Topology and flows | engine tables `topic_topology`, `replication_flows` | Active region, epoch, generation and one live source per target. |

No Kafka wire protocol and no custom consensus: the agent uses `aiokafka`; placement and promotion decisions are in the policy engine.

## 2. Delivery path and what is guaranteed

1. The agent reads records from the source partition, builds a batch (limits: records, bytes, linger) and sends it with a deterministic `batch_id`.
2. The receiver checks, in order: credentials (a per-source-region secret, bound to the claimed `source_region`); destination region; **policy admission** against its own signed bundle (version, legality, staleness); **topology** (an enabled flow source→this region, not into the active region, the current `replication_generation` and `leadership_epoch`); batch structure (contiguous offsets, size limits, every value a valid event envelope with a UUID `event_id`).
3. It writes every record to the same partition number of the same topic and waits for all producer acknowledgements (`acks=all`, idempotent producer). Only then does it answer `200` with `{source_offset → destination_partition, destination_offset}`. Any failure returns a structured error `{code, message, retryable, details}` and is never acknowledged.
4. The agent persists the checkpoint only after that `200`. If PostgreSQL is down it stalls and retries the checkpoint write; it does not skip it and does not send further batches.

**Guaranteed (tested):** a checkpoint never runs ahead of a destination acknowledgement; it never moves backwards within a generation (code and a database trigger); a batch is not acknowledged unless it was written; order within a source partition is preserved at the destination for first deliveries; different targets and different partitions progress independently.

**Not guaranteed:** no duplicates. If the destination wrote the batch but the response was lost, the agent retries and the destination holds the records twice. The receiver deliberately does **not** de-duplicate.

## 3. Record metadata

The event envelope (value bytes) is copied unchanged, so `event_id`, `origin_cluster`, `origin_region`, the **original** `leadership_epoch`/`replication_generation`, `event_timestamp` and `payload` survive. Kafka record key and timestamp are preserved. The receiver adds headers:

`ss-origin-cluster/topic/partition/offset` (where it was first written; kept across hops), `ss-src-cluster/topic/partition/offset` (this hop), `ss-event-id`, `ss-replication-generation` and `ss-leadership-epoch` (current values at replication time), `ss-batch-id`, `ss-delivery-attempt`, `ss-redelivery`, `ss-hop-path` (for example `kafka-a>kafka-b>kafka-c`).

## 4. Flow controls

Batch limits (records/bytes/linger); `SS_MAX_INFLIGHT` batches in flight per partition (default 1); a bounded number of unacknowledged batches held in memory per partition (`SS_QUEUE_MAX_BATCHES`, the rest stays in Kafka, so no unbounded queue); request timeout; exponential backoff with jitter (`min(cap, base·2ⁿ)` with half jitter); persistent checkpoints; pause/resume per flow (engine API, persisted); graceful shutdown (stop reading, finish in-flight batches, checkpoint what was acknowledged). A failed batch is resent together with every later in-flight batch (go-back-N); the receiver rejects a batch whose predecessor has not been written (`ORDER_GAP`).

## 5. Duplicates and the idempotent consumer

Each copy carries `ss-delivery-attempt`, and `ss-redelivery: "true"` when the receiver *knows* the range was already written in the same stream. A retried batch with `redelivery: "false"` may be a duplicate or may be a retry of a rejected batch; consumers de-duplicate on `event_id` either way. `python -m ss_replication.tools idempotent-demo --topic T` (run in a region's container) processes a topic once per `event_id` and reports naive vs idempotent totals. The response-loss test shows 50 events arriving as more than 50 records, with the idempotent total exactly right.

## 6. Topology, promotion and cutover

Initial: A→B and A→C. Rules (database index and trigger, pure rules, and receiver checks): every target has exactly one live source; nothing replicates into the active region; no cycles; a source must be the active region or fed from it.

`POST /api/v1/replication/topics/{t}/promote` (admin) with `candidate_region`: checks the policy part of promotion (storage and processing legality, usable replica), then **in one transaction** disables every old flow, advances epoch and generation, makes the candidate active, and creates candidate→remaining-target flows as `PENDING_CUTOVER`. The old active region is not made a target (it must be re-seeded, not fed).

The new source (agent in B) then waits until the receivers' bundles include the promotion (so no old-flow batch can still be admitted), scans its own log per partition, and computes the cutover: C already holds A-offsets through `ackC`; B→C starts at the first B record whose origin offset exceeds it. It rejects an inconsistent state (old-source records after native ones, checkpoint claims B does not hold), writes starting checkpoints for all partitions atomically, and asks the engine to activate; the engine activates only if every partition has its checkpoint at the new generation. The report (stored with the flow) classifies per partition: already in C, A-origin records missing from C, B-native records, and records C holds that B never received (kept, flagged `target_ahead`, never merged or deleted). Tests verify the start offset per partition, that C's earlier data is untouched, exactly 1,000 A-origin + 30 B-native records in C with no duplicates, and the lineage headers.

## 7. Tested, simulated, and not done

**Tested against the running stack (14 integration + 19 pure tests):** 1,000 events A→B and A→C with ids, ordering, offsets and headers; separate checkpoints; agent restart (graceful and `kill`); destination Kafka killed mid-transmission and caught up, with the other target unaffected; response loss and the resulting detectable duplicate; checkpoint held back while the acknowledgement is delayed; PostgreSQL restart; independent progress via pause; unauthorized topic, generation, epoch, policy version, credentials, unknown flow; promotion of B and B→C cutover; no flow into the active region or second source for a target.

**Simulated / test-only:** response loss, delays and refusals come from a fault-injection endpoint on the receiver (`SS_FAULT_INJECTION`, on in the dev stack, off in production). The "B-native" records after promotion are produced by a tool, because no gateway runs in B.

**Not done (do not assume it exists):**
- **Promotion safety:** the promote call is an operator action. There is **no lag/RPO check, no fencing of the old primary, no coordination quorum or lease**, and no automatic failure detection. The gateway's active-region file is not connected to the replication topology. Those belong to the disaster-recovery phase.
- **`SS_MAX_INFLIGHT > 1`** is implemented (go-back-N, receiver ordering gate) but **only the default of 1 is exercised by tests**.
- No replication-lag, backlog or throughput metrics are exported yet; no route/path selection (one receiver URL per region).
- A record that is not a valid envelope blocks its partition (the receiver refuses it permanently); there is no dead-letter handling.
- Receiver ordering state is in memory; after a receiver or agent restart a new stream session starts.
- The cutover scans the whole partition log (fine here, not for large logs).
- Revoked or old-primary data is not purged; the old primary is not re-seeded.
- Single PostgreSQL and single engine; the dev PostgreSQL role is a superuser.
- B and C have one broker each: replication targets there have no local fault tolerance.
