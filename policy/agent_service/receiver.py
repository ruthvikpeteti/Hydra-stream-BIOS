"""Regional replication receiver: POST /internal/v1/replication/batches.

A batch is acknowledged ONLY after every record was written to this region's Kafka and the producer acknowledged it.
Anything that fails validation or persistence returns a structured error and is never acknowledged. The receiver does
not de-duplicate: a retried batch is written again. Every copy carries ss-delivery-attempt (how many times the agent sent
it), and ss-redelivery="true" when this receiver KNOWS the range was already written in the same stream (a certain
duplicate). attempt > 1 with redelivery="false" means "retried, but not known to be a duplicate" (for example the first
attempt was rejected, or the receiver restarted); consumers should de-duplicate on event_id either way."""

import asyncio
import hmac
import json
import logging
import os
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from ss_policy.agent_core import AgentRequest
from ss_policy.models import Decision, DecisionKind, EnforcementPoint, Operation, Reason
from ss_replication.batch import BatchIn, decode_record, validate_batch

log = logging.getLogger("ss.receiver")
MAX_RECORDS = int(os.environ.get("SS_RECEIVER_MAX_RECORDS", "1000"))
MAX_BYTES = int(os.environ.get("SS_RECEIVER_MAX_BYTES", str(4 * 1024 * 1024)))
WRITE_TIMEOUT_S = float(os.environ.get("SS_RECEIVER_WRITE_TIMEOUT_S", "15"))


def err(status: int, code: str, message: str, *, retryable: bool = False, decision: Optional[Decision] = None, **details) -> JSONResponse:
    body = {"error": {"code": code, "message": message, "retryable": retryable, "details": details}}
    if decision is not None:
        body["error"]["decision"] = decision.model_dump(mode="json")
    return JSONResponse(body, status_code=status)


class Fault(BaseModel):
    model_config = ConfigDict(extra="forbid")
    drop_response_after_write: int = Field(0, ge=0)  # write, then answer 504 as if the response was lost
    reject_before_write: int = Field(0, ge=0)  # refuse with 503 before writing anything
    delay_response_ms: int = Field(0, ge=0, le=60000)  # write, then wait before answering (next response only)
    delay_responses: int = Field(0, ge=0)  # how many responses to delay
    topic: Optional[str] = None  # apply only to batches of this topic (other flows are untouched)


def install(app, M):
    """M is agent_service.main (State, record, ensure_fresh, check_key)."""
    keys = json.loads(os.environ.get("SS_REPL_AGENT_KEYS", "{}"))  # source region -> secret
    faults_enabled = os.environ.get("SS_FAULT_INJECTION") == "1"
    fault = {"drop": 0, "reject": 0, "delay_ms": 0, "delays": 0, "topic": None}
    stats = {"records_written": 0, "batches_written": 0}  # test-only visibility (fault injection enabled)
    gate: dict[tuple, tuple[str, int]] = {}  # (topic, source, partition) -> (session, last accepted offset)
    locks: dict[tuple, asyncio.Lock] = {}

    async def producer(st) -> AIOKafkaProducer:
        if getattr(st, "producer", None) is None:
            p = AIOKafkaProducer(bootstrap_servers=os.environ["SS_KAFKA_BOOTSTRAP"], acks="all", enable_idempotence=True, linger_ms=2, request_timeout_ms=10000)
            await asyncio.wait_for(p.start(), 15)
            st.producer = p
        return st.producer

    async def drop_producer(st) -> None:
        p, st.producer = getattr(st, "producer", None), None
        if p:
            try:
                await asyncio.wait_for(p.stop(), 5)
            except Exception:  # noqa: BLE001
                pass

    def authenticate(request: Request, region: Optional[str], auth: Optional[str]) -> Optional[str]:
        if not region or not auth or not auth.startswith("Bearer "):
            return None
        key = keys.get(region)
        return region if key and hmac.compare_digest(auth[7:], key) else None

    async def audit(st, d: Decision, body: BatchIn, requester: str) -> None:
        await M.record(
            st,
            d,
            EnforcementPoint.BATCH_ADMISSION,
            {
                "region": st.region,
                "source_region": body.source_region,
                "flow_id": f"{body.topic}:{body.source_region}->{st.region}:g{body.replication_generation}",
                "record_count": len(body.records),
                "first_offset": body.first_offset,
                "last_offset": body.last_offset,
                "partitions": [body.partition],
                "generation": body.replication_generation,
                "epoch": body.leadership_epoch,
            },
            requester,
            body.batch_id,
            sync=False,
        )

    def rejection(code: str, topic: str, version, message: str, region: Optional[str] = None) -> Decision:
        return Decision(
            decision=DecisionKind.REJECTED,
            operation="BATCH_ADMISSION",
            topic=topic,
            policy_version=version,
            reasons=[Reason(code=code, message=message, category="TOPOLOGY", region=region)],
        )

    @app.post("/internal/v1/replication/batches")
    async def receive(
        body: BatchIn,
        request: Request,
        authorization: Optional[str] = Header(None),
        x_replication_agent: Optional[str] = Header(None),
        x_replication_path: Optional[str] = Header(None),
    ):
        st = request.app.state.s
        who = authenticate(request, x_replication_agent, authorization)
        if who is None:  # 1. authenticate
            return err(401, "UNAUTHENTICATED", "valid replication agent credentials are required")
        if body.source_region != who:  # 2. regions
            return err(403, "SOURCE_MISMATCH", f"credentials belong to {who}, batch claims {body.source_region}")
        if body.destination_region != st.region:
            return err(403, "DESTINATION_MISMATCH", f"this receiver serves {st.region}, batch is addressed to {body.destination_region}")
        mine = faults_enabled and fault["topic"] in (None, body.topic)  # a fault may be scoped to one topic
        if mine and fault["reject"] > 0:
            fault["reject"] -= 1
            return err(503, "FAULT_INJECTED_REJECT", "rejected before write by fault injection", retryable=True)
        await M.ensure_fresh(st, body.topic, body.policy_version)
        flow_id = f"{body.topic}:{who}->{st.region}:g{body.replication_generation}"
        d = st.agent.admit_batch(body.topic, flow_id, body.policy_version, who, None)  # 3. policy + version
        if not d.allowed and any(r.code in ("UNKNOWN_FLOW", "STALE_FLOW") for r in d.reasons):
            init = st.agent.authorize(AgentRequest(body.topic, Operation.ASSIGN, who, None, body.policy_version, flow_id))
            d = st.agent.admit_batch(body.topic, flow_id, body.policy_version, who, None) if init.allowed else init
        if not d.allowed:
            await audit(st, d, body, who)
            status = {DecisionKind.POLICY_UNAVAILABLE: 503}.get(d.decision, 409 if any(r.code.startswith("STALE") for r in d.reasons) else 403)
            return err(
                status,
                "POLICY_UNAVAILABLE" if d.decision == DecisionKind.POLICY_UNAVAILABLE else "POLICY_REJECTED",
                "; ".join(r.message for r in d.reasons),
                retryable=d.decision == DecisionKind.POLICY_UNAVAILABLE,
                decision=d,
            )
        topo = st.agent.topology(body.topic)  # 4. generation + authorization
        pv = d.policy_version
        if topo is None:
            await audit(st, rejection("FLOW_NOT_AUTHORIZED", body.topic, pv, "topic has no replication topology"), body, who)
            return err(403, "FLOW_NOT_AUTHORIZED", "topic has no replication topology")
        if st.region == topo["active_region"]:
            await audit(st, rejection("REPLICATION_INTO_ACTIVE", body.topic, pv, "target is the active region", st.region), body, who)
            return err(403, "REPLICATION_LOOP", f"{st.region} is the active region for {body.topic}; nothing may replicate into it")
        flow = next((f for f in topo["flows"] if f["source_region"] == who and f["target_region"] == st.region and f["state"] == "ENABLED"), None)
        if flow is None:
            await audit(st, rejection("FLOW_NOT_AUTHORIZED", body.topic, pv, f"no enabled flow {who} -> {st.region}", who), body, who)
            return err(403, "FLOW_NOT_AUTHORIZED", f"no enabled flow {who} -> {st.region} for {body.topic}")
        if body.replication_generation != topo["replication_generation"]:
            code = "STALE_GENERATION" if body.replication_generation < topo["replication_generation"] else "UNAUTHORIZED_GENERATION"
            await audit(
                st, rejection(code, body.topic, pv, f"batch generation {body.replication_generation}, current {topo['replication_generation']}", who), body, who
            )
            return err(
                409 if code == "STALE_GENERATION" else 403,
                code,
                f"batch is for generation {body.replication_generation}; the authorized generation is {topo['replication_generation']}",
                current_generation=topo["replication_generation"],
            )
        if body.leadership_epoch != topo["leadership_epoch"]:
            await audit(
                st, rejection("STALE_EPOCH", body.topic, pv, f"batch epoch {body.leadership_epoch}, current {topo['leadership_epoch']}", who), body, who
            )
            return err(
                409, "STALE_EPOCH", f"batch is for epoch {body.leadership_epoch}; current is {topo['leadership_epoch']}", current_epoch=topo["leadership_epoch"]
            )
        pd = st.agent.authorize_path(body.topic, x_replication_path, who)  # 4b. the egress path itself
        if not pd.allowed:
            await audit(st, pd.model_copy(update={"operation": "BATCH_ADMISSION"}), body, who)
            return err(
                403 if pd.decision == DecisionKind.REJECTED else 503,
                "PATH_NOT_AUTHORIZED",
                "; ".join(r.message for r in pd.reasons),
                retryable=pd.decision != DecisionKind.REJECTED,
                decision=pd,
                path=x_replication_path,
            )
        if body.source_cluster != st.agent.cluster_of(who):  # 5. metadata
            return err(422, "BATCH_INVALID", f"source_cluster {body.source_cluster!r} is not the cluster of {who}")
        problems = validate_batch(body, MAX_RECORDS, MAX_BYTES)
        decoded = []
        if not problems:
            try:
                decoded = [decode_record(r) for r in body.records]
            except ValueError as exc:
                problems.append(str(exc))
        if problems:
            return err(422, "BATCH_INVALID", "; ".join(problems))
        lock = locks.setdefault((body.topic, who, body.partition), asyncio.Lock())
        async with lock:  # per-partition ordering gate
            sess, last = gate.get((body.topic, who, body.partition), (None, -1))
            redelivery = sess == body.stream_session and body.last_offset <= last
            if sess == body.stream_session and body.prev_offset > last:
                return err(409, "ORDER_GAP", "an earlier batch of this partition has not been written yet", retryable=True, last_accepted=last)
            try:  # 6. write + wait for the ack
                p = await producer(st)
                n = (await asyncio.wait_for(p.partitions_for(body.topic), 10)) or set()
                if body.partition not in n:
                    return err(
                        409, "DESTINATION_TOPIC_MISMATCH", f"destination topic has partitions {sorted(n)}; batch is for {body.partition}", retryable=True
                    )
                my_cluster = st.agent.cluster_of(st.region)
                futs = []
                for r, (key, value, eid) in zip(body.records, decoded):
                    hops = f"{r.hop_path}>{my_cluster}"
                    hdr = {
                        "ss-origin-cluster": r.origin.cluster,
                        "ss-origin-topic": r.origin.topic,
                        "ss-origin-partition": str(r.origin.partition),
                        "ss-origin-offset": str(r.origin.offset),
                        "ss-src-cluster": body.source_cluster,
                        "ss-src-topic": body.topic,
                        "ss-src-partition": str(body.partition),
                        "ss-src-offset": str(r.offset),
                        "ss-event-id": eid,
                        "ss-replication-generation": str(body.replication_generation),
                        "ss-leadership-epoch": str(body.leadership_epoch),
                        "ss-batch-id": body.batch_id,
                        "ss-delivery-attempt": str(body.attempt),
                        "ss-redelivery": "true" if redelivery else "false",
                        "ss-hop-path": hops,
                    }
                    futs.append(
                        await p.send(
                            body.topic, value, key=key, partition=body.partition, timestamp_ms=r.timestamp, headers=[(k, v.encode()) for k, v in hdr.items()]
                        )
                    )
                metas = await asyncio.wait_for(asyncio.gather(*futs), WRITE_TIMEOUT_S)
            except (KafkaError, asyncio.TimeoutError, OSError) as exc:
                log.warning("destination write failed for %s: %r", body.batch_id, exc)
                await drop_producer(st)
                return err(
                    503,
                    "DESTINATION_UNAVAILABLE",
                    f"destination Kafka did not acknowledge the write ({type(exc).__name__})",
                    retryable=True,
                    partial_write_possible=True,
                )
            gate[(body.topic, who, body.partition)] = (body.stream_session, max(last, body.last_offset) if sess == body.stream_session else body.last_offset)
        stats["records_written"] += len(metas)
        stats["batches_written"] += 1
        resp = {
            "batch_id": body.batch_id,
            "status": "WRITTEN",
            "destination_cluster": st.agent.cluster_of(st.region),
            "redelivery": bool(redelivery),
            "possible_duplicate": bool(redelivery or body.attempt > 1),
            "count": len(metas),
            "mappings": [
                {"source_offset": r.offset, "destination_partition": m.partition, "destination_offset": m.offset} for r, m in zip(body.records, metas)
            ],
            "last_destination_offset": metas[-1].offset,
        }
        await audit(st, d.model_copy(update={"details": {}}), body, who)
        if mine:  # test-only response faults
            if fault["delay_ms"] > 0 and fault["delays"] > 0:
                fault["delays"] -= 1
                await asyncio.sleep(fault["delay_ms"] / 1000)
            if fault["drop"] > 0:
                fault["drop"] -= 1
                return err(504, "FAULT_INJECTED_RESPONSE_LOSS", "the write succeeded but the response was dropped by fault injection", retryable=True)
        return JSONResponse(resp)

    @app.post("/internal/v1/replication/probe")
    async def probe(request: Request, authorization: Optional[str] = Header(None), x_replication_agent: Optional[str] = Header(None)):
        """Echo-size endpoint for the source agent's RTT and bandwidth probes (reads the body, returns its size)."""
        if authenticate(request, x_replication_agent, authorization) is None:
            return err(401, "UNAUTHENTICATED", "valid replication agent credentials are required")
        n = 0
        async for chunk in request.stream():
            n += len(chunk)
            if n > 1_048_576:
                return err(413, "PROBE_TOO_LARGE", "probe bodies are limited to 1 MiB")
        return {"received": n}

    @app.post("/internal/v1/replication/_fault")
    async def set_fault(f: Fault, request: Request, x_service_key: Optional[str] = Header(None, alias="X-Service-Key")):
        if not faults_enabled:
            return err(404, "NOT_FOUND", "fault injection is disabled")
        if bad := M.check_key(request, x_service_key):
            return bad
        fault.update(drop=f.drop_response_after_write, reject=f.reject_before_write, delay_ms=f.delay_response_ms, delays=f.delay_responses, topic=f.topic)
        return {"fault": dict(fault)}

    @app.get("/internal/v1/replication/_stats")
    async def get_stats(request: Request, x_service_key: Optional[str] = Header(None, alias="X-Service-Key")):
        if not faults_enabled:
            return err(404, "NOT_FOUND", "fault injection is disabled")
        if bad := M.check_key(request, x_service_key):
            return bad
        return {**stats, "fault": dict(fault)}

    app.state.stop_producer = drop_producer
