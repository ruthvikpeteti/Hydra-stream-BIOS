"""Regional policy agent. One per region, on that region's network.

It holds its own signed copy of the policy state and re-validates every request that arrives, whatever the sender
decided. It is also the only component that creates replica topics in its region's Kafka cluster.
"""

import asyncio
import hmac
import json
import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError, TopicAlreadyExistsError
from fastapi import FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from ss_policy.agent_core import AgentRequest, RegionalPolicyAgent
from ss_policy.bundle import BundleError
from ss_policy.models import Decision, DecisionKind, EnforcementPoint, Operation, Reason, ReplicaStatus, TOPIC_RE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("ss.agent")
HTTP = {DecisionKind.ALLOWED: 200, DecisionKind.REJECTED: 403, DecisionKind.INSUFFICIENT_ELIGIBLE_REGIONS: 409, DecisionKind.POLICY_UNAVAILABLE: 503}


class State:
    agent: RegionalPolicyAgent
    region: str
    engine_url: str
    key: str
    audit_q: deque
    audit_dropped: int = 0
    refresh_lock: asyncio.Lock


@asynccontextmanager
async def lifespan(app: FastAPI):
    st = State()
    st.region = os.environ["SS_REGION"]
    st.engine_url = os.environ["SS_ENGINE_URL"].rstrip("/")
    st.key = os.environ["SS_SERVICE_KEY"]
    st.agent = RegionalPolicyAgent(st.region, Path(os.environ.get("SS_BUNDLE_PUBLIC_KEY", "/run/ss/bundle-public.pem")).read_bytes())
    st.audit_q, st.refresh_lock = deque(maxlen=10_000), asyncio.Lock()
    st.http = httpx.AsyncClient(timeout=3)
    st.admin = AIOKafkaAdminClient(bootstrap_servers=os.environ["SS_KAFKA_BOOTSTRAP"])
    await st.admin.start()
    st.producer = None
    app.state.s = st
    tasks = [asyncio.create_task(_refresher(st)), asyncio.create_task(_audit_flusher(st))]
    await refresh(st)
    yield
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await _flush(st)
    await st.http.aclose()
    await st.admin.close()
    await app.state.stop_producer(st)


app = FastAPI(title="SovereignStream Regional Policy Agent", version="0.3.0", lifespan=lifespan)


# ---- policy state -----------------------------------------------------------------------------------------
async def refresh(st: State) -> bool:
    async with st.refresh_lock:
        try:
            r = await st.http.get(f"{st.engine_url}/api/v1/bundle", headers={"X-Service-Key": st.key})
            r.raise_for_status()
            st.agent.install(r.json())
            return True
        except (httpx.HTTPError, BundleError, ValueError) as exc:
            log.warning("bundle refresh failed: %r", exc)
            return False


async def _refresher(st: State) -> None:
    every = float(os.environ.get("SS_BUNDLE_REFRESH_S", "5"))
    while True:
        await asyncio.sleep(every)
        await refresh(st)


async def ensure_fresh(st: State, topic: str, caller_version: Optional[int]) -> None:
    if st.agent.needs_refresh(topic, caller_version):
        await refresh(st)  # a failed refresh is fine: authorize() then fails closed on its own


# ---- audit ------------------------------------------------------------------------------------------------
def audit_item(d: Decision, point: EnforcementPoint, subject: dict, requester, request_id) -> dict:
    return {
        "operation": d.operation,
        "enforcement_point": point.value,
        "topic": d.topic,
        "policy_version": d.policy_version,
        "decision": d.decision.value,
        "reasons": [{"code": r.code, "category": r.category, "region": r.region, "message": r.message[:300]} for r in d.reasons],
        "subject": {k: v for k, v in subject.items() if v is not None},
        "requester": requester,
        "request_id": request_id,
    }


async def _post_audit(st: State, items: list[dict]) -> bool:
    try:
        r = await st.http.post(f"{st.engine_url}/api/v1/audit/decisions", json=items, headers={"X-Service-Key": st.key, "X-Audit-Source": st.region})
        return r.status_code == 202
    except httpx.HTTPError:
        return False


async def record(st: State, d: Decision, point: EnforcementPoint, subject: dict, requester, request_id, *, sync: bool) -> None:
    item = audit_item(d, point, subject, requester, request_id)
    log.info("decision %s", json.dumps({"region": st.region, **item}))  # always logged locally (metadata only)
    if sync and await _post_audit(st, [item]):
        return
    if len(st.audit_q) == st.audit_q.maxlen:
        st.audit_dropped += 1
    st.audit_q.append(item)


async def _flush(st: State) -> None:
    while st.audit_q:
        batch = [st.audit_q.popleft() for _ in range(min(100, len(st.audit_q)))]
        if not await _post_audit(st, batch):
            st.audit_q.extendleft(reversed(batch))
            return


async def _audit_flusher(st: State) -> None:
    while True:
        await asyncio.sleep(1)
        await _flush(st)


# ---- API --------------------------------------------------------------------------------------------------
@app.exception_handler(RequestValidationError)
async def _validation(_: Request, e: RequestValidationError):
    return JSONResponse(
        {
            "decision": "REJECTED",
            "operation": "VALIDATE",
            "policy_version": None,
            "reasons": [{"code": "REQUEST_INVALID", "category": "SYSTEM", "message": "request validation failed"}],
            "details": {"errors": [{"loc": [str(x) for x in er["loc"]], "msg": er["msg"]} for er in e.errors()]},
        },
        status_code=422,
    )


def check_key(request: Request, key: Optional[str]) -> Optional[JSONResponse]:
    if not key or not hmac.compare_digest(key, request.app.state.s.key):
        return JSONResponse(
            {
                "decision": "REJECTED",
                "operation": "AUTH",
                "policy_version": None,
                "reasons": [{"code": "UNAUTHORIZED", "category": "SYSTEM", "message": "service credentials required"}],
            },
            status_code=401,
        )
    return None


class AuthorizeIn(BaseModel):
    """Metadata only (extra fields rejected): no message payloads can be submitted."""

    model_config = ConfigDict(extra="forbid")
    topic: str = Field(pattern=TOPIC_RE)
    operation: Operation
    source_region: Optional[str] = None
    route: Optional[list[str]] = None
    policy_version: Optional[int] = Field(None, ge=0)
    flow_id: Optional[str] = Field(None, max_length=128)
    replica_status: Optional[ReplicaStatus] = None
    original_region: Optional[str] = None
    required_bytes: int = Field(0, ge=0)
    enforcement_point: EnforcementPoint = EnforcementPoint.API
    requester: Optional[str] = Field(None, max_length=128)
    request_id: Optional[str] = Field(None, max_length=128)


class BatchIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    topic: str = Field(pattern=TOPIC_RE)
    flow_id: str = Field(max_length=128)
    policy_version: int = Field(ge=0)
    source_region: str
    route: Optional[list[str]] = None
    record_count: int = Field(ge=0)
    first_offset: int = Field(ge=0)
    last_offset: int = Field(ge=0)
    bytes: int = Field(0, ge=0)
    requester: Optional[str] = Field(None, max_length=128)
    request_id: Optional[str] = Field(None, max_length=128)


class ReplicaIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    topic: str = Field(pattern=TOPIC_RE)
    partition_count: int = Field(ge=1, le=1024)
    source_region: str
    policy_version: int = Field(ge=0)
    route: Optional[list[str]] = None
    requester: Optional[str] = Field(None, max_length=128)
    request_id: Optional[str] = Field(None, max_length=128)


def reply(d: Decision, status: Optional[int] = None, **extra) -> JSONResponse:
    d = d.model_copy(update={"details": {**d.details, **extra}}) if extra else d
    return JSONResponse(d.model_dump(mode="json"), status_code=status or HTTP[d.decision])


@app.post("/agent/v1/authorize")
async def authorize(body: AuthorizeIn, request: Request, x_service_key: Optional[str] = Header(None, alias="X-Service-Key")):
    if bad := check_key(request, x_service_key):
        return bad
    st: State = request.app.state.s
    await ensure_fresh(st, body.topic, body.policy_version)
    flow = body.flow_id if body.enforcement_point == EnforcementPoint.FLOW_INIT else None
    req = AgentRequest(
        body.topic, body.operation, body.source_region, body.route, body.policy_version, flow, body.replica_status, body.original_region, body.required_bytes
    )
    d = st.agent.authorize(req)
    await record(
        st,
        d,
        body.enforcement_point,
        {
            "region": st.region,
            "source_region": body.source_region,
            "route": body.route,
            "flow_id": body.flow_id,
            "original_region": body.original_region,
            "replica_status": body.replica_status.value if body.replica_status else None,
        },
        body.requester,
        body.request_id,
        sync=True,
    )
    return reply(d)


@app.post("/agent/v1/batches")
async def admit_batch(body: BatchIn, request: Request, x_service_key: Optional[str] = Header(None, alias="X-Service-Key")):
    if bad := check_key(request, x_service_key):
        return bad
    st: State = request.app.state.s
    await ensure_fresh(st, body.topic, body.policy_version)
    d = st.agent.admit_batch(body.topic, body.flow_id, body.policy_version, body.source_region, body.route)
    await record(
        st,
        d,
        EnforcementPoint.BATCH_ADMISSION,
        {
            "region": st.region,
            "source_region": body.source_region,
            "flow_id": body.flow_id,
            "record_count": body.record_count,
            "first_offset": body.first_offset,
            "last_offset": body.last_offset,
            "bytes": body.bytes,
        },
        body.requester,
        body.request_id,
        sync=False,
    )  # high volume: queued, flushed every second
    return reply(d)


@app.post("/agent/v1/replicas")
async def create_replica(body: ReplicaIn, request: Request, x_service_key: Optional[str] = Header(None, alias="X-Service-Key")):
    """Replica creation: validated here first, and only then is the topic created in THIS region's cluster."""
    if bad := check_key(request, x_service_key):
        return bad
    st: State = request.app.state.s
    await ensure_fresh(st, body.topic, body.policy_version)
    d = st.agent.authorize(AgentRequest(body.topic, Operation.ASSIGN, body.source_region, body.route, body.policy_version))
    d = d.model_copy(update={"operation": "REPLICA_CREATE"})
    subject = {"region": st.region, "source_region": body.source_region, "partitions": [body.partition_count]}
    if not d.allowed:
        await record(st, d, EnforcementPoint.REPLICA_CREATION, subject, body.requester, body.request_id, sync=True)
        return reply(d)
    pol = st.agent._bundle.policies[body.topic]
    rf = pol.replication_factor
    try:
        await st.admin.create_topics([NewTopic(body.topic, body.partition_count, rf, topic_configs={"min.insync.replicas": str(rf // 2 + 1)})])
        created = True
    except TopicAlreadyExistsError:
        created = False
    except KafkaError as exc:
        d = Decision(
            decision=DecisionKind.POLICY_UNAVAILABLE,
            operation="REPLICA_CREATE",
            topic=body.topic,
            policy_version=pol.policy_version,
            reasons=[Reason(code="KAFKA_ERROR", message=f"replica topic could not be created: {exc!r}"[:300], category="TECHNICAL", region=st.region)],
        )
    await record(st, d, EnforcementPoint.REPLICA_CREATION, subject, body.requester, body.request_id, sync=True)
    return reply(d, 201 if (d.allowed and created) else None, created=created if d.allowed else None, replication_factor=rf)


@app.get("/agent/v1/status")
async def status(request: Request, x_service_key: Optional[str] = Header(None, alias="X-Service-Key")):
    if bad := check_key(request, x_service_key):
        return bad
    st: State = request.app.state.s
    return {
        "region": st.region,
        "bundle_version": st.agent.bundle_version,
        "policy_age_s": round(st.agent.age_s(), 1) if st.agent.bundle_version is not None else None,
        "flows": len(st.agent.flows),
        "audit_queue": len(st.audit_q),
        "audit_dropped": st.audit_dropped,
    }


@app.get("/agent/v1/health")
async def health(request: Request):
    st: State = request.app.state.s
    ok = st.agent.bundle_version is not None
    return JSONResponse({"status": "ok" if ok else "degraded", "region": st.region, "has_policy_bundle": ok}, status_code=200 if ok else 503)


import sys  # noqa: E402
from . import receiver  # noqa: E402

receiver.install(app, sys.modules[__name__])
