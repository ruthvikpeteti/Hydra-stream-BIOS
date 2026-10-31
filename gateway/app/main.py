"""SovereignStream application gateway (Phase 2): HTTP API in front of the regional Kafka cluster.

This is NOT a Kafka protocol endpoint. Clients never talk to Kafka and never see broker addresses or
credentials; see GATEWAY.md.
"""

import hmac
import re
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .config import Settings
from .consumer import ConsumerManager
from .errors import ApiError
from .events import EventService
from .idempotency import IdempotencyStore
from .leases import DevFileLeaseSource, LeaseState
from .models import EventIn, OffsetCommit, TopicCreate
from .policy_client import PolicyClient
from .producer import ProducerManager
from .registry import Registry
from .topics import TopicService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("ss.gateway")
CONSUMER_ID_RE = r"^[A-Za-z0-9._-]{1,128}$"


@asynccontextmanager
async def lifespan(app: FastAPI):
    s = Settings.from_env()
    reg = Registry.load(s.registry_path)
    if s.region not in reg.regions:
        raise RuntimeError(f"SS_REGION {s.region} is not in the registry")
    if not s.api_keys:
        log.warning("SS_API_KEYS is empty: client authentication is DISABLED (development only)")
    leases: DevFileLeaseSource
    if s.coord_endpoints:
        from .etcd_lease import EtcdLeaseSource

        leases = EtcdLeaseSource(list(s.coord_endpoints), s.region, s.lease_ttl_ms, s.lease_renew_ms)
    else:
        leases = DevFileLeaseSource(s.control_state_path, s.region, s.lease_ttl_ms, s.lease_renew_ms)
    producer = ProducerManager(s)
    policy = PolicyClient(s) if s.policy_url else None
    if policy is None:
        log.warning("SS_POLICY_URL is empty: the policy engine is NOT consulted (development only)")
    topics = TopicService(s, reg, producer, policy)
    idem = IdempotencyStore(s, producer)
    await leases.start()
    await producer.start()
    try:
        await topics.start()
        await idem.load()
    except BaseException:
        await producer.stop()
        await leases.stop()
        raise
    app.state.s, app.state.leases, app.state.producer = s, leases, producer
    app.state.topics, app.state.consumers = topics, ConsumerManager(s)
    app.state.events = EventService(s, leases, topics, producer, idem)
    log.info("gateway ready: region=%s", s.region)
    yield
    log.info("shutting down: flushing producer")
    await producer.stop()
    await topics.stop()
    if policy:
        await policy.close()
    await leases.stop()


app = FastAPI(title="SovereignStream Gateway", version="0.2.0", lifespan=lifespan)


# ---- errors / middleware ----------------------------------------------------------------------------
@app.exception_handler(ApiError)
async def _api_error(_: Request, exc: ApiError):
    return JSONResponse(exc.body(), status_code=exc.status, headers=exc.headers)


@app.exception_handler(RequestValidationError)
async def _validation_error(_: Request, exc: RequestValidationError):
    errs = [{"loc": [str(x) for x in e["loc"]], "msg": e["msg"]} for e in exc.errors()]
    return JSONResponse(ApiError(400, "ENVELOPE_INVALID", "request validation failed", details={"errors": errs}).body(), status_code=400)


@app.exception_handler(Exception)
async def _unhandled(_: Request, exc: Exception):
    log.exception("unhandled error")
    return JSONResponse(ApiError(500, "INTERNAL", "internal error").body(), status_code=500)


@app.middleware("http")
async def _limit_body(request: Request, call_next):
    limit = request.app.state.s.max_event_bytes * 2 if hasattr(request.app.state, "s") else 2_097_152
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > limit:
        return JSONResponse(ApiError(413, "ENVELOPE_INVALID", "request body too large").body(), status_code=413)
    return await call_next(request)


async def require_client(request: Request, x_api_key: str | None = Header(None, alias="X-API-Key")) -> None:
    keys = request.app.state.s.api_keys
    if not keys:
        return
    if not x_api_key or not any(hmac.compare_digest(x_api_key, k) for k in keys):
        raise ApiError(401, "UNAUTHENTICATED", "missing or invalid X-API-Key")


def _lease_headers(resp: Response, s: Settings, lease: LeaseState) -> None:
    resp.headers["X-SS-Region"] = s.region
    resp.headers["X-SS-Region-Role"] = lease.role
    resp.headers["X-SS-Epoch"] = str(lease.leadership_epoch)


# ---- routes -----------------------------------------------------------------------------------------
@app.post("/api/v1/topics", status_code=201, dependencies=[Depends(require_client)])
async def create_topic(body: TopicCreate, request: Request, response: Response):
    rec = await request.app.state.topics.create(body.model_dump())
    return await _topic_view(request, rec)


@app.get("/api/v1/topics/{topic}", dependencies=[Depends(require_client)])
async def get_topic(topic: str, request: Request, response: Response):
    rec = await request.app.state.topics.get(topic)
    _lease_headers(response, request.app.state.s, request.app.state.leases.resolve(topic))
    return await _topic_view(request, rec)


async def _topic_view(request: Request, rec: dict) -> dict:
    st = request.app.state
    lease = st.leases.resolve(rec["topic"])
    offs = await st.consumers.offsets(rec["topic"], rec["partitions"])
    p = rec["policy"]
    return {
        "topic": rec["topic"],
        "partitions": [{"partition": k, **v} for k, v in sorted(offs.items())],
        "policy": p,
        "policy_revision": rec["revision"],
        "policy_version": rec.get("policy_version"),
        "replica_plan": rec.get("replica_plan", []),
        "created_at": rec["created_at"],
        "active_region": lease.active_region,
        "role": lease.role,
        "leadership_epoch": lease.leadership_epoch,
        "replication_generation": lease.replication_generation,
        "replication_status": "NOT_CONFIGURED" if p["regional_replica_count"] else "NOT_APPLICABLE",
    }


@app.post("/api/v1/events", dependencies=[Depends(require_client)])
async def post_event(ev: EventIn, request: Request, response: Response, idempotency_key: str | None = Header(None, alias="Idempotency-Key")):
    st = request.app.state
    body, replay, lease = await st.events.publish(ev, idempotency_key)
    _lease_headers(response, st.s, lease)
    if replay:
        response.headers["Idempotent-Replay"] = "true"
    return body


@app.get("/api/v1/events/{topic}", dependencies=[Depends(require_client)])
async def get_events(
    topic: str,
    request: Request,
    response: Response,
    partition: int | None = Query(None, ge=0),
    offset: int | None = Query(None, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    wait_ms: int = Query(0, ge=0, le=30_000),
    consumer_id: str | None = Query(None, pattern=CONSUMER_ID_RE),
    commit: bool = Query(False),
):
    st = request.app.state
    rec = await st.topics.current(await st.topics.get(topic))
    if st.s.region not in rec["policy"]["allowed_processing_regions"]:
        raise ApiError(403, "POLICY_VIOLATION", "this region may not process the topic", topic=topic, details={"policy_version": rec.get("policy_version")})
    if offset is not None and partition is None:
        raise ApiError(400, "ENVELOPE_INVALID", "offset requires partition (offsets are per partition)", topic=topic)
    if partition is not None and partition >= rec["partitions"]:
        raise ApiError(404, "NOT_FOUND", "unknown partition", topic=topic)
    if commit and not consumer_id:
        raise ApiError(400, "ENVELOPE_INVALID", "commit=true requires consumer_id", topic=topic)
    lease = st.leases.resolve(topic)
    _lease_headers(response, st.s, lease)
    if not st.leases.lease_fresh():
        response.headers["X-SS-Stale"] = "true"
    return await st.consumers.read(
        topic, rec["partitions"], partition=partition, offset=offset, limit=limit, wait_ms=wait_ms, consumer_id=consumer_id, commit=commit, region=st.s.region
    )


@app.put("/api/v1/consumers/{consumer_id}/offsets", status_code=204, dependencies=[Depends(require_client)])
async def commit_offsets(consumer_id: str, body: OffsetCommit, request: Request):
    """Acknowledge: store the next offset to read per partition for this consumer_id."""
    if not re.match(CONSUMER_ID_RE, consumer_id):
        raise ApiError(400, "ENVELOPE_INVALID", "invalid consumer_id")
    st = request.app.state
    rec = await st.topics.get(body.topic)
    await st.consumers.commit(consumer_id, body.topic, rec["partitions"], body.offsets)
    return Response(status_code=204)


@app.get("/api/v1/active-region", dependencies=[Depends(require_client)])
async def active_region(request: Request, response: Response, topic: str | None = Query(None)):
    st = request.app.state
    if topic:
        await st.topics.get(topic)
    lease = st.leases.resolve(topic)
    _lease_headers(response, st.s, lease)
    return {
        "region": st.s.region,
        "topic": topic,
        "active_region": lease.active_region,
        "role": lease.role,
        "leadership_epoch": lease.leadership_epoch,
        "replication_generation": lease.replication_generation,
        "lease": {"valid": lease.lease_valid, "ttl_remaining_ms": lease.ttl_remaining_ms},
    }


@app.get("/metrics")
async def prom_metrics(request: Request):
    from . import metrics

    st = request.app.state
    lease = st.leases.resolve(None)
    return Response(metrics.render(st.s.region, lease.lease_valid, lease.leadership_epoch), media_type="text/plain; version=0.0.4")


@app.get("/api/v1/health")
async def health(request: Request, response: Response):
    st = request.app.state
    kafka_ok = await st.topics.kafka_reachable()
    lease = st.leases.resolve(None)
    fresh = st.leases.lease_fresh()
    ok = kafka_ok and st.producer.started and fresh
    response.status_code = 200 if ok else 503
    return {
        "status": "ok" if ok else "degraded",
        "region": st.s.region,
        "kafka_reachable": kafka_ok,
        "producer_running": st.producer.started,
        "control_state": {"fresh": fresh, "ttl_remaining_ms": lease.ttl_remaining_ms},
    }
