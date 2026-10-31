"""SovereignStream policy engine service (Phase 3). Enforces organisation-supplied policy; it does not decide the law."""

import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import asyncpg
import httpx
from fastapi import Body, Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from ss_policy import engine as eng
from ss_policy.bundle import sign_bundle
from ss_policy.models import NetworkPath, Decision, DecisionKind, EvaluateRequest, Link, Operation, Reason, Region, ReplicaStatus, TopicPolicy, TopicPolicyIn

from . import audit, db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("ss.policy")
ROLE_RANK = {"service": 1, "writer": 2, "admin": 3}
HTTP = {DecisionKind.ALLOWED: 200, DecisionKind.REJECTED: 403, DecisionKind.INSUFFICIENT_ELIGIBLE_REGIONS: 409, DecisionKind.POLICY_UNAVAILABLE: 503}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await db.connect(os.environ["SS_DATABASE_URL"])
    app.state.keys = {
        "admin": os.environ.get("SS_POLICY_ADMIN_KEY", ""),
        "writer": os.environ.get("SS_POLICY_WRITER_KEY", ""),
        "service": os.environ.get("SS_POLICY_SERVICE_KEY", ""),
    }
    if not all(app.state.keys.values()):
        raise RuntimeError("SS_POLICY_ADMIN_KEY, SS_POLICY_WRITER_KEY and SS_POLICY_SERVICE_KEY must all be set")
    app.state.private_pem = Path(os.environ.get("SS_BUNDLE_PRIVATE_KEY", "/run/ss/bundle-private.pem")).read_bytes()
    app.state.agents = json.loads(os.environ.get("SS_AGENT_URLS", "{}"))
    await _seed(app)
    await _seed_paths(app)
    log.info("policy engine ready")
    yield
    await app.state.pool.close()


app = FastAPI(title="SovereignStream Policy Engine", version="0.3.0", lifespan=lifespan)


# ---- plumbing ---------------------------------------------------------------------------------------------
class ApiErr(Exception):
    def __init__(self, status: int, d: Decision):
        self.status, self.d = status, d


def unavailable(op: str, topic: Optional[str], msg: str) -> Decision:
    return Decision(
        decision=DecisionKind.POLICY_UNAVAILABLE,
        operation=op,
        topic=topic,
        policy_version=None,
        reasons=[Reason(code="POLICY_SERVICE_ERROR", message=msg, category="SYSTEM")],
    )


@app.exception_handler(ApiErr)
async def _apierr(_: Request, e: ApiErr):
    return JSONResponse(e.d.model_dump(mode="json"), status_code=e.status)


@app.exception_handler(RequestValidationError)
async def _validation(_: Request, e: RequestValidationError):
    errs = [{"loc": [str(x) for x in er["loc"]], "msg": er["msg"]} for er in e.errors()]
    return JSONResponse(
        {
            "decision": "REJECTED",
            "operation": "VALIDATE",
            "policy_version": None,
            "reasons": [{"code": "REQUEST_INVALID", "category": "SYSTEM", "message": "request validation failed"}],
            "details": {"errors": errs},
        },
        status_code=422,
    )


@app.exception_handler(asyncpg.PostgresError)
@app.exception_handler(OSError)
async def _db_down(_: Request, e: Exception):
    log.error("database error: %r", e)
    return JSONResponse(unavailable("DB", None, "policy store is unavailable; failing closed").model_dump(mode="json"), status_code=503)


def role(minimum: str):
    async def dep(request: Request, x_service_key: Optional[str] = Header(None, alias="X-Service-Key")) -> str:
        have = None
        for name, key in request.app.state.keys.items():
            if x_service_key and hmac.compare_digest(x_service_key, key):
                have = name if have is None or ROLE_RANK[name] > ROLE_RANK[have] else have
        if have is None or ROLE_RANK[have] < ROLE_RANK[minimum]:
            raise ApiErr(
                401 if have is None else 403,
                Decision(
                    decision=DecisionKind.REJECTED,
                    operation="AUTH",
                    reasons=[Reason(code="UNAUTHORIZED", message=f"{minimum} credentials required", category="SYSTEM")],
                ),
            )
        return have

    return dep


def respond(d: Decision, status: Optional[int] = None, **extra) -> JSONResponse:
    code = status or HTTP[d.decision]
    return JSONResponse({**d.model_dump(mode="json"), **extra}, status_code=code)


def status_for(d: Decision, *, create: bool = False) -> int:
    if d.allowed:
        return 201 if create else 200
    if any(r.code in ("STALE_POLICY_VERSION",) for r in d.reasons):
        return 409
    if d.decision == DecisionKind.REJECTED and any(r.code in ("POLICY_EXISTS",) for r in d.reasons):
        return 409
    if d.decision == DecisionKind.POLICY_UNAVAILABLE and any(r.code == "POLICY_NOT_FOUND" for r in d.reasons):
        return 404
    return HTTP[d.decision]


async def _seed(app: FastAPI) -> None:
    path = os.environ.get("SS_SEED_REGISTRY")
    if not path or not Path(path).exists():
        return
    async with app.state.pool.acquire() as c:
        if await c.fetchval("SELECT count(*) FROM regions"):
            return
        seed = json.loads(Path(path).read_text())
        async with c.transaction():
            for r in seed["regions"]:
                await upsert_region(c, Region.model_validate(r))
            for l in seed["links"]:
                await upsert_link(c, Link.model_validate(l))
            await db.bump_bundle(c)
    log.info("seeded registry from %s", path)


async def _seed_paths(app: FastAPI) -> None:
    path = os.environ.get("SS_SEED_PATHS")
    if not path or not Path(path).exists():
        return
    async with app.state.pool.acquire() as c:
        if await c.fetchval("SELECT count(*) FROM network_paths"):
            return
        async with c.transaction():
            for p in json.loads(Path(path).read_text())["paths"]:
                await db.upsert_path(c, NetworkPath.model_validate(p))
            await db.bump_bundle(c)
    log.info("seeded %s", path)


async def upsert_region(c, r: Region) -> None:
    await c.execute(
        """INSERT INTO regions (region_id, legal_zone, availability_status, storage_total_bytes, storage_used_bytes,
               processing_capabilities, supported_transit_zones, failure_domain, compute_headroom, max_replication_factor, cluster_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
           ON CONFLICT (region_id) DO UPDATE SET legal_zone=$2, availability_status=$3, storage_total_bytes=$4,
               storage_used_bytes=$5, processing_capabilities=$6, supported_transit_zones=$7, failure_domain=$8,
               compute_headroom=$9, max_replication_factor=$10, cluster_id=$11, updated_at=now()""",
        r.region_id,
        r.legal_zone,
        r.availability_status.value,
        r.storage_capacity.total_bytes,
        r.storage_capacity.used_bytes,
        r.processing_capabilities,
        r.supported_transit_zones,
        r.failure_domain,
        r.compute_headroom,
        r.max_replication_factor,
        r.cluster_id,
    )


async def upsert_link(c, l: Link) -> None:
    await c.execute(
        """INSERT INTO links (from_region, to_region, transit_zones, latency_ms, status) VALUES ($1,$2,$3,$4,$5)
           ON CONFLICT (from_region, to_region) DO UPDATE SET transit_zones=$3, latency_ms=$4, status=$5, updated_at=now()""",
        l.from_region,
        l.to_region,
        l.transit_zones,
        l.latency_ms,
        l.status,
    )


async def audit_decision(c, d: Decision, point: str, subject: Optional[dict], requester: Optional[str], request_id: Optional[str]) -> Decision:
    d.decision_id = await audit.record(c, d, point=point, subject=subject, requester=requester, request_id=request_id)
    return d


async def reconcile(c, requester: str, topics: Optional[list[str]] = None) -> list[dict]:
    """Re-judge live assignments against the CURRENT registry and policy. Anything LEGALLY illegal is REVOKED
    (technical problems such as an outage do not revoke). Returns what was revoked."""
    regions, links = await db.load_world(c)
    out = []
    pols = await db.all_policies(c)
    for topic, pol in pols.items():
        if topics is not None and topic not in topics:
            continue
        ctx = eng.Ctx(regions, links, pol)
        for a in await db.get_assignments(c, topic):
            if a.replica_status == ReplicaStatus.REVOKED:
                continue
            d = eng.evaluate_assignment(ctx, a.source_region, a.destination_region)
            if any(r.category == "LEGAL" for r in d.reasons):
                await c.execute(
                    "UPDATE replica_assignments SET replica_status='REVOKED', policy_version=$4 WHERE topic=$1 AND partition=$2 AND destination_region=$3",
                    topic,
                    a.partition,
                    a.destination_region,
                    pol.policy_version,
                )
                rd = d.model_copy(update={"operation": "REVOKE"})
                await audit_decision(
                    c, rd, "API", {"partitions": [a.partition], "destination_region": a.destination_region, "source_region": a.source_region}, requester, None
                )
                out.append(
                    {
                        "topic": topic,
                        "partition": a.partition,
                        "destination_region": a.destination_region,
                        "reasons": [r.code for r in d.reasons if r.category == "LEGAL"],
                        "policy_version": pol.policy_version,
                    }
                )
            elif a.policy_version != pol.policy_version:
                await c.execute(
                    "UPDATE replica_assignments SET policy_version=$4 WHERE topic=$1 AND partition=$2 AND destination_region=$3",
                    topic,
                    a.partition,
                    a.destination_region,
                    pol.policy_version,
                )
    return out


# ---- health, registry -------------------------------------------------------------------------------------
@app.get("/api/v1/health")
async def health(request: Request):
    try:
        async with request.app.state.pool.acquire() as c:
            await c.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception:  # noqa: BLE001
        return JSONResponse({"status": "degraded", "database": False}, status_code=503)


@app.get("/api/v1/regions", dependencies=[Depends(role("service"))])
async def list_regions(request: Request):
    async with request.app.state.pool.acquire() as c:
        regions, _ = await db.load_world(c)
    return {"regions": [r.model_dump(mode="json") for r in regions.values()]}


@app.put("/api/v1/regions/{region_id}", dependencies=[Depends(role("admin"))])
async def put_region(region_id: str, region: Region, request: Request, who: str = Depends(role("admin"))):
    if region.region_id != region_id:
        raise ApiErr(
            422,
            Decision(
                decision=DecisionKind.REJECTED,
                operation="REGION_UPDATE",
                reasons=[Reason(code="REGION_ID_MISMATCH", message="path and body region_id differ", category="SYSTEM")],
            ),
        )
    async with request.app.state.pool.acquire() as c, c.transaction():
        await upsert_region(c, region)
        v = await db.bump_bundle(c)
        revoked = await reconcile(c, who)
        d = Decision(decision=DecisionKind.ALLOWED, operation="REGION_UPDATE", details={"region": region_id})
        await audit_decision(
            c, d, "API", {"region": region_id, "revoked": [f"{r['topic']}/{r['partition']}/{r['destination_region']}" for r in revoked]}, who, None
        )
    return {"region": region.model_dump(mode="json"), "bundle_version": v, "revoked_assignments": revoked}


@app.get("/api/v1/links", dependencies=[Depends(role("service"))])
async def list_links(request: Request):
    async with request.app.state.pool.acquire() as c:
        _, links = await db.load_world(c)
    return {"links": [l.model_dump(mode="json") for l in links.values()]}


@app.put("/api/v1/links/{from_region}/{to_region}", dependencies=[Depends(role("admin"))])
async def put_link(from_region: str, to_region: str, link: Link, request: Request, who: str = Depends(role("admin"))):
    if (link.from_region, link.to_region) != (from_region, to_region):
        raise ApiErr(
            422,
            Decision(
                decision=DecisionKind.REJECTED,
                operation="LINK_UPDATE",
                reasons=[Reason(code="LINK_ID_MISMATCH", message="path and body regions differ", category="SYSTEM")],
            ),
        )
    async with request.app.state.pool.acquire() as c, c.transaction():
        try:
            await upsert_link(c, link)
        except asyncpg.ForeignKeyViolationError:
            raise ApiErr(
                422,
                Decision(
                    decision=DecisionKind.REJECTED,
                    operation="LINK_UPDATE",
                    reasons=[Reason(code="UNKNOWN_REGION", message="both link endpoints must be registered", category="LEGAL")],
                ),
            )
        v = await db.bump_bundle(c)
        revoked = await reconcile(c, who)
        d = Decision(decision=DecisionKind.ALLOWED, operation="LINK_UPDATE")
        await audit_decision(c, d, "API", {"source_region": from_region, "destination_region": to_region, "latency_ms": link.latency_ms}, who, None)
    return {"link": link.model_dump(mode="json"), "bundle_version": v, "revoked_assignments": revoked}


# ---- policies ---------------------------------------------------------------------------------------------
def _planned(pol_in: TopicPolicyIn, version: int, regions, links) -> tuple[TopicPolicy, Decision]:
    pol = TopicPolicy(**pol_in.model_dump(), policy_version=version)
    return pol, eng.plan_placement(eng.Ctx(regions, links, pol), pol.home_region)


@app.post("/api/v1/policies")
async def create_policy(pol_in: TopicPolicyIn, request: Request, who: str = Depends(role("writer")), x_request_id: Optional[str] = Header(None)):
    async with request.app.state.pool.acquire() as c, c.transaction():
        regions, links = await db.load_world(c)
        existing = await db.get_policy(c, pol_in.topic)
        if existing:
            d = Decision(
                decision=DecisionKind.REJECTED,
                operation="POLICY_CREATE",
                topic=pol_in.topic,
                policy_version=existing.policy_version,
                reasons=[Reason(code="POLICY_EXISTS", message="a policy already exists; update it with PUT and If-Match", category="SYSTEM")],
            )
            await audit_decision(c, d, "TOPIC_CREATION", None, who, x_request_id)
            return respond(d, 409)
        reasons = eng.validate_policy(regions, links, pol_in)
        if reasons:
            d = Decision(
                decision=DecisionKind.REJECTED,
                operation="POLICY_CREATE",
                topic=pol_in.topic,
                policy_version=1,
                reasons=reasons,
                details={"policy_version_kind": "proposed"},
            )
            await audit_decision(c, d, "TOPIC_CREATION", None, who, x_request_id)
            return respond(d, 422)
        pol, plan = _planned(pol_in, 1, regions, links)
        if not plan.allowed:
            plan = plan.model_copy(update={"operation": "POLICY_CREATE", "details": {**plan.details, "policy_version_kind": "proposed"}})
            await audit_decision(c, plan, "TOPIC_CREATION", {"count": pol.regional_replica_count}, who, x_request_id)
            return respond(plan, 409 if plan.decision == DecisionKind.INSUFFICIENT_ELIGIBLE_REGIONS else 422)
        created = await db.insert_policy(c, pol_in, 1, who, update=False)
        v = await db.bump_bundle(c)
        d = Decision(
            decision=DecisionKind.ALLOWED,
            operation="POLICY_CREATE",
            topic=created.topic,
            policy_version=1,
            details={"placement": plan.details, "bundle_version": v},
        )
        await audit_decision(
            c, d, "TOPIC_CREATION", {"count": created.regional_replica_count, "chosen": [x["region"] for x in plan.details["chosen"]]}, who, x_request_id
        )
    return respond(d, 201, policy=created.model_dump(mode="json"))


@app.get("/api/v1/policies/{topic}")
async def get_policy(topic: str, request: Request, who: str = Depends(role("service"))):
    async with request.app.state.pool.acquire() as c:
        pol = await db.get_policy(c, topic)
        if pol is None:
            return respond(eng.no_policy("POLICY_GET", topic), 404)
        versions = await c.fetch("SELECT policy_version, actor, created_at FROM topic_policy_versions WHERE topic=$1 ORDER BY policy_version", topic)
    return {
        "decision": "ALLOWED",
        "operation": "POLICY_GET",
        "topic": topic,
        "policy_version": pol.policy_version,
        "policy": pol.model_dump(mode="json"),
        "history": [{"policy_version": r["policy_version"], "actor": r["actor"], "at": r["created_at"].isoformat()} for r in versions],
    }


@app.put("/api/v1/policies/{topic}")
async def update_policy(
    topic: str,
    pol_in: TopicPolicyIn,
    request: Request,
    who: str = Depends(role("admin")),
    if_match: Optional[str] = Header(None, alias="If-Match"),
    x_request_id: Optional[str] = Header(None),
):
    if pol_in.topic != topic:
        raise ApiErr(
            422,
            Decision(
                decision=DecisionKind.REJECTED,
                operation="POLICY_UPDATE",
                topic=topic,
                reasons=[Reason(code="TOPIC_MISMATCH", message="path and body topic differ", category="SYSTEM")],
            ),
        )
    async with request.app.state.pool.acquire() as c, c.transaction():
        cur = await db.get_policy(c, topic)
        if cur is None:
            return respond(eng.no_policy("POLICY_UPDATE", topic), 404)
        if if_match is None or not if_match.strip('"').isdigit() or int(if_match.strip('"')) != cur.policy_version:
            d = Decision(
                decision=DecisionKind.REJECTED,
                operation="POLICY_UPDATE",
                topic=topic,
                policy_version=cur.policy_version,
                reasons=[
                    Reason(code="STALE_POLICY_VERSION", message=f"If-Match must equal the current policy_version {cur.policy_version}", category="VERSION")
                ],
            )
            await audit_decision(c, d, "API", None, who, x_request_id)
            return respond(d, 412)
        regions, links = await db.load_world(c)
        reasons = eng.validate_policy(regions, links, pol_in)
        nv = cur.policy_version + 1
        if reasons:
            d = Decision(
                decision=DecisionKind.REJECTED,
                operation="POLICY_UPDATE",
                topic=topic,
                policy_version=nv,
                reasons=reasons,
                details={"policy_version_kind": "proposed", "current_policy_version": cur.policy_version},
            )
            await audit_decision(c, d, "API", None, who, x_request_id)
            return respond(d, 422)
        updated = await db.insert_policy(c, pol_in, nv, who, update=True)
        v = await db.bump_bundle(c)
        revoked = await reconcile(c, who, [topic])
        plan = eng.plan_placement(eng.Ctx(regions, links, updated), updated.home_region)
        d = Decision(
            decision=DecisionKind.ALLOWED,
            operation="POLICY_UPDATE",
            topic=topic,
            policy_version=nv,
            details={"previous_policy_version": cur.policy_version, "bundle_version": v, "placement_decision": plan.decision.value},
        )
        await audit_decision(c, d, "API", {"revoked": [f"{r['partition']}/{r['destination_region']}" for r in revoked]}, who, x_request_id)
    return respond(d, 200, policy=updated.model_dump(mode="json"), revoked_assignments=revoked, placement=plan.model_dump(mode="json"))


# ---- placement --------------------------------------------------------------------------------------------
def _subject(req: EvaluateRequest) -> dict:
    return {
        "region": req.region,
        "source_region": req.source_region,
        "destination_region": req.destination_region,
        "route": req.route,
        "failed_regions": req.failed_regions or None,
        "capability": req.capability,
        "required_bytes": req.required_bytes or None,
        "replica_status": req.replica_status.value if req.replica_status else None,
        "count": req.count,
    }


def _need(req: EvaluateRequest, *fields: str) -> None:
    missing = [f for f in fields if getattr(req, f) in (None, [])]
    if missing:
        raise ApiErr(
            422,
            Decision(
                decision=DecisionKind.REJECTED,
                operation=req.operation.value,
                topic=req.topic,
                reasons=[Reason(code="REQUEST_INVALID", message=f"{req.operation.value} requires {missing}", category="SYSTEM")],
            ),
        )


async def run_evaluation(c, req: EvaluateRequest) -> Decision:
    regions, links = await db.load_world(c)
    pol = await db.get_policy(c, req.topic)
    if pol is None and req.inline_policy is not None:
        pol = TopicPolicy(**req.inline_policy.model_dump(), policy_version=1)
        pol = pol.model_copy()
    if pol is None:
        return eng.no_policy(req.operation.value, req.topic)
    if req.policy_version is not None and req.policy_version != pol.policy_version:
        return Decision(
            decision=DecisionKind.REJECTED,
            operation=req.operation.value,
            topic=req.topic,
            policy_version=pol.policy_version,
            reasons=[
                Reason(code="STALE_POLICY_VERSION", message=f"caller used policy v{req.policy_version}; current is v{pol.policy_version}", category="VERSION")
            ],
        )
    ctx = eng.Ctx(regions, links, pol)
    op = req.operation
    if op == Operation.STORE:
        _need(req, "region")
        return eng.evaluate_store(ctx, req.region, req.required_bytes)
    if op == Operation.PROCESS:
        _need(req, "region")
        return eng.evaluate_process(ctx, req.region, req.capability or "gateway")
    if op == Operation.ROUTE:
        _need(req, "route")
        return eng.evaluate_route(ctx, req.route)
    if op == Operation.ASSIGN:
        _need(req, "source_region", "destination_region")
        return eng.evaluate_assignment(ctx, req.source_region, req.destination_region, req.route, req.required_bytes)
    if op == Operation.BACKUP:
        _need(req, "source_region", "destination_region")
        return eng.evaluate_backup(ctx, req.source_region, req.destination_region, req.route, req.required_bytes)
    if op == Operation.PROMOTE:
        _need(req, "region")
        status = req.replica_status
        if status is None:
            rows = [a for a in await db.get_assignments(c, req.topic) if a.destination_region == req.region]
            if rows:
                order = [
                    ReplicaStatus.REVOKED,
                    ReplicaStatus.FAILED,
                    ReplicaStatus.PENDING,
                    ReplicaStatus.PROVISIONED,
                    ReplicaStatus.SYNCING,
                    ReplicaStatus.IN_SYNC,
                ]
                status = min((a.replica_status for a in rows), key=order.index)  # the weakest partition decides
        return eng.evaluate_promotion(ctx, req.region, status)
    if op == Operation.PLAN:
        return eng.plan_placement(ctx, req.source_region or pol.home_region, req.count, req.required_bytes)
    if op == Operation.RECOVERY:
        existing = {a.destination_region for a in await db.get_assignments(c, req.topic) if a.replica_status != ReplicaStatus.REVOKED}
        return eng.select_recovery_destination(ctx, req.source_region or pol.home_region, req.failed_regions, existing, req.count or 1, req.required_bytes)
    if op == Operation.FAILBACK:
        _need(req, "region", "source_region")
        return eng.evaluate_failback(ctx, req.region, req.source_region)
    raise AssertionError(op)


@app.post("/api/v1/placement/evaluate")
async def evaluate(req: EvaluateRequest, request: Request, who: str = Depends(role("service"))):
    async with request.app.state.pool.acquire() as c, c.transaction():
        d = await run_evaluation(c, req)
        d = await audit_decision(c, d, req.enforcement_point.value, _subject(req), req.requester or who, req.request_id)
    return respond(d, status_for(d))


@app.get("/api/v1/placement/{topic}")
async def placement(topic: str, request: Request, who: str = Depends(role("service"))):
    async with request.app.state.pool.acquire() as c, c.transaction():
        pol = await db.get_policy(c, topic)
        if pol is None:
            d = eng.no_policy("PLAN", topic)
            await audit_decision(c, d, "API", None, who, None)
            return respond(d, 404)
        regions, links = await db.load_world(c)
        ctx = eng.Ctx(regions, links, pol)
        plan = eng.plan_placement(ctx, pol.home_region)
        assignments = await db.get_assignments(c, topic)
        checked = []
        for a in assignments:
            ad = eng.evaluate_assignment(ctx, a.source_region, a.destination_region)
            checked.append(
                {
                    **a.model_dump(mode="json"),
                    "currently_legal": ad.allowed or not any(r.category == "LEGAL" for r in ad.reasons),
                    "reasons": [r.code for r in ad.reasons],
                }
            )
        await audit_decision(c, plan, "API", {"count": pol.regional_replica_count}, who, None)
    return {**plan.model_dump(mode="json"), "assignments": checked}


class AssignRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    partitions: int = Field(1, ge=1, le=1024)
    source_region: Optional[str] = None


async def create_assignments(c, topic: str, partitions: int, source: Optional[str], who: str, point: str) -> tuple[Decision, list]:
    regions, links = await db.load_world(c)
    pol = await db.get_policy(c, topic)
    if pol is None:
        d = eng.no_policy("ASSIGN", topic)
        await audit_decision(c, d, point, None, who, None)
        return d, []
    ctx = eng.Ctx(regions, links, pol)
    src = source or pol.home_region
    plan = eng.plan_placement(ctx, src, op="ASSIGN")
    if not plan.allowed:
        await audit_decision(c, plan, point, {"partitions": [partitions], "source_region": src}, who, None)
        return plan, []
    made = []
    for ch in plan.details["chosen"]:
        dest = ch["region"]
        gate = eng.evaluate_assignment(ctx, src, dest)  # per-assignment re-check before any write
        if not gate.allowed:
            await audit_decision(c, gate, point, {"source_region": src, "destination_region": dest}, who, None)
            return gate, []
        for part in range(partitions):
            await c.execute(
                """INSERT INTO replica_assignments (topic, partition, source_region, destination_region, replica_status, policy_version)
                   VALUES ($1,$2,$3,$4,'PENDING',$5)
                   ON CONFLICT (topic, partition, destination_region) DO UPDATE
                   SET source_region=$3, policy_version=$5, assigned_at=now(),
                       replica_status = CASE WHEN replica_assignments.replica_status IN ('REVOKED','FAILED') THEN 'PENDING' ELSE replica_assignments.replica_status END""",
                topic,
                part,
                src,
                dest,
                pol.policy_version,
            )
        made.append(dest)
    await audit_decision(c, plan, point, {"partitions": [partitions], "source_region": src, "chosen": made}, who, None)
    return plan, made


@app.post("/api/v1/placement/{topic}/assignments")
async def post_assignments(topic: str, body: AssignRequest, request: Request, who: str = Depends(role("writer"))):
    async with request.app.state.pool.acquire() as c, c.transaction():
        d, made = await create_assignments(c, topic, body.partitions, body.source_region, who, "REPLICA_CREATION")
        rows = [a.model_dump(mode="json") for a in await db.get_assignments(c, topic)] if d.allowed else []
    return respond(d, status_for(d), assignments=rows)


class StatusUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    replica_status: ReplicaStatus


@app.put("/api/v1/placement/{topic}/assignments/{partition}/{destination_region}/status")
async def put_status(topic: str, partition: int, destination_region: str, body: StatusUpdate, request: Request, who: str = Depends(role("service"))):
    async with request.app.state.pool.acquire() as c, c.transaction():
        regions, links = await db.load_world(c)
        pol = await db.get_policy(c, topic)
        if pol is None:
            return respond(eng.no_policy("STATUS", topic), 404)
        row = await c.fetchrow(
            "SELECT * FROM replica_assignments WHERE topic=$1 AND partition=$2 AND destination_region=$3 FOR UPDATE", topic, partition, destination_region
        )
        if row is None:
            d = Decision(
                decision=DecisionKind.REJECTED,
                operation="STATUS",
                topic=topic,
                policy_version=pol.policy_version,
                reasons=[Reason(code="NO_REPLICA", message="no such assignment", category="TECHNICAL", region=destination_region)],
            )
            return respond(d, 404)
        if body.replica_status != ReplicaStatus.REVOKED:  # a revoked/illegal replica can never advance
            ad = eng.evaluate_assignment(eng.Ctx(regions, links, pol), row["source_region"], destination_region)
            legal = [r for r in ad.reasons if r.category == "LEGAL"]
            if legal or row["replica_status"] == "REVOKED":
                d = Decision(
                    decision=DecisionKind.REJECTED,
                    operation="STATUS",
                    topic=topic,
                    policy_version=pol.policy_version,
                    reasons=legal
                    or [
                        Reason(
                            code="REPLICA_REVOKED", message="revoked replicas cannot be re-activated; re-assign", category="LEGAL", region=destination_region
                        )
                    ],
                )
                await audit_decision(c, d, "API", {"destination_region": destination_region, "partitions": [partition]}, who, None)
                return respond(d, 403)
        await c.execute(
            "UPDATE replica_assignments SET replica_status=$4 WHERE topic=$1 AND partition=$2 AND destination_region=$3",
            topic,
            partition,
            destination_region,
            body.replica_status.value,
        )
    return {
        "decision": "ALLOWED",
        "operation": "STATUS",
        "topic": topic,
        "policy_version": pol.policy_version,
        "partition": partition,
        "destination_region": destination_region,
        "replica_status": body.replica_status.value,
    }


class ProvisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    partitions: int = Field(1, ge=1, le=1024)
    source_region: Optional[str] = None


@app.post("/api/v1/placement/{topic}/provision")
async def provision(topic: str, body: ProvisionRequest, request: Request, who: str = Depends(role("writer")), x_request_id: Optional[str] = Header(None)):
    """Create assignments, then ask each destination's regional agent to create the replica topic. The agent
    validates the request against its own policy state and may refuse."""
    st = request.app.state
    async with st.pool.acquire() as c, c.transaction():
        plan, made = await create_assignments(c, topic, body.partitions, body.source_region, who, "REPLICA_CREATION")
        pol = await db.get_policy(c, topic) if plan.allowed else None
    if not plan.allowed:
        return respond(plan, status_for(plan), results=[])
    results = []
    async with httpx.AsyncClient(timeout=15) as client:
        for dest in made:
            url = st.agents.get(dest)
            r = {"destination_region": dest}
            if not url:
                r.update(outcome="NO_AGENT", detail="no regional agent is registered for this region")
                results.append(r)
                continue
            try:
                resp = await client.post(
                    f"{url}/agent/v1/replicas",
                    headers={"X-Service-Key": st.keys["service"]},
                    json={
                        "topic": topic,
                        "partition_count": body.partitions,
                        "source_region": body.source_region or pol.home_region,
                        "policy_version": pol.policy_version,
                        "request_id": x_request_id,
                        "requester": who,
                    },
                )
                j = resp.json()
                r.update(outcome=j.get("decision"), http_status=resp.status_code, reasons=[x["code"] for x in j.get("reasons", [])])
                if resp.status_code in (200, 201) and j.get("decision") == "ALLOWED":
                    async with st.pool.acquire() as c:
                        for part in range(body.partitions):
                            await c.execute(
                                "UPDATE replica_assignments SET replica_status='PROVISIONED' WHERE topic=$1 AND partition=$2 AND destination_region=$3 AND replica_status='PENDING'",
                                topic,
                                part,
                                dest,
                            )
            except Exception as exc:  # noqa: BLE001
                r.update(outcome="POLICY_UNAVAILABLE", detail=f"agent unreachable: {exc!r}")
            results.append(r)
    return {
        "decision": "ALLOWED" if all(x.get("outcome") == "ALLOWED" for x in results) else "REJECTED",
        "operation": "PROVISION",
        "topic": topic,
        "policy_version": pol.policy_version,
        "results": results,
    }


# ---- bundle & audit ---------------------------------------------------------------------------------------
@app.get("/api/v1/bundle")
async def bundle(request: Request, who: str = Depends(role("service"))):
    st = request.app.state
    async with st.pool.acquire() as c, c.transaction(isolation="repeatable_read", readonly=True):
        regions, links = await db.load_world(c)
        pols = await db.all_policies(c)
        v = await db.bundle_version(c)
        topology = await replication_topology(c)
        paths = await db.all_paths(c)
    body = {
        "bundle_version": v,
        "topology": topology,
        "paths": [p.model_dump(mode="json") for p in paths],
        "issued_at": int(time.time() * 1000),
        "regions": [r.model_dump(mode="json") for r in regions.values()],
        "links": [l.model_dump(mode="json") for l in links.values()],
        "policies": {t: p.model_dump(mode="json") for t, p in pols.items()},
    }
    return sign_bundle(body, st.private_pem)


@app.post("/api/v1/audit/decisions", status_code=202)
async def ingest_audit(
    items: list[audit.AuditIn] = Body(..., max_length=500),
    request: Request = None,
    who: str = Depends(role("service")),
    x_source: Optional[str] = Header(None, alias="X-Audit-Source"),
):
    src = f"agent:{x_source}"[:64] if x_source else "agent"
    async with request.app.state.pool.acquire() as c, c.transaction():
        for i in items:
            if i.decision not in {k.value for k in DecisionKind}:
                continue
            await audit.append(
                c,
                source=src,
                operation=i.operation,
                point=i.enforcement_point.value,
                topic=i.topic,
                policy_version=i.policy_version,
                decision=i.decision,
                reasons=i.reasons,
                subject=i.subject,
                requester=i.requester,
                request_id=i.request_id,
            )
    return {"accepted": len(items)}


@app.get("/api/v1/policies")
async def list_policies(request: Request, who: str = Depends(role("service")), limit: int = Query(200, ge=1, le=1000)):
    """Summary of every topic policy for the dashboard: classification, zone, regions, replica placement. No payloads, no secrets."""
    out = []
    async with request.app.state.pool.acquire() as c:
        regions, _ = await db.load_world(c)
        for r in await c.fetch("SELECT topic FROM topic_policies ORDER BY topic LIMIT $1", limit):
            pol = await db.get_policy(c, r["topic"])
            if pol is None:
                continue
            by_region: dict[str, list[str]] = {}
            for a in await db.get_assignments(c, r["topic"]):
                by_region.setdefault(a.destination_region, []).append(a.replica_status.value)
            order = ["REVOKED", "FAILED", "PENDING", "PROVISIONED", "SYNCING", "IN_SYNC"]
            allowed = set(pol.allowed_storage_regions)
            out.append(
                {
                    "topic": pol.topic,
                    "classification": pol.classification.value,
                    "legal_zone": pol.legal_zone,
                    "home_region": pol.home_region,
                    "allowed_storage_regions": sorted(allowed),
                    "allowed_processing_regions": sorted(pol.allowed_processing_regions),
                    "excluded_regions": sorted(pol.excluded_regions),
                    "prohibited_regions": sorted((set(regions) - allowed) | set(pol.excluded_regions)),
                    "policy_version": pol.policy_version,
                    "replicas": [{"region": reg, "status": min(st, key=order.index)} for reg, st in sorted(by_region.items())],
                }
            )
    return {"policies": out}


@app.get("/api/v1/audit/recent")
async def audit_recent(request: Request, who: str = Depends(role("service")), limit: int = Query(100, ge=1, le=500)):
    """The newest decisions first (the dashboard's audit view)."""
    async with request.app.state.pool.acquire() as c:
        rows = await c.fetch("SELECT * FROM policy_decisions ORDER BY id DESC LIMIT $1", limit)
    return {
        "decisions": [
            {
                "id": r["id"],
                "ts": r["ts"].isoformat(),
                "source": r["source"],
                "operation": r["operation"],
                "enforcement_point": r["enforcement_point"],
                "topic": r["topic"],
                "policy_version": r["policy_version"],
                "subject": json.loads(r["subject"]),
                "decision": r["decision"],
                "reasons": json.loads(r["reasons"]),
                "requester": r["requester"],
            }
            for r in rows
        ]
    }


@app.get("/api/v1/audit/stats")
async def audit_stats(request: Request, who: str = Depends(role("service"))):
    """Counts of recorded decisions by operation and decision (the source of the dashboard's policy metrics)."""
    async with request.app.state.pool.acquire() as c:
        rows = await c.fetch("SELECT operation, enforcement_point, decision, count(*) AS n FROM policy_decisions GROUP BY 1,2,3 ORDER BY 1,2,3")
        assign = await c.fetch("SELECT replica_status, count(*) AS n FROM replica_assignments GROUP BY 1")
    return {
        "decisions": [{"operation": r["operation"], "enforcement_point": r["enforcement_point"], "decision": r["decision"], "count": r["n"]} for r in rows],
        "replica_assignments": {r["replica_status"]: r["n"] for r in assign},
    }


@app.get("/api/v1/audit")
async def get_audit(
    request: Request,
    who: str = Depends(role("service")),
    topic: Optional[str] = None,
    decision: Optional[str] = None,
    source: Optional[str] = None,
    request_id: Optional[str] = None,
    from_id: int = 0,
    limit: int = Query(100, ge=1, le=1000),
):
    q, args = "SELECT * FROM policy_decisions WHERE id > $1", [from_id]
    for col, val in (("topic", topic), ("decision", decision), ("source", source), ("request_id", request_id)):
        if val is not None:
            args.append(val)
            q += f" AND {col} = ${len(args)}"
    q += f" ORDER BY id LIMIT {int(limit)}"
    async with request.app.state.pool.acquire() as c:
        rows = await c.fetch(q, *args)
    out = [
        {
            "id": r["id"],
            "ts": r["ts"].isoformat(),
            "source": r["source"],
            "operation": r["operation"],
            "enforcement_point": r["enforcement_point"],
            "topic": r["topic"],
            "policy_version": r["policy_version"],
            "subject": json.loads(r["subject"]),
            "decision": r["decision"],
            "reasons": json.loads(r["reasons"]),
            "requester": r["requester"],
            "request_id": r["request_id"],
            "prev_hash": r["prev_hash"],
            "hash": r["hash"],
        }
        for r in rows
    ]
    return {"decisions": out}


@app.get("/api/v1/audit/verify")
async def verify_audit(request: Request, who: str = Depends(role("service"))):
    async with request.app.state.pool.acquire() as c:
        return await audit.verify_chain(c)


async def replication_topology(c) -> dict:
    """topic -> active region, epoch, generation and every non-disabled flow (what receivers validate against)."""
    out = {
        r["topic"]: {
            "active_region": r["active_region"],
            "leadership_epoch": r["leadership_epoch"],
            "replication_generation": r["replication_generation"],
            "flows": [],
        }
        for r in await c.fetch("SELECT * FROM topic_topology")
    }
    for f in await c.fetch("SELECT * FROM replication_flows WHERE state <> 'DISABLED' ORDER BY flow_id"):
        if f["topic"] in out:
            out[f["topic"]]["flows"].append(
                {
                    "flow_id": f["flow_id"],
                    "source_region": f["source_region"],
                    "target_region": f["target_region"],
                    "state": f["state"],
                    "generation": f["generation"],
                    "paused": f["paused"],
                }
            )
    return out


import sys  # noqa: E402
from . import replication  # noqa: E402

replication.install(app, sys.modules[__name__])
