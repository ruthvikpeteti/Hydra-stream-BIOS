"""Replication topology and flow control API (Phase 4). Installed into the engine app by main.install()."""

import json
from typing import Optional

import asyncpg
from fastapi import Depends, Header, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from ss_policy import engine as eng
from ss_policy.models import Decision, DecisionKind, NetworkPath, Reason, ReplicaStatus
from ss_policy.topology import validate_new_flow

from . import db


def install(app, M):
    role, respond, audit_decision, ApiErr = M.role, M.respond, M.audit_decision, M.ApiErr

    def bad(code: str, msg: str, category: str = "TOPOLOGY", region: Optional[str] = None) -> Reason:
        return Reason(code=code, message=msg, category=category, region=region)

    async def cluster_of(c, region: str) -> str:
        return await c.fetchval("SELECT COALESCE(cluster_id, region_id) FROM regions WHERE region_id=$1", region) or region

    async def flow_json(c, f) -> dict:
        n = await c.fetchval(
            "SELECT count(DISTINCT partition) FROM replica_assignments WHERE topic=$1 AND destination_region=$2", f["topic"], f["target_region"]
        )
        return {
            "flow_id": f["flow_id"],
            "topic": f["topic"],
            "source_region": f["source_region"],
            "target_region": f["target_region"],
            "source_cluster": await cluster_of(c, f["source_region"]),
            "target_cluster": await cluster_of(c, f["target_region"]),
            "state": f["state"],
            "paused": f["paused"],
            "generation": f["generation"],
            "partitions": max(1, n or 0),
            "fence_bundle_version": f["fence_bundle_version"],
            "disabled_reason": f["disabled_reason"],
            "backlog_soft_bytes": f["backlog_soft_bytes"],
            "backlog_hard_bytes": f["backlog_hard_bytes"],
            "cutover_report": json.loads(f["cutover_report"]) if f["cutover_report"] else None,
        }

    async def topology_of(c, topic: str):
        return await c.fetchrow("SELECT * FROM topic_topology WHERE topic=$1", topic)

    async def live_edges(c, topic: str) -> list[tuple[str, str]]:
        return [
            (r["source_region"], r["target_region"])
            for r in await c.fetch("SELECT source_region, target_region FROM replication_flows WHERE topic=$1 AND state <> 'DISABLED'", topic)
        ]

    async def try_add_flow(
        c, pol, topo, ctx, source: str, target: str, state: str, who: str, point: str, fence: Optional[int] = None
    ) -> tuple[Optional[int], Optional[Decision]]:
        """Policy (assignment legality) + topology rules, then insert. Returns (flow_id, None) or (None, rejection)."""
        reasons: list[Reason] = []
        d = eng.evaluate_assignment(ctx, source, target)
        reasons += [r for r in d.reasons if r.category == "LEGAL"]  # an outage must not stop us creating the flow
        reasons += validate_new_flow(topo["active_region"], await live_edges(c, pol.topic), (source, target))
        if reasons:
            rej = Decision(decision=DecisionKind.REJECTED, operation="FLOW_CREATE", topic=pol.topic, policy_version=pol.policy_version, reasons=reasons)
            await audit_decision(c, rej, point, {"source_region": source, "destination_region": target}, who, None)
            return None, rej
        fid = await c.fetchval(
            """INSERT INTO replication_flows (topic, source_region, target_region, generation, state, fence_bundle_version)
               VALUES ($1,$2,$3,$4,$5,$6) RETURNING flow_id""",
            pol.topic,
            source,
            target,
            topo["replication_generation"],
            state,
            fence,
        )
        ok = Decision(
            decision=DecisionKind.ALLOWED, operation="FLOW_CREATE", topic=pol.topic, policy_version=pol.policy_version, details={"flow_id": fid, "state": state}
        )
        await audit_decision(
            c, ok, point, {"source_region": source, "destination_region": target, "flow_id": fid, "generation": topo["replication_generation"]}, who, None
        )
        return fid, None

    class StartRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")
        partitions: int = Field(1, ge=1, le=1024)

    @app.post("/api/v1/replication/topics/{topic}/start")
    async def start(topic: str, body: StartRequest, request: Request, who: str = Depends(role("writer")), x_request_id: Optional[str] = Header(None)):
        """Provision replicas (each destination's agent validates), then enable flows active -> replica."""
        resp = await M.provision(topic, M.ProvisionRequest(partitions=body.partitions), request, who, x_request_id)
        prov = json.loads(resp.body) if hasattr(resp, "body") else resp  # dict on success, JSONResponse on rejection
        status_code = getattr(resp, "status_code", 200)
        results = prov.get("results") or []
        if not results:
            return respond(
                Decision.model_validate({k: prov[k] for k in ("decision", "operation", "policy_version", "reasons", "details", "topic") if k in prov}),
                status_code,
            )
        created, blocked = [], []
        async with request.app.state.pool.acquire() as c, c.transaction():
            regions, links = await db.load_world(c)
            pol = await db.get_policy(c, topic)
            topo = await topology_of(c, topic)
            if topo is None:
                await c.execute(
                    "INSERT INTO topic_topology (topic, active_region, leadership_epoch, replication_generation) VALUES ($1,$2,1,1)", topic, pol.home_region
                )
                topo = await topology_of(c, topic)
            ctx = eng.Ctx(regions, links, pol)
            for r in results:
                dest = r["destination_region"]
                if r.get("outcome") != "ALLOWED":
                    blocked.append({"destination_region": dest, "reasons": r.get("reasons") or [r.get("detail")]})
                    continue
                if await c.fetchval("SELECT 1 FROM replication_flows WHERE topic=$1 AND target_region=$2 AND state <> 'DISABLED'", topic, dest):
                    continue
                fid, rej = await try_add_flow(c, pol, topo, ctx, topo["active_region"], dest, "ENABLED", who, "FLOW_INIT")
                (created if fid else blocked).append(
                    fid and {"flow_id": fid, "target_region": dest} or {"destination_region": dest, "reasons": [x.code for x in rej.reasons]}
                )
            v = await db.bump_bundle(c)
            flows = [await flow_json(c, f) for f in await c.fetch("SELECT * FROM replication_flows WHERE topic=$1 ORDER BY flow_id", topic)]
        return {
            "decision": "ALLOWED" if not blocked else "REJECTED",
            "operation": "REPLICATION_START",
            "topic": topic,
            "policy_version": pol.policy_version,
            "topology": dict(topo),
            "flows": flows,
            "created": created,
            "blocked": blocked,
            "provision": results,
            "bundle_version": v,
        }

    class FlowIn(BaseModel):
        model_config = ConfigDict(extra="forbid")
        source_region: str
        target_region: str

    @app.post("/api/v1/replication/topics/{topic}/flows")
    async def add_flow(topic: str, body: FlowIn, request: Request, who: str = Depends(role("admin"))):
        async with request.app.state.pool.acquire() as c, c.transaction():
            pol = await db.get_policy(c, topic)
            topo = await topology_of(c, topic)
            if pol is None or topo is None:
                return respond(eng.no_policy("FLOW_CREATE", topic), 404)
            regions, links = await db.load_world(c)
            fid, rej = await try_add_flow(c, pol, topo, eng.Ctx(regions, links, pol), body.source_region, body.target_region, "ENABLED", who, "FLOW_INIT")
            if rej:
                return respond(rej, 403)
            await db.bump_bundle(c)
        return {"decision": "ALLOWED", "operation": "FLOW_CREATE", "topic": topic, "flow_id": fid, "policy_version": pol.policy_version}

    @app.get("/api/v1/replication/topics/{topic}")
    async def get_topology(topic: str, request: Request, who: str = Depends(role("service"))):
        async with request.app.state.pool.acquire() as c:
            topo = await topology_of(c, topic)
            if topo is None:
                return respond(eng.no_policy("TOPOLOGY", topic), 404)
            flows = [await flow_json(c, f) for f in await c.fetch("SELECT * FROM replication_flows WHERE topic=$1 ORDER BY flow_id", topic)]
            cps = [dict(r) for r in await c.fetch("SELECT * FROM replication_checkpoints WHERE source_topic=$1 ORDER BY 1,2,4", topic)]
        for r in cps:
            for k in ("last_success_timestamp", "updated_at"):
                r[k] = r[k].isoformat() if r[k] else None
        return {
            "topic": topic,
            "topology": {k: (v if not hasattr(v, "isoformat") else v.isoformat()) for k, v in dict(topo).items()},
            "flows": flows,
            "checkpoints": cps,
        }

    @app.get("/api/v1/replication/flows")
    async def discover(request: Request, source_region: str = Query(...), who: str = Depends(role("service"))):
        """What a source agent may replicate: live flows from `source_region` whose policy still LEGALLY allows them."""
        out, blocked = [], []
        async with request.app.state.pool.acquire() as c:
            regions, links = await db.load_world(c)
            for f in await c.fetch("SELECT * FROM replication_flows WHERE source_region=$1 AND state <> 'DISABLED' ORDER BY flow_id", source_region):
                pol, topo = await db.get_policy(c, f["topic"]), await topology_of(c, f["topic"])
                if pol is None or topo is None:
                    continue
                d = eng.evaluate_assignment(eng.Ctx(regions, links, pol), f["source_region"], f["target_region"])
                legal = [r.code for r in d.reasons if r.category == "LEGAL"]
                j = await flow_json(c, f)
                j.update(
                    policy_version=pol.policy_version,
                    leadership_epoch=topo["leadership_epoch"],
                    previous_active_region=topo["previous_active_region"],
                    previous_active_cluster=await cluster_of(c, topo["previous_active_region"]) if topo["previous_active_region"] else None,
                    active_region=topo["active_region"],
                )
                ctx = eng.Ctx(regions, links, pol)
                j["paths"] = []
                for np_ in await db.all_paths(c):
                    if np_.source_region == f["source_region"] and np_.target_region == f["target_region"]:
                        pd = eng.evaluate_path(ctx, np_)  # legal / security eligibility is decided HERE, never by the router
                        j["paths"].append({**np_.model_dump(mode="json"), "policy_eligible": pd.allowed, "reasons": [r.code for r in pd.reasons]})
                (blocked if legal else out).append({**j, "blocked_reasons": legal} if legal else j)
        return {"flows": out, "blocked": blocked}

    async def set_paused(flow_id: int, paused: bool, request: Request, who: str):
        async with request.app.state.pool.acquire() as c, c.transaction():
            f = await c.fetchrow("UPDATE replication_flows SET paused=$2 WHERE flow_id=$1 AND state <> 'DISABLED' RETURNING *", flow_id, paused)
            if f is None:
                raise ApiErr(
                    404, Decision(decision=DecisionKind.REJECTED, operation="FLOW_PAUSE", reasons=[bad("NO_SUCH_FLOW", "no live flow with that id", "SYSTEM")])
                )
            pol = await db.get_policy(c, f["topic"])
            d = Decision(
                decision=DecisionKind.ALLOWED, operation="FLOW_PAUSE" if paused else "FLOW_RESUME", topic=f["topic"], policy_version=pol.policy_version
            )
            await audit_decision(c, d, "API", {"flow_id": flow_id, "source_region": f["source_region"], "destination_region": f["target_region"]}, who, None)
            await db.bump_bundle(c)
            return await flow_json(c, f)

    @app.post("/api/v1/replication/flows/{flow_id}/pause")
    async def pause(flow_id: int, request: Request, who: str = Depends(role("admin"))):
        return await set_paused(flow_id, True, request, who)

    @app.post("/api/v1/replication/flows/{flow_id}/resume")
    async def resume(flow_id: int, request: Request, who: str = Depends(role("admin"))):
        return await set_paused(flow_id, False, request, who)

    class PromoteRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")
        candidate_region: str
        reason: str = Field("operator request", max_length=200)
        # Phase 6: the failover controller obtains the epoch from the coordinator (the authority) and passes it here, so the
        # topology can never disagree with what the gateways stamp. Retrying the same request is a no-op (idempotent).
        epoch: Optional[int] = Field(None, ge=1)
        generation: Optional[int] = Field(None, ge=1)
        dry_run: bool = False
        reseed: bool = False  # failback: the other replicas are rebuilt from the new active (reseed endpoint), no cutover flows

    @app.post("/api/v1/replication/topics/{topic}/promote")
    async def promote(topic: str, body: PromoteRequest, request: Request, who: str = Depends(role("admin")), x_request_id: Optional[str] = Header(None)):
        """Make a replica region active. Obsolete flows are disabled in the SAME transaction, the epoch and generation
        advance, and flows from the new active region to the remaining replicas are created PENDING_CUTOVER: they only
        start once their new source has validated a cutover position (see the source agent)."""
        async with request.app.state.pool.acquire() as c, c.transaction():
            pol = await db.get_policy(c, topic)
            topo = await c.fetchrow("SELECT * FROM topic_topology WHERE topic=$1 FOR UPDATE", topic)
            if pol is None or topo is None:
                return respond(eng.no_policy("PROMOTE_TOPOLOGY", topic), 404)
            cand, old = body.candidate_region, topo["active_region"]
            if body.epoch is not None and cand == old and topo["leadership_epoch"] == body.epoch:
                flows = [
                    await flow_json(c, f)
                    for f in await c.fetch("SELECT * FROM replication_flows WHERE topic=$1 AND state <> 'DISABLED' ORDER BY flow_id", topic)
                ]
                return {
                    "decision": "ALLOWED",
                    "operation": "PROMOTE_TOPOLOGY",
                    "topic": topic,
                    "idempotent": True,
                    "policy_version": pol.policy_version,
                    "active_region": cand,
                    "previous_active_region": topo["previous_active_region"],
                    "leadership_epoch": topo["leadership_epoch"],
                    "replication_generation": topo["replication_generation"],
                    "live_flows": flows,
                }
            regions, links = await db.load_world(c)
            ctx = eng.Ctx(regions, links, pol)
            rows = [a for a in await db.get_assignments(c, topic) if a.destination_region == cand]
            order = [
                ReplicaStatus.REVOKED,
                ReplicaStatus.FAILED,
                ReplicaStatus.PENDING,
                ReplicaStatus.PROVISIONED,
                ReplicaStatus.SYNCING,
                ReplicaStatus.IN_SYNC,
            ]
            status = min((a.replica_status for a in rows), key=order.index) if rows else None
            d = eng.evaluate_promotion(ctx, cand, status)
            reasons = list(d.reasons)
            if cand == old:
                reasons.append(bad("ALREADY_ACTIVE", f"{cand} is already the active region"))
            if not await c.fetchval(
                "SELECT 1 FROM replication_flows WHERE topic=$1 AND source_region=$2 AND target_region=$3 AND state='ENABLED'", topic, old, cand
            ):
                reasons.append(bad("NO_FLOW_FROM_ACTIVE", f"{cand} is not fed by an enabled flow from {old}", region=cand))
            if body.epoch is not None and body.epoch <= topo["leadership_epoch"]:
                reasons.append(bad("STALE_EPOCH", f"requested epoch {body.epoch} is not above the topology epoch {topo['leadership_epoch']}"))
            if reasons:
                rej = Decision(decision=DecisionKind.REJECTED, operation="PROMOTE_TOPOLOGY", topic=topic, policy_version=pol.policy_version, reasons=reasons)
                if not body.dry_run:
                    await audit_decision(c, rej, "PROMOTION", {"region": cand, "source_region": old}, who, x_request_id)
                return respond(rej, 403)
            if body.dry_run:
                return {"decision": "ALLOWED", "operation": "PROMOTE_TOPOLOGY", "topic": topic, "dry_run": True, "policy_version": pol.policy_version}
            new_epoch = body.epoch or topo["leadership_epoch"] + 1
            new_gen = max(body.generation or topo["replication_generation"] + 1, new_epoch)
            disabled = await c.fetch(
                "UPDATE replication_flows SET state='DISABLED', disabled_at=now(), disabled_reason=$2 "
                "WHERE topic=$1 AND state <> 'DISABLED' RETURNING flow_id, source_region, target_region",
                topic,
                f"promotion of {cand} (epoch {new_epoch})",
            )
            await c.execute(
                "UPDATE topic_topology SET active_region=$2, previous_active_region=$3, leadership_epoch=$4, "
                "replication_generation=$5, updated_at=now() WHERE topic=$1",
                topic,
                cand,
                old,
                new_epoch,
                new_gen,
            )
            topo = await topology_of(c, topic)
            targets = sorted({a.destination_region for a in await db.get_assignments(c, topic) if a.replica_status != ReplicaStatus.REVOKED} - {cand, old})
            pending, blocked, new_ids = [], [], []
            for t in [] if body.reseed else targets:  # the old active is NOT a target: it must be re-seeded, never fed (INV-9)
                fid, rej = await try_add_flow(c, pol, topo, ctx, cand, t, "PENDING_CUTOVER", who, "PROMOTION")
                if fid:
                    new_ids.append(fid)
                    pending.append({"flow_id": fid, "source_region": cand, "target_region": t})
                else:
                    blocked.append({"target_region": t, "reasons": [x.code for x in rej.reasons]})
            fence = await db.bump_bundle(c)
            if new_ids:
                await c.execute("UPDATE replication_flows SET fence_bundle_version=$2 WHERE flow_id = ANY($1::bigint[])", new_ids, fence)
            ok = Decision(
                decision=DecisionKind.ALLOWED,
                operation="PROMOTE_TOPOLOGY",
                topic=topic,
                policy_version=pol.policy_version,
                details={"previous_active_region": old, "new_epoch": new_epoch, "new_generation": new_gen},
            )
            await audit_decision(
                c, ok, "PROMOTION", {"region": cand, "source_region": old, "epoch": new_epoch, "generation": new_gen, "count": len(disabled)}, who, x_request_id
            )
        return {
            "decision": "ALLOWED",
            "operation": "PROMOTE_TOPOLOGY",
            "topic": topic,
            "policy_version": pol.policy_version,
            "active_region": cand,
            "previous_active_region": old,
            "leadership_epoch": new_epoch,
            "replication_generation": new_gen,
            "disabled_flows": [dict(r) for r in disabled],
            "pending_flows": pending,
            "blocked_targets": blocked,
            "fence_bundle_version": fence,
        }

    class ReseedRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")
        region: str
        partitions: int = Field(ge=1, le=1024)
        reason: str = Field("failback re-seed", max_length=200)
        dry_run: bool = False

    @app.post("/api/v1/replication/topics/{topic}/reseed")
    async def reseed(topic: str, body: ReseedRequest, request: Request, who: str = Depends(role("admin"))):
        """Rebuild `region` as a replica of the ACTIVE region from offset 0 (its copy of the topic must have been recreated EMPTY by the
        caller). Any live flow into the region and every checkpoint into it are dropped first, a replica assignment is (re)created and a
        new ENABLED flow active -> region is added at the current generation. A region is re-seeded, never fed on top of old history (INV-9)."""
        async with request.app.state.pool.acquire() as c, c.transaction():
            pol = await db.get_policy(c, topic)
            topo = await c.fetchrow("SELECT * FROM topic_topology WHERE topic=$1 FOR UPDATE", topic)
            if pol is None or topo is None:
                return respond(eng.no_policy("RESEED", topic), 404)
            active = topo["active_region"]
            if body.region == active:
                raise ApiErr(
                    409,
                    Decision(
                        decision=DecisionKind.REJECTED,
                        operation="RESEED",
                        topic=topic,
                        policy_version=pol.policy_version,
                        reasons=[bad("ALREADY_ACTIVE", f"{body.region} is the active region and cannot be re-seeded")],
                    ),
                )
            regions, links = await db.load_world(c)
            ctx = eng.Ctx(regions, links, pol)
            gate = eng.evaluate_assignment(ctx, active, body.region)
            legal = [r for r in gate.reasons if r.category == "LEGAL"]
            if legal:  # the policy is judged BEFORE anything is touched: a refusal leaves no trace except its audit entry
                rej = Decision(decision=DecisionKind.REJECTED, operation="RESEED", topic=topic, policy_version=pol.policy_version, reasons=legal)
                if not body.dry_run:
                    await audit_decision(c, rej, "FAILBACK", {"region": body.region, "source_region": active}, who, None)
                return respond(rej, 403)
            if body.dry_run:
                return {"decision": "ALLOWED", "operation": "RESEED", "topic": topic, "dry_run": True}
            tgt_cluster = await cluster_of(c, body.region)
            await c.execute(
                "UPDATE replication_flows SET state='DISABLED', disabled_at=now(), disabled_reason=$3 WHERE topic=$1 AND target_region=$2 AND state <> 'DISABLED'",
                topic,
                body.region,
                f"re-seed of {body.region}",
            )
            await c.execute("DELETE FROM replication_checkpoints WHERE source_topic=$1 AND target_cluster=$2", topic, tgt_cluster)
            for part in range(body.partitions):
                await c.execute(
                    """INSERT INTO replica_assignments (topic, partition, source_region, destination_region, replica_status, policy_version)
                       VALUES ($1,$2,$3,$4,'PROVISIONED',$5)
                       ON CONFLICT (topic, partition, destination_region) DO UPDATE
                       SET source_region=$3, policy_version=$5, assigned_at=now(), replica_status='PROVISIONED'""",
                    topic,
                    part,
                    active,
                    body.region,
                    pol.policy_version,
                )
            topo = await topology_of(c, topic)
            fid, rej = await try_add_flow(c, pol, topo, ctx, active, body.region, "ENABLED", who, "FAILBACK")
            if rej:
                return respond(rej, 403)
            v = await db.bump_bundle(c)
            d = Decision(decision=DecisionKind.ALLOWED, operation="RESEED", topic=topic, policy_version=pol.policy_version, details={"flow_id": fid})
            await audit_decision(c, d, "FAILBACK", {"region": body.region, "source_region": active, "flow_id": fid}, who, None)
        return {
            "decision": "ALLOWED",
            "operation": "RESEED",
            "topic": topic,
            "flow_id": fid,
            "source_region": active,
            "target_region": body.region,
            "bundle_version": v,
        }

    @app.get("/api/v1/replication/topics")
    async def list_topologies(request: Request, who: str = Depends(role("service"))):
        """Every replicated topic with its topology, partition count and live flows (used by the failover controller)."""
        out = []
        async with request.app.state.pool.acquire() as c:
            for t in await c.fetch("SELECT * FROM topic_topology ORDER BY topic"):
                pol = await db.get_policy(c, t["topic"])
                n = await c.fetchval("SELECT count(DISTINCT partition) FROM replica_assignments WHERE topic=$1", t["topic"])
                flows = [
                    await flow_json(c, f)
                    for f in await c.fetch("SELECT * FROM replication_flows WHERE topic=$1 AND state <> 'DISABLED' ORDER BY flow_id", t["topic"])
                ]
                out.append(
                    {
                        "topic": t["topic"],
                        "active_region": t["active_region"],
                        "leadership_epoch": t["leadership_epoch"],
                        "replication_generation": t["replication_generation"],
                        "previous_active_region": t["previous_active_region"],
                        "partitions": n or 0,
                        "max_lag_ms": pol.maximum_replication_lag_ms if pol else None,
                        "classification": pol.classification.value if pol else None,
                        "flows": flows,
                    }
                )
        return {"topics": out}

    class ActivateRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")
        report: dict

    @app.post("/api/v1/replication/flows/{flow_id}/activate")
    async def activate(flow_id: int, body: ActivateRequest, request: Request, who: str = Depends(role("service"))):
        """A new source reports its validated cutover. The flow starts only if a starting checkpoint exists for every
        partition at the flow's generation."""
        async with request.app.state.pool.acquire() as c, c.transaction():
            f = await c.fetchrow("SELECT * FROM replication_flows WHERE flow_id=$1 FOR UPDATE", flow_id)
            if f is None or f["state"] != "PENDING_CUTOVER":
                raise ApiErr(
                    409,
                    Decision(
                        decision=DecisionKind.REJECTED,
                        operation="FLOW_ACTIVATE",
                        reasons=[bad("NOT_PENDING_CUTOVER", "flow is not awaiting cutover", "SYSTEM")],
                    ),
                )
            pol = await db.get_policy(c, f["topic"])
            j = await flow_json(c, f)
            have = await c.fetchval(
                "SELECT count(*) FROM replication_checkpoints WHERE source_cluster=$1 AND target_cluster=$2 AND source_topic=$3 AND replication_generation=$4",
                j["source_cluster"],
                j["target_cluster"],
                f["topic"],
                f["generation"],
            )
            if have != j["partitions"]:
                d = Decision(
                    decision=DecisionKind.REJECTED,
                    operation="FLOW_ACTIVATE",
                    topic=f["topic"],
                    policy_version=pol.policy_version,
                    reasons=[
                        bad("CUTOVER_CHECKPOINT_MISSING", f"{have} of {j['partitions']} partitions have a cutover checkpoint at generation {f['generation']}")
                    ],
                )
                await audit_decision(c, d, "FLOW_INIT", {"flow_id": flow_id}, who, None)
                return respond(d, 409)
            await c.execute("UPDATE replication_flows SET state='ENABLED', cutover_report=$2::jsonb WHERE flow_id=$1", flow_id, json.dumps(body.report))
            v = await db.bump_bundle(c)
            d = Decision(decision=DecisionKind.ALLOWED, operation="FLOW_ACTIVATE", topic=f["topic"], policy_version=pol.policy_version)
            await audit_decision(
                c,
                d,
                "FLOW_INIT",
                {"flow_id": flow_id, "source_region": f["source_region"], "destination_region": f["target_region"], "generation": f["generation"]},
                who,
                None,
            )
        return {"decision": "ALLOWED", "flow_id": flow_id, "bundle_version": v}

    # ===== Phase 5: network paths, backlog, limits, backpressure =====================================================
    @app.get("/api/v1/network/paths")
    async def list_paths(request: Request, who: str = Depends(role("service"))):
        async with request.app.state.pool.acquire() as c:
            return {"paths": [p.model_dump(mode="json") for p in await db.all_paths(c)]}

    @app.put("/api/v1/network/paths/{path_id}")
    async def put_path(path_id: str, path: NetworkPath, request: Request, who: str = Depends(role("admin"))):
        if path.path_id != path_id:
            raise ApiErr(
                422,
                Decision(decision=DecisionKind.REJECTED, operation="PATH_UPDATE", reasons=[bad("PATH_ID_MISMATCH", "path and body path_id differ", "SYSTEM")]),
            )
        async with request.app.state.pool.acquire() as c, c.transaction():
            try:
                await db.upsert_path(c, path)
            except asyncpg.ForeignKeyViolationError:
                raise ApiErr(
                    422,
                    Decision(
                        decision=DecisionKind.REJECTED,
                        operation="PATH_UPDATE",
                        reasons=[bad("UNKNOWN_REGION", "source and target regions must be registered", "LEGAL")],
                    ),
                )
            v = await db.bump_bundle(c)
            d = Decision(decision=DecisionKind.ALLOWED, operation="PATH_UPDATE", details={"path_id": path_id})
            await audit_decision(
                c, d, "ROUTING", {"path_id": path_id, "source_region": path.source_region, "destination_region": path.target_region}, who, None
            )
        return {"path": path.model_dump(mode="json"), "bundle_version": v}

    class BacklogReport(BaseModel):
        model_config = ConfigDict(extra="forbid")
        backlog_records: int = Field(ge=0)
        backlog_bytes: int = Field(ge=0)
        drain_seconds: Optional[float] = Field(None, ge=0)
        throughput_bps: float = Field(0.0, ge=0)
        state: str = Field(pattern="^(OK|SOFT|HARD)$")
        route: Optional[str] = None

    @app.put("/api/v1/replication/flows/{flow_id}/backlog")
    async def report_backlog(flow_id: int, body: BacklogReport, request: Request, who: str = Depends(role("service"))):
        async with request.app.state.pool.acquire() as c, c.transaction():
            f = await c.fetchrow("SELECT topic FROM replication_flows WHERE flow_id=$1", flow_id)
            if f is None:
                return respond(Decision(decision=DecisionKind.REJECTED, operation="BACKLOG", reasons=[bad("NO_SUCH_FLOW", "no such flow", "SYSTEM")]), 404)
            prev = await c.fetchval("SELECT state FROM flow_backlog WHERE flow_id=$1", flow_id)
            await c.execute(
                """INSERT INTO flow_backlog (flow_id, topic, backlog_records, backlog_bytes, drain_seconds, throughput_bps, state, route)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                   ON CONFLICT (flow_id) DO UPDATE SET backlog_records=$3, backlog_bytes=$4, drain_seconds=$5, throughput_bps=$6,
                       state=$7, route=$8, updated_at=now()""",
                flow_id,
                f["topic"],
                body.backlog_records,
                body.backlog_bytes,
                body.drain_seconds,
                body.throughput_bps,
                body.state,
                body.route,
            )
            if prev != body.state:  # audit the TRANSITIONS (OK/SOFT/HARD), not every report
                pol = await db.get_policy(c, f["topic"])
                d = Decision(
                    decision=DecisionKind.ALLOWED if body.state != "HARD" else DecisionKind.REJECTED,
                    operation="BACKLOG_STATE",
                    topic=f["topic"],
                    policy_version=pol.policy_version if pol else None,
                    reasons=[]
                    if body.state != "HARD"
                    else [bad("BACKLOG_HARD_LIMIT", "unreplicated backlog reached the hard limit; producers are backpressured", "TECHNICAL")],
                )
                await audit_decision(c, d, "API", {"flow_id": flow_id, "backlog_bytes": body.backlog_bytes}, who, None)
        return {"ok": True}

    @app.get("/api/v1/replication/backlog")
    async def all_backlog(request: Request, topic: Optional[str] = None, who: str = Depends(role("service"))):
        q = (
            "SELECT b.*, f.source_region, f.target_region, f.state AS flow_state, f.backlog_soft_bytes, f.backlog_hard_bytes, extract(epoch from now()-b.updated_at) AS age_s "
            "FROM flow_backlog b JOIN replication_flows f USING (flow_id)" + (" WHERE b.topic=$1" if topic else "") + " ORDER BY flow_id"
        )
        async with request.app.state.pool.acquire() as c:
            rows = await c.fetch(q, *([topic] if topic else []))
        return {
            "flows": [
                {
                    "flow_id": r["flow_id"],
                    "topic": r["topic"],
                    "source_region": r["source_region"],
                    "target_region": r["target_region"],
                    "flow_state": r["flow_state"],
                    "backlog_records": r["backlog_records"],
                    "backlog_bytes": r["backlog_bytes"],
                    "drain_seconds": r["drain_seconds"],
                    "throughput_bps": r["throughput_bps"],
                    "backlog_state": r["state"],
                    "route": r["route"],
                    "soft_limit_bytes": r["backlog_soft_bytes"],
                    "hard_limit_bytes": r["backlog_hard_bytes"],
                    "report_age_s": round(float(r["age_s"]), 1),
                }
                for r in rows
            ]
        }

    class LimitsIn(BaseModel):
        model_config = ConfigDict(extra="forbid")
        soft_bytes: Optional[int] = Field(None, ge=0)
        hard_bytes: Optional[int] = Field(None, ge=0)

    @app.put("/api/v1/replication/flows/{flow_id}/limits")
    async def set_limits(flow_id: int, body: LimitsIn, request: Request, who: str = Depends(role("admin"))):
        if body.soft_bytes is not None and body.hard_bytes is not None and body.soft_bytes > body.hard_bytes:
            raise ApiErr(
                422,
                Decision(decision=DecisionKind.REJECTED, operation="FLOW_LIMITS", reasons=[bad("LIMITS_INVALID", "soft limit exceeds hard limit", "SYSTEM")]),
            )
        async with request.app.state.pool.acquire() as c, c.transaction():
            f = await c.fetchrow(
                "UPDATE replication_flows SET backlog_soft_bytes=$2, backlog_hard_bytes=$3 WHERE flow_id=$1 AND state <> 'DISABLED' RETURNING *",
                flow_id,
                body.soft_bytes,
                body.hard_bytes,
            )
            if f is None:
                return respond(
                    Decision(decision=DecisionKind.REJECTED, operation="FLOW_LIMITS", reasons=[bad("NO_SUCH_FLOW", "no live flow with that id", "SYSTEM")]), 404
                )
            pol = await db.get_policy(c, f["topic"])
            d = Decision(decision=DecisionKind.ALLOWED, operation="FLOW_LIMITS", topic=f["topic"], policy_version=pol.policy_version)
            await audit_decision(c, d, "API", {"flow_id": flow_id, "backlog_bytes": body.hard_bytes}, who, None)
            return await flow_json(c, f)

    @app.get("/api/v1/replication/topics/{topic}/backpressure")
    async def backpressure(topic: str, request: Request, who: str = Depends(role("service"))):
        """blocked is true while any live flow of the topic reports a HARD backlog. A stale HARD report still blocks
        (we cannot tell that the backlog has drained); a stale OK does not."""
        async with request.app.state.pool.acquire() as c:
            rows = await c.fetch(
                "SELECT b.*, extract(epoch from now()-b.updated_at) AS age_s FROM flow_backlog b "
                "JOIN replication_flows f USING (flow_id) WHERE b.topic=$1 AND f.state <> 'DISABLED' ORDER BY flow_id",
                topic,
            )
        flows = [
            {
                "flow_id": r["flow_id"],
                "state": r["state"],
                "backlog_records": r["backlog_records"],
                "backlog_bytes": r["backlog_bytes"],
                "report_age_s": round(float(r["age_s"]), 1),
            }
            for r in rows
        ]
        return {"topic": topic, "blocked": any(f["state"] == "HARD" for f in flows), "flows": flows}
