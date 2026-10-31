"""Collector for the dashboard API. The dashboard is a VIEW of authoritative state: this module only reads it (etcd coordinator, DR
controller, policy engine, replicators, gateways) and reshapes it. It never decides, infers or caches a conclusion (for example it
never says a promotion succeeded: it reports the coordinator's phase)."""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import httpx

log = logging.getLogger("ss.obs")
REGIONS = ("region-a", "region-b", "region-c")
SHORT = {"region-a": "a", "region-b": "b", "region-c": "c"}


def iso(ts: Optional[float]) -> Optional[str]:
    return None if not ts else datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds")


# ============================== I/O ==============================================================================================
class Sources:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.http = httpx.AsyncClient(timeout=3.0)
        self.last: dict[str, dict] = {}
        self._coord: Any = None

    async def close(self) -> None:
        await self.http.aclose()
        if self._coord:
            await self._coord.close()

    async def _get(self, url: str, headers: Optional[dict] = None) -> Any:
        r = await self.http.get(url, headers=headers or {})
        r.raise_for_status()
        return r.json() if "json" in r.headers.get("content-type", "") else r.text

    async def _engine(self, path: str) -> Any:
        return await self._get(f"{self.cfg['engine_url']}/api/v1{path}", {"X-Service-Key": self.cfg["service_key"]})

    # each fetcher returns plain data or raises
    async def f_coordinator(self) -> dict:
        from ss_dr.coord import Coordinator

        if self._coord is None:
            self._coord = Coordinator(self.cfg["coord_endpoints"], timeout_s=2.0)
        st, _ = await self._coord.state()
        members = await self._coord.members_alive()
        try:
            audit = await self._coord.audit(200)
        except Exception:  # noqa: BLE001
            audit = []
        return {"state": st, "members": members, "audit": audit}

    async def f_dr(self) -> dict:
        best = None
        for url in self.cfg["dr_urls"]:
            try:
                j = await self._get(f"{url}/dr/v1/status")
            except Exception:  # noqa: BLE001
                continue
            if j.get("leader"):
                return j
            best = best or j
        if best is None:
            raise RuntimeError("no DR controller answered")
        return best

    async def f_engine(self) -> dict:
        regions, policies, topics, backlog, paths, recent, stats = await asyncio.gather(
            self._engine("/regions"),
            self._engine("/policies"),
            self._engine("/replication/topics"),
            self._engine("/replication/backlog"),
            self._engine("/network/paths"),
            self._engine("/audit/recent?limit=100"),
            self._engine("/audit/stats"),
        )
        details = {}
        for t in topics["topics"][:100]:
            try:
                details[t["topic"]] = await self._engine(f"/replication/topics/{t['topic']}")
            except Exception:  # noqa: BLE001
                pass
        return {
            "regions": regions["regions"],
            "policies": policies["policies"],
            "topics": topics["topics"],
            "backlog": backlog["flows"],
            "paths": paths["paths"],
            "recent": recent["decisions"],
            "stats": stats,
            "details": details,
        }

    async def f_replicators(self) -> dict:
        out = {}
        for region, url in self.cfg["replicator_urls"].items():
            try:
                out[region] = await self._get(f"{url}/status")
            except Exception as exc:  # noqa: BLE001
                out[region] = {"error": type(exc).__name__}
        if all("error" in v for v in out.values()):
            raise RuntimeError("no replicator answered")
        return out

    async def f_gateways(self) -> dict:
        out = {}
        for region, url in self.cfg["gateway_urls"].items():
            row: dict = {"healthy": False}
            try:
                r = await self.http.get(f"{url}/api/v1/health")
                row["healthy"] = r.status_code == 200
                row["health"] = r.json()
                m = await self.http.get(f"{url}/metrics")
                row["metrics"] = m.text if m.status_code == 200 else ""
            except Exception as exc:  # noqa: BLE001
                row["error"] = type(exc).__name__
            out[region] = row
        return out

    async def refresh(self) -> dict[str, dict]:
        fetchers: dict[str, Callable] = {
            "coordinator": self.f_coordinator,
            "dr": self.f_dr,
            "engine": self.f_engine,
            "replicators": self.f_replicators,
            "gateways": self.f_gateways,
        }

        async def one(name: str, fn: Callable) -> None:
            try:
                data = await asyncio.wait_for(fn(), 12)
                self.last[name] = {"ok": True, "data": data, "fetched_at": time.time(), "error": None}
            except Exception as exc:  # noqa: BLE001
                prev = self.last.get(name, {})
                self.last[name] = {
                    "ok": False,
                    "data": prev.get("data"),
                    "fetched_at": prev.get("fetched_at"),
                    "error": f"{type(exc).__name__}: {str(exc)[:120]}",
                }

        await asyncio.gather(*(one(n, f) for n, f in fetchers.items()))
        return self.last


# ============================== pure views ===========================================================================================
def _src(raw: dict, name: str) -> Optional[dict]:
    s = raw.get(name)
    return s["data"] if s and s.get("data") is not None else None


def categorize(operation: str, point: str = "") -> str:
    op = operation or ""
    if op.startswith("DR_FAILBACK") or "FAILBACK" in op or point == "FAILBACK":
        return "failback"
    if op.startswith("DR_") or op in ("PROMOTE_TOPOLOGY", "PROMOTION") or point == "PROMOTION":
        return "failover"
    if op == "ROUTE_SWITCH" or point == "ROUTING":
        return "route"
    if op.startswith("DASH_"):
        return "admin"
    if op.startswith(("FLOW", "REPLICATION", "BACKLOG", "RESEED")) or point in ("FLOW_INIT", "BATCH_ADMISSION", "REPLICA_CREATION"):
        return "replication"
    return "policy"


def build_topology(raw: dict) -> dict:
    coord, dr, rep, gw = _src(raw, "coordinator"), _src(raw, "dr"), _src(raw, "replicators"), _src(raw, "gateways")
    st = (coord or {}).get("state") or (dr or {}).get("state") or {}
    active, fenced = st.get("active_region"), st.get("fenced", {}) or {}
    regions = []
    for r in REGIONS:
        h = ((dr or {}).get("health") or {}).get(r) or {}
        sample = h.get("sample") or {}
        role = "unknown"
        if st:
            role = "active" if r == active and active else ("fenced" if r in fenced else "standby")
        regions.append(
            {
                "region": r,
                "role": role,
                "declared_health": h.get("declared", "UNKNOWN"),
                "brokers_up": sample.get("brokers_up"),
                "brokers_expected": sample.get("brokers_expected"),
                "controller_quorum": sample.get("controller") if sample else None,
                "kafka_reachable": sample.get("kafka"),
                "gateway_healthy": ((gw or {}).get(r) or {}).get("healthy"),
                "agent_healthy": sample.get("agent") if sample else None,
                "replicator_up": bool(rep and r in rep and "error" not in rep[r]),
                "leadership_epoch": st.get("leadership_epoch"),
                "fenced_at_epoch": fenced.get(r),
            }
        )
    conns: dict[tuple[str, str], dict] = {}
    for row in build_replication(raw):
        k = (row["source_region"], row["target_region"])
        c = conns.setdefault(k, {"source_region": k[0], "target_region": k[1], "flows": 0, "states": {}, "route": None})
        c["flows"] += 1
        c["states"][row["status"]] = c["states"].get(row["status"], 0) + 1
        c["route"] = c["route"] or row.get("route")
    return {
        "active_region": active or None,
        "phase": st.get("phase"),
        "leadership_epoch": st.get("leadership_epoch"),
        "replication_generation": st.get("replication_generation"),
        "regions": regions,
        "connections": sorted(conns.values(), key=lambda c: (c["source_region"], c["target_region"])),
        "coordinator_members": (coord or {}).get("members", {}),
    }


def build_policies(raw: dict) -> dict:
    eng = _src(raw, "engine") or {}
    pols = [
        {
            k: p[k]
            for k in (
                "topic",
                "classification",
                "legal_zone",
                "home_region",
                "allowed_storage_regions",
                "allowed_processing_regions",
                "prohibited_regions",
                "policy_version",
                "replicas",
            )
        }
        for p in eng.get("policies", [])
    ]
    active = {t["topic"]: t["active_region"] for t in eng.get("topics", [])}
    for p in pols:
        p["active_region"] = active.get(p["topic"])
    blocked = [_audit_row(d) for d in eng.get("recent", []) if d["decision"] != "ALLOWED"][:20]
    return {"policies": pols, "recent_blocked": blocked}


def _audit_row(d: dict) -> dict:
    subj = d.get("subject") or {}
    return {
        "id": d.get("id"),
        "ts": d.get("ts"),
        "source": "policy",
        "category": categorize(d.get("operation", ""), d.get("enforcement_point", "")),
        "operation": d.get("operation"),
        "enforcement_point": d.get("enforcement_point"),
        "decision": d.get("decision"),
        "topic": d.get("topic"),
        "region": subj.get("region") or subj.get("destination_region"),
        "requester": d.get("requester"),
        "reasons": [r.get("code") for r in d.get("reasons", []) if isinstance(r, dict)],
        "policy_version": d.get("policy_version"),
    }


def build_routing(raw: dict) -> list[dict]:
    rep, eng = _src(raw, "replicators"), _src(raw, "engine") or {}
    kinds = {p["path_id"]: p.get("kind") for p in eng.get("paths", [])}
    out = {}
    for region, st in (rep or {}).items():
        for f in st.get("flows", []) if isinstance(st, dict) else []:
            k = (f["source"], f["target"])
            if k in out:
                continue
            r = f["route"]
            paths = []
            for pid, m in sorted(r["measurements"].items()):
                sc = (r["scores"] or {}).get(pid) or {}
                paths.append(
                    {
                        "path_id": pid,
                        "kind": kinds.get(pid),
                        "selected": pid == r["current"],
                        "rtt_ms": m["rtt_ms"],
                        "loss": m["loss"],
                        "bandwidth_bps": m["bandwidth_bps"],
                        "score": sc.get("score"),
                        "breaker": m["breaker"],
                        "excluded": (r.get("excluded") or {}).get(pid) or (r.get("policy_rejected") or {}).get(pid),
                        "last_sample_age_s": m["last_sample_age_s"],
                    }
                )
            out[k] = {
                "source_region": k[0],
                "target_region": k[1],
                "selected_path": r["current"],
                "route_changes": r["changes"],
                "last_route_change": iso(r["last_change"]),
                "reason": r["reason"],
                "paths": paths,
            }
    return sorted(out.values(), key=lambda x: (x["source_region"], x["target_region"]))


def build_replication(raw: dict) -> list[dict]:
    eng, rep = _src(raw, "engine") or {}, _src(raw, "replicators") or {}
    backlog = {b["flow_id"]: b for b in eng.get("backlog", [])}
    pipes = {}
    for st in rep.values():
        for f in st.get("flows", []) if isinstance(st, dict) else []:
            pipes[f["flow_id"]] = sorted({p["state"] for p in f["partitions"].values()})
    rows = []
    for t in eng.get("topics", []):
        det = eng.get("details", {}).get(t["topic"], {})
        for f in t["flows"]:
            cps = [c for c in det.get("checkpoints", []) if c["source_cluster"] == f["source_cluster"] and c["target_cluster"] == f["target_cluster"]]
            b = backlog.get(f["flow_id"]) or {}
            stamps = [c["last_success_timestamp"] for c in cps if c.get("last_success_timestamp")]
            rows.append(
                {
                    "topic": t["topic"],
                    "flow_id": f["flow_id"],
                    "source_region": f["source_region"],
                    "target_region": f["target_region"],
                    "source_cluster": f["source_cluster"],
                    "target_cluster": f["target_cluster"],
                    "status": f["state"] + (" (paused)" if f["paused"] else ""),
                    "pipeline_states": pipes.get(f["flow_id"], []),
                    "generation": f["generation"],
                    "last_source_offset": max((c["last_acknowledged_source_offset"] for c in cps), default=None),
                    "last_destination_offset": max((c["last_known_destination_offset"] for c in cps), default=None),
                    "last_ack_at": max(stamps) if stamps else None,
                    "lag_records": b.get("backlog_records"),
                    "lag_seconds": b.get("drain_seconds"),
                    "backlog_bytes": b.get("backlog_bytes"),
                    "throughput_bps": b.get("throughput_bps"),
                    "backlog_state": b.get("backlog_state"),
                    "report_age_s": b.get("report_age_s"),
                    "route": b.get("route"),
                }
            )
    return rows


def build_dr(raw: dict) -> dict:
    dr, coord = _src(raw, "dr") or {}, _src(raw, "coordinator") or {}
    st = coord.get("state") or dr.get("state") or {}
    lf, lb = st.get("last_failover"), st.get("last_failback")
    elig = {}
    for region, es in (dr.get("eligibility") or {}).items():
        reasons = sorted({c for e in es for c in e["reasons"]})
        elig[region] = {
            "eligible": all(e["eligible"] for e in es) if es else False,
            "reasons": reasons,
            "topics_checked": len(es),
            "degraded_rpo": any(e["degraded_rpo"] for e in es),
            "max_lag_ms": max((e["lag_ms"] or 0 for e in es), default=None),
        }
    if not elig and st.get("eligibility"):  # summary persisted in the coordinator while NO_ELIGIBLE_TARGET
        elig = {r: {"eligible": False, "reasons": c, "topics_checked": None, "degraded_rpo": False, "max_lag_ms": None} for r, c in st["eligibility"].items()}
    last = None
    if lf:
        last = {
            "from": lf["from"],
            "to": lf["to"],
            "epoch": lf["epoch"],
            "activated_at": iso(lf["activated_ms"] / 1000),
            "degraded_rpo": lf["degraded_rpo"],
            "approved_by": lf.get("approved_by"),
            "measured_rto_s": round((lf["activated_ms"] - lf["failure_boundary_ms"]) / 1000, 2) if lf.get("failure_boundary_ms") else None,
            "fence_wait_s": round((lf["topology_ms"] - lf["fenced_ms"]) / 1000, 2) if lf.get("topology_ms") else None,
            "replication_reconfigured_s": round((lf["replication_ready_ms"] - lf["activated_ms"]) / 1000, 2) if lf.get("replication_ready_ms") else None,
            "expected_rpo_bound_s": round(max((v for v in (lf.get("expected_rpo_ms") or {}).values()), default=0) / 1000, 2),
        }
    actual = None
    if lb:
        actual = {"records": sum(q.get("records", 0) for q in (lb.get("quarantine") or {}).values()), "source": "quarantine after failback"}
    fb = st.get("failback")
    return {
        "phase": st.get("phase"),
        "coordination_reachable": bool(coord) and raw.get("coordinator", {}).get("ok", False),
        "active_region": st.get("active_region") or None,
        "standby_regions": [r for r in REGIONS if r != st.get("active_region") and r not in (st.get("fenced") or {})],
        "leadership_epoch": st.get("leadership_epoch"),
        "fenced": st.get("fenced", {}),
        "transition": ({k: st["transition"].get(k) for k in ("from", "to", "step", "epoch", "degraded_rpo")} if st.get("transition") else None),
        "last_failover": last,
        "candidate_eligibility": elig,
        "actual_rpo": actual,
        "failback": ({k: fb.get(k) for k in ("region", "stage", "requested_by", "approved_by", "from")} if fb else None),
        "last_failback": (
            {"from": lb["from"], "to": lb["to"], "epoch": lb["epoch"], "approved_by": lb.get("approved_by"), "activated_at": iso(lb["activated_ms"] / 1000)}
            if lb
            else None
        ),
        "controller": {
            "instance": dr.get("instance"),
            "is_leader": dr.get("leader"),
            "auto": dr.get("auto"),
            "allow_degraded_rpo": dr.get("allow_degraded_rpo"),
        },
    }


def build_audit(raw: dict, limit: int = 150) -> list[dict]:
    eng, coord = _src(raw, "engine") or {}, _src(raw, "coordinator") or {}
    rows = [_audit_row(d) for d in eng.get("recent", [])]
    for e in coord.get("audit", []):
        rows.append(
            {
                "id": f"c{e.get('seq')}",
                "ts": iso(e.get("wall_ms", 0) / 1000),
                "source": "coordinator",
                "category": categorize("DR_" + e.get("event", "")),
                "operation": e.get("event"),
                "enforcement_point": "COORDINATOR",
                "decision": "ALLOWED",
                "topic": None,
                "region": e.get("active_region") or None,
                "requester": e.get("actor"),
                "reasons": [],
                "policy_version": None,
                "detail": f"{e.get('from')} -> {e.get('to')} (epoch {e.get('epoch')})",
            }
        )
    rows.sort(key=lambda r: r["ts"] or "", reverse=True)
    return rows[:limit]


def build_snapshot(raw: dict, interval_s: float, now: Optional[float] = None) -> dict:
    now = now or time.time()
    sources = {}
    for name in ("coordinator", "dr", "engine", "replicators", "gateways"):
        s = raw.get(name) or {}
        age = None if not s.get("fetched_at") else round(now - s["fetched_at"], 1)
        sources[name] = {"ok": bool(s.get("ok")), "age_s": age, "error": s.get("error"), "stale": (age is None) or age > max(10.0, 4 * interval_s)}
    return {
        "generated_at": iso(now),
        "generated_epoch": now,
        "interval_s": interval_s,
        "sources": sources,
        "topology": build_topology(raw),
        "policies": build_policies(raw),
        "routing": build_routing(raw),
        "replication": build_replication(raw),
        "dr": build_dr(raw),
        "audit": build_audit(raw),
    }


# ============================== metrics =============================================================================================
HEALTH_VALUE = {"UP": 2, "DEGRADED": 1, "DOWN": 0}


def build_metrics(raw: dict, snap: dict, reg) -> None:
    eng, gw = _src(raw, "engine") or {}, _src(raw, "gateways") or {}
    topo, dr = snap["topology"], snap["dr"]
    for r in topo["regions"]:
        reg.add("ss_active_region", "gauge", "1 for the region the coordinator names active.", 1 if r["role"] == "active" else 0, region=r["region"])
        reg.add(
            "ss_region_health",
            "gauge",
            "Declared health: 2 UP, 1 DEGRADED, 0 DOWN (unknown is omitted).",
            HEALTH_VALUE.get(r["declared_health"], -1),
            region=r["region"],
        )
        if r["brokers_up"] is not None:
            reg.add("ss_region_brokers_up", "gauge", "Kafka brokers answering in the region.", r["brokers_up"], region=r["region"])
        reg.add("ss_worker_up", "gauge", "1 when the worker answered its last probe.", 1 if r["gateway_healthy"] else 0, worker="gateway", region=r["region"])
        reg.add("ss_worker_up", "gauge", "1 when the worker answered its last probe.", 1 if r["agent_healthy"] else 0, worker="agent", region=r["region"])
        reg.add("ss_worker_up", "gauge", "1 when the worker answered its last probe.", 1 if r["replicator_up"] else 0, worker="replicator", region=r["region"])
    if topo["leadership_epoch"] is not None:
        reg.add("ss_leadership_epoch", "gauge", "Active leadership epoch from the coordinator.", topo["leadership_epoch"])
    reg.add(
        "ss_worker_up",
        "gauge",
        "1 when the worker answered its last probe.",
        sum(1 for ok in (topo.get("coordinator_members") or {}).values() if ok),
        worker="coordinator_members",
        region="coordinator",
    )
    reg.add(
        "ss_worker_up", "gauge", "1 when the worker answered its last probe.", 1 if snap["sources"]["dr"]["ok"] else 0, worker="dr_controller", region="control"
    )
    reg.add(
        "ss_worker_up",
        "gauge",
        "1 when the worker answered its last probe.",
        1 if snap["sources"]["engine"]["ok"] else 0,
        worker="policy_engine",
        region="control",
    )
    if dr.get("phase"):
        for s in (
            "HEALTHY_A",
            "LOCAL_DEGRADED_A",
            "REGION_A_UNAVAILABLE",
            "PROMOTING_B",
            "HEALTHY_B",
            "REGION_B_UNAVAILABLE",
            "PROMOTING_C",
            "HEALTHY_C",
            "NO_ELIGIBLE_TARGET",
            "COORDINATION_UNAVAILABLE",
            "FAILBACK_PENDING",
        ):
            reg.add("ss_dr_state", "gauge", "One-hot failover state.", 1 if s == dr["phase"] else 0, state=s)
    pair: dict[tuple, dict] = {}
    for row in snap["replication"]:
        p = pair.setdefault((SHORT[row["source_region"]], SHORT[row["target_region"]]), {"rec": 0, "bytes": 0, "bps": 0.0, "lag": 0.0})
        p["rec"] += row["lag_records"] or 0
        p["bytes"] += row["backlog_bytes"] or 0
        p["bps"] += row["throughput_bps"] or 0.0
        p["lag"] = max(p["lag"], row["lag_seconds"] or 0.0)
    for (s, t), p in sorted(pair.items()):
        reg.add("ss_replication_backlog_records", "gauge", "Unreplicated records per region pair (all topics).", p["rec"], source=s, target=t)
        reg.add("ss_replication_backlog_bytes", "gauge", "Estimated unreplicated bytes per region pair.", p["bytes"], source=s, target=t)
        reg.add("ss_replication_throughput_bytes_per_second", "gauge", "Replication throughput per region pair.", p["bps"], source=s, target=t)
        reg.add("ss_replication_lag_seconds", "gauge", "Largest estimated time to drain the backlog (max over topics).", p["lag"], source=s, target=t)
    for r in snap["routing"]:
        reg.add(
            "ss_route_switches_total",
            "counter",
            "Route changes since the replicator started.",
            r["route_changes"],
            source=SHORT[r["source_region"]],
            target=SHORT[r["target_region"]],
        )
        for p in r["paths"]:
            health = {"CLOSED": 1.0, "HALF_OPEN": 0.5, "OPEN": 0.0}.get(p["breaker"], 0.0)
            reg.add("ss_path_health", "gauge", "1 breaker closed, 0.5 half-open, 0 open.", health, path=p["path_id"])
            if p["rtt_ms"] is not None:
                reg.add("ss_path_rtt_seconds", "gauge", "Measured round-trip time of a path.", p["rtt_ms"] / 1000.0, path=p["path_id"])
    lf = dr.get("last_failover")
    if lf and lf.get("measured_rto_s") is not None:
        reg.add(
            "ss_failover_duration_seconds",
            "gauge",
            "Seconds from the failure boundary to activation of the last failover.",
            lf["measured_rto_s"],
            **{"from": lf["from"], "to": lf["to"]},
        )
    if lf:
        reg.add(
            "ss_rpo_expected_bound_seconds",
            "gauge",
            "Lag bound at the time of the last failover decision.",
            lf["expected_rpo_bound_s"],
            **{"from": lf["from"], "to": lf["to"]},
        )
    if dr.get("actual_rpo"):
        reg.add(
            "ss_rpo_gap_records",
            "gauge",
            "Acknowledged records missing from the live history (quarantined), known after a failback.",
            dr["actual_rpo"]["records"],
        )
    stats = eng.get("stats") or {}
    rejected: dict[str, int] = {}
    for d in stats.get("decisions", []):
        reg.add(
            "ss_policy_decisions_total",
            "counter",
            "Recorded policy decisions.",
            d["count"],
            operation=d["operation"][:40],
            decision=d["decision"],
            point=d["enforcement_point"],
        )
        if d["decision"] != "ALLOWED":
            rejected[d["enforcement_point"]] = rejected.get(d["enforcement_point"], 0) + d["count"]
    for point, n in sorted(rejected.items()):
        reg.add("ss_policy_rejections_total", "counter", "Decisions that blocked an operation.", n, point=point)
    for status, n in sorted((stats.get("replica_assignments") or {}).items()):
        reg.add("ss_replica_placements", "gauge", "Replica placements by status (REVOKED = blocked by policy).", n, status=status, **{})
    for region, g in gw.items():
        if g.get("metrics"):
            reg.add_raw_histogram(
                [ln for ln in g["metrics"].splitlines() if ln.startswith(("# HELP ss_producer", "# TYPE ss_producer", "ss_producer_ack_latency"))]
            )
    for name, s in snap["sources"].items():
        reg.add(
            "ss_dashboard_source_age_seconds",
            "gauge",
            "Age of the data the dashboard serves, per source.",
            s["age_s"] if s["age_s"] is not None else -1,
            worker=name,
        )
