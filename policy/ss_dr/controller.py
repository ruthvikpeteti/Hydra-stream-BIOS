"""Failover controller (Phase 6).

What it does NOT do: elect Kafka partition leaders (Kafka does, Tier 1) or run its own consensus (etcd does). It observes,
decides eligibility deterministically (model.py) and drives the transition through compare-and-swap on the coordinator.

Authority and fencing, in one paragraph: the active region, the leadership epoch and the fenced set live in ONE etcd key.
A promotion is  (1) CAS: epoch+1, active_region = "" (nobody may write), old region added to `fenced`;  (2) wait
lease_ttl + drift, so every gateway that could not hear the fence has expired its own lease on its monotonic clock;
(3) make the topology (policy engine) agree with the new epoch;  (4) CAS: active_region = target.  Steps 3 and 4 are
retried until they succeed; a crash at any point resumes from the persisted step. While steps 1-4 are incomplete NOBODY
can write: fail closed.  No quorum means no read and no CAS, so no promotion is possible without quorum."""

import asyncio
import logging
import os
import time
import uuid
from typing import Optional

from datetime import datetime

import httpx

from . import model as M
from .adapters import Config, Engine, EngineError, Kafkas, http_ok, now_ms
from .coord import CoordinationUnavailable, Coordinator
from .failback import FailbackMixin

log = logging.getLogger("ss.dr")


class Conflict(Exception):
    """The compare-and-swap lost: another controller (or an earlier attempt of ours) changed the state first."""


class DR(FailbackMixin):
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.coord = Coordinator(cfg.coord_endpoints)
        self.engine = Engine(cfg)
        self.kafkas = Kafkas(cfg)
        self.http = httpx.AsyncClient()
        self.regions = list(cfg.order)
        self.trackers = {r: M.HealthTracker(r, cfg.fail_after, cfg.recover_after) for r in self.regions}
        self.state: dict = {}
        self.rev = 0
        self.coord_ok = False
        self.last_known_phase: Optional[str] = None
        self.is_leader = False
        self.leader_id: Optional[str] = None
        self._lease: Optional[int] = None
        self._tasks: list[asyncio.Task] = []
        self._tick_n = 0
        self.last_eval: dict = {}
        self.link_slow: dict = {}
        self.avail_sent: dict[str, str] = {}
        self.fence_applied: dict[str, float] = {}
        self.crash_at: Optional[str] = None
        self.forced_down: set = set()  # TEST ONLY: regions a (simulated) false-positive detector declares dead
        self.metrics = {"promotions": 0, "cas_conflicts": 0, "refused_no_quorum": 0, "refused_ineligible": 0}
        self._busy = asyncio.Lock()

    # ---- lifecycle -------------------------------------------------------------------------------------------
    async def start(self) -> None:
        self._tasks = [asyncio.create_task(self._bootstrap_then_loop(), name="dr-main"), asyncio.create_task(self._leader_loop(), name="dr-leader")]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for c in (self.coord.close(), self.engine.close(), self.kafkas.close(), self.http.aclose()):
            try:
                await c
            except Exception:  # noqa: BLE001
                pass

    async def _bootstrap_then_loop(self) -> None:
        while True:
            try:
                if await self.coord.bootstrap(M.initial_state()):
                    log.info("coordinator bootstrapped with the initial DR state (active region-a, epoch 1)")
                break
            except CoordinationUnavailable as exc:
                log.warning("waiting for the coordinator: %s", exc)
                await asyncio.sleep(2)
        while True:
            t0 = time.monotonic()
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the loop must survive anything
                log.exception("tick failed")
            await asyncio.sleep(max(0.05, self.cfg.probe_interval_s - (time.monotonic() - t0)))

    async def _leader_loop(self) -> None:
        """Controller leader election: a key tied to an etcd lease. It narrows who ACTS; it is not what makes a
        promotion safe (the CAS is), so two controllers that both believe they lead still cannot both promote."""
        while True:
            try:
                if self._lease is None:
                    self._lease = await self.coord.lease_grant(self.cfg.leader_ttl_s)
                lead, holder = await self.coord.try_lead(self.cfg.instance, self._lease)
                if lead and not await self.coord.lease_keepalive(self._lease):
                    lead, self._lease = False, None
                self.is_leader, self.leader_id = lead, holder
            except CoordinationUnavailable:
                self.is_leader, self._lease = False, None
            except Exception:  # noqa: BLE001
                log.exception("leader loop")
                self.is_leader = False
            await asyncio.sleep(max(1.0, self.cfg.leader_ttl_s / 3))

    # ---- health manager --------------------------------------------------------------------------------------
    async def _probe(self, region: str) -> dict:
        s = {
            "kafka": False,
            "gateway": False,
            "agent": False,
            "brokers_up": 0,
            "brokers_expected": self.cfg.expected_brokers.get(region, 1),
            "leaders_ok": None,
            "isr_ok": None,
            "controller": False,
        }

        async def kafka():
            try:
                c = await self.kafkas.cluster(region)
                s.update(
                    kafka=True, brokers_up=len(c.get("brokers", [])), controller=c.get("controller_id", -1) not in (-1, None), cluster_id=c.get("cluster_id")
                )
                names = [t["topic"] for t in getattr(self, "_topic_cache", [])]
                if names and s["brokers_up"]:
                    parts = await self.kafkas.partitions(region, names)
                    allp = [p for ps in parts.values() for p in ps]
                    s["leaders_ok"] = all(p["leader"] not in (-1, None) for p in allp)
                    s["isr_ok"] = all(len(p["isr"]) >= min(self.cfg.min_isr.get(region, 1), len(p["replicas"])) for p in allp)
            except Exception:  # noqa: BLE001
                pass

        async def gw():
            s["gateway"] = bool(self.cfg.gateways.get(region)) and await http_ok(self.http, f"{self.cfg.gateways[region]}/api/v1/health")

        async def ag():
            s["agent"] = bool(self.cfg.agents.get(region)) and await http_ok(self.http, f"{self.cfg.agents[region]}/agent/v1/health")

        await asyncio.gather(kafka(), gw(), ag())
        return s

    async def _tick(self) -> None:
        self._tick_n += 1
        now = time.time()  # the failure boundary is when the probe round STARTED, not after its timeouts
        samples = await asyncio.gather(*(self._probe(r) for r in self.regions))
        for r, s in zip(self.regions, samples):
            self.trackers[r].observe(s, now)
            if r in self.forced_down:
                tr = self.trackers[r]
                tr.declared, tr.failure_boundary = M.DOWN, tr.failure_boundary or now
        try:
            st, rev = await self.coord.state()
        except CoordinationUnavailable:
            if self.coord_ok:
                log.error("COORDINATION_UNAVAILABLE: no quorum can be reached; automatic promotion is refused")
            self.coord_ok = False
            return
        self.coord_ok, self.state, self.rev = True, st, rev
        self.last_known_phase = st["phase"]
        if self._tick_n % 5 == 1:
            try:
                self._topic_cache = await self.engine.topics()
            except EngineError:
                pass
        if self.cfg.auto and self.is_leader and not self._busy.locked():
            async with self._busy:
                await self._decide(st, rev)

    # ---- state transitions -------------------------------------------------------------------------------------
    async def _cas(self, st: dict, rev: int, phase: Optional[str] = None, *, event: str, **changes) -> tuple[dict, int]:
        new = {**st, **changes, "seq": int(st["seq"]) + 1}
        if phase is not None and phase != st["phase"]:
            if not M.can_transition(st["phase"], phase):
                raise ValueError(f"illegal transition {st['phase']} -> {phase}")
            new["phase"] = phase
        audit = {
            "event": event,
            "from": st["phase"],
            "to": new["phase"],
            "active_region": new["active_region"],
            "epoch": new["leadership_epoch"],
            "generation": new["replication_generation"],
            "actor": self.cfg.instance,
            "transition_id": (new.get("transition") or {}).get("id"),
        }
        try:
            ok = await self.coord.cas_state(rev, new, audit)
        except CoordinationUnavailable:
            st2, rev2 = await self.coord.state()  # outcome unknown: the state decides, not our hope
            if st2.get("seq") == new["seq"] and st2.get("transition") == new.get("transition") and st2["phase"] == new["phase"]:
                ok, rev = True, rev2
            else:
                raise
        if not ok:
            self.metrics["cas_conflicts"] += 1
            raise Conflict(f"state changed under {event}")
        st2, rev2 = await self.coord.state()
        self.state, self.rev, self.last_known_phase = st2, rev2, st2["phase"]
        asyncio.create_task(self._forward_audit(event, audit))
        return st2, rev2

    async def _forward_audit(self, event: str, a: dict) -> None:
        subj = {"region": a["active_region"]} if a["active_region"] else {}
        reasons = (
            []
            if event not in ("NO_ELIGIBLE_TARGET",)
            else [{"code": "NO_ELIGIBLE_TARGET", "message": "no region passed every promotion check", "category": "TECHNICAL"}]
        )
        await self.engine.audit(
            [
                {
                    "operation": f"DR_{event}"[:32],
                    "enforcement_point": "FAILBACK" if "FAILBACK" in event else "PROMOTION",
                    "decision": "REJECTED" if reasons else "ALLOWED",
                    "reasons": reasons,
                    "subject": subj,
                    "requester": a["actor"],
                    "request_id": a.get("transition_id"),
                }
            ],
            f"dr:{self.cfg.instance}",
        )

    def _crash_point(self, step: str) -> None:
        if self.crash_at == step:
            log.error("FAULT INJECTION: crashing after step %s", step)
            os._exit(17)

    async def _decide(self, st: dict, rev: int) -> None:
        phase = st["phase"]
        try:
            await self._sync_availability()
            if phase in (M.PROMOTING_B, M.PROMOTING_C):
                await self._continue_promotion(st, rev)
            elif phase in (M.HEALTHY_A, M.LOCAL_DEGRADED_A, M.HEALTHY_B, M.HEALTHY_C):
                reg = st["active_region"]
                t = self.trackers[reg]
                if t.declared == M.DOWN:
                    await self._begin_unavailable(st, rev, reg)
                elif phase == M.HEALTHY_A and t.declared == M.DEGRADED:
                    await self._cas(st, rev, M.LOCAL_DEGRADED_A, event="LOCAL_DEGRADED")
                elif phase == M.LOCAL_DEGRADED_A and t.declared == M.UP:
                    await self._cas(st, rev, M.HEALTHY_A, event="LOCAL_RECOVERED")
                else:
                    await self._steady(st, rev)
            elif phase in (M.REGION_A_UNAVAILABLE, M.REGION_B_UNAVAILABLE, M.NO_ELIGIBLE_TARGET):
                await self._try_promote(st, rev)
            elif phase == M.FAILBACK_PENDING:
                await self._failback_step(st, rev)
        except Conflict:
            log.info("lost a compare-and-swap; another controller acted first")
        except CoordinationUnavailable as exc:
            self.metrics["refused_no_quorum"] += 1
            log.error("coordination unavailable mid-decision: %s", exc)
        except EngineError as exc:
            log.error("the policy engine could not complete a step (%s); it is retried on the next cycle, nothing is changed meanwhile", exc)

    async def _begin_unavailable(self, st: dict, rev: int, region: str) -> None:
        t = self.trackers[region]
        target = M.UNAVAILABLE_OF.get(region)
        if target is None:  # region-c has no successor: it is simply unavailable
            return
        st, rev = await self._cas(
            st,
            rev,
            target,
            event="REGION_UNAVAILABLE",
            failed_region=region,
            failure_boundary_ms=int((t.failure_boundary or time.time()) * 1000),
            detected_ms=now_ms(),
        )
        await self._try_promote(st, rev)

    # ---- eligibility -----------------------------------------------------------------------------------------
    def candidates(self, st: dict) -> list[str]:
        failed = st.get("failed_region") or st["active_region"]
        return (
            [r for r in self.regions if r != failed and r != st["active_region"] and r not in st.get("fenced", {})]
            if st["phase"] not in (M.HEALTHY_A, M.HEALTHY_B, M.HEALTHY_C)
            else [r for r in self.regions if r != st["active_region"] and r not in st.get("fenced", {})]
        )

    async def evaluate(self, st: dict, approval: bool = False) -> dict[str, list[M.Eligibility]]:
        """Deterministic eligibility of every candidate for every topic being failed over. Read-only."""
        old = st.get("failed_region") or st["active_region"]
        E, G = st["leadership_epoch"] + 1, st["replication_generation"] + 1
        cands = self.candidates(st)
        try:
            topics = [t for t in await self.engine.topics() if t["active_region"] == old]
            regions = {r["region_id"]: r for r in await self.engine.regions()}
            backlog = await self.engine.backlog()
        except EngineError as exc:
            topics, regions, backlog = None, {}, []
            log.error("policy engine unavailable while evaluating eligibility: %s", exc)
        coord_ok = await self.coord.reachable()
        boundary = st.get("failure_boundary_ms")
        out: dict[str, list[M.Eligibility]] = {}
        for cand in cands:
            tr = self.trackers[cand]
            reg = regions.get(cand, {})
            cap = reg.get("storage_capacity", {})
            used_ratio = (cap.get("used_bytes", 0) / cap["total_bytes"]) if cap.get("total_bytes") else 0.0
            base = dict(
                region=cand,
                health=tr.declared,
                controller_quorum=bool(tr.last_sample.get("controller")) and tr.last_sample.get("kafka", False),
                capacity_ok=used_ratio <= self.cfg.capacity_limit,
                coordination_ok=coord_ok,
                lease_acquirable=coord_ok,
                can_fence_old=coord_ok,
            )
            res = [M.evaluate_candidate(M.CandidateFacts(topic="*", lag_ms=0.0, **base), approval)]  # type: ignore[arg-type]  # region-level facts (no lag semantics), always present
            if topics is None:  # fail closed: no policy answer = no promotion
                res.append(M.evaluate_candidate(M.CandidateFacts(topic="*", lag_ms=0.0, policy_codes=("ENGINE_UNAVAILABLE",), **base), approval))  # type: ignore[arg-type]
            for t in topics or []:
                res.append(await self._topic_facts(t, cand, old, E, G, base, backlog, boundary, approval))
            out[cand] = res
        self.last_eval = {r: [e.as_dict() for e in es] for r, es in out.items()}
        return out

    async def _topic_facts(
        self, t: dict, cand: str, old: str, E: int, G: int, base: dict, backlog: list, boundary_ms: Optional[int], approval: bool
    ) -> M.Eligibility:
        topic, parts = t["topic"], max(1, t["partitions"])
        codes: list[str] = []
        try:
            status, body = await self.engine.promote(topic, cand, E, G, "eligibility check", dry_run=True)
            if status == 403:
                codes = sorted({r["code"] for r in body.get("reasons", [])})
        except Exception as exc:  # noqa: BLE001
            codes = ["ENGINE_UNAVAILABLE"]
            log.error("engine dry-run failed: %r", exc)
        flow = next((f for f in t["flows"] if f["source_region"] == old and f["target_region"] == cand), None)
        data_present = checkpoints_ok = True
        cutover_ok = True
        last_ack_ts: Optional[float] = None
        if flow is not None and base["health"] == M.UP:
            try:
                detail = await self.engine.topic(topic)
                cps = {
                    c["source_partition"]: c
                    for c in detail["checkpoints"]
                    if c["source_cluster"] == flow["source_cluster"] and c["target_cluster"] == flow["target_cluster"]
                }
                stamps = [datetime.fromisoformat(c["last_success_timestamp"]).timestamp() for c in cps.values() if c.get("last_success_timestamp")]
                last_ack_ts = max(stamps) if stamps else None
                ends = await self.kafkas.end_offsets(cand, topic, parts)
                if len(ends) != parts:
                    data_present = False
                for p, cp in cps.items():
                    if ends.get(p, 0) < cp["last_known_destination_offset"] + 1:
                        data_present = False
                checkpoints_ok = all(p in cps or ends.get(p, 0) == 0 for p in range(parts))  # an idle partition has no row and holds nothing
                for f2 in t["flows"]:  # cutover toward the other replicas needs a live, activated flow to build on
                    if f2["source_region"] == old and f2["target_region"] != cand and f2["state"] != "ENABLED":
                        cutover_ok = False
            except Exception as exc:  # noqa: BLE001
                data_present = False
                log.warning("could not verify data in %s for %s: %r", cand, topic, exc)
        elif flow is None:
            data_present = checkpoints_ok = False
        return M.evaluate_candidate(
            M.CandidateFacts(
                topic=topic,
                policy_codes=tuple(codes),
                data_present=data_present,
                checkpoints_complete=checkpoints_ok,
                lag_ms=self._lag_ms(flow, backlog, boundary_ms, last_ack_ts),
                max_lag_ms=t["max_lag_ms"],
                cutover_valid=cutover_ok,
                **base,
            ),
            approval,
        )

    def _lag_ms(self, flow: Optional[dict], backlog: list, boundary_ms: Optional[int], last_ack_ts: Optional[float] = None) -> Optional[float]:
        """Replication lag of old-active -> candidate AS LAST REPORTED before the failure boundary. Once the source is dead the
        true lag cannot be measured, and a DRAIN-time estimate is meaningless (it assumes the source survives to drain). The bound
        used: with nothing unreplicated, the age of the report at the boundary; with a backlog, the larger of the drain estimate and
        the time since the replica last acknowledged anything (a backlog that is not being worked off is as old as that), plus the
        report age. Unknown (None) if there is no report, or it was already stale when the failure began."""
        if flow is None:
            return None
        rep = next((b for b in backlog if b["flow_id"] == flow["flow_id"]), None)
        if rep is None:
            return None
        reported_at = time.time() - rep["report_age_s"]
        boundary = (boundary_ms / 1000.0) if boundary_ms else time.time()
        age_at_boundary = boundary - reported_at
        if age_at_boundary < -1.0:  # reported AFTER the failure began: not a pre-failure measurement (the source is reporting again)
            return None
        if age_at_boundary > self.cfg.report_freshness_s:
            return None
        if rep["backlog_records"] == 0:
            return max(0.0, age_at_boundary) * 1000.0
        drain = rep["drain_seconds"]
        drain_ms = 10**12 if drain is None else drain * 1000.0
        stalled_ms = 10**12 if last_ack_ts is None else max(0.0, boundary - last_ack_ts) * 1000.0
        return max(drain_ms, stalled_ms) + max(0.0, age_at_boundary) * 1000.0

    # ---- promotion ---------------------------------------------------------------------------------------------
    async def _try_promote(self, st: dict, rev: int, approval: Optional[dict] = None) -> dict:
        failed = st["failed_region"]
        if self.trackers[failed].declared == M.UP and approval is None:  # recovered before anything was promoted
            await self._cas(st, rev, M.HEALTHY_OF[st["active_region"]], event="REGION_RECOVERED_BEFORE_PROMOTION", failed_region=None, failure_boundary_ms=None)
            return {"result": "RECOVERED"}
        ftr = self.trackers[failed]
        if failed not in self.forced_down and ftr.declared == M.DOWN and ftr.ok_streak > 0:
            # the "failed" region is answering probes again but its recovery window has not elapsed: promoting now would race a
            # region that is coming back, and its replicator is already reporting fresh (misleading) numbers. Wait for the window.
            return {"result": "WAITING_FOR_RECOVERY_WINDOW"}
        allow_waiver = bool(approval and self.cfg.allow_degraded_rpo)
        evals = await self.evaluate(st, allow_waiver)
        target = M.choose_target(self.candidates(st), evals)
        if target is None:
            self.metrics["refused_ineligible"] += 1
            summary = {r: sorted({c for e in es for c in (*e.hard, *e.waivable)}) for r, es in evals.items()}
            if st["phase"] != M.NO_ELIGIBLE_TARGET:
                await self._cas(st, rev, M.NO_ELIGIBLE_TARGET, event="NO_ELIGIBLE_TARGET", eligibility=summary)
            return {"result": "NO_ELIGIBLE_TARGET", "eligibility": self.last_eval}
        degraded = any(e.degraded_rpo for e in evals[target])
        E, G = st["leadership_epoch"] + 1, st["replication_generation"] + 1
        old = st["failed_region"]
        offsets = await self._translation(old, target)
        tr = {
            "id": uuid.uuid4().hex[:12],
            "from": old,
            "to": target,
            "epoch": E,
            "generation": G,
            "step": "FENCED",
            "fenced_wall_ms": now_ms(),
            "failure_boundary_ms": st.get("failure_boundary_ms"),
            "detected_ms": st.get("detected_ms"),
            "degraded_rpo": degraded,
            "approved_by": (approval or {}).get("approved_by") if degraded else None,
            "expected_rpo_ms": {e.topic: e.lag_ms for e in evals[target] if e.lag_ms is not None},
            "offsets": offsets,
        }
        fenced = {**st.get("fenced", {}), old: E}
        # ONE atomic write: next epoch + nobody active + old region fenced. This is the fencing instant.
        st, rev = await self._cas(
            st,
            rev,
            M.PROMOTING_OF[target],
            event="PROMOTION_STARTED",
            active_region="",
            leadership_epoch=E,
            replication_generation=G,
            fenced=fenced,
            transition=tr,
            eligibility=None,
        )
        self.metrics["promotions"] += 1
        self._crash_point("AFTER_FENCE")
        return await self._continue_promotion(st, rev)

    async def _translation(self, old: str, new: str) -> dict:
        """Consumer resume positions: for every partition, the OLD region's offset the new region is known to hold through, and the
        NEW region's offset of that record (from the replication checkpoints). Consumers that read the old region resume in the new
        one at destination+1; the controller records whether the mapping is the identity (offsets aligned) or not."""
        out: dict = {}
        try:
            for t in await self.engine.topics():
                if t["active_region"] != old:
                    continue
                flow = next((f for f in t["flows"] if f["source_region"] == old and f["target_region"] == new), None)
                if not flow:
                    continue
                cps = [
                    c
                    for c in (await self.engine.topic(t["topic"]))["checkpoints"]
                    if c["source_cluster"] == flow["source_cluster"] and c["target_cluster"] == flow["target_cluster"]
                ]
                out[t["topic"]] = {str(c["source_partition"]): [c["last_acknowledged_source_offset"], c["last_known_destination_offset"]] for c in cps}
                if len(out) >= 100:
                    break
        except EngineError as exc:
            log.warning("could not compute the offset translation: %s", exc)
        return out

    async def _continue_promotion(self, st: dict, rev: int) -> dict:
        tr = st["transition"]
        wait_ms = self.cfg.lease_ttl_ms + self.cfg.drift_ms
        remaining = tr["fenced_wall_ms"] + wait_ms - now_ms()
        if remaining > 0:  # every gateway that missed the fence has expired its lease by then
            await asyncio.sleep(remaining / 1000.0)
        E, G, to = tr["epoch"], tr["generation"], tr["to"]
        blocked = await self._reconcile_topology(to, E, G, f"failover {tr['from']}->{to} ({tr['id']})")
        if blocked:
            log.error("promotion of %s is incomplete; topics blocked by the policy engine: %s. Nobody can write until this is resolved", to, blocked)
            return {"result": "BLOCKED", "blocked": blocked}
        if tr["step"] == "FENCED":
            st, rev = await self._cas(st, rev, event="TOPOLOGY_RECONCILED", transition={**tr, "step": "TOPOLOGY", "topology_ms": now_ms()})
        self._crash_point("AFTER_TOPOLOGY")
        tr = st["transition"]
        done = {
            "id": tr["id"],
            "from": tr["from"],
            "to": to,
            "epoch": E,
            "generation": G,
            "failure_boundary_ms": tr["failure_boundary_ms"],
            "detected_ms": tr["detected_ms"],
            "fenced_ms": tr["fenced_wall_ms"],
            "topology_ms": tr.get("topology_ms"),
            "activated_ms": now_ms(),
            "degraded_rpo": tr["degraded_rpo"],
            "approved_by": tr["approved_by"],
            "expected_rpo_ms": tr["expected_rpo_ms"],
            "offsets": tr.get("offsets"),
            "replication_ready_ms": None,
        }
        st, rev = await self._cas(
            st, rev, M.HEALTHY_OF[to], event="PROMOTED", active_region=to, transition=None, last_failover=done, failed_region=None, failure_boundary_ms=None
        )
        log.warning(
            "PROMOTED %s at epoch %s (RTO from failure boundary %s ms)",
            to,
            E,
            (done["activated_ms"] - done["failure_boundary_ms"]) if done["failure_boundary_ms"] else "n/a",
        )
        self._crash_point("AFTER_ACTIVATE")
        return {"result": "PROMOTED", "active_region": to, "epoch": E, "last_failover": done}

    async def _reconcile_topology(self, active: str, E: int, G: int, why: str) -> list:
        """Make every topic's topology equal (active, epoch). Idempotent: topics already there are untouched."""
        blocked = []
        try:
            topics = await self.engine.topics()
        except EngineError as exc:
            return [{"topic": "*", "reasons": [f"ENGINE_UNAVAILABLE {exc}"]}]
        for t in topics:
            if t["active_region"] == active and t["leadership_epoch"] == E:
                continue
            if t["leadership_epoch"] >= E:
                continue  # a topic created later at a higher epoch: not ours to rewrite
            try:
                status, body = await self.engine.promote(t["topic"], active, E, G, why)
            except Exception as exc:  # noqa: BLE001
                blocked.append({"topic": t["topic"], "reasons": [repr(exc)]})
                continue
            if status == 403:
                blocked.append({"topic": t["topic"], "reasons": [r["code"] for r in body.get("reasons", [])]})
        return blocked

    # ---- steady state ----------------------------------------------------------------------------------------
    async def _steady(self, st: dict, rev: int) -> None:
        if st.get("reprotect"):
            await self._reprotect(st, rev)
            return
        if self._tick_n % 3 == 0:
            blocked = await self._reconcile_topology(st["active_region"], st["leadership_epoch"], st["replication_generation"], "reconcile")
            if blocked:
                log.error("topology reconcile blocked: %s", blocked)
            lf = st.get("last_failover")
            if lf and not lf.get("replication_ready_ms") and not blocked:
                if await self._replication_ready(st["active_region"]):
                    await self._cas(st, rev, event="REPLICATION_RECONFIGURED", last_failover={**lf, "replication_ready_ms": now_ms()})
                    return
        await self._enforce_fences(st)

    async def _replication_ready(self, active: str) -> bool:
        try:
            for t in await self.engine.topics():
                if t["active_region"] != active:
                    continue
                if any(f["state"] == "PENDING_CUTOVER" and self.trackers[f["target_region"]].declared == M.UP for f in t["flows"]):
                    return False
            return True
        except EngineError:
            return False

    async def _enforce_fences(self, st: dict) -> None:
        """Cluster-level fence on every region the coordinator lists as fenced and that is reachable: max.message.bytes=1 on its
        topics, so Kafka itself refuses every produce request, even from a producer that bypasses the gateway. Re-applied every
        few seconds (a restored cluster may come back with old settings, and topics may have been created meanwhile)."""
        for r in st.get("fenced", {}):
            if r == st["active_region"] or self.trackers[r].last_class == M.DOWN or not self.trackers[r].last_sample.get("kafka"):
                continue
            if time.monotonic() - self.fence_applied.get(r, -1e9) < 5:
                continue
            try:
                names = [t["topic"] for t in await self.engine.topics()]
                parts = await self.kafkas.partitions(r, names)
                n = 0
                for topic, ps in parts.items():
                    if await self.kafkas.set_fence(r, topic, True):
                        n += 1
                self.fence_applied[r] = time.monotonic()
                if n:
                    log.warning("FENCED region %s at cluster level: max.message.bytes=1 on %d topic(s)", r, n)
            except Exception as exc:  # noqa: BLE001
                log.warning("could not apply the cluster fence to %s yet: %r", r, exc)

    async def _sync_availability(self) -> None:
        """Keep the policy engine's availability of each region in line with measured health, so policy decisions and the
        engine's own promotion check use facts, not stale configuration."""
        for r, tr in self.trackers.items():
            want = {M.DOWN: "UNAVAILABLE", M.UP: "AVAILABLE"}.get(tr.declared)
            if want is None or self.avail_sent.get(r) == want:
                continue
            try:
                regs = {x["region_id"]: x for x in await self.engine.regions()}
                if r in regs:
                    if regs[r]["availability_status"] != want:
                        await self.engine.put_region({**regs[r], "availability_status": want})
                    self.avail_sent[r] = want
            except EngineError as exc:
                log.warning("could not sync availability of %s: %s", r, exc)

    # ---- operator API entry points ----------------------------------------------------------------------------
    async def manual_failover(self, target: Optional[str], reason: str, approved_by: Optional[str], accept_degraded_rpo: bool) -> dict:
        """Idempotent promotion request. Safe to send to every controller at once: only one compare-and-swap can win; the
        others report the transition that did."""
        try:
            st, rev = await self.coord.state()
        except CoordinationUnavailable as exc:
            self.metrics["refused_no_quorum"] += 1
            return {"result": "REFUSED", "reason": "COORDINATION_UNAVAILABLE", "detail": str(exc)}
        async with self._busy:
            try:
                st, rev = await self.coord.state()
                phase = st["phase"]
                if phase in (M.PROMOTING_B, M.PROMOTING_C):
                    return {"result": "ALREADY_IN_PROGRESS", "transition": st["transition"]}
                if phase in (M.HEALTHY_A, M.LOCAL_DEGRADED_A, M.HEALTHY_B, M.HEALTHY_C):
                    reg = st["active_region"]
                    if self.trackers[reg].declared != M.DOWN:
                        return {"result": "REFUSED", "reason": "ACTIVE_REGION_NOT_FAILED", "detail": f"{reg} is {self.trackers[reg].declared}"}
                    await self._begin_unavailable(st, rev, reg)
                    st, rev = await self.coord.state()
                    return {"result": st["phase"], "state": st}
                if phase in (M.REGION_A_UNAVAILABLE, M.REGION_B_UNAVAILABLE, M.NO_ELIGIBLE_TARGET):
                    if accept_degraded_rpo and not approved_by:
                        return {"result": "REFUSED", "reason": "APPROVER_REQUIRED"}
                    if accept_degraded_rpo and not self.cfg.allow_degraded_rpo:
                        return {
                            "result": "REFUSED",
                            "reason": "DEGRADED_RPO_NOT_PERMITTED",
                            "detail": "this deployment's recovery policy forbids promoting a replica that is outside its RPO",
                        }
                    r = await self._try_promote(st, rev, {"approved_by": approved_by} if accept_degraded_rpo else None)
                    return r
                return {"result": "REFUSED", "reason": "WRONG_PHASE", "detail": phase}
            except Conflict:
                st, _ = await self.coord.state()
                return {"result": "ALREADY_IN_PROGRESS", "transition": st.get("transition"), "phase": st["phase"]}

    def snapshot(self) -> dict:
        return {
            "instance": self.cfg.instance,
            "leader": self.is_leader,
            "leader_id": self.leader_id,
            "coordination_reachable": self.coord_ok,
            "phase": self.state.get("phase") if self.coord_ok else M.COORDINATION_UNAVAILABLE,
            "last_known_phase": self.last_known_phase,
            "state": self.state,
            "health": {
                r: {"declared": t.declared, "last": t.last_class, "down_streak": t.down_streak, "ok_streak": t.ok_streak, "sample": t.last_sample}
                for r, t in self.trackers.items()
            },
            "eligibility": self.last_eval,
            "metrics": self.metrics,
            "auto": self.cfg.auto,
            "allow_degraded_rpo": self.cfg.allow_degraded_rpo,
        }
