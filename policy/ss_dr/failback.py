"""Controlled failback (Phase 6, section H). A recovered region NEVER becomes active by itself. The path back is:

  start (operator)  ->  validate the restored region (health, cluster identity, receiver)  ->  FAILBACK_PENDING
  RESYNC            ->  quarantine the restored region's divergent suffix, recreate its copy EMPTY, re-seed it from the active region
  FRESHNESS         ->  wait until the re-seeded replica is within the recovery freshness
  READY             ->  wait for an explicit, named approval (failback/approve)
  SWITCHING         ->  fence the current active region (epoch+1, nobody writes), wait out the leases, wait until the restored region
                        holds EVERYTHING the fenced region holds, verify offsets and event lineage, promote it, activate it
  RE-PROTECT        ->  the other regions are rebuilt as replicas of the new active region (their copies are recreated and re-seeded)

Failure policy: any step that cannot be completed leaves the system FENCED (no writer) or on the old active region, never with two
writers. There is no automatic rollback of a half-done switch: it fails closed and an operator resumes or inspects it.
Limitation (documented in DR.md): re-seeding recreates a replica's copy from offset 0, so during RE-PROTECT the topic has fewer
replicas than its policy asks for."""

import asyncio
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any, Optional

from . import model as M
from .adapters import Config, Engine, EngineError, Kafkas, now_ms
from .coord import CoordinationUnavailable, Coordinator

log = logging.getLogger("ss.dr")


class FailbackMixin:
    if TYPE_CHECKING:  # what the controller (DR) provides to this mixin
        cfg: Config
        coord: Coordinator
        engine: Engine
        kafkas: Kafkas
        trackers: dict[str, M.HealthTracker]
        regions: list[str]
        fence_applied: dict[str, float]
        _busy: asyncio.Lock
        _tick_n: int

        async def _cas(self, st: dict, rev: int, phase: Optional[str] = None, *, event: str, **changes: Any) -> tuple[dict, int]: ...
        def _crash_point(self, step: str) -> None: ...

    # ---- operator API --------------------------------------------------------------------------------------------
    async def failback_start(self, region: str, requested_by: str) -> dict:
        try:
            st, rev = await self.coord.state()
        except CoordinationUnavailable as exc:
            return {"result": "REFUSED", "reason": "COORDINATION_UNAVAILABLE", "detail": str(exc)}
        async with self._busy:
            if st["phase"] not in (M.HEALTHY_B, M.HEALTHY_C):
                return {"result": "REFUSED", "reason": "WRONG_PHASE", "detail": st["phase"]}
            if region not in st.get("fenced", {}) or region == st["active_region"]:
                return {"result": "REFUSED", "reason": "REGION_NOT_FENCED", "detail": f"{region} is not a fenced former active region"}
            bad = await self._validate_restored(region)
            if bad:
                return {"result": "REFUSED", "reason": "RESTORED_REGION_NOT_VALID", "detail": bad}
            fb = {
                "id": uuid.uuid4().hex[:12],
                "region": region,
                "stage": "RESYNC",
                "requested_by": requested_by,
                "approved_by": None,
                "started_ms": now_ms(),
                "seeded": {},
                "quarantine": {},
                "from": st["active_region"],
            }
            try:
                st, rev = await self._cas(st, rev, M.FAILBACK_PENDING, event="FAILBACK_STARTED", failback=fb)
            except Exception as exc:  # noqa: BLE001
                return {"result": "REFUSED", "reason": "CONFLICT", "detail": repr(exc)}
            return {"result": "STARTED", "failback": fb}

    async def failback_approve(self, approved_by: str) -> dict:
        try:
            st, rev = await self.coord.state()
        except CoordinationUnavailable as exc:
            return {"result": "REFUSED", "reason": "COORDINATION_UNAVAILABLE", "detail": str(exc)}
        async with self._busy:
            fb = st.get("failback")
            if st["phase"] != M.FAILBACK_PENDING or not fb:
                return {"result": "REFUSED", "reason": "NO_FAILBACK_IN_PROGRESS"}
            if fb["stage"] != "READY":
                return {"result": "REFUSED", "reason": "NOT_READY", "detail": f"stage is {fb['stage']}; approval is accepted only at READY"}
            bad = await self._failback_ready(st)
            if bad:
                return {"result": "REFUSED", "reason": "FRESHNESS_NOT_MET", "detail": bad}
            await self._cas(st, rev, event="FAILBACK_APPROVED", failback={**fb, "stage": "SWITCHING", "approved_by": approved_by, "approved_ms": now_ms()})
            return {"result": "APPROVED"}

    async def failback_abort(self, who: str) -> dict:
        try:
            st, rev = await self.coord.state()
        except CoordinationUnavailable as exc:
            return {"result": "REFUSED", "reason": "COORDINATION_UNAVAILABLE", "detail": str(exc)}
        async with self._busy:
            fb = st.get("failback")
            if st["phase"] != M.FAILBACK_PENDING or not fb or fb["stage"] == "SWITCHING":
                return {
                    "result": "REFUSED",
                    "reason": "CANNOT_ABORT",
                    "detail": "no failback, or the switch has begun (it fails closed and is resumed, not rolled back)",
                }
            await self._cas(st, rev, M.HEALTHY_OF[st["active_region"]], event="FAILBACK_ABORTED", failback=None)
            return {"result": "ABORTED"}

    # ---- validation ------------------------------------------------------------------------------------------------
    async def _validate_restored(self, region: str) -> Optional[str]:
        tr = self.trackers[region]
        if tr.declared != M.UP:
            return f"{region} is {tr.declared}, not UP"
        want = self.cfg.expected_cluster_ids.get(region)
        got = tr.last_sample.get("cluster_id")
        if want and got != want:
            return f"cluster identity mismatch: {region} reports {got}, expected {want}"
        if not tr.last_sample.get("agent"):
            return f"the replication receiver of {region} is not healthy"
        return None

    # ---- steps (run by the leader) -----------------------------------------------------------------------------------
    async def _failback_step(self, st: dict, rev: int) -> None:
        fb = st.get("failback")
        if not fb:
            return
        region = fb["region"]
        try:
            if fb["stage"] == "RESYNC":
                await self._failback_resync(st, rev)
            elif fb["stage"] == "FRESHNESS":
                if self._tick_n % 2 == 0 and not await self._failback_ready(st):
                    await self._cas(st, rev, event="FAILBACK_READY", failback={**fb, "stage": "READY", "ready_ms": now_ms()})
            elif fb["stage"] == "SWITCHING":
                await self._failback_switch(st, rev)
        except (CoordinationUnavailable, EngineError) as exc:
            log.error("failback of %s is waiting: %s", region, exc)

    async def _topics_of(self, active: str) -> list[dict]:
        return [t for t in await self.engine.topics() if t["active_region"] == active]

    async def _unfence_region(self, region: str) -> int:
        names = [t["topic"] for t in await self.engine.topics()]
        parts = await self.kafkas.partitions(region, names)
        n = 0
        for topic in parts:
            if await self.kafkas.set_fence(region, topic, False):
                n += 1
        self.fence_applied.pop(region, None)
        return n

    async def _seed(self, topic_info: dict, region: str, acked_into_active: Optional[dict]) -> Optional[dict]:
        """Quarantine (if the region holds a divergent suffix), recreate the region's copy empty, re-seed it from the active region.
        The policy engine is asked FIRST: nothing is created in a region the policy forbids for this topic (returns None)."""
        topic, n = topic_info["topic"], max(1, topic_info["partitions"])
        if await self.engine.reseed(topic, region, n, dry_run=True) is None:
            log.warning("re-seed of %s in %s skipped: the policy does not allow this topic there", topic, region)
            return None
        q = {"records": 0}
        if acked_into_active is not None:
            q = await self.kafkas.quarantine(region, topic, n, acked_into_active)
        rf = await self.kafkas.recreate_topic(region, topic, n)
        r = await self.engine.reseed(topic, region, n)
        if r is None:
            raise RuntimeError(f"the policy engine refused the re-seed of {topic} in {region} after approving it: the policy changed mid-way")
        return {"quarantine": q, "rf": rf, "flow_id": r["flow_id"]}

    async def _failback_resync(self, st: dict, rev: int) -> None:
        fb = st["failback"]
        region, active = fb["region"], st["active_region"]
        if await self._unfence_region(region):
            log.warning("cluster-level fence lifted on %s: it is being re-seeded as a replica (gateway fence in the coordinator stays)", region)
        topics = await self._topics_of(active)
        for t in topics:
            if t["topic"] in fb["seeded"]:
                continue
            # what the restored region acknowledged to the active region before the failure: everything beyond it is divergent
            cps = {}
            try:
                src_cluster = await self._cluster(region)
                dst_cluster = await self._cluster(st["failback"]["from"])
                cps = {
                    c["source_partition"]: c["last_acknowledged_source_offset"]
                    for c in (await self.engine.topic(t["topic"]))["checkpoints"]
                    if c["source_cluster"] == src_cluster and c["target_cluster"] != src_cluster and c["target_cluster"] == dst_cluster
                }
                if not cps:  # the checkpoints of the original A->active flow (any generation): the best record of what was replicated
                    cps = {
                        c["source_partition"]: c["last_acknowledged_source_offset"]
                        for c in (await self.engine.topic(t["topic"]))["checkpoints"]
                        if c["source_cluster"] == src_cluster
                    }
            except EngineError:
                raise
            res = await self._seed(t, region, cps)
            if res is None:
                raise RuntimeError(f"the restored region {region} may not hold {t['topic']} under the current policy: failback cannot proceed")
            fb = {**fb, "seeded": {**fb["seeded"], t["topic"]: res["flow_id"]}, "quarantine": {**fb["quarantine"], t["topic"]: res["quarantine"]}}
            st, rev = await self._cas(st, rev, event="FAILBACK_TOPIC_SEEDED", failback=fb)
            self._crash_point("FAILBACK_AFTER_SEED")
        await self._cas(st, rev, event="FAILBACK_RESYNCED", failback={**fb, "stage": "FRESHNESS", "resynced_ms": now_ms()})

    async def _cluster(self, region: str) -> str:
        regs = {r["region_id"]: r for r in await self.engine.regions()}
        return regs[region].get("cluster_id") or region

    async def _failback_ready(self, st: dict) -> Optional[str]:
        """None when every topic's re-seeded replica is within the recovery freshness; otherwise why not."""
        fb = st["failback"]
        region, active = fb["region"], st["active_region"]
        try:
            backlog = await self.engine.backlog()
            topics = await self._topics_of(active)
        except EngineError as exc:
            return f"policy engine unavailable: {exc}"
        for t in topics:
            flow = next((f for f in t["flows"] if f["source_region"] == active and f["target_region"] == region and f["state"] == "ENABLED"), None)
            if flow is None:
                return f"{t['topic']}: no ENABLED flow {active} -> {region}"
            rep = next((b for b in backlog if b["flow_id"] == flow["flow_id"]), None)
            if rep is None or rep["report_age_s"] > 10:
                return f"{t['topic']}: no fresh backlog report yet"
            if rep["backlog_records"] > self.cfg.failback_max_lag_records:
                return f"{t['topic']}: {rep['backlog_records']} records still to replicate"
            if t["max_lag_ms"] is not None and rep["backlog_records"] > 0 and (rep["drain_seconds"] is None or rep["drain_seconds"] * 1000 > t["max_lag_ms"]):
                return f"{t['topic']}: drain time {rep['drain_seconds']} s exceeds the lag budget"
        return None

    async def _failback_switch(self, st: dict, rev: int) -> None:
        fb = st["failback"]
        region, old = fb["region"], fb["from"]
        sw = fb.get("switch")
        if sw is None:
            E, G = st["leadership_epoch"] + 1, st["replication_generation"] + 1
            sw = {"step": "FENCED", "epoch": E, "generation": G, "fenced_wall_ms": now_ms(), "from": old, "to": region}
            # the fencing instant: next epoch, nobody active, the current active region fenced: ONE atomic write
            st, rev = await self._cas(
                st,
                rev,
                event="FAILBACK_FENCE",
                active_region="",
                leadership_epoch=E,
                replication_generation=G,
                fenced={**st.get("fenced", {}), old: E},
                failback={**fb, "switch": sw},
            )
            self._crash_point("FAILBACK_AFTER_FENCE")
            fb = st["failback"]
        E, G = sw["epoch"], sw["generation"]
        remaining = sw["fenced_wall_ms"] + self.cfg.lease_ttl_ms + self.cfg.drift_ms - now_ms()
        if remaining > 0:
            await asyncio.sleep(remaining / 1000.0)
        topics = await self._topics_of(old)
        deadline = time.monotonic() + self.cfg.failback_catchup_s
        verified: dict = {}
        for t in topics:  # the fenced region cannot grow: the restored one must reach it exactly
            n = max(1, t["partitions"])
            while True:
                a = await self.kafkas.end_offsets(region, t["topic"], n)
                b = await self.kafkas.end_offsets(old, t["topic"], n)
                if a == b:
                    break
                if time.monotonic() > deadline:
                    log.error(
                        "failback of %s: still behind %s on %s after %.0f s (%s vs %s). Staying FENCED (no writer); the step is retried",
                        region,
                        old,
                        t["topic"],
                        self.cfg.failback_catchup_s,
                        a,
                        b,
                    )
                    return
                await asyncio.sleep(1.0)
            ok = True
            for p in range(n):
                if await self.kafkas.tail_ids(region, t["topic"], p, 50) != await self.kafkas.tail_ids(old, t["topic"], p, 50):
                    ok = False
            if not ok:
                log.error("failback of %s: event lineage differs from %s on %s. Staying FENCED; operator action required", region, old, t["topic"])
                return
            verified[t["topic"]] = {str(p): b[p] for p in range(n)}
        blocked = []
        for t in topics:
            status, body = await self.engine.promote(t["topic"], region, E, G, f"failback {old}->{region}", reseed=True)
            if status == 403:
                blocked.append({"topic": t["topic"], "reasons": [r["code"] for r in body.get("reasons", [])]})
        if blocked:
            log.error("failback blocked by the policy engine: %s (staying fenced)", blocked)
            return
        fenced = {k: v for k, v in st.get("fenced", {}).items() if k != region}
        done = {
            "id": fb["id"],
            "from": old,
            "to": region,
            "epoch": E,
            "generation": G,
            "started_ms": fb["started_ms"],
            "approved_ms": fb.get("approved_ms"),
            "approved_by": fb.get("approved_by"),
            "requested_by": fb["requested_by"],
            "fenced_ms": sw["fenced_wall_ms"],
            "activated_ms": now_ms(),
            "verified_end_offsets": verified,
            "quarantine": fb["quarantine"],
        }
        st, rev = await self._cas(
            st,
            rev,
            M.HEALTHY_OF[region],
            event="FAILBACK_COMPLETED",
            active_region=region,
            fenced=fenced,
            failback=None,
            transition=None,
            last_failback=done,
            last_failover=None,
            reprotect={"regions": [r for r in self.regions if r != region], "done": []},
        )
        log.warning("FAILBACK completed: %s is active at epoch %s; %s is fenced and will be re-protected", region, E, old)
        self._crash_point("FAILBACK_AFTER_ACTIVATE")

    # ---- re-protect: the other regions become replicas of the new active region -------------------------------------------
    async def _reprotect(self, st: dict, rev: int) -> None:
        rp = st["reprotect"]
        active = st["active_region"]
        todo = [r for r in rp["regions"] if r not in rp["done"]]
        for r in todo:
            if self.trackers[r].declared != M.UP:
                continue  # a region that is down is re-protected when it returns
            if await self._unfence_region(r):
                log.warning("cluster-level fence lifted on %s: it is being re-seeded as a replica", r)
            for t in await self._topics_of(active):
                if r not in await self.engine.replica_regions(t["topic"]):
                    continue  # this region never held a replica of this topic (and may be forbidden to): nothing to rebuild there
                await self._seed(t, r, None)
            fenced = {k: v for k, v in st.get("fenced", {}).items() if k != r}
            st, rev = await self._cas(st, rev, event="REPROTECTED", fenced=fenced, reprotect={**rp, "done": rp["done"] + [r]})
            rp = st["reprotect"]
        if len([r for r in rp["regions"] if r not in rp["done"]]) == 0:
            await self._cas(st, rev, event="REPROTECT_COMPLETE", reprotect=None)
