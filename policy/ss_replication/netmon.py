"""Network monitoring for replication paths (I/O wrapper around the pure routing core).

Probes go through each path's own endpoint (a distinct Toxiproxy listener in the test stack) to the target's receiver, so
RTT, loss, bandwidth, failures and timeouts are measured on the same route the replication traffic would take.
Measured at the application level (HTTP over TCP): "loss" is the share of failed probes and sends in the rolling window,
not ICMP packet loss. Real replication sends feed the same statistics."""

import asyncio
import time
from typing import Callable, Optional

import httpx

from .routing import CircuitBreaker, PathInfo, PathStats, RouteDecision, RouteSelector, RoutingConfig, Sample

PROBE_PATH = "/internal/v1/replication/probe"


class PairMonitor:
    """Measurements for every path between this region and one target region."""

    def __init__(
        self,
        source_region: str,
        target_region: str,
        agent_key: str,
        http: httpx.AsyncClient,
        cfg: RoutingConfig,
        probe_timeout_s: float = 2.0,
        bw_every: int = 5,
        bw_bytes: int = 16384,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.source, self.target, self.key, self.http, self.cfg, self.clock = source_region, target_region, agent_key, http, cfg, clock
        self.probe_timeout_s, self.bw_every, self.bw_bytes = probe_timeout_s, bw_every, bw_bytes
        self.paths: dict[str, dict] = {}
        self.stats: dict[str, PathStats] = {}
        self.breakers: dict[str, CircuitBreaker] = {}
        self._cycle = 0

    def set_paths(self, paths: list[dict]) -> None:
        self.paths = {p["path_id"]: p for p in paths}
        for pid in self.paths:
            self.stats.setdefault(pid, PathStats())
            self.breakers.setdefault(pid, CircuitBreaker(self.cfg))

    def _auth(self) -> dict:
        return {"Authorization": f"Bearer {self.key}", "X-Replication-Agent": self.source}

    def record(
        self, path_id: str, ok: bool, rtt_ms: Optional[float] = None, timeout: bool = False, bandwidth_bps: Optional[float] = None, kind: str = "probe"
    ) -> None:
        if path_id not in self.stats:
            return
        now = self.clock()
        self.stats[path_id].add(Sample(now, ok, rtt_ms if ok else None, timeout, bandwidth_bps if ok else None, kind))
        self.breakers[path_id].record(ok, now)

    def record_send(self, path_id: str, ok: bool, seconds: float = 0.0, nbytes: int = 0, timeout: bool = False) -> None:
        """Feed a real replication request into the same statistics."""
        bw = (nbytes / seconds) if ok and seconds > 0 and nbytes > 0 else None
        self.record(path_id, ok, seconds * 1000.0 if ok else None, timeout, bw, "send")

    async def _probe(self, path: dict, big: bool) -> None:
        body = b"x" * (self.bw_bytes if big else 16)
        t0 = time.monotonic()
        try:
            r = await self.http.post(path["endpoint_url"] + PROBE_PATH, content=body, headers=self._auth(), timeout=10.0 if big else self.probe_timeout_s)
        except httpx.TimeoutException:
            self.record(path["path_id"], False, timeout=True)
            return
        except httpx.TransportError:
            self.record(path["path_id"], False)
            return
        dt = time.monotonic() - t0
        if r.status_code != 200:
            self.record(path["path_id"], False)
        elif big:
            self.record(path["path_id"], True, None, False, len(body) / max(dt, 1e-6))
        else:
            self.record(path["path_id"], True, dt * 1000.0)

    async def probe_once(self) -> None:
        """One probe per path (all paths, including those whose breaker is open: that is the recovery probing)."""
        self._cycle += 1
        big = self.bw_every > 0 and self._cycle % self.bw_every == 0
        await asyncio.gather(*(self._probe(p, False) for p in self.paths.values()))
        if big:
            await asyncio.gather(*(self._probe(p, True) for p in self.paths.values()))

    def snapshot(self) -> dict:
        now = self.clock()
        out = {}
        for pid, st in self.stats.items():
            m = st.measure(now, self.cfg.sample_max_age_s)
            out[pid] = {
                "breaker": self.breakers[pid].state(now),
                "rtt_ms": m.rtt_ms,
                "loss": m.loss,
                "bandwidth_bps": m.bandwidth_bps,
                "samples": m.samples,
                "consecutive_failures": m.consecutive_failures,
                "timeouts": m.timeouts,
                "last_sample_age_s": None if m.last_sample_ts is None else round(now - m.last_sample_ts, 2),
                "invalid_samples": st.invalid,
            }
        return out


class FlowRoute:
    """Route state of one replication flow. Legal eligibility comes from the engine (discovery); health and scoring from
    the monitor. `current_path()` is what every partition pipeline of the flow sends through (None = pause)."""

    def __init__(self, monitor: PairMonitor, cfg: RoutingConfig, clock: Callable[[], float] = time.monotonic):
        self.monitor, self.cfg = monitor, cfg
        self.selector = RouteSelector(cfg, clock)
        self.policy_eligible: dict[str, dict] = {}
        self.policy_rejected: dict[str, list] = {}
        self.last_decision: Optional[RouteDecision] = None
        self.last_change_wall: Optional[float] = None

    def update_paths(self, paths: list[dict]) -> None:
        self.policy_eligible = {p["path_id"]: p for p in paths if p.get("policy_eligible")}
        self.policy_rejected = {p["path_id"]: p.get("reasons", []) for p in paths if not p.get("policy_eligible")}

    def decide(self, backlog_bytes: float) -> RouteDecision:
        infos = [PathInfo(p["path_id"], float(p.get("cost", 1.0)), bool(p.get("fallback"))) for p in self.policy_eligible.values()]
        for i in infos:  # make sure every eligible path has stats and a breaker
            self.monitor.stats.setdefault(i.path_id, PathStats())
            self.monitor.breakers.setdefault(i.path_id, CircuitBreaker(self.cfg))
        d = self.selector.decide(infos, self.monitor.stats, self.monitor.breakers, backlog_bytes)
        self.last_decision = d
        if d.changed:
            self.last_change_wall = time.time()
        return d

    def current_path(self) -> Optional[dict]:
        pid = self.selector.current
        return self.policy_eligible.get(pid) if pid else None

    def snapshot(self) -> dict:
        d = self.last_decision
        return {
            "current": self.selector.current,
            "changes": self.selector.changes,
            "last_change": self.last_change_wall,
            "reason": d.reason if d else None,
            "scores": d.scores if d else {},
            "excluded": d.excluded if d else {},
            "policy_rejected": self.policy_rejected,
            "measurements": self.monitor.snapshot(),
        }
