"""Adaptive route selection (pure, deterministic, injectable clock).

Order of work, always:
  1. LEGAL / SECURITY eligibility is decided elsewhere (ss_policy.engine.eligible_paths) and is the INPUT here: this
     module can only choose among paths already allowed. A score can never put a prohibited path back.
  2. HEALTH eligibility: circuit breaker, endpoint availability, RTT / loss / bandwidth thresholds.
  3. SCORING of the healthy paths (lowest wins), identical inputs give identical scores.
  4. SWITCHING with an observation window, hysteresis, a sustained-improvement hold and a cooldown, so small
     fluctuations do not cause flapping. A path that stops working is replaced immediately (cooldown does not
     apply to an unusable current path).
"""

import math
import statistics
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Optional

CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"


@dataclass(frozen=True)
class RoutingConfig:
    # score weights (need not sum to 1; only their ratios matter)
    w_latency: float = 0.30
    w_loss: float = 0.30
    w_backlog: float = 0.30
    w_cost: float = 0.10
    # normalisation ceilings: a value at or above its ceiling scores 1.0 for that metric
    latency_ceiling_ms: float = 1000.0
    loss_ceiling: float = 0.5
    drain_ceiling_s: float = 300.0
    # health thresholds: a path outside them is not eligible at all
    max_rtt_ms: float = 5000.0
    max_loss: float = 0.5
    min_bandwidth_bps: float = 0.0
    # measurements
    sample_max_age_s: float = 15.0
    min_samples: int = 3
    # switching
    min_observation_s: float = 4.0  # a candidate must have been observed this long (and min_samples) before we move to it
    hysteresis: float = 0.25  # a candidate must beat the current path's score by this fraction
    improvement_hold_s: float = 4.0  # ... continuously for this long
    cooldown_s: float = 10.0  # minimum time between voluntary switches
    # circuit breaker
    breaker_failures: int = 3
    breaker_open_s: float = 5.0
    breaker_close_successes: int = 2


@dataclass(frozen=True)
class Sample:
    ts: float
    ok: bool
    rtt_ms: Optional[float] = None
    timeout: bool = False
    bandwidth_bps: Optional[float] = None
    kind: str = "probe"  # probe | send


def _valid(x: Optional[float]) -> bool:
    return x is not None and isinstance(x, (int, float)) and math.isfinite(x) and x >= 0


@dataclass(frozen=True)
class Measurement:
    rtt_ms: Optional[float]
    loss: Optional[float]
    bandwidth_bps: Optional[float]
    samples: int
    observed_s: float
    consecutive_failures: int
    timeouts: int
    last_ok_ts: Optional[float]
    last_sample_ts: Optional[float]

    @property
    def known(self) -> bool:
        return self.samples > 0 and self.rtt_ms is not None


class PathStats:
    """Rolling window of timestamped samples for one path. Invalid samples are dropped and counted."""

    def __init__(self, max_samples: int = 200):
        self._s: deque[Sample] = deque(maxlen=max_samples)
        self.invalid = 0
        self._consecutive_failures = 0
        self._last_ok: Optional[float] = None

    def add(self, s: Sample) -> bool:
        if not math.isfinite(s.ts) or (s.rtt_ms is not None and not _valid(s.rtt_ms)) or (s.bandwidth_bps is not None and not _valid(s.bandwidth_bps)):
            self.invalid += 1
            return False
        self._s.append(s)
        if s.ok:
            self._consecutive_failures, self._last_ok = 0, s.ts
        else:
            self._consecutive_failures += 1
        return True

    def measure(self, now: float, max_age_s: float) -> Measurement:
        fresh = [s for s in self._s if 0 <= now - s.ts <= max_age_s]
        if not fresh:
            return Measurement(None, None, None, 0, 0.0, self._consecutive_failures, 0, self._last_ok, None)
        rtts = [s.rtt_ms for s in fresh if s.ok and s.rtt_ms is not None]
        bws = [s.bandwidth_bps for s in fresh if s.ok and s.bandwidth_bps is not None]
        return Measurement(
            rtt_ms=statistics.median(rtts) if rtts else None,
            loss=sum(1 for s in fresh if not s.ok) / len(fresh),
            bandwidth_bps=statistics.median(bws) if bws else None,
            samples=len(fresh),
            observed_s=now - min(s.ts for s in fresh),
            consecutive_failures=self._consecutive_failures,
            timeouts=sum(1 for s in fresh if s.timeout),
            last_ok_ts=self._last_ok,
            last_sample_ts=max(s.ts for s in fresh),
        )


class CircuitBreaker:
    """CLOSED -> OPEN after N consecutive failures; OPEN -> HALF_OPEN after a quiet period; HALF_OPEN -> CLOSED after M
    consecutive successful probes, or back to OPEN on any failure. Probes continue while OPEN (recovery probing)."""

    def __init__(self, cfg: RoutingConfig):
        self.cfg, self._state, self._fails, self._succ, self._opened_at = cfg, CLOSED, 0, 0, 0.0

    def state(self, now: float) -> str:
        if self._state == OPEN and now - self._opened_at >= self.cfg.breaker_open_s:
            self._state, self._succ = HALF_OPEN, 0
        return self._state

    def record(self, ok: bool, now: float) -> str:
        st = self.state(now)
        if ok:
            self._fails = 0
            if st == HALF_OPEN:
                self._succ += 1
                if self._succ >= self.cfg.breaker_close_successes:
                    self._state = CLOSED
        else:
            self._succ = 0
            if st == HALF_OPEN:
                self._state, self._opened_at = OPEN, now
            elif st == CLOSED:
                self._fails += 1
                if self._fails >= self.cfg.breaker_failures:
                    self._state, self._opened_at = OPEN, now
            else:
                self._opened_at = now  # still failing while open: extend the quiet period
        return self.state(now)


@dataclass(frozen=True)
class PathInfo:
    path_id: str
    cost: float = 1.0
    fallback: bool = False


def _clamp(x: float) -> float:
    return 0.0 if x <= 0 else 1.0 if x >= 1 else x


def score_path(cfg: RoutingConfig, m: Measurement, p: PathInfo, max_cost: float, backlog_bytes: float) -> Optional[dict]:
    """Weighted, normalised score (lower is better). None when there is nothing valid to score."""
    if not m.known:
        return None
    nl = _clamp(m.rtt_ms / cfg.latency_ceiling_ms)
    nloss = _clamp((m.loss or 0.0) / cfg.loss_ceiling)
    if backlog_bytes <= 0:
        nb, drain = 0.0, 0.0
    elif m.bandwidth_bps and m.bandwidth_bps > 0:
        drain = backlog_bytes / m.bandwidth_bps
        nb = _clamp(drain / cfg.drain_ceiling_s)
    else:
        drain, nb = None, 1.0  # a backlog that cannot be drained at an unknown speed counts as worst case
    nc = 0.0 if max_cost <= 0 else _clamp(p.cost / max_cost)
    total = cfg.w_latency * nl + cfg.w_loss * nloss + cfg.w_backlog * nb + cfg.w_cost * nc
    return {"score": round(total, 9), "latency": nl, "loss": nloss, "backlog_delay": nb, "cost": nc, "drain_s": drain}


@dataclass
class RouteDecision:
    selected: Optional[str]
    changed: bool
    reason: str
    previous: Optional[str] = None
    scores: dict = field(default_factory=dict)  # path_id -> score components (healthy, measured paths)
    excluded: dict = field(default_factory=dict)  # path_id -> why it is not selectable right now


class RouteSelector:
    def __init__(self, cfg: RoutingConfig, clock: Callable[[], float]):
        self.cfg, self.clock = cfg, clock
        self.current: Optional[str] = None
        self.last_change: Optional[float] = None
        self.changes = 0
        self._better: Optional[tuple[str, float]] = None  # (candidate, since)

    def health(self, m: Measurement, br_state: str) -> Optional[str]:
        """None if the path is healthy enough to carry traffic, else the reason it is not."""
        c = self.cfg
        if br_state == OPEN:
            return "CIRCUIT_OPEN"
        if br_state == HALF_OPEN:
            return "CIRCUIT_HALF_OPEN"
        if not m.known:
            return "NO_FRESH_MEASUREMENT"
        if m.consecutive_failures > 0 and (m.last_ok_ts is None or m.last_sample_ts is None or m.last_ok_ts < m.last_sample_ts):
            return "ENDPOINT_UNAVAILABLE"
        if m.rtt_ms > c.max_rtt_ms:
            return "RTT_ABOVE_THRESHOLD"
        if (m.loss or 0.0) > c.max_loss:
            return "LOSS_ABOVE_THRESHOLD"
        if c.min_bandwidth_bps > 0 and m.bandwidth_bps is not None and m.bandwidth_bps < c.min_bandwidth_bps:
            return "BANDWIDTH_BELOW_THRESHOLD"
        return None

    def decide(self, paths: list[PathInfo], stats: dict[str, PathStats], breakers: dict[str, CircuitBreaker], backlog_bytes: float = 0.0) -> RouteDecision:
        """`paths` must already be the legally/securely eligible set; nothing outside it can ever be returned."""
        now, c = self.clock(), self.cfg
        allowed = {p.path_id: p for p in paths}
        prev = self.current
        if self.current is not None and self.current not in allowed:  # became illegal: drop it at once
            self.current = None
        meas = {
            pid: stats[pid].measure(now, c.sample_max_age_s) if pid in stats else stats.setdefault(pid, PathStats()).measure(now, c.sample_max_age_s)
            for pid in sorted(allowed)
        }
        excluded, scores = {}, {}
        max_cost = max((p.cost for p in paths), default=0.0)
        for pid in sorted(allowed):
            why = self.health(meas[pid], breakers[pid].state(now))
            if why:
                excluded[pid] = why
                continue
            scores[pid] = score_path(c, meas[pid], allowed[pid], max_cost, backlog_bytes)
        healthy = sorted(scores, key=lambda pid: (scores[pid]["score"], pid))

        def switch(to: Optional[str], reason: str) -> RouteDecision:
            changed = to != self.current
            was = self.current
            if changed:
                self.current, self.last_change, self._better = to, now, None
                self.changes += 1
            return RouteDecision(self.current, changed, reason, was if changed else prev, scores, excluded)

        # --- the current path is not usable (or there is none): choose now, no cooldown, no hysteresis
        if self.current is None or self.current not in scores:
            if healthy:
                return switch(healthy[0], "INITIAL_SELECTION" if prev is None else "CURRENT_PATH_UNUSABLE")
            # Nothing is measured as healthy.
            if self.current is not None and excluded.get(self.current) == "NO_FRESH_MEASUREMENT":
                return RouteDecision(self.current, False, "KEEP_UNMEASURED_CURRENT", prev, scores, excluded)
            untested = [p for p in paths if excluded.get(p.path_id) == "NO_FRESH_MEASUREMENT"]  # breaker closed, just not measured
            if untested:
                if prev is None:  # first choice: cheapest ordinary path
                    pick = sorted(untested, key=lambda x: (x.fallback, x.cost, x.path_id))[0]
                else:  # the working path failed: the designated fallback first, then the cheapest
                    pick = sorted(untested, key=lambda x: (not x.fallback, x.cost, x.path_id))[0]
                return switch(pick.path_id, "FALLBACK_UNMEASURED")
            return switch(None, "NO_USABLE_PATH")

        # --- the current path is healthy: only move for a clear, sustained, observed improvement
        cur_score = scores[self.current]["score"]
        best = next((pid for pid in healthy if pid != self.current), None)
        if best is None:
            self._better = None
            return RouteDecision(self.current, False, "ONLY_ONE_HEALTHY_PATH", prev, scores, excluded)
        m = meas[best]
        observed = m.samples >= c.min_samples and m.observed_s >= c.min_observation_s
        clearly_better = scores[best]["score"] < cur_score * (1.0 - c.hysteresis)
        if not (observed and clearly_better):
            self._better = None
            return RouteDecision(self.current, False, "NO_CLEAR_IMPROVEMENT" if observed else "CANDIDATE_NOT_OBSERVED_LONG_ENOUGH", prev, scores, excluded)
        if self._better is None or self._better[0] != best:
            self._better = (best, now)
        if now - self._better[1] < c.improvement_hold_s:
            return RouteDecision(self.current, False, "IMPROVEMENT_NOT_SUSTAINED_YET", prev, scores, excluded)
        if self.last_change is not None and now - self.last_change < c.cooldown_s:
            return RouteDecision(self.current, False, "COOLDOWN", prev, scores, excluded)
        return switch(best, "SUSTAINED_IMPROVEMENT")
