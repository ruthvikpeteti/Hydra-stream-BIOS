"""Pure DR core (Phase 6): the failover state machine, region health classification and the promotion-eligibility
rules. No I/O. Everything here is deterministic so that two controllers looking at the same facts reach the same answer."""

from dataclasses import dataclass, field
from typing import Optional

# ---- state machine -------------------------------------------------------------------------------------------
HEALTHY_A, LOCAL_DEGRADED_A, REGION_A_UNAVAILABLE = "HEALTHY_A", "LOCAL_DEGRADED_A", "REGION_A_UNAVAILABLE"
PROMOTING_B, HEALTHY_B, REGION_B_UNAVAILABLE = "PROMOTING_B", "HEALTHY_B", "REGION_B_UNAVAILABLE"
PROMOTING_C, HEALTHY_C = "PROMOTING_C", "HEALTHY_C"
NO_ELIGIBLE_TARGET, COORDINATION_UNAVAILABLE, FAILBACK_PENDING = "NO_ELIGIBLE_TARGET", "COORDINATION_UNAVAILABLE", "FAILBACK_PENDING"

STATES = (
    HEALTHY_A,
    LOCAL_DEGRADED_A,
    REGION_A_UNAVAILABLE,
    PROMOTING_B,
    HEALTHY_B,
    REGION_B_UNAVAILABLE,
    PROMOTING_C,
    HEALTHY_C,
    NO_ELIGIBLE_TARGET,
    COORDINATION_UNAVAILABLE,
    FAILBACK_PENDING,
)

# Persisted transitions (COORDINATION_UNAVAILABLE is the controller's local view: it cannot be written while the
# coordinator is unavailable, so it is only ever entered from, and left to, the persisted state it interrupted).
TRANSITIONS: dict[str, frozenset[str]] = {
    HEALTHY_A: frozenset({LOCAL_DEGRADED_A, REGION_A_UNAVAILABLE}),
    LOCAL_DEGRADED_A: frozenset({HEALTHY_A, REGION_A_UNAVAILABLE}),
    REGION_A_UNAVAILABLE: frozenset({PROMOTING_B, PROMOTING_C, NO_ELIGIBLE_TARGET, HEALTHY_A}),
    NO_ELIGIBLE_TARGET: frozenset({PROMOTING_B, PROMOTING_C, HEALTHY_A, HEALTHY_B, HEALTHY_C}),
    PROMOTING_B: frozenset({HEALTHY_B}),
    HEALTHY_B: frozenset({REGION_B_UNAVAILABLE, FAILBACK_PENDING}),
    REGION_B_UNAVAILABLE: frozenset({PROMOTING_C, NO_ELIGIBLE_TARGET, HEALTHY_B}),
    PROMOTING_C: frozenset({HEALTHY_C}),
    HEALTHY_C: frozenset({FAILBACK_PENDING}),
    FAILBACK_PENDING: frozenset({HEALTHY_A, HEALTHY_B, HEALTHY_C}),
    COORDINATION_UNAVAILABLE: frozenset(),
}
HEALTHY_OF = {"region-a": HEALTHY_A, "region-b": HEALTHY_B, "region-c": HEALTHY_C}
PROMOTING_OF = {"region-b": PROMOTING_B, "region-c": PROMOTING_C}
UNAVAILABLE_OF = {"region-a": REGION_A_UNAVAILABLE, "region-b": REGION_B_UNAVAILABLE}
ACTIVE_OF = {
    HEALTHY_A: "region-a",
    LOCAL_DEGRADED_A: "region-a",
    REGION_A_UNAVAILABLE: "region-a",
    HEALTHY_B: "region-b",
    REGION_B_UNAVAILABLE: "region-b",
    HEALTHY_C: "region-c",
}


def can_transition(a: str, b: str) -> bool:
    return b in TRANSITIONS.get(a, frozenset())


def initial_state(active: str = "region-a") -> dict:
    return {
        "phase": HEALTHY_OF[active],
        "active_region": active,
        "leadership_epoch": 1,
        "replication_generation": 1,
        "fenced": {},
        "transition": None,
        "seq": 0,
    }


# ---- region health -------------------------------------------------------------------------------------------
UP, DEGRADED, DOWN, SUSPECT = "UP", "DEGRADED", "DOWN", "SUSPECT"


def classify_sample(s: dict) -> str:
    """One probe round of ONE region. `kafka` (cluster metadata reachable), `gateway`, `agent` are independent signals.
    DOWN needs Kafka unreachable AND another independent signal failing: a dead gateway, a dead agent or a broken
    controller-to-Kafka path alone never declares a region failed (SUSPECT / DEGRADED instead).
    A slow replication link or a slow consumer is not a signal at all: it never reaches this function."""
    if not s.get("kafka"):
        return DOWN if (not s.get("gateway") or not s.get("agent")) else SUSPECT
    if s.get("brokers_up", 0) < s.get("brokers_expected", 1) or s.get("leaders_ok") is False or s.get("isr_ok") is False:
        return DEGRADED
    if not s.get("gateway") or not s.get("agent"):
        return DEGRADED
    return UP


@dataclass
class HealthTracker:
    """Consecutive-sample thresholds: `fail_after` DOWN samples to declare a region failed, `recover_after` non-DOWN
    samples to declare it recovered (the recovery window). `failure_boundary` is the time of the first sample of the
    current DOWN streak: the boundary used to judge replication freshness."""

    region: str
    fail_after: int = 3
    recover_after: int = 3
    declared: str = UP  # UP | DEGRADED | DOWN
    down_streak: int = 0
    ok_streak: int = 0
    failure_boundary: Optional[float] = None
    last_sample: dict = field(default_factory=dict)
    last_class: str = UP

    def observe(self, sample: dict, now: float) -> str:
        c = classify_sample(sample)
        self.last_sample, self.last_class = sample, c
        if c == DOWN:
            if self.down_streak == 0:
                self.failure_boundary = now
            self.down_streak += 1
            self.ok_streak = 0
            if self.down_streak >= self.fail_after:
                self.declared = DOWN
        else:
            self.ok_streak += 1
            if self.declared != DOWN:
                self.down_streak = 0
                self.failure_boundary = None
                self.declared = DEGRADED if c in (DEGRADED, SUSPECT) else UP
            elif self.ok_streak >= self.recover_after:
                self.down_streak, self.failure_boundary = 0, None
                self.declared = DEGRADED if c in (DEGRADED, SUSPECT) else UP
        return self.declared


# ---- eligibility ---------------------------------------------------------------------------------------------
HARD, WAIVABLE = "HARD", "WAIVABLE"
# codes in WAIVABLE can be waived by an authorised operator ONLY for degraded-RPO recovery; policy, health, quorum and
# fencing reasons can never be waived.
WAIVABLE_CODES = frozenset({"LAG_EXCEEDS_RPO", "LAG_UNKNOWN"})


@dataclass(frozen=True)
class CandidateFacts:
    region: str
    topic: str
    policy_codes: tuple = ()  # engine reasons that make promotion illegal (empty = policy permits)
    health: str = UP
    controller_quorum: bool = True
    data_present: bool = True  # topic + all partitions exist and hold what the checkpoints claim
    checkpoints_complete: bool = True  # a replication position exists for every partition
    lag_ms: Optional[float] = None  # None = not measurable
    max_lag_ms: Optional[int] = None
    capacity_ok: bool = True
    coordination_ok: bool = True  # a quorum can authorise the next epoch
    lease_acquirable: bool = True
    can_fence_old: bool = True
    cutover_valid: bool = True
    detail: dict = field(default_factory=dict, compare=False, hash=False)


@dataclass(frozen=True)
class Eligibility:
    region: str
    topic: str
    eligible: bool
    degraded_rpo: bool  # eligible ONLY because an operator waived the RPO check
    hard: tuple
    waivable: tuple
    lag_ms: Optional[float]
    max_lag_ms: Optional[int]

    def as_dict(self) -> dict:
        return {
            "region": self.region,
            "topic": self.topic,
            "eligible": self.eligible,
            "degraded_rpo": self.degraded_rpo,
            "reasons": list(self.hard) + list(self.waivable),
            "lag_ms": self.lag_ms,
            "max_lag_ms": self.max_lag_ms,
        }


def evaluate_candidate(f: CandidateFacts, approval_for_degraded_rpo: bool = False) -> Eligibility:
    hard: list[str] = [f"POLICY:{c}" for c in f.policy_codes]
    if f.health != UP:
        hard.append("CANDIDATE_UNHEALTHY")
    if not f.controller_quorum:
        hard.append("CONTROLLER_QUORUM_NOT_OPERATIONAL")
    if not f.data_present:
        hard.append("PARTITION_DATA_MISSING")
    if not f.checkpoints_complete:
        hard.append("NO_CHECKPOINT")
    if not f.capacity_ok:
        hard.append("INSUFFICIENT_CAPACITY")
    if not f.coordination_ok:
        hard.append("COORDINATION_QUORUM_UNAVAILABLE")
    if not f.lease_acquirable:
        hard.append("LEASE_NOT_ACQUIRABLE")
    if not f.can_fence_old:
        hard.append("OLD_LEADERSHIP_NOT_FENCEABLE")
    if not f.cutover_valid:
        hard.append("CUTOVER_POSITION_INVALID")
    soft: list[str] = []
    if f.lag_ms is None:
        soft.append("LAG_UNKNOWN")
    elif f.max_lag_ms is not None and f.lag_ms > f.max_lag_ms:
        soft.append("LAG_EXCEEDS_RPO")
    waived = bool(soft) and approval_for_degraded_rpo and not hard
    ok = not hard and (not soft or waived)
    return Eligibility(f.region, f.topic, ok, waived and ok, tuple(sorted(hard)), tuple(sorted(soft)), f.lag_ms, f.max_lag_ms)


def choose_target(order: list[str], results: dict[str, list[Eligibility]]) -> Optional[str]:
    """First region in `order` that is eligible for EVERY topic. All-or-nothing: a region-level failover never leaves a
    topic behind silently (a topic with no eligible target blocks the automatic promotion and is reported)."""
    for r in order:
        rs = results.get(r)
        if rs and all(e.eligible for e in rs):
            return r
    return None


# ---- RPO / RTO arithmetic ------------------------------------------------------------------------------------
def rpo_gap(acked_by_client: set, present_in_new_active: set) -> dict:
    """MEASURED recovery-point gap of a test or a reconciliation: acknowledged events missing from the new active."""
    missing = acked_by_client - present_in_new_active
    return {"acked": len(acked_by_client), "present": len(acked_by_client & present_in_new_active), "lost": len(missing), "lost_ids": sorted(missing)[:20]}
