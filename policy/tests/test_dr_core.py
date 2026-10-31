"""Phase 6 pure tests: state machine, region health classification, eligibility. No infrastructure needed."""

import pytest
from hypothesis import given, strategies as st

from ss_dr import model as M

UPS = {"kafka": True, "gateway": True, "agent": True, "brokers_up": 3, "brokers_expected": 3, "leaders_ok": True, "isr_ok": True}


# ---- state machine -------------------------------------------------------------------------------------------
def test_all_eleven_required_states_exist():
    assert set(M.STATES) == {
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
    }


def test_transitions_follow_the_tiers_and_forbid_shortcuts():
    assert M.can_transition(M.HEALTHY_A, M.REGION_A_UNAVAILABLE) and M.can_transition(M.REGION_A_UNAVAILABLE, M.PROMOTING_B)
    assert M.can_transition(M.PROMOTING_B, M.HEALTHY_B) and M.can_transition(M.HEALTHY_B, M.REGION_B_UNAVAILABLE)
    assert M.can_transition(M.REGION_B_UNAVAILABLE, M.PROMOTING_C) and M.can_transition(M.PROMOTING_C, M.HEALTHY_C)
    assert M.can_transition(M.REGION_A_UNAVAILABLE, M.PROMOTING_C)  # A and B lost together: Tier 3 directly
    # a promotion is never skipped, never abandoned half-way, and A never becomes active by itself
    assert not M.can_transition(M.HEALTHY_A, M.PROMOTING_B) and not M.can_transition(M.HEALTHY_A, M.HEALTHY_B)
    assert not M.can_transition(M.PROMOTING_B, M.HEALTHY_A) and not M.can_transition(M.HEALTHY_B, M.HEALTHY_A)
    assert not M.can_transition(M.HEALTHY_C, M.HEALTHY_A) and M.can_transition(M.FAILBACK_PENDING, M.HEALTHY_A)
    assert not M.can_transition(M.COORDINATION_UNAVAILABLE, M.PROMOTING_B)  # no promotion without the coordinator


def test_every_transition_target_is_a_known_state():
    for a, bs in M.TRANSITIONS.items():
        assert a in M.STATES and bs <= set(M.STATES)


# ---- health ---------------------------------------------------------------------------------------------------
def test_region_is_down_only_when_kafka_and_another_independent_signal_fail():
    assert M.classify_sample(UPS) == M.UP
    assert M.classify_sample({**UPS, "kafka": False, "gateway": False}) == M.DOWN
    assert M.classify_sample({**UPS, "kafka": False, "agent": False}) == M.DOWN
    assert M.classify_sample({**UPS, "kafka": False}) == M.SUSPECT  # the controller's own path to Kafka may be broken
    assert M.classify_sample({**UPS, "gateway": False}) == M.DEGRADED  # a dead gateway is not a dead region
    assert M.classify_sample({**UPS, "brokers_up": 2}) == M.DEGRADED  # a lost broker is Tier 1, not a failover
    assert M.classify_sample({**UPS, "leaders_ok": False}) == M.DEGRADED and M.classify_sample({**UPS, "isr_ok": False}) == M.DEGRADED


def test_a_slow_replication_link_is_not_a_health_signal():
    # the sample has no replication-lag field at all: lag, backlog and consumer slowness cannot reach classify_sample
    assert M.classify_sample({**UPS, "link_slow": True, "backlog": 10**9}) == M.UP


def test_tracker_needs_consecutive_failures_and_a_recovery_window():
    t = M.HealthTracker("region-a", fail_after=3, recover_after=3)
    dead = {**UPS, "kafka": False, "gateway": False}
    t.observe(dead, 1.0)
    t.observe(UPS, 2.0)
    t.observe(dead, 3.0)
    t.observe(dead, 4.0)
    assert t.declared != M.DOWN  # the streak was broken
    t.observe(dead, 5.0)
    assert t.declared == M.DOWN and t.failure_boundary == 3.0  # boundary = first sample of the streak
    t.observe(UPS, 6.0)
    t.observe(UPS, 7.0)
    assert t.declared == M.DOWN  # not yet recovered
    t.observe(dead, 8.0)
    assert t.declared == M.DOWN
    for i in range(3):
        t.observe(UPS, 9.0 + i)
    assert t.declared == M.UP and t.failure_boundary is None


# ---- eligibility ----------------------------------------------------------------------------------------------
def facts(**kw):
    return M.CandidateFacts(region="region-b", topic="t", lag_ms=100.0, max_lag_ms=5000, **kw)


def test_healthy_current_policy_permitted_candidate_is_eligible():
    e = M.evaluate_candidate(facts())
    assert e.eligible and not e.degraded_rpo and e.hard == () and e.waivable == ()


@pytest.mark.parametrize(
    "kw,code",
    [
        ({"policy_codes": ("LEGAL_STORAGE",)}, "POLICY:LEGAL_STORAGE"),
        ({"health": M.DOWN}, "CANDIDATE_UNHEALTHY"),
        ({"health": M.DEGRADED}, "CANDIDATE_UNHEALTHY"),
        ({"controller_quorum": False}, "CONTROLLER_QUORUM_NOT_OPERATIONAL"),
        ({"data_present": False}, "PARTITION_DATA_MISSING"),
        ({"checkpoints_complete": False}, "NO_CHECKPOINT"),
        ({"capacity_ok": False}, "INSUFFICIENT_CAPACITY"),
        ({"coordination_ok": False}, "COORDINATION_QUORUM_UNAVAILABLE"),
        ({"lease_acquirable": False}, "LEASE_NOT_ACQUIRABLE"),
        ({"can_fence_old": False}, "OLD_LEADERSHIP_NOT_FENCEABLE"),
        ({"cutover_valid": False}, "CUTOVER_POSITION_INVALID"),
    ],
)
def test_every_hard_check_blocks_promotion_and_no_approval_waives_it(kw, code):
    for approval in (False, True):
        e = M.evaluate_candidate(facts(**kw), approval_for_degraded_rpo=approval)
        assert not e.eligible and code in e.hard


def test_stale_or_unmeasured_replica_is_not_promoted_silently():
    stale = M.evaluate_candidate(M.CandidateFacts(region="region-c", topic="t", lag_ms=9000.0, max_lag_ms=5000))
    unknown = M.evaluate_candidate(M.CandidateFacts(region="region-c", topic="t", lag_ms=None, max_lag_ms=5000))
    assert not stale.eligible and stale.waivable == ("LAG_EXCEEDS_RPO",)
    assert not unknown.eligible and unknown.waivable == ("LAG_UNKNOWN",)


def test_degraded_rpo_needs_explicit_approval_and_is_flagged():
    f = M.CandidateFacts(region="region-c", topic="t", lag_ms=9000.0, max_lag_ms=5000)
    e = M.evaluate_candidate(f, approval_for_degraded_rpo=True)
    assert e.eligible and e.degraded_rpo and e.lag_ms == 9000.0
    # approval never rescues a candidate that also fails a hard check
    assert not M.evaluate_candidate(M.CandidateFacts(region="region-c", topic="t", lag_ms=9000.0, max_lag_ms=5000, policy_codes=("X",)), True).eligible


def test_choose_target_is_all_or_nothing_per_region_and_follows_priority():
    ok = lambda r, t: M.evaluate_candidate(M.CandidateFacts(region=r, topic=t, lag_ms=0.0, max_lag_ms=1))
    bad = lambda r, t: M.evaluate_candidate(M.CandidateFacts(region=r, topic=t, lag_ms=0.0, max_lag_ms=1, health=M.DOWN))
    assert M.choose_target(["region-b", "region-c"], {"region-b": [ok("region-b", "x"), ok("region-b", "y")], "region-c": [ok("region-c", "x")]}) == "region-b"
    assert M.choose_target(["region-b", "region-c"], {"region-b": [ok("region-b", "x"), bad("region-b", "y")], "region-c": [ok("region-c", "x")]}) == "region-c"
    assert M.choose_target(["region-b", "region-c"], {"region-b": [bad("region-b", "x")], "region-c": [bad("region-c", "x")]}) is None
    assert M.choose_target(["region-b"], {}) is None


@given(
    lag=st.one_of(st.none(), st.floats(0, 1e6)),
    mx=st.one_of(st.none(), st.integers(0, 10**6)),
    health=st.sampled_from([M.UP, M.DEGRADED, M.DOWN]),
    flags=st.lists(st.booleans(), min_size=8, max_size=8),
    approval=st.booleans(),
)
def test_eligibility_is_deterministic_and_never_promotes_through_a_hard_failure(lag, mx, health, flags, approval):
    f = M.CandidateFacts(
        region="r",
        topic="t",
        lag_ms=lag,
        max_lag_ms=mx,
        health=health,
        controller_quorum=flags[0],
        data_present=flags[1],
        checkpoints_complete=flags[2],
        capacity_ok=flags[3],
        coordination_ok=flags[4],
        lease_acquirable=flags[5],
        can_fence_old=flags[6],
        cutover_valid=flags[7],
    )
    a, b = M.evaluate_candidate(f, approval), M.evaluate_candidate(f, approval)
    assert a == b
    if a.hard:
        assert not a.eligible
    if a.eligible and a.waivable:
        assert approval and a.degraded_rpo


def test_measured_rpo_gap():
    g = M.rpo_gap({"a", "b", "c", "d"}, {"a", "b", "z"})
    assert (g["acked"], g["present"], g["lost"], g["lost_ids"]) == (4, 2, 2, ["c", "d"])
    assert M.rpo_gap({"a"}, {"a"})["lost"] == 0
