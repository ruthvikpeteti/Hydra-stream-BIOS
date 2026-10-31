"""Property-based tests: random legal zones, regions, links, statuses and policies.

The oracle below is written independently of engine.py (plain set logic, no shared helpers).
"""

from hypothesis import HealthCheck, assume, given, settings, strategies as st

from ss_policy.engine import (
    Ctx,
    RankingWeights,
    eligible_destinations,
    evaluate_assignment,
    evaluate_process,
    evaluate_promotion,
    evaluate_route,
    evaluate_store,
    plan_placement,
)
from ss_policy.models import Availability, DecisionKind, Link, Region, ReplicaStatus, StorageCapacity, TopicPolicy

ZONES = ["EU", "US", "APAC", "GLOBAL"]
TRANSIT = ["EU", "US", "APAC", "ATLANTIC", "PACIFIC"]
NAMES = [f"r{i}" for i in range(7)]
SETTINGS = settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])


@st.composite
def worlds(draw):
    n = draw(st.integers(2, 7))
    regs = {}
    for name in NAMES[:n]:
        total = draw(st.integers(0, 10**12))
        regs[name] = Region(
            region_id=name,
            legal_zone=draw(st.sampled_from(["EU", "US", "APAC"])),
            availability_status=draw(st.sampled_from(list(Availability))),
            storage_capacity=StorageCapacity(total_bytes=total, used_bytes=draw(st.integers(0, 10**12))),
            processing_capabilities=draw(st.lists(st.sampled_from(["gateway", "replication", "other"]), unique=True)),
            supported_transit_zones=draw(st.lists(st.sampled_from(TRANSIT), unique=True)),
            failure_domain=draw(st.sampled_from(["d1", "d2", "d3"])),
            compute_headroom=draw(st.floats(0, 1)),
            max_replication_factor=draw(st.integers(1, 4)),
        )
    links = {}
    for a in regs:
        for b in regs:
            if a != b and draw(st.booleans()):
                links[(a, b)] = Link(
                    from_region=a,
                    to_region=b,
                    transit_zones=draw(st.lists(st.sampled_from(TRANSIT), unique=True, max_size=3)),
                    latency_ms=draw(st.integers(0, 5000)),
                    status=draw(st.sampled_from(["UP", "UP", "DOWN"])),
                )
    return regs, links


@st.composite
def scenarios(draw):
    regs, links = draw(worlds())
    names = list(regs) + ["ghost"]  # include a region that is not registered
    pick = lambda: draw(st.lists(st.sampled_from(names), unique=True))  # noqa: E731
    storage, processing = pick(), pick()
    assume(storage)
    home = draw(st.sampled_from(storage))
    count = draw(st.integers(0, 4))
    pol = TopicPolicy(
        topic="t",
        classification=draw(st.sampled_from(["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"])),
        legal_zone=draw(st.sampled_from(ZONES)),
        home_region=home,
        allowed_storage_regions=storage,
        allowed_processing_regions=processing,
        allowed_transit_zones=draw(st.lists(st.sampled_from(TRANSIT), unique=True)),
        replication_factor=draw(st.integers(1, 4)),
        regional_replica_count=count,
        preferred_regions=draw(st.lists(st.sampled_from(names), unique=True)),
        excluded_regions=draw(st.lists(st.sampled_from(names), unique=True)),
        maximum_replication_lag_ms=None if count == 0 else 1000,
        durability_mode="LOCAL_DURABLE" if count == 0 else "ASYNC_REGIONAL",
        policy_version=draw(st.integers(1, 50)),
    )
    return regs, links, pol


def oracle_may_store(regs, pol, rid):
    """Hard constraints for holding a copy, written from the spec, not from engine.py."""
    r = regs.get(rid)
    if r is None or rid in pol.excluded_regions or rid not in pol.allowed_storage_regions:
        return False
    if pol.legal_zone != "GLOBAL" and r.legal_zone != pol.legal_zone:
        return False
    return r.availability_status not in (Availability.UNAVAILABLE, Availability.DRAINING) and pol.replication_factor <= r.max_replication_factor


def oracle_hop_ok(regs, links, pol, a, b):
    l = links.get((a, b))
    if l is None or l.status != "UP" or not set(l.transit_zones) <= set(pol.allowed_transit_zones):
        return False
    ra, rb = regs.get(a), regs.get(b)
    return bool(ra and rb and set(l.transit_zones) <= set(ra.supported_transit_zones) & set(rb.supported_transit_zones))


@SETTINGS
@given(scenarios(), st.data())
def test_chosen_destinations_always_pass_every_hard_constraint(sc, data):
    regs, links, pol = sc
    src = data.draw(st.sampled_from(sorted(regs)))
    weights = RankingWeights(*[data.draw(st.floats(0, 5)) for _ in range(5)])  # any weights at all
    plan = plan_placement(Ctx(regs, links, pol), src, weights=weights)
    for c in plan.details.get("chosen", []):
        rid = c["region"]
        assert oracle_may_store(regs, pol, rid), f"{rid} chosen but violates a hard constraint"
        assert rid != src and rid in regs
        route = c["route"]
        assert route[0] == src and route[-1] == rid
        assert all(oracle_hop_ok(regs, links, pol, a, b) for a, b in zip(route, route[1:]))
        for relay in route[1:-1]:
            r = regs[relay]
            assert relay in pol.allowed_processing_regions and relay not in pol.excluded_regions
            assert pol.legal_zone == "GLOBAL" or r.legal_zone == pol.legal_zone


@SETTINGS
@given(scenarios(), st.data())
def test_ranking_inputs_cannot_make_an_ineligible_region_eligible(sc, data):
    """Changing latency, headroom, pressure or preferences never changes WHICH regions are eligible."""
    regs, links, pol = sc
    src = data.draw(st.sampled_from(sorted(regs)))
    base = {c.region for c in eligible_destinations(Ctx(regs, links, pol), src)[0]}
    regs2 = {
        k: v.model_copy(
            update={
                "compute_headroom": data.draw(st.floats(0, 1)),
                "storage_capacity": StorageCapacity(
                    total_bytes=v.storage_capacity.total_bytes, used_bytes=data.draw(st.integers(0, v.storage_capacity.total_bytes or 1))
                ),
            }
        )
        for k, v in regs.items()
    }
    links2 = {k: v.model_copy(update={"latency_ms": data.draw(st.integers(0, 10**6))}) for k, v in links.items()}
    pol2 = pol.model_copy(update={"preferred_regions": data.draw(st.lists(st.sampled_from(sorted(regs) + ["ghost"]), unique=True))})
    after = {c.region for c in eligible_destinations(Ctx(regs2, links2, pol2), src)[0]}
    # eligibility may only change through hard facts; capacity changes can matter only with required_bytes > 0 (0 here)
    assert base == after


@SETTINGS
@given(scenarios(), st.data())
def test_decision_matches_independent_oracle_for_storage(sc, data):
    regs, links, pol = sc
    rid = data.draw(st.sampled_from(sorted(regs) + ["ghost", "nowhere"]))
    d = evaluate_store(Ctx(regs, links, pol), rid)
    assert d.allowed == oracle_may_store(regs, pol, rid)
    assert d.policy_version == pol.policy_version
    if not d.allowed:
        assert d.reasons and all(r.code and r.message for r in d.reasons)


@SETTINGS
@given(scenarios(), st.data())
def test_deny_by_default_for_unregistered_regions(sc, data):
    regs, links, pol = sc
    c = Ctx(regs, links, pol)
    assert evaluate_store(c, "ghost").decision == DecisionKind.REJECTED
    assert evaluate_process(c, "ghost").decision == DecisionKind.REJECTED
    assert evaluate_promotion(c, "ghost", ReplicaStatus.IN_SYNC).decision == DecisionKind.REJECTED
    assert not evaluate_assignment(c, sorted(regs)[0], "ghost").allowed
    assert "ghost" not in {x.region for x in eligible_destinations(c, sorted(regs)[0])[0]}


@SETTINGS
@given(scenarios(), st.data())
def test_excluded_and_wrong_zone_regions_are_never_allowed(sc, data):
    regs, links, pol = sc
    c = Ctx(regs, links, pol)
    for rid, r in regs.items():
        if rid in pol.excluded_regions:
            assert not evaluate_store(c, rid).allowed and not evaluate_process(c, rid).allowed
            assert not evaluate_promotion(c, rid, ReplicaStatus.IN_SYNC).allowed
        if pol.legal_zone != "GLOBAL" and r.legal_zone != pol.legal_zone:
            assert not evaluate_store(c, rid).allowed and not evaluate_process(c, rid).allowed


@SETTINGS
@given(scenarios(), st.data())
def test_any_hop_outside_allowed_transit_zones_rejects_the_route(sc, data):
    regs, links, pol = sc
    assume(links)
    (a, b) = data.draw(st.sampled_from(sorted(links)))
    route = [a, b]
    d = evaluate_route(Ctx(regs, links, pol), route)
    if not set(links[(a, b)].transit_zones) <= set(pol.allowed_transit_zones):
        assert d.decision == DecisionKind.REJECTED
    if d.allowed:
        assert oracle_hop_ok(regs, links, pol, a, b)


@SETTINGS
@given(scenarios(), st.data())
def test_promotion_allowed_implies_storage_and_processing_allowed(sc, data):
    regs, links, pol = sc
    rid = data.draw(st.sampled_from(sorted(regs)))
    status = data.draw(st.sampled_from(list(ReplicaStatus) + [None]))
    d = evaluate_promotion(Ctx(regs, links, pol), rid, status)
    if d.allowed:
        assert rid in pol.allowed_storage_regions and rid in pol.allowed_processing_regions
        assert rid not in pol.excluded_regions and status in (ReplicaStatus.SYNCING, ReplicaStatus.IN_SYNC)


@SETTINGS
@given(scenarios(), st.data())
def test_narrowing_policy_never_creates_new_permissions(sc, data):
    """Monotonicity: removing a region from allowed_storage_regions, or adding an exclusion, cannot turn a
    rejection into an acceptance."""
    regs, links, pol = sc
    rid = data.draw(st.sampled_from(sorted(regs)))
    narrowed = pol.model_copy(update={"allowed_storage_regions": [r for r in pol.allowed_storage_regions if r != rid]})
    assert not (evaluate_store(Ctx(regs, links, narrowed), rid).allowed and not evaluate_store(Ctx(regs, links, pol), rid).allowed)
    assert not evaluate_store(Ctx(regs, links, narrowed), rid).allowed
    excl = pol.model_copy(update={"excluded_regions": sorted(set(pol.excluded_regions) | {rid})})
    assert not evaluate_store(Ctx(regs, links, excl), rid).allowed
    # fewer allowed transit zones can only shrink the eligible set
    fewer = pol.model_copy(update={"allowed_transit_zones": pol.allowed_transit_zones[:-1]})
    src = sorted(regs)[0]
    before = {c.region for c in eligible_destinations(Ctx(regs, links, pol), src)[0]}
    after = {c.region for c in eligible_destinations(Ctx(regs, links, fewer), src)[0]}
    assert after <= before


@SETTINGS
@given(scenarios(), st.data())
def test_decisions_do_not_depend_on_registry_ordering(sc, data):
    regs, links, pol = sc
    shuffled = dict(data.draw(st.permutations(list(regs.items()))))
    links_s = dict(data.draw(st.permutations(list(links.items()))))
    src = sorted(regs)[0]
    d1 = plan_placement(Ctx(regs, links, pol), src)
    d2 = plan_placement(Ctx(shuffled, links_s, pol), src)
    assert d1.decision == d2.decision
    assert [c["region"] for c in d1.details.get("chosen", [])] == [c["region"] for c in d2.details.get("chosen", [])]


@SETTINGS
@given(scenarios(), st.data())
def test_insufficient_iff_fewer_survivors_than_requested(sc, data):
    regs, links, pol = sc
    src = data.draw(st.sampled_from(sorted(regs)))
    c = Ctx(regs, links, pol)
    plan = plan_placement(c, src)
    if plan.decision == DecisionKind.REJECTED:  # unusable source: nothing is planned
        assert not plan.details.get("chosen")
        return
    survivors = eligible_destinations(c, src)[0]
    if len(survivors) < pol.regional_replica_count:
        assert plan.decision == DecisionKind.INSUFFICIENT_ELIGIBLE_REGIONS and plan.policy_version == pol.policy_version
    else:
        assert plan.decision == DecisionKind.ALLOWED and len(plan.details["chosen"]) == pol.regional_replica_count
        assert len({x["region"] for x in plan.details["chosen"]}) == pol.regional_replica_count
    assert src not in {x["region"] for x in plan.details.get("chosen", [])}


@SETTINGS
@given(scenarios(), st.data())
def test_every_rejection_has_a_reason_and_a_policy_version(sc, data):
    regs, links, pol = sc
    c = Ctx(regs, links, pol)
    rid = data.draw(st.sampled_from(sorted(regs) + ["ghost"]))
    for d in (
        evaluate_store(c, rid),
        evaluate_process(c, rid),
        evaluate_assignment(c, sorted(regs)[0], rid),
        evaluate_promotion(c, rid, ReplicaStatus.IN_SYNC),
        plan_placement(c, sorted(regs)[0]),
    ):
        if d.decision != DecisionKind.ALLOWED:
            assert d.reasons and d.policy_version == pol.policy_version
