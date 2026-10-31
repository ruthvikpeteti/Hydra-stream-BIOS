"""The twelve required scenarios, against the pure engine (no services)."""

import pytest

from fixtures import ctx, eu_policy, region, world
from ss_policy.engine import (
    evaluate_assignment,
    evaluate_backup,
    evaluate_failback,
    evaluate_process,
    evaluate_promotion,
    evaluate_route,
    evaluate_store,
    no_policy,
    plan_placement,
    select_recovery_destination,
    validate_policy,
)
from ss_policy.models import Availability, DecisionKind, Link, ReplicaStatus, TopicPolicyIn


def codes(d):
    return {r.code for r in d.reasons}


def test_01_eu_topic_to_eu_west_allowed():
    d = evaluate_assignment(ctx(), "eu-central", "eu-west")
    assert d.decision == DecisionKind.ALLOWED and d.policy_version == 1
    assert d.details["route"] == ["eu-central", "eu-west"]


def test_02_eu_topic_to_eu_central_allowed():
    assert evaluate_store(ctx(), "eu-central").allowed
    assert evaluate_assignment(ctx(), "eu-west", "eu-central").allowed


def test_03_eu_topic_to_us_east_rejected_with_reason_and_version():
    for d in (evaluate_store(ctx(version=7), "us-east"), evaluate_assignment(ctx(version=7), "eu-central", "us-east")):
        assert d.decision == DecisionKind.REJECTED and d.policy_version == 7
        assert {"LEGAL_ZONE_MISMATCH", "REGION_NOT_ALLOWED_STORAGE"} <= codes(d)
        assert all(r.message for r in d.reasons)


def test_04_unauthorized_transit_zone_rejected():
    regs, links = world()
    links[("eu-central", "eu-west")] = Link(
        from_region="eu-central", to_region="eu-west", transit_zones=["EU", "ATLANTIC"], latency_ms=5
    )  # leased line via the Atlantic
    c = ctx(regs_links=(regs, links))
    d = evaluate_route(c, ["eu-central", "eu-west"])
    assert d.decision == DecisionKind.REJECTED and "TRANSIT_ZONE_NOT_ALLOWED" in codes(d)
    # relaying through a prohibited region is rejected too, even though the endpoints are both legal
    d = evaluate_route(c, ["eu-central", "us-east", "eu-west"])
    assert d.decision == DecisionKind.REJECTED and {"RELAY_REGION_NOT_ALLOWED", "TRANSIT_ZONE_NOT_ALLOWED"} <= codes(d)
    # and no assignment can use the bad direct link (a legal detour via eu-north exists, so it must take that one)
    a = evaluate_assignment(c, "eu-central", "eu-west")
    assert a.allowed and a.details["route"] == ["eu-central", "eu-north", "eu-west"]
    # remove the detour: now the destination has no approved route at all
    for k in [k for k in links if "eu-north" in k or "eu-south" in k]:  # no legal detour left either
        del links[k]
    a = evaluate_assignment(ctx(regs_links=(regs, links)), "eu-central", "eu-west")
    assert a.decision == DecisionKind.REJECTED and "TRANSIT_ZONE_NOT_ALLOWED" in codes(a)


def test_05_healthy_but_prohibited_destination_rejected_regardless_of_quality():
    c = ctx()
    assert c.regions["us-east"].availability_status == Availability.AVAILABLE
    assert evaluate_assignment(c, "eu-central", "us-east").decision == DecisionKind.REJECTED
    plan = plan_placement(c, "eu-central", count=4)  # more than the 3 legal ones exist
    assert plan.decision == DecisionKind.INSUFFICIENT_ELIGIBLE_REGIONS
    assert "us-east" not in {x["region"] for x in plan.details["eligible"]}
    rejected = {x["region"]: x for x in plan.details["rejected"]}
    assert "us-east" in rejected and rejected["us-east"]["reasons"]
    # even if the policy PREFERS it, preference never grants permission
    p = eu_policy(preferred_regions=["eu-west"])
    plan = plan_placement(ctx(p), "eu-central", count=2)
    assert "us-east" not in {x["region"] for x in plan.details["chosen"]}


def test_06_slow_but_permitted_destination_stays_eligible():
    plan = plan_placement(ctx(), "eu-central", count=3)
    assert plan.decision == DecisionKind.ALLOWED
    assert "eu-south" in [x["region"] for x in plan.details["chosen"]]  # needed => used despite 900 ms
    one = plan_placement(ctx(), "eu-central", count=1)  # not needed => outranked, yet still eligible
    assert one.details["chosen"][0]["region"] == "eu-west"
    assert "eu-south" in [x["region"] for x in one.details["eligible"]]
    assert evaluate_assignment(ctx(), "eu-central", "eu-south").allowed
    # with equal failure domains, latency alone decides and the slow region ranks last
    regs, links = world(**{"eu-south": region("eu-south", "EU", "dc-2")})
    ranked = [x["region"] for x in plan_placement(ctx(regs_links=(regs, links)), "eu-central", count=3).details["eligible"]]
    assert ranked[-1] == "eu-south"


def test_07_unknown_region_rejected():
    c = ctx()
    for d in (
        evaluate_store(c, "mars-1"),
        evaluate_process(c, "mars-1"),
        evaluate_assignment(c, "eu-central", "mars-1"),
        evaluate_promotion(c, "mars-1", ReplicaStatus.IN_SYNC),
        evaluate_backup(c, "eu-central", "mars-1"),
        evaluate_route(c, ["eu-central", "mars-1"]),
    ):
        assert d.decision == DecisionKind.REJECTED and "UNKNOWN_REGION" in codes(d), d.operation
    assert "UNKNOWN_REGION" in {
        r.code
        for r in validate_policy(
            *world(), TopicPolicyIn(**{**eu_policy().model_dump(exclude={"policy_version"}), "allowed_storage_regions": ["eu-central", "mars-1"]})
        )
    }


def test_08_policy_not_available_fails_closed():
    d = no_policy("STORE", "orders.eu")
    assert d.decision == DecisionKind.POLICY_UNAVAILABLE and not d.allowed and d.reasons[0].code == "POLICY_NOT_FOUND"


def test_10_promotion_into_prohibited_region_rejected():
    c = ctx()
    d = evaluate_promotion(c, "us-east", ReplicaStatus.IN_SYNC)  # even a perfectly synced replica there is illegal
    assert d.decision == DecisionKind.REJECTED and "REGION_NOT_ALLOWED_PROCESSING" in codes(d)
    assert evaluate_promotion(c, "eu-west", ReplicaStatus.IN_SYNC).allowed
    assert "NO_REPLICA" in codes(evaluate_promotion(c, "eu-west", None))
    assert "REPLICA_NOT_USABLE" in codes(evaluate_promotion(c, "eu-west", ReplicaStatus.PROVISIONED))
    # storage-allowed but not processing-allowed: also refused (promotion makes it process the topic)
    p = eu_policy(allowed_processing_regions=["eu-central"])
    assert "REGION_NOT_ALLOWED_PROCESSING" in codes(evaluate_promotion(ctx(p), "eu-west", ReplicaStatus.IN_SYNC))


def test_11_backup_outside_permitted_set_rejected():
    c = ctx()
    d = evaluate_backup(c, "eu-central", "us-east")
    assert d.decision == DecisionKind.REJECTED and "REGION_NOT_ALLOWED_STORAGE" in codes(d)
    assert evaluate_backup(c, "eu-central", "eu-south").allowed


def test_12_processing_from_unauthorized_region_rejected():
    c = ctx()
    d = evaluate_process(c, "us-east")
    assert d.decision == DecisionKind.REJECTED and "REGION_NOT_ALLOWED_PROCESSING" in codes(d) and d.policy_version == 1
    assert evaluate_process(c, "eu-west").allowed
    p = eu_policy(allowed_processing_regions=["eu-central"])  # stores in eu-west but may not process there
    assert not evaluate_process(ctx(p), "eu-west").allowed


# ---- extra behaviour the design promises ------------------------------------------------------------------
def test_excluded_region_is_vetoed_even_if_allowed_and_preferred():
    p = eu_policy(preferred_regions=["eu-west"], excluded_regions=["eu-west"])
    d = evaluate_store(ctx(p), "eu-west")
    assert "REGION_EXCLUDED" in codes(d)
    assert "eu-west" not in {x["region"] for x in plan_placement(ctx(p), "eu-central", count=2).details["chosen"]}


def test_technical_constraints_eliminate_but_are_labelled_technical():
    regs, links = world(**{"eu-west": region("eu-west", "EU", "dc-2", avail=Availability.UNAVAILABLE)})
    d = evaluate_assignment(ctx(regs_links=(regs, links)), "eu-central", "eu-west")
    assert d.decision == DecisionKind.REJECTED and {r.category for r in d.reasons} == {"TECHNICAL"}
    regs, links = world(**{"eu-west": region("eu-west", "EU", "dc-2", max_rf=1)})
    assert "RF_EXCEEDS_REGION_LIMIT" in codes(evaluate_store(ctx(regs_links=(regs, links)), "eu-west"))
    regs, links = world(**{"eu-west": region("eu-west", "EU", "dc-2", used=999 * 10**9)})
    assert "INSUFFICIENT_CAPACITY" in codes(evaluate_store(ctx(regs_links=(regs, links)), "eu-west", required_bytes=10**11))


def test_ranking_prefers_failure_domain_independence_and_preference():
    plan = plan_placement(ctx(eu_policy(regional_replica_count=2)), "eu-central", count=2)
    chosen = [x["region"] for x in plan.details["chosen"]]
    assert not ({"eu-west", "eu-north"} <= set(chosen)), "two replicas in one failure domain when others exist"
    p = eu_policy(regional_replica_count=1, preferred_regions=["eu-north"])  # a hint that beats a near-tie ...
    assert plan_placement(ctx(p), "eu-central", count=1).details["chosen"][0]["region"] == "eu-north"
    p = eu_policy(regional_replica_count=1, preferred_regions=["eu-south"])  # ... but not a 900 ms penalty
    assert plan_placement(ctx(p), "eu-central", count=1).details["chosen"][0]["region"] == "eu-west"


def test_recovery_destination_never_picks_failed_or_illegal_regions():
    d = select_recovery_destination(ctx(), "eu-central", failed={"eu-west"}, existing={"eu-north"}, count=1)
    assert d.allowed and d.details["chosen"][0]["region"] == "eu-south"
    d = select_recovery_destination(ctx(), "eu-central", failed={"eu-west", "eu-south"}, existing={"eu-north"}, count=1)
    assert d.decision == DecisionKind.INSUFFICIENT_ELIGIBLE_REGIONS  # us-east is healthy but illegal


def test_failback_uses_current_policy_and_requires_healthy_original():
    assert evaluate_failback(ctx(), "eu-central", "eu-west").allowed
    p = eu_policy(
        allowed_storage_regions=["eu-west", "eu-north", "eu-south"], allowed_processing_regions=["eu-west", "eu-north", "eu-south"], home_region="eu-west"
    )  # policy tightened since: eu-central no longer allowed
    d = evaluate_failback(ctx(p), "eu-central", "eu-west")
    assert d.decision == DecisionKind.REJECTED and "REGION_NOT_ALLOWED_STORAGE" in codes(d)
    regs, links = world(**{"eu-central": region("eu-central", "EU", "dc-1", avail=Availability.DEGRADED)})
    assert "REGION_UNAVAILABLE" in codes(evaluate_failback(ctx(regs_links=(regs, links)), "eu-central", "eu-west"))
    # failing back OUT of a region that has become illegal is allowed to proceed (judged on the destination)
    p2 = eu_policy(allowed_storage_regions=["eu-central", "eu-north"], allowed_processing_regions=["eu-central", "eu-north"])
    assert evaluate_failback(ctx(p2), "eu-central", "eu-west").allowed


def test_assignment_from_stale_policy_version_rejected():
    d = evaluate_assignment(ctx(version=3), "eu-central", "eu-west", assignment_policy_version=2)
    assert "STALE_POLICY_VERSION" in codes(d) and d.policy_version == 3


def test_policy_validation_rejects_unsafe_policies():
    regs, links = world()
    base = eu_policy().model_dump(exclude={"policy_version"})

    def bad(**kw):
        return {r.code for r in validate_policy(regs, links, TopicPolicyIn(**{**base, **kw}))}

    assert "LEGAL_ZONE_MISMATCH" in bad(allowed_storage_regions=["eu-central", "us-east"])
    assert "HOME_REGION_NOT_ALLOWED" in bad(home_region="eu-west", allowed_storage_regions=["eu-central"])
    assert "PREFERRED_NOT_ALLOWED" in bad(preferred_regions=["us-east"])
    assert "HOME_REGION_EXCLUDED" in bad(excluded_regions=["eu-central"])
    assert validate_policy(regs, links, TopicPolicyIn(**base)) == []
    with pytest.raises(ValueError):
        TopicPolicyIn(**{**base, "durability_mode": "LOCAL_DURABLE"})  # shape rule: LOCAL needs 0 replicas
