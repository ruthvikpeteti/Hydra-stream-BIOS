"""Pure unit tests (no Kafka, no Docker): lease source rules and policy validation."""

import asyncio
import json

import pytest

from app.errors import ApiError
from app.leases import DevFileLeaseSource
from app.registry import Registry, apply_defaults, validate_policy

REG = Registry.load(str(__import__("pathlib").Path(__file__).resolve().parents[1] / "config" / "registry.json"))


class Clock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


def write(p, default_epoch=1, region="region-a", topics=None):
    p.write_text(
        json.dumps({"default": {"active_region": region, "leadership_epoch": default_epoch, "replication_generation": default_epoch}, "topics": topics or {}})
    )


def make(tmp_path, clock):
    f = tmp_path / "s.json"
    write(f)
    return f, DevFileLeaseSource(str(f), "region-a", 10_000, 3_000, clock=clock)


def test_lease_expires_without_renewal_and_recovers(tmp_path):
    clock = Clock()
    f, src = make(tmp_path, clock)
    assert asyncio.run(src.renew_once())
    assert src.resolve("t").lease_valid
    clock.t += 9.9
    assert src.resolve("t").lease_valid
    clock.t += 0.2  # TTL (10s) elapsed since the last successful renewal
    assert not src.resolve("t").lease_valid
    assert asyncio.run(src.renew_once())
    assert src.resolve("t").lease_valid


def test_failed_read_does_not_renew(tmp_path):
    clock = Clock()
    f, src = make(tmp_path, clock)
    asyncio.run(src.renew_once())
    f.write_text("{not json")
    clock.t += 5
    assert not asyncio.run(src.renew_once())
    clock.t += 5.1
    assert not src.resolve("t").lease_valid


def test_epoch_never_regresses(tmp_path):
    clock = Clock()
    f, src = make(tmp_path, clock)
    write(f, default_epoch=3)
    asyncio.run(src.renew_once())
    assert src.resolve("t").leadership_epoch == 3
    write(f, default_epoch=2)  # attempt to go back
    assert not asyncio.run(src.renew_once())
    assert src.resolve("t").leadership_epoch == 3


def test_removed_override_is_not_accepted(tmp_path):
    clock = Clock()
    f, src = make(tmp_path, clock)
    write(f, topics={"t": {"active_region": "region-a", "leadership_epoch": 2, "replication_generation": 2}})
    assert asyncio.run(src.renew_once())
    write(f)  # override dropped: would silently lower t's epoch
    assert not asyncio.run(src.renew_once())
    assert src.resolve("t").leadership_epoch == 2


def test_passive_region_never_holds_lease(tmp_path):
    clock = Clock()
    f, src = make(tmp_path, clock)
    write(f, region="region-b")
    asyncio.run(src.renew_once())
    st = src.resolve("t")
    assert st.role == "passive" and not st.lease_valid and st.active_region == "region-b"


def policy(**kw):
    return apply_defaults(kw, REG, "region-a")


MVP_RF = dict(local_replication_factor=1, local_min_insync_replicas=1)  # B/C capacity is RF 1 in the MVP profile


def test_regional_topic_with_rf3_exceeds_mvp_replica_capacity():
    with pytest.raises(ApiError) as e:
        validate_policy(
            policy(
                allowed_storage_regions=["region-a", "region-b"],
                allowed_processing_regions=["region-a", "region-b"],
                failover_priority=["region-a", "region-b"],
                regional_replica_count=1,
                durability_mode="ASYNC_REGIONAL",
                allowed_transit_zones=["EU"],
            ),
            REG,
            "region-a",
        )
    assert e.value.details["rule"] == "V5"


def test_default_policy_is_valid_local_durable_rf3():
    p = policy()
    validate_policy(p, REG, "region-a")
    assert (p["local_replication_factor"], p["local_min_insync_replicas"], p["durability_mode"]) == (3, 2, "LOCAL_DURABLE")


@pytest.mark.parametrize(
    "kw,rule",
    [
        (
            dict(
                allowed_storage_regions=["region-a", "region-c"],
                allowed_processing_regions=["region-a", "region-c"],
                failover_priority=["region-a", "region-c"],
                regional_replica_count=1,
                durability_mode="ASYNC_REGIONAL",
                allowed_transit_zones=["EU", "ATLANTIC", "US"],
            ),
            "V2",
        ),  # AS-1: C is not in zone EU
        (
            dict(
                allowed_storage_regions=["region-a", "region-b"],
                allowed_processing_regions=["region-a", "region-b"],
                failover_priority=["region-a", "region-b"],
                regional_replica_count=1,
                durability_mode="ASYNC_REGIONAL",
                allowed_transit_zones=[],
                **MVP_RF,
            ),
            "V6",
        ),  # no approved path (transit zone)
        (
            dict(
                allowed_storage_regions=["region-a", "region-b"],
                allowed_processing_regions=["region-a"],
                failover_priority=["region-a", "region-b"],
                regional_replica_count=1,
                durability_mode="ASYNC_REGIONAL",
                allowed_transit_zones=["EU"],
            ),
            "V4",
        ),
        (dict(durability_mode="LOCAL_DURABLE", regional_replica_count=1), "V3"),
        (dict(local_replication_factor=2, local_min_insync_replicas=3), "V9"),
        (dict(classification="RESTRICTED", lag_breach_action="CONTINUE_ALERT"), "V10"),
    ],
)
def test_policy_rules_reject(kw, rule):
    with pytest.raises(ApiError) as e:
        validate_policy(policy(**kw), REG, "region-a")
    assert e.value.code == "POLICY_INVALID" and e.value.details["rule"] == rule


def test_valid_regional_policy_accepted():
    validate_policy(
        policy(
            allowed_storage_regions=["region-a", "region-b"],
            allowed_processing_regions=["region-a", "region-b"],
            failover_priority=["region-a", "region-b"],
            regional_replica_count=1,
            durability_mode="ASYNC_REGIONAL",
            allowed_transit_zones=["EU"],
            **MVP_RF,
        ),
        REG,
        "region-a",
    )


def test_topic_must_be_created_in_home_region():
    with pytest.raises(ApiError) as e:
        validate_policy(
            policy(
                allowed_storage_regions=["region-b", "region-a"],
                allowed_processing_regions=["region-b", "region-a"],
                failover_priority=["region-b", "region-a"],
                regional_replica_count=1,
                durability_mode="ASYNC_REGIONAL",
                allowed_transit_zones=["EU"],
                **MVP_RF,
            ),
            REG,
            "region-a",
        )
    assert e.value.code == "POLICY_VIOLATION"
