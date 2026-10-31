"""Shared builders for pure tests: a small EU/US world."""

from ss_policy.engine import Ctx
from ss_policy.models import Availability, Link, Region, StorageCapacity, TopicPolicy

GB = 1_000_000_000


def region(
    rid,
    zone,
    fd,
    *,
    avail=Availability.AVAILABLE,
    used=100 * GB,
    total=1000 * GB,
    headroom=0.5,
    max_rf=3,
    caps=("gateway", "replication"),
    zones=("EU", "ATLANTIC", "US"),
) -> Region:
    return Region(
        region_id=rid,
        legal_zone=zone,
        availability_status=avail,
        storage_capacity=StorageCapacity(total_bytes=total, used_bytes=used),
        processing_capabilities=list(caps),
        supported_transit_zones=list(zones),
        failure_domain=fd,
        compute_headroom=headroom,
        max_replication_factor=max_rf,
    )


def world(**overrides):
    regs = {
        "eu-central": region("eu-central", "EU", "dc-1"),
        "eu-west": region("eu-west", "EU", "dc-2"),
        "eu-north": region("eu-north", "EU", "dc-2"),  # same failure domain as eu-west
        "eu-south": region("eu-south", "EU", "dc-3"),  # permitted but slow
        "us-east": region("us-east", "US", "dc-4", headroom=1.0, used=0),  # healthy, huge, prohibited for EU data
    }
    regs.update(overrides)
    lat = {
        ("eu-central", "eu-west"): 20,
        ("eu-central", "eu-north"): 35,
        ("eu-west", "eu-north"): 15,
        ("eu-central", "eu-south"): 900,
        ("eu-west", "eu-south"): 900,
        ("eu-north", "eu-south"): 900,
    }
    links = {}
    for (a, b), ms in lat.items():
        for x, y in ((a, b), (b, a)):
            links[(x, y)] = Link(from_region=x, to_region=y, transit_zones=["EU"], latency_ms=ms)
    for eu in ("eu-central", "eu-west", "eu-north", "eu-south"):
        for x, y in ((eu, "us-east"), ("us-east", eu)):
            links[(x, y)] = Link(from_region=x, to_region=y, transit_zones=["EU", "ATLANTIC", "US"], latency_ms=1)
    return regs, links


def eu_policy(version=1, **kw) -> TopicPolicy:
    base = dict(
        topic="orders.eu",
        classification="INTERNAL",
        legal_zone="EU",
        home_region="eu-central",
        allowed_storage_regions=["eu-central", "eu-west", "eu-north", "eu-south"],
        allowed_processing_regions=["eu-central", "eu-west", "eu-north", "eu-south"],
        allowed_transit_zones=["EU"],
        replication_factor=3,
        regional_replica_count=2,
        maximum_replication_lag_ms=30000,
        durability_mode="ASYNC_REGIONAL",
        policy_version=version,
    )
    base.update(kw)
    return TopicPolicy(**base)


def ctx(policy=None, regs_links=None, **policy_kw) -> Ctx:
    regs, links = regs_links or world()
    return Ctx(regs, links, policy or eu_policy(**policy_kw))
