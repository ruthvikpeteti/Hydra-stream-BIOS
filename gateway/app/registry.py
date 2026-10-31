"""Region/link registry (DATA_MODEL §1) and topic policy validation (DATA_MODEL §2.3).

Pure functions: no Kafka, no I/O after load. The control plane will own this later; the rules are
the same ones, so behaviour will not change when it does.
"""

import json
from dataclasses import dataclass

from .errors import ApiError

CLASSIFICATIONS = ("PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED")
DURABILITY = ("LOCAL_DURABLE", "ASYNC_REGIONAL", "SYNC_REGIONAL")
LAG_ACTIONS = ("REJECT_WRITES", "CONTINUE_ALERT")


@dataclass(frozen=True)
class Registry:
    regions: dict[str, dict]
    links: dict[tuple[str, str], frozenset[str]]

    @classmethod
    def load(cls, path: str) -> "Registry":
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        return cls({r["id"]: r for r in raw["regions"]}, {(l["from"], l["to"]): frozenset(l["transit_zones"]) for l in raw["links"]})

    def path_approved(self, src: str, dst: str, transit: set[str], processing: set[str]) -> bool:
        """INV-2: direct link or relay via regions in `processing`, every link's zones within `transit`."""
        seen, frontier = {src}, [src]
        while frontier:
            cur = frontier.pop()
            for (a, b), zones in self.links.items():
                if a != cur or b in seen or not zones <= transit:
                    continue
                if b == dst:
                    return True
                if b in processing:
                    seen.add(b)
                    frontier.append(b)
        return False


def apply_defaults(p: dict, reg: Registry, region: str) -> dict:
    """Fill unset policy fields for a topic created through this gateway's region."""
    p = {k: v for k, v in p.items() if v is not None}
    me = reg.regions[region]
    p.setdefault("classification", "INTERNAL")
    p.setdefault("legal_zone", me["legal_zones"][0])
    p.setdefault("allowed_storage_regions", [region])
    p.setdefault("allowed_processing_regions", [region])
    p.setdefault("allowed_transit_zones", [])
    p.setdefault("durability_mode", "LOCAL_DURABLE")
    p.setdefault("regional_replica_count", 0)
    p.setdefault("local_replication_factor", min(3, me["capacity"]["max_local_rf"]))
    if "failover_priority" not in p:
        others = [r for r in p["allowed_storage_regions"] if r != region and r in p["allowed_processing_regions"]]
        p["failover_priority"] = [region] + others[: p["regional_replica_count"]]
    if p["durability_mode"] != "LOCAL_DURABLE":
        p.setdefault("max_tolerated_replication_lag_ms", 30_000)
    else:
        p.setdefault("max_tolerated_replication_lag_ms", None)
    if p["durability_mode"] == "SYNC_REGIONAL":
        p.setdefault("sync_remote_acks", 1)
    else:
        p.setdefault("sync_remote_acks", None)
    p.setdefault("local_min_insync_replicas", p["local_replication_factor"] // 2 + 1)
    p.setdefault("lag_breach_action", "REJECT_WRITES" if p["classification"] in ("CONFIDENTIAL", "RESTRICTED") else "CONTINUE_ALERT")
    return p


def _bad(rule: str, msg: str, **details) -> ApiError:
    return ApiError(422, "POLICY_INVALID", msg, details={"rule": rule, **details})


def validate_policy(p: dict, reg: Registry, region: str) -> None:
    storage, proc = set(p["allowed_storage_regions"]), set(p["allowed_processing_regions"])
    fo = p["failover_priority"]
    for r in storage | proc | set(fo):  # V1
        if r not in reg.regions:
            raise _bad("V1", f"unknown region {r}", region=r)
    if p["classification"] not in CLASSIFICATIONS:  # V10
        raise _bad("V10", "invalid classification")
    if p["durability_mode"] not in DURABILITY:
        raise _bad("V7", "invalid durability_mode")
    if p["lag_breach_action"] not in LAG_ACTIONS:
        raise _bad("V10", "invalid lag_breach_action")
    for r in sorted(storage | proc):  # V2
        if p["legal_zone"] not in reg.regions[r]["legal_zones"]:
            raise _bad("V2", f"region {r} is not in legal zone {p['legal_zone']}", region=r)
    if len(set(fo)) != len(fo) or len(fo) != 1 + p["regional_replica_count"]:  # V3
        raise _bad("V3", "failover_priority must be unique with length 1 + regional_replica_count")
    for r in fo:  # V4
        if r not in storage or r not in proc:
            raise _bad("V4", f"failover region {r} must be in both allowed storage and processing regions", region=r)
    for r in fo:  # V5
        if p["local_replication_factor"] > reg.regions[r]["capacity"]["max_local_rf"]:
            raise _bad("V5", f"local_replication_factor exceeds capacity of {r}", region=r)
    transit = set(p["allowed_transit_zones"])
    for r in fo[1:]:  # V6
        if not reg.path_approved(fo[0], r, transit, proc):
            raise _bad("V6", f"no approved replication path {fo[0]} -> {r}", region=r)
    mode, cnt = p["durability_mode"], p["regional_replica_count"]  # V7
    if mode == "LOCAL_DURABLE" and (cnt != 0 or p["max_tolerated_replication_lag_ms"] is not None):
        raise _bad("V7", "LOCAL_DURABLE requires regional_replica_count=0 and no max lag")
    if mode != "LOCAL_DURABLE" and (cnt < 1 or p["max_tolerated_replication_lag_ms"] is None):
        raise _bad("V7", f"{mode} requires regional_replica_count>=1 and max_tolerated_replication_lag_ms")
    sra = p["sync_remote_acks"]  # V8
    if (sra is not None) != (mode == "SYNC_REGIONAL") or (sra is not None and not 1 <= sra <= cnt):
        raise _bad("V8", "sync_remote_acks is required with SYNC_REGIONAL (1..regional_replica_count) and invalid otherwise")
    if not 1 <= p["local_min_insync_replicas"] <= p["local_replication_factor"]:  # V9
        raise _bad("V9", "local_min_insync_replicas must be within 1..local_replication_factor")
    if p["classification"] == "RESTRICTED" and p["lag_breach_action"] != "REJECT_WRITES":  # V10
        raise _bad("V10", "RESTRICTED topics require lag_breach_action=REJECT_WRITES")
    # Gateway-local rules (Phase 2): the topic is created in its home region only.
    if region not in storage or region not in proc:
        raise ApiError(403, "POLICY_VIOLATION", f"{region} is not an allowed storage/processing region for this topic")
    if fo[0] != region:
        raise ApiError(403, "POLICY_VIOLATION", "a topic must be created through the gateway of its home region (failover_priority[0])")
