"""Deny-by-default policy evaluation and placement.

Everything here is a pure function of (regions, links, policy). Order of work:
  1. HARD constraints (legal, then technical) eliminate destinations. Nothing eliminated can come back.
  2. Only the survivors are ranked. Ranking can reorder eligible regions; it can never add one.
A missing region, link, policy field or capability means "no".
"""

from dataclasses import dataclass, field
from typing import Iterable, Optional

from .models import (
    NetworkPath,
    GLOBAL_ZONE,
    USABLE_FOR_PROMOTION,
    Availability,
    Decision,
    DecisionKind,
    Link,
    Reason,
    Region,
    ReplicaStatus,
    TopicPolicy,
    TopicPolicyIn,
)

MAX_ROUTE_HOPS = 3
UNHEALTHY_FOR_NEW_DATA = {Availability.UNAVAILABLE, Availability.DRAINING}


@dataclass(frozen=True)
class Ctx:
    regions: dict[str, Region]
    links: dict[tuple[str, str], Link]
    policy: Optional[TopicPolicy]


@dataclass(frozen=True)
class RankingWeights:
    # Failure-domain independence weighs most: replicas that fail together are not resilience.
    latency: float = 0.25
    capacity: float = 0.10
    storage_pressure: float = 0.15
    failure_domain: float = 0.35
    preference: float = 0.15


def R(code: str, message: str, category: str = "LEGAL", region: Optional[str] = None) -> Reason:
    return Reason(code=code, message=message, category=category, region=region)


def _decide(
    op: str,
    ctx: Ctx,
    reasons: list[Reason],
    details: Optional[dict] = None,
    ok_kind: DecisionKind = DecisionKind.ALLOWED,
    bad_kind: DecisionKind = DecisionKind.REJECTED,
) -> Decision:
    reasons = _dedupe(reasons)
    return Decision(
        decision=ok_kind if not reasons else bad_kind,
        operation=op,
        topic=ctx.policy.topic if ctx.policy else None,
        policy_version=ctx.policy.policy_version if ctx.policy else None,
        reasons=reasons,
        details=details or {},
    )


def no_policy(op: str, topic: Optional[str]) -> Decision:
    return Decision(
        decision=DecisionKind.POLICY_UNAVAILABLE,
        operation=op,
        topic=topic,
        policy_version=None,
        reasons=[R("POLICY_NOT_FOUND", "no policy is available for this topic; denying", "SYSTEM")],
    )


# ---- hard constraints: legal ----------------------------------------------------------------------------
def legal_storage(ctx: Ctx, rid: str) -> list[Reason]:
    p, reg = ctx.policy, ctx.regions.get(rid)
    if reg is None:
        return [R("UNKNOWN_REGION", f"region {rid!r} is not registered", "LEGAL", rid)]
    out = []
    if rid in p.excluded_regions:
        out.append(R("REGION_EXCLUDED", f"{rid} is excluded by the topic policy", "LEGAL", rid))
    if p.legal_zone != GLOBAL_ZONE and reg.legal_zone != p.legal_zone:
        out.append(R("LEGAL_ZONE_MISMATCH", f"{rid} is in legal zone {reg.legal_zone}, topic is bound to {p.legal_zone}", "LEGAL", rid))
    if rid not in p.allowed_storage_regions:
        out.append(R("REGION_NOT_ALLOWED_STORAGE", f"{rid} is not in allowed_storage_regions", "LEGAL", rid))
    return out


def legal_processing(ctx: Ctx, rid: str) -> list[Reason]:
    p, reg = ctx.policy, ctx.regions.get(rid)
    if reg is None:
        return [R("UNKNOWN_REGION", f"region {rid!r} is not registered", "LEGAL", rid)]
    out = []
    if rid in p.excluded_regions:
        out.append(R("REGION_EXCLUDED", f"{rid} is excluded by the topic policy", "LEGAL", rid))
    if p.legal_zone != GLOBAL_ZONE and reg.legal_zone != p.legal_zone:
        out.append(R("LEGAL_ZONE_MISMATCH", f"{rid} is in legal zone {reg.legal_zone}, topic is bound to {p.legal_zone}", "LEGAL", rid))
    if rid not in p.allowed_processing_regions:
        out.append(R("REGION_NOT_ALLOWED_PROCESSING", f"{rid} is not in allowed_processing_regions", "LEGAL", rid))
    return out


# ---- hard constraints: technical -------------------------------------------------------------------------
def technical_storage(ctx: Ctx, rid: str, required_bytes: int = 0) -> list[Reason]:
    reg = ctx.regions.get(rid)
    if reg is None:
        return []  # already reported as UNKNOWN_REGION by the legal check
    out = []
    if reg.availability_status in UNHEALTHY_FOR_NEW_DATA:
        out.append(R("REGION_UNAVAILABLE", f"{rid} is {reg.availability_status.value}", "TECHNICAL", rid))
    if reg.storage_capacity.free_bytes < required_bytes:
        out.append(R("INSUFFICIENT_CAPACITY", f"{rid} has {reg.storage_capacity.free_bytes} free bytes, needs {required_bytes}", "TECHNICAL", rid))
    if ctx.policy.replication_factor > reg.max_replication_factor:
        out.append(
            R(
                "RF_EXCEEDS_REGION_LIMIT",
                f"replication_factor {ctx.policy.replication_factor} exceeds {rid}'s limit {reg.max_replication_factor}",
                "TECHNICAL",
                rid,
            )
        )
    return out


# ---- routes ----------------------------------------------------------------------------------------------
def _hop_legal(ctx: Ctx, a: str, b: str) -> list[Reason]:
    link = ctx.links.get((a, b))
    if link is None:
        return [R("NO_LINK", f"no registered link {a} -> {b}", "LEGAL")]
    bad = sorted(set(link.transit_zones) - set(ctx.policy.allowed_transit_zones))
    if bad:
        return [R("TRANSIT_ZONE_NOT_ALLOWED", f"link {a} -> {b} crosses zone(s) {bad} not in allowed_transit_zones", "LEGAL")]
    return []


def _hop_technical(ctx: Ctx, a: str, b: str) -> list[Reason]:
    link, out = ctx.links.get((a, b)), []
    if link is None:
        return out
    if link.status != "UP":
        out.append(R("LINK_DOWN", f"link {a} -> {b} is {link.status}", "TECHNICAL"))
    ra, rb = ctx.regions.get(a), ctx.regions.get(b)
    if ra and rb:
        unsupported = sorted(set(link.transit_zones) - (set(ra.supported_transit_zones) & set(rb.supported_transit_zones)))
        if unsupported:
            out.append(R("TRANSIT_ZONE_UNSUPPORTED", f"{a} or {b} does not support transit zone(s) {unsupported}", "TECHNICAL"))
    return out


def _relay_ok(ctx: Ctx, rid: str) -> list[Reason]:
    """A relay stores/handles the data in flight, so it must be legal to process it."""
    out = [r.model_copy(update={"code": "RELAY_REGION_NOT_ALLOWED"}) for r in legal_processing(ctx, rid)]
    reg = ctx.regions.get(rid)
    if reg and reg.availability_status in UNHEALTHY_FOR_NEW_DATA:
        out.append(R("REGION_UNAVAILABLE", f"relay {rid} is {reg.availability_status.value}", "TECHNICAL", rid))
    return out


def evaluate_route(ctx: Ctx, route: list[str]) -> Decision:
    reasons: list[Reason] = []
    if len(route) < 2:
        reasons.append(R("ROUTE_TOO_SHORT", "a route needs at least a source and a destination", "LEGAL"))
    if len(set(route)) != len(route):
        reasons.append(R("ROUTE_HAS_LOOP", "route visits a region twice", "LEGAL"))
    for rid in route:
        if rid not in ctx.regions:
            reasons.append(R("UNKNOWN_REGION", f"region {rid!r} is not registered", "LEGAL", rid))
    for a, b in zip(route, route[1:]):
        reasons += _hop_legal(ctx, a, b) + _hop_technical(ctx, a, b)
    for rid in route[1:-1]:
        reasons += _relay_ok(ctx, rid)
    return _decide("ROUTE", ctx, reasons, {"route": route})


def _paths(ctx: Ctx, src: str, dst: str, hop_ok, relay_ok) -> list[tuple[list[str], int]]:
    """All simple paths src->dst (<= MAX_ROUTE_HOPS hops) whose hops and relays pass the given predicates."""
    found: list[tuple[list[str], int]] = []

    def walk(path: list[str], lat: int) -> None:
        cur = path[-1]
        if len(path) - 1 >= MAX_ROUTE_HOPS:
            return
        for (a, b), link in ctx.links.items():
            if a != cur or b in path or not hop_ok(a, b):
                continue
            if b == dst:
                found.append((path + [b], lat + link.latency_ms))
            elif relay_ok(b):
                walk(path + [b], lat + link.latency_ms)

    walk([src], 0)
    return sorted(found, key=lambda t: (t[1], len(t[0]), t[0]))


def find_route(ctx: Ctx, src: str, dst: str) -> tuple[Optional[tuple[list[str], int]], list[Reason]]:
    """Best (lowest latency) route that is legal AND technically available. Reasons explain a failure."""
    legal_hop = lambda a, b: not _hop_legal(ctx, a, b)  # noqa: E731
    legal_relay = lambda r: r in ctx.regions and not legal_processing(ctx, r)  # noqa: E731
    if not _paths(ctx, src, dst, legal_hop, legal_relay):
        why = _hop_legal(ctx, src, dst) or [
            R("NO_APPROVED_ROUTE", f"no route {src} -> {dst} stays inside the allowed transit zones and processing regions", "LEGAL")
        ]
        return None, why
    ok_hop = lambda a, b: legal_hop(a, b) and not _hop_technical(ctx, a, b)  # noqa: E731
    ok_relay = lambda r: legal_relay(r) and not _relay_ok(ctx, r)  # noqa: E731
    paths = _paths(ctx, src, dst, ok_hop, ok_relay)
    if not paths:
        return None, [R("NO_AVAILABLE_ROUTE", f"approved routes {src} -> {dst} exist but none is currently usable", "TECHNICAL")]
    return paths[0], []


# ---- network paths -------------------------------------------------------------------------------------------
SECURE_CLASSES = ("CONFIDENTIAL", "RESTRICTED")


def evaluate_path(ctx: Ctx, path: NetworkPath) -> Decision:
    """May this egress path carry this topic? Legal and security rules only (health is the router's job). A path's
    performance can never change this answer."""
    p, reasons = ctx.policy, []
    if not path.enabled:
        reasons.append(R("PATH_DISABLED", f"path {path.path_id} is disabled", "TECHNICAL"))
    bad = sorted(set(path.transit_zones) - set(p.allowed_transit_zones))
    if bad:
        reasons.append(R("PATH_TRANSIT_ZONE_NOT_ALLOWED", f"path {path.path_id} crosses zone(s) {bad} not in allowed_transit_zones", "LEGAL"))
    for rid in path.via_regions:
        if rid in (path.source_region, path.target_region):
            reasons.append(R("PATH_LOOP", f"relay {rid} is an endpoint of the path", "LEGAL", rid))
        reasons += _relay_ok(ctx, rid)
    if p.classification.value in SECURE_CLASSES and not path.encrypted:
        reasons.append(R("PATH_NOT_ENCRYPTED", f"{p.classification.value} topics require an encrypted path; {path.path_id} is not", "SECURITY"))
    return _decide("PATH", ctx, reasons, {"path_id": path.path_id})


def eligible_paths(ctx: Ctx, paths: list[NetworkPath]) -> tuple[list[NetworkPath], dict[str, list[Reason]]]:
    ok, rejected = [], {}
    for path in sorted(paths, key=lambda x: x.path_id):
        d = evaluate_path(ctx, path)
        if d.allowed:
            ok.append(path)
        else:
            rejected[path.path_id] = d.reasons
    return ok, rejected


# ---- single-subject operations ---------------------------------------------------------------------------
def evaluate_store(ctx: Ctx, rid: str, required_bytes: int = 0) -> Decision:
    return _decide("STORE", ctx, legal_storage(ctx, rid) + technical_storage(ctx, rid, required_bytes), {"region": rid})


def evaluate_process(ctx: Ctx, rid: str, capability: str = "gateway") -> Decision:
    reasons = legal_processing(ctx, rid)
    reg = ctx.regions.get(rid)
    if reg is not None:
        if reg.availability_status == Availability.UNAVAILABLE:
            reasons.append(R("REGION_UNAVAILABLE", f"{rid} is UNAVAILABLE", "TECHNICAL", rid))
        if capability not in reg.processing_capabilities:
            reasons.append(R("MISSING_CAPABILITY", f"{rid} lacks processing capability {capability!r}", "TECHNICAL", rid))
    return _decide("PROCESS", ctx, reasons, {"region": rid, "capability": capability})


def _pair_reasons(
    ctx: Ctx, op: str, source: str, dest: str, route: Optional[list[str]], required_bytes: int, check_source: bool = True
) -> tuple[list[Reason], dict]:
    reasons: list[Reason] = []
    if source == dest:
        reasons.append(R("SAME_REGION", "source and destination are the same region", "LEGAL", dest))
    src_legal = legal_storage(ctx, source) if check_source else []
    reasons += [r.model_copy(update={"code": "SOURCE_" + r.code if r.code != "UNKNOWN_REGION" else r.code}) for r in src_legal]
    reasons += legal_storage(ctx, dest) + technical_storage(ctx, dest, required_bytes)
    details: dict = {"source_region": source, "destination_region": dest}
    if route is not None:
        if not route or route[0] != source or route[-1] != dest:
            reasons.append(R("ROUTE_ENDPOINT_MISMATCH", "route must start at the source and end at the destination", "LEGAL"))
        reasons += evaluate_route(ctx, route).reasons
        details["route"] = route
    elif source in ctx.regions and dest in ctx.regions and source != dest:
        best, why = find_route(ctx, source, dest)
        reasons += why
        if best:
            details["route"], details["latency_ms"] = best
    return reasons, details


def evaluate_assignment(
    ctx: Ctx, source: str, dest: str, route: Optional[list[str]] = None, required_bytes: int = 0, assignment_policy_version: Optional[int] = None
) -> Decision:
    reasons, details = _pair_reasons(ctx, "ASSIGN", source, dest, route, required_bytes)
    if assignment_policy_version is not None and assignment_policy_version != ctx.policy.policy_version:
        reasons.append(
            R("STALE_POLICY_VERSION", f"assignment was made under policy v{assignment_policy_version}, current is v{ctx.policy.policy_version}", "VERSION")
        )
    return _decide("ASSIGN", ctx, reasons, details)


def evaluate_backup(ctx: Ctx, source: str, dest: str, route: Optional[list[str]] = None, required_bytes: int = 0) -> Decision:
    reasons, details = _pair_reasons(ctx, "BACKUP", source, dest, route, required_bytes)
    return _decide("BACKUP", ctx, reasons, details)


def evaluate_promotion(ctx: Ctx, candidate: str, replica_status: Optional[ReplicaStatus]) -> Decision:
    """Policy part of INV-5. Replication lag is judged separately; neither check can waive this one."""
    reasons = legal_storage(ctx, candidate) + legal_processing(ctx, candidate)
    reg = ctx.regions.get(candidate)
    if reg is not None:
        if reg.availability_status in UNHEALTHY_FOR_NEW_DATA:
            reasons.append(R("REGION_UNAVAILABLE", f"{candidate} is {reg.availability_status.value}", "TECHNICAL", candidate))
        if "gateway" not in reg.processing_capabilities:
            reasons.append(R("MISSING_CAPABILITY", f"{candidate} lacks processing capability 'gateway'", "TECHNICAL", candidate))
    if replica_status is None:
        reasons.append(R("NO_REPLICA", f"{candidate} holds no replica of this topic", "TECHNICAL", candidate))
    elif replica_status not in USABLE_FOR_PROMOTION:
        reasons.append(R("REPLICA_NOT_USABLE", f"replica in {candidate} is {replica_status.value}", "TECHNICAL", candidate))
    return _decide("PROMOTE", ctx, reasons, {"region": candidate})


def evaluate_failback(ctx: Ctx, original: str, current: str) -> Decision:
    """Moving the topic back to `original` must satisfy today's policy, not the policy of the original placement."""
    reasons = legal_storage(ctx, original) + legal_processing(ctx, original)
    reg = ctx.regions.get(original)
    if reg is not None:
        if reg.availability_status != Availability.AVAILABLE:
            reasons.append(R("REGION_UNAVAILABLE", f"{original} is {reg.availability_status.value}; failback needs AVAILABLE", "TECHNICAL", original))
        if "gateway" not in reg.processing_capabilities:
            reasons.append(R("MISSING_CAPABILITY", f"{original} lacks processing capability 'gateway'", "TECHNICAL", original))
    # The current region may itself have become illegal (that can be the reason for failing back), so only the
    # destination and the route are judged.
    pair, details = _pair_reasons(ctx, "FAILBACK", current, original, None, 0, check_source=False)
    return _decide("FAILBACK", ctx, reasons + pair, {**details, "original_region": original, "current_region": current})


def _dedupe(rs: Iterable[Reason]) -> list[Reason]:
    seen, out = set(), []
    for r in rs:
        k = (r.code, r.region, r.message)
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


# ---- placement: eliminate, then rank ---------------------------------------------------------------------
@dataclass
class Candidate:
    region: str
    route: list[str]
    latency_ms: int
    components: dict = field(default_factory=dict)
    score: float = 0.0


def eligible_destinations(ctx: Ctx, source: str, required_bytes: int = 0, exclude: frozenset = frozenset()) -> tuple[list[Candidate], dict[str, list[Reason]]]:
    """STAGE 1 (hard constraints only). Returns survivors and, for every eliminated region, why."""
    names = sorted(set(ctx.regions) | set(ctx.policy.allowed_storage_regions) | set(ctx.policy.preferred_regions))
    survivors: list[Candidate] = []
    rejected: dict[str, list[Reason]] = {}
    for rid in names:
        if rid == source:
            continue
        if rid in exclude:
            rejected[rid] = [R("ALREADY_USED_OR_FAILED", f"{rid} is failed or already holds a copy", "TECHNICAL", rid)]
            continue
        reasons = legal_storage(ctx, rid) + technical_storage(ctx, rid, required_bytes)
        route = None
        if not any(r.code == "UNKNOWN_REGION" for r in reasons) and source in ctx.regions:
            route, why = find_route(ctx, source, rid)
            reasons += why
        if reasons:
            rejected[rid] = _dedupe(reasons)
        else:
            survivors.append(Candidate(rid, route[0], route[1]))
    return survivors, rejected


def rank(ctx: Ctx, source: str, survivors: list[Candidate], count: int, weights: RankingWeights = RankingWeights()) -> list[Candidate]:
    """STAGE 2. Orders ELIGIBLE candidates only; the input set is the output set (a permutation/prefix of it)."""
    if not survivors or count <= 0:
        return []
    lats = [c.latency_ms for c in survivors]
    lo, hi = min(lats), max(lats)
    pref = ctx.policy.preferred_regions
    src_domain = ctx.regions[source].failure_domain if source in ctx.regions else None
    chosen: list[Candidate] = []
    pool = list(survivors)
    while pool and len(chosen) < count:
        used_domains = {src_domain} | {ctx.regions[c.region].failure_domain for c in chosen}
        for c in pool:
            reg = ctx.regions[c.region]
            comp = {
                "latency": 1.0 if hi == lo else 1.0 - (c.latency_ms - lo) / (hi - lo),
                "capacity": reg.compute_headroom,
                "storage_pressure": 1.0 - reg.storage_capacity.pressure,
                "failure_domain": 0.0 if reg.failure_domain in used_domains else 1.0,
                "preference": (1.0 - pref.index(c.region) / len(pref)) if c.region in pref else 0.0,
            }
            c.components = comp
            c.score = (
                weights.latency * comp["latency"]
                + weights.capacity * comp["capacity"]
                + weights.storage_pressure * comp["storage_pressure"]
                + weights.failure_domain * comp["failure_domain"]
                + weights.preference * comp["preference"]
            )
        best = min(pool, key=lambda c: (-round(c.score, 12), c.region))  # deterministic tie-break
        chosen.append(best)
        pool.remove(best)
    return chosen


def _cand_json(c: Candidate) -> dict:
    return {
        "region": c.region,
        "route": c.route,
        "latency_ms": c.latency_ms,
        "score": round(c.score, 6),
        "components": {k: round(v, 6) for k, v in c.components.items()},
    }


def plan_placement(
    ctx: Ctx,
    source: str,
    count: Optional[int] = None,
    required_bytes: int = 0,
    exclude: frozenset = frozenset(),
    weights: RankingWeights = RankingWeights(),
    op: str = "PLAN",
) -> Decision:
    count = ctx.policy.regional_replica_count if count is None else count
    src_reasons = legal_storage(ctx, source) + legal_processing(ctx, source)
    if src_reasons:
        return _decide(
            op,
            ctx,
            [r.model_copy(update={"code": "SOURCE_" + r.code if r.code != "UNKNOWN_REGION" else r.code}) for r in src_reasons],
            {"source_region": source},
        )
    survivors, rejected = eligible_destinations(ctx, source, required_bytes, exclude)
    ordered = rank(ctx, source, survivors, len(survivors), weights)  # full ranking of survivors (for display)
    chosen = ordered[:count]
    details = {
        "source_region": source,
        "requested": count,
        "eligible": [_cand_json(c) for c in ordered],
        "chosen": [_cand_json(c) for c in chosen],
        "rejected": [{"region": k, "reasons": [r.model_dump() for r in v]} for k, v in sorted(rejected.items())],
    }
    if len(survivors) < count:
        return Decision(
            decision=DecisionKind.INSUFFICIENT_ELIGIBLE_REGIONS,
            operation=op,
            topic=ctx.policy.topic,
            policy_version=ctx.policy.policy_version,
            reasons=[R("INSUFFICIENT_ELIGIBLE_REGIONS", f"need {count} eligible destination(s), only {len(survivors)} pass the hard constraints", "LEGAL")],
            details=details,
        )
    return _decide(op, ctx, [], details)


def select_recovery_destination(
    ctx: Ctx,
    source: str,
    failed: Iterable[str],
    existing: Iterable[str] = (),
    count: int = 1,
    required_bytes: int = 0,
    weights: RankingWeights = RankingWeights(),
) -> Decision:
    """Pick replacement destination(s) after a failure: same hard constraints as any placement."""
    return plan_placement(ctx, source, count, required_bytes, frozenset(failed) | frozenset(existing), weights, "RECOVERY")


# ---- policy validation (creation / update) ---------------------------------------------------------------
def validate_policy(regions: dict[str, Region], links: dict[tuple[str, str], Link], p: TopicPolicyIn) -> list[Reason]:
    out: list[Reason] = []
    named = set(p.allowed_storage_regions) | set(p.allowed_processing_regions) | set(p.preferred_regions) | set(p.excluded_regions) | {p.home_region}
    for rid in sorted(named):
        if rid not in regions:
            out.append(R("UNKNOWN_REGION", f"region {rid!r} is not registered", "LEGAL", rid))
    for rid in sorted(set(p.allowed_storage_regions) | set(p.allowed_processing_regions)):
        reg = regions.get(rid)
        if reg and p.legal_zone != GLOBAL_ZONE and reg.legal_zone != p.legal_zone:
            out.append(R("LEGAL_ZONE_MISMATCH", f"{rid} is in zone {reg.legal_zone}, topic is bound to {p.legal_zone}", "LEGAL", rid))
    if p.home_region not in p.allowed_storage_regions or p.home_region not in p.allowed_processing_regions:
        out.append(R("HOME_REGION_NOT_ALLOWED", "home_region must be in both allowed_storage_regions and allowed_processing_regions", "LEGAL", p.home_region))
    if p.home_region in p.excluded_regions:
        out.append(R("HOME_REGION_EXCLUDED", "home_region is excluded", "LEGAL", p.home_region))
    for rid in p.preferred_regions:
        if rid not in p.allowed_storage_regions:
            out.append(
                R("PREFERRED_NOT_ALLOWED", f"preferred region {rid} is not an allowed storage region (preference never grants permission)", "LEGAL", rid)
            )
    home = regions.get(p.home_region)
    if home and p.replication_factor > home.max_replication_factor:
        out.append(
            R(
                "RF_EXCEEDS_REGION_LIMIT",
                f"replication_factor {p.replication_factor} exceeds {p.home_region}'s limit {home.max_replication_factor}",
                "TECHNICAL",
                p.home_region,
            )
        )
    return _dedupe(out)


def policy_as_ctx(regions: dict[str, Region], links: dict[tuple[str, str], Link], p: TopicPolicy) -> Ctx:
    return Ctx(regions, links, p)
