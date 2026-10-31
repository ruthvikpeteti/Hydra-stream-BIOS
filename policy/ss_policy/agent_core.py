"""Receiving-side (regional) policy agent logic.

Each region's agent keeps ITS OWN verified copy of the policy state and re-validates every request that
arrives, whatever the sender already decided. It never trusts the sender's verdict, only the signed bundle.

Fail-closed rules:
  * no verified bundle                     -> POLICY_UNAVAILABLE
  * policy for the topic not in the bundle -> POLICY_UNAVAILABLE
  * bundle older than the classification's staleness limit (RESTRICTED: shortest) -> POLICY_UNAVAILABLE
  * sender's policy_version < ours         -> REJECTED  (a stale worker cannot keep going)
  * sender's policy_version > ours         -> POLICY_UNAVAILABLE (we are behind; refresh first)
"""

import time
from dataclasses import dataclass
from typing import Callable, Optional

from .bundle import BundleError, VerifiedBundle, verify_bundle
from .engine import (
    evaluate_path,
    Ctx,
    R,
    evaluate_assignment,
    evaluate_backup,
    evaluate_failback,
    evaluate_process,
    evaluate_promotion,
    evaluate_route,
    no_policy,
)
from .models import Classification, Decision, DecisionKind, Operation, ReplicaStatus

# Max age (seconds, on the agent's monotonic clock since the last successful refresh) of the policy state
# that may still be used for a topic of each classification. RESTRICTED gets the tightest limit.
DEFAULT_STALENESS_S = {Classification.RESTRICTED: 30.0, Classification.CONFIDENTIAL: 120.0, Classification.INTERNAL: 600.0, Classification.PUBLIC: 3600.0}


@dataclass
class AgentRequest:
    topic: str
    operation: Operation
    source_region: Optional[str] = None
    route: Optional[list[str]] = None
    policy_version: Optional[int] = None
    flow_id: Optional[str] = None
    replica_status: Optional[ReplicaStatus] = None
    original_region: Optional[str] = None  # FAILBACK
    required_bytes: int = 0


class RegionalPolicyAgent:
    def __init__(self, region_id: str, public_pem: bytes, clock: Callable[[], float] = time.monotonic, staleness_s: Optional[dict] = None):
        self.region_id, self._pub, self._clock = region_id, public_pem, clock
        self.staleness = dict(staleness_s or DEFAULT_STALENESS_S)
        self._bundle: Optional[VerifiedBundle] = None
        self._fetched_at = float("-inf")
        self.flows: dict[str, dict] = {}

    # ---- state -----------------------------------------------------------------------------------------
    def install(self, signed: dict) -> None:
        """Verify and adopt a bundle. A forged bundle or an older bundle_version is refused."""
        vb = verify_bundle(signed, self._pub)
        if self._bundle and vb.bundle_version < self._bundle.bundle_version:
            raise BundleError(f"bundle_version went backwards ({vb.bundle_version} < {self._bundle.bundle_version})")
        self._bundle, self._fetched_at = vb, self._clock()

    @property
    def bundle_version(self) -> Optional[int]:
        return self._bundle.bundle_version if self._bundle else None

    def age_s(self) -> float:
        return self._clock() - self._fetched_at

    def policy_version(self, topic: str) -> Optional[int]:
        p = self._bundle.policies.get(topic) if self._bundle else None
        return p.policy_version if p else None

    def topology(self, topic: str) -> Optional[dict]:
        return self._bundle.topology.get(topic) if self._bundle else None

    def cluster_of(self, region_id: str) -> Optional[str]:
        r = self._bundle.regions.get(region_id) if self._bundle else None
        return (r.cluster_id or r.region_id) if r else None

    def authorize_path(self, topic: str, path_id: Optional[str], source_region: str) -> Decision:
        """Is this egress path legal for the topic, judged from THIS region's own signed bundle? Deny by default."""
        pol = self._bundle.policies.get(topic) if self._bundle else None
        if pol is None:
            return Decision(
                decision=DecisionKind.POLICY_UNAVAILABLE,
                operation="PATH",
                topic=topic,
                reasons=[R("POLICY_NOT_FOUND", "no policy available for this topic; denying", "SYSTEM")],
            )
        path = self._bundle.paths.get(path_id) if path_id else None
        base = dict(operation="PATH", topic=topic, policy_version=pol.policy_version)
        if path is None:
            return Decision(
                decision=DecisionKind.REJECTED,
                **base,
                reasons=[R("PATH_UNKNOWN", f"path {path_id!r} is not in the registry (a path declaration is required)", "LEGAL")],
            )
        if path.source_region != source_region or path.target_region != self.region_id:
            return Decision(
                decision=DecisionKind.REJECTED,
                **base,
                reasons=[R("PATH_ENDPOINT_MISMATCH", f"path {path_id} does not connect {source_region} to {self.region_id}", "LEGAL")],
            )
        return evaluate_path(Ctx(self._bundle.regions, self._bundle.links, pol), path)

    def needs_refresh(self, topic: str, caller_version: Optional[int]) -> bool:
        """True if the service layer should try to refresh before deciding."""
        if self._bundle is None or topic not in self._bundle.policies:
            return True
        pol = self._bundle.policies[topic]
        if caller_version is not None and caller_version != pol.policy_version:
            return True
        return self.age_s() > self.staleness[pol.classification] * 0.5

    # ---- decisions -------------------------------------------------------------------------------------
    def authorize(self, req: AgentRequest) -> Decision:
        op = req.operation.value
        if self._bundle is None:
            return Decision(
                decision=DecisionKind.POLICY_UNAVAILABLE,
                operation=op,
                topic=req.topic,
                reasons=[R("BUNDLE_UNAVAILABLE", f"{self.region_id} has no verified policy bundle; denying", "SYSTEM")],
            )
        pol = self._bundle.policies.get(req.topic)
        if pol is None:
            return no_policy(op, req.topic)
        limit = self.staleness[pol.classification]
        if self.age_s() > limit:
            return Decision(
                decision=DecisionKind.POLICY_UNAVAILABLE,
                operation=op,
                topic=req.topic,
                policy_version=pol.policy_version,
                reasons=[
                    R("POLICY_STALE", f"policy state is {self.age_s():.0f}s old; {pol.classification.value} topics allow {limit:.0f}s; denying", "SYSTEM")
                ],
            )
        if req.policy_version is not None:
            if req.policy_version < pol.policy_version:
                return Decision(
                    decision=DecisionKind.REJECTED,
                    operation=op,
                    topic=req.topic,
                    policy_version=pol.policy_version,
                    reasons=[R("STALE_POLICY_VERSION", f"sender used policy v{req.policy_version}; {self.region_id} holds v{pol.policy_version}", "VERSION")],
                )
            if req.policy_version > pol.policy_version:
                return Decision(
                    decision=DecisionKind.POLICY_UNAVAILABLE,
                    operation=op,
                    topic=req.topic,
                    policy_version=pol.policy_version,
                    reasons=[
                        R("AGENT_POLICY_BEHIND", f"sender holds v{req.policy_version}, {self.region_id} only v{pol.policy_version}; refresh needed", "SYSTEM")
                    ],
                )
        ctx = Ctx(self._bundle.regions, self._bundle.links, pol)
        me = self.region_id
        if me not in self._bundle.regions:
            return Decision(
                decision=DecisionKind.REJECTED,
                operation=op,
                topic=req.topic,
                policy_version=pol.policy_version,
                reasons=[R("UNKNOWN_REGION", f"{me} is not registered", "LEGAL", me)],
            )
        d = self._dispatch(ctx, req, me)
        if d.allowed and req.operation == Operation.ASSIGN and req.flow_id:
            self.flows[req.flow_id] = {"topic": req.topic, "policy_version": pol.policy_version, "source": req.source_region}
        return d

    def _dispatch(self, ctx: Ctx, req: AgentRequest, me: str) -> Decision:
        op = req.operation
        src = req.source_region or ""
        if op == Operation.ASSIGN:  # replica creation and flow initialisation: data will land HERE
            return evaluate_assignment(ctx, src, me, req.route, req.required_bytes, req.policy_version)
        if op == Operation.BACKUP:
            return evaluate_backup(ctx, src, me, req.route, req.required_bytes)
        if op == Operation.PROCESS:
            return evaluate_process(ctx, me)
        if op == Operation.PROMOTE:
            return evaluate_promotion(ctx, me, req.replica_status)
        if op == Operation.FAILBACK:
            return evaluate_failback(ctx, req.original_region or me, src or me)
        if op == Operation.ROUTE:
            return evaluate_route(ctx, req.route or [])
        return Decision(
            decision=DecisionKind.REJECTED,
            operation=op.value,
            topic=req.topic,
            policy_version=ctx.policy.policy_version,
            reasons=[R("UNSUPPORTED_OPERATION", f"agents do not evaluate {op.value}", "SYSTEM")],
        )

    def admit_batch(self, topic: str, flow_id: str, policy_version: int, source_region: str, route: Optional[list[str]]) -> Decision:
        """Per-batch admission: the flow must have been admitted under the CURRENT policy version, and the
        full assignment check is repeated for every batch (cheap, pure, no network)."""
        flow = self.flows.get(flow_id)
        base = AgentRequest(topic, Operation.ASSIGN, source_region, route, policy_version)
        d = self.authorize(base)
        if not d.allowed:
            return d.model_copy(update={"operation": "BATCH_ADMISSION"})
        if flow is None or flow["topic"] != topic:
            return Decision(
                decision=DecisionKind.REJECTED,
                operation="BATCH_ADMISSION",
                topic=topic,
                policy_version=d.policy_version,
                reasons=[R("UNKNOWN_FLOW", "flow was not initialised here; initialise first", "SYSTEM")],
            )
        if flow["policy_version"] != d.policy_version:
            return Decision(
                decision=DecisionKind.REJECTED,
                operation="BATCH_ADMISSION",
                topic=topic,
                policy_version=d.policy_version,
                reasons=[
                    R("STALE_FLOW", f"flow was admitted under policy v{flow['policy_version']}; current is v{d.policy_version}; re-initialise", "VERSION")
                ],
            )
        return d.model_copy(update={"operation": "BATCH_ADMISSION"})
