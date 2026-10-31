"""Pydantic models shared by the engine service, the regional agents and the tests."""

from enum import Enum
from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

ID_RE = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"
TOPIC_RE = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$"
GLOBAL_ZONE = "GLOBAL"  # a topic bound to GLOBAL may live in any legal zone


class Availability(str, Enum):
    AVAILABLE = "AVAILABLE"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"
    DRAINING = "DRAINING"


class Classification(str, Enum):
    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED = "RESTRICTED"


class Durability(str, Enum):
    LOCAL_DURABLE = "LOCAL_DURABLE"
    ASYNC_REGIONAL = "ASYNC_REGIONAL"
    SYNC_REGIONAL = "SYNC_REGIONAL"


class DecisionKind(str, Enum):
    ALLOWED = "ALLOWED"
    REJECTED = "REJECTED"
    INSUFFICIENT_ELIGIBLE_REGIONS = "INSUFFICIENT_ELIGIBLE_REGIONS"
    POLICY_UNAVAILABLE = "POLICY_UNAVAILABLE"


class ReplicaStatus(str, Enum):
    PENDING = "PENDING"  # planned, nothing created yet
    PROVISIONED = "PROVISIONED"  # replica topic exists in the destination, no data yet
    SYNCING = "SYNCING"
    IN_SYNC = "IN_SYNC"
    FAILED = "FAILED"
    REVOKED = "REVOKED"  # policy no longer permits it: must be removed, never used


USABLE_FOR_PROMOTION = {ReplicaStatus.SYNCING, ReplicaStatus.IN_SYNC}


class Operation(str, Enum):
    STORE = "STORE"
    PROCESS = "PROCESS"
    ROUTE = "ROUTE"
    ASSIGN = "ASSIGN"
    PROMOTE = "PROMOTE"
    BACKUP = "BACKUP"
    PLAN = "PLAN"
    RECOVERY = "RECOVERY"
    FAILBACK = "FAILBACK"


class EnforcementPoint(str, Enum):
    """Where in the system a decision was requested (recorded in the audit log)."""

    ROUTING = "ROUTING"
    API = "API"
    TOPIC_CREATION = "TOPIC_CREATION"
    REPLICA_CREATION = "REPLICA_CREATION"
    FLOW_INIT = "FLOW_INIT"
    BATCH_ADMISSION = "BATCH_ADMISSION"
    RECOVERY = "RECOVERY"
    FAILBACK = "FAILBACK"
    PROMOTION = "PROMOTION"
    BACKUP = "BACKUP"
    PROCESSING = "PROCESSING"


class StorageCapacity(BaseModel):
    model_config = ConfigDict(extra="forbid")
    total_bytes: int = Field(ge=0)
    used_bytes: int = Field(0, ge=0)

    @property
    def free_bytes(self) -> int:
        return max(0, self.total_bytes - self.used_bytes)

    @property
    def pressure(self) -> float:
        """Fraction used, 0..1 (a region with no capacity counts as fully pressured)."""
        return 1.0 if self.total_bytes == 0 else min(1.0, self.used_bytes / self.total_bytes)


class Region(BaseModel):
    model_config = ConfigDict(extra="forbid")
    region_id: str = Field(pattern=ID_RE)
    legal_zone: str = Field(min_length=1)
    availability_status: Availability = Availability.AVAILABLE
    storage_capacity: StorageCapacity
    processing_capabilities: list[str] = Field(default_factory=lambda: ["gateway", "replication"])
    supported_transit_zones: list[str] = Field(default_factory=list)
    # Supporting attributes used by hard technical constraints and by ranking:
    failure_domain: str = Field(min_length=1)  # regions sharing a value fail together
    compute_headroom: float = Field(0.5, ge=0.0, le=1.0)  # infrastructure capacity, 0..1
    max_replication_factor: int = Field(3, ge=1)  # most replicas one cluster here can hold
    cluster_id: Optional[str] = Field(None, pattern=ID_RE)  # Kafka cluster identity stamped on records; defaults to region_id


class Link(BaseModel):
    model_config = ConfigDict(extra="forbid")
    from_region: str = Field(pattern=ID_RE)
    to_region: str = Field(pattern=ID_RE)
    transit_zones: list[str] = Field(default_factory=list)  # every zone the traffic crosses
    latency_ms: int = Field(ge=0)
    status: str = Field("UP", pattern="^(UP|DOWN)$")


class NetworkPath(BaseModel):
    """One egress path for a region pair. Each has its own endpoint, exposed to the replication agent."""

    model_config = ConfigDict(extra="forbid")
    path_id: str = Field(pattern=ID_RE)
    source_region: str = Field(pattern=ID_RE)
    target_region: str = Field(pattern=ID_RE)
    kind: str = Field("DIRECT", pattern="^(DIRECT|PEERING|RELAY)$")
    via_regions: list[str] = Field(default_factory=list)  # regions the traffic is relayed through
    transit_zones: list[str] = Field(default_factory=list)  # every zone this path crosses
    endpoint_url: str = Field(min_length=1)  # what the source agent connects to
    encrypted: bool = False
    cost: float = Field(1.0, ge=0.0)
    fallback: bool = False  # the designated last-resort path
    enabled: bool = True


class _PolicyFields(BaseModel):
    topic: str = Field(pattern=TOPIC_RE)
    classification: Classification
    legal_zone: str = Field(min_length=1)
    home_region: str = Field(pattern=ID_RE)  # where the topic is active/created
    allowed_storage_regions: list[str]
    allowed_processing_regions: list[str]
    allowed_transit_zones: list[str] = Field(default_factory=list)
    replication_factor: int = Field(3, ge=1, le=10)  # Kafka RF inside each region holding the topic
    regional_replica_count: int = Field(0, ge=0, le=10)  # remote regions holding a replica
    preferred_regions: list[str] = Field(default_factory=list)  # ranking hint only, never a permission
    excluded_regions: list[str] = Field(default_factory=list)  # hard veto, overrides everything
    maximum_replication_lag_ms: Optional[int] = Field(None, ge=0, validation_alias=AliasChoices("maximum_replication_lag_ms", "maximum_replication_lag"))
    durability_mode: Durability = Durability.LOCAL_DURABLE

    @field_validator("allowed_storage_regions", "allowed_processing_regions", "allowed_transit_zones", "preferred_regions", "excluded_regions")
    @classmethod
    def _unique(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("duplicate entries")
        return v

    @model_validator(mode="after")
    def _mode_shape(self):
        if self.durability_mode == Durability.LOCAL_DURABLE:
            if self.regional_replica_count != 0 or self.maximum_replication_lag_ms is not None:
                raise ValueError("LOCAL_DURABLE requires regional_replica_count=0 and no maximum_replication_lag_ms")
        elif self.regional_replica_count < 1 or self.maximum_replication_lag_ms is None:
            raise ValueError(f"{self.durability_mode.value} requires regional_replica_count>=1 and maximum_replication_lag_ms")
        return self


class TopicPolicyIn(_PolicyFields):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class TopicPolicy(_PolicyFields):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    policy_version: int = Field(ge=1)


class ReplicaAssignment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    topic: str
    partition: int = Field(ge=0)
    source_region: str
    destination_region: str
    replica_status: ReplicaStatus = ReplicaStatus.PENDING
    policy_version: int = Field(ge=1)
    assigned_at: Optional[str] = None


class Reason(BaseModel):
    code: str
    message: str
    category: str = "LEGAL"  # LEGAL | TECHNICAL | VERSION | SYSTEM
    region: Optional[str] = None


class Decision(BaseModel):
    decision: DecisionKind
    operation: str
    topic: Optional[str] = None
    policy_version: Optional[int] = None
    reasons: list[Reason] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)
    decision_id: Optional[int] = None

    @property
    def allowed(self) -> bool:
        return self.decision == DecisionKind.ALLOWED


class EvaluateRequest(BaseModel):
    """Metadata only. Extra fields are rejected so message payloads can never reach the decision log."""

    model_config = ConfigDict(extra="forbid")
    topic: str = Field(pattern=TOPIC_RE)
    operation: Operation
    region: Optional[str] = None  # STORE / PROCESS / PROMOTE subject
    source_region: Optional[str] = None
    destination_region: Optional[str] = None
    route: Optional[list[str]] = None
    failed_regions: list[str] = Field(default_factory=list)
    replica_status: Optional[ReplicaStatus] = None
    capability: Optional[str] = None
    policy_version: Optional[int] = Field(None, ge=0)  # version the CALLER believes is current
    required_bytes: int = Field(0, ge=0)
    count: Optional[int] = Field(None, ge=0, le=10)
    enforcement_point: EnforcementPoint = EnforcementPoint.API
    requester: Optional[str] = Field(None, max_length=128)
    request_id: Optional[str] = Field(None, max_length=128)
    inline_policy: Optional[TopicPolicyIn] = None  # what-if for a topic that does not exist yet
