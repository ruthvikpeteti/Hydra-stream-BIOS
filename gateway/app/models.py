"""Request models. Validation rules for events live here (event validation component)."""

import uuid
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

TOPIC_RE = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$"
RESERVED_EVENT_FIELDS = {"leadership_epoch", "replication_generation", "origin_region", "origin_cluster", "source_cluster", "source_partition", "source_offset"}


class TopicCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(pattern=TOPIC_RE)
    partitions: int = Field(3, ge=1, le=64)
    classification: Optional[str] = None
    legal_zone: Optional[str] = None
    allowed_storage_regions: Optional[list[str]] = None
    allowed_processing_regions: Optional[list[str]] = None
    allowed_transit_zones: Optional[list[str]] = None
    local_replication_factor: Optional[int] = Field(None, ge=1, le=10)
    regional_replica_count: Optional[int] = Field(None, ge=0, le=10)
    max_tolerated_replication_lag_ms: Optional[int] = Field(None, ge=0)
    failover_priority: Optional[list[str]] = None
    durability_mode: Optional[str] = None
    local_min_insync_replicas: Optional[int] = Field(None, ge=1)
    sync_remote_acks: Optional[int] = Field(None, ge=1)
    lag_breach_action: Optional[str] = None

    @field_validator("name")
    @classmethod
    def _not_internal(cls, v: str) -> str:
        if v.startswith(("_", ".")):
            raise ValueError("topic names may not start with '_' or '.'")
        return v


class EventIn(BaseModel):
    """Client-supplied event. Server-stamped fields (epoch, generation, origin_*) are never read from the
    client: they are ignored and logged (API_SPEC §3.1)."""

    model_config = ConfigDict(extra="allow")
    topic: str = Field(pattern=TOPIC_RE)
    event_id: str
    partition_key: str = Field(min_length=1)
    payload: Any
    timestamp: Optional[int] = Field(None, ge=0)
    expected_epoch: Optional[int] = Field(None, ge=0)

    @field_validator("event_id")
    @classmethod
    def _uuid(cls, v: str) -> str:
        try:
            return str(uuid.UUID(v))
        except ValueError as exc:
            raise ValueError("event_id must be a UUID") from exc

    @field_validator("partition_key")
    @classmethod
    def _key_size(cls, v: str) -> str:
        if len(v.encode()) > 256:
            raise ValueError("partition_key must be at most 256 bytes")
        return v

    @field_validator("payload")
    @classmethod
    def _payload_present(cls, v: Any) -> Any:
        if v is None:
            raise ValueError("payload must not be null")
        return v


class OffsetCommit(BaseModel):
    model_config = ConfigDict(extra="forbid")
    topic: str = Field(pattern=TOPIC_RE)
    offsets: dict[int, int]  # partition -> next offset to read
