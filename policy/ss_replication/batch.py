"""Wire format of a replication batch and its structural validation (pure)."""

import base64
import json
import uuid
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from ss_policy.models import ID_RE, TOPIC_RE


class OriginIn(BaseModel):
    """Where the event was FIRST written. Survives every hop."""

    model_config = ConfigDict(extra="forbid")
    cluster: str = Field(pattern=ID_RE)
    topic: str = Field(pattern=TOPIC_RE)
    partition: int = Field(ge=0)
    offset: int = Field(ge=0)


class RecordIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    offset: int = Field(ge=0)  # offset in the SOURCE cluster (this hop)
    timestamp: int = Field(ge=0)
    key: Optional[str] = None  # base64
    value: str  # base64; the event envelope, copied byte for byte
    origin: OriginIn
    hop_path: str = Field(max_length=300)  # clusters traversed so far, e.g. "kafka-a>kafka-b"


class BatchIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    batch_id: str = Field(max_length=300)
    attempt: int = Field(ge=1)
    stream_session: str = Field(max_length=100)
    source_region: str = Field(pattern=ID_RE)
    source_cluster: str = Field(pattern=ID_RE)
    destination_region: str = Field(pattern=ID_RE)
    topic: str = Field(pattern=TOPIC_RE)
    partition: int = Field(ge=0)
    policy_version: int = Field(ge=0)
    replication_generation: int = Field(ge=0)
    leadership_epoch: int = Field(ge=0)  # the failover epoch the flow operates under
    prev_offset: int = Field(ge=-1)  # last source offset of the batch before this one in the stream (-1: none)
    first_offset: int = Field(ge=0)
    last_offset: int = Field(ge=0)
    records: list[RecordIn] = Field(min_length=1, max_length=5000)


def validate_batch(b: BatchIn, max_records: int, max_bytes: int) -> list[str]:
    errs: list[str] = []
    if len(b.records) > max_records:
        errs.append(f"batch has {len(b.records)} records, limit {max_records}")
    offs = [r.offset for r in b.records]
    if offs != sorted(set(offs)):
        errs.append("record offsets must be strictly increasing")
    if b.first_offset != offs[0] or b.last_offset != offs[-1]:
        errs.append("first_offset/last_offset do not match the records")
    if b.prev_offset >= b.first_offset:
        errs.append("prev_offset must be lower than first_offset")
    size = 0
    for r in b.records:
        size += len(r.value) + len(r.key or "")
        if r.origin.topic != b.topic:
            errs.append(f"record {r.offset}: origin topic {r.origin.topic!r} differs from batch topic")
            break
    if size > max_bytes:
        errs.append(f"batch is {size} bytes, limit {max_bytes}")
    return errs


def decode_record(r: RecordIn) -> tuple[Optional[bytes], bytes, str]:
    """(key, value, event_id). The value must be a SovereignStream envelope with a UUID event_id."""
    try:
        value = base64.b64decode(r.value, validate=True)
        key = base64.b64decode(r.key, validate=True) if r.key is not None else None
        env = json.loads(value)
        event_id = str(uuid.UUID(env["event_id"]))
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError(f"record at offset {r.offset} is not a valid event envelope: {exc}") from exc
    return key, value, event_id
