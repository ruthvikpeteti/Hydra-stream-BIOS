"""Event service: the write path (API_SPEC §3.1 check order) and envelope construction."""

import asyncio
import hashlib
import json
import logging
import re
import time
from contextlib import AsyncExitStack

from .config import Settings
from .errors import ApiError
from .idempotency import IdempotencyStore
from . import metrics
from .leases import DevFileLeaseSource, LeaseState
from .models import RESERVED_EVENT_FIELDS, EventIn
from .producer import ProducerManager
from .topics import TopicService

log = logging.getLogger("ss.events")
IDEM_KEY_RE = re.compile(r"^[\x21-\x7e]{1,255}$")
STRIPES = 64


def body_hash(ev: EventIn) -> str:
    """Hash of the client-controlled content. Excludes expected_epoch (a precondition, not content)."""
    canon = {"topic": ev.topic, "event_id": ev.event_id, "partition_key": ev.partition_key, "payload": ev.payload, "timestamp": ev.timestamp}
    return hashlib.sha256(json.dumps(canon, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class EventService:
    def __init__(self, s: Settings, leases: DevFileLeaseSource, topics: TopicService, producer: ProducerManager, idem: IdempotencyStore):
        self._s, self._leases, self._topics, self._producer, self._idem = s, leases, topics, producer, idem
        self._locks = [asyncio.Lock() for _ in range(STRIPES)]

    def _stripes(self, *keys: str) -> list[int]:
        return sorted({hash(k) % STRIPES for k in keys})

    async def publish(self, ev: EventIn, idem_header: str | None) -> tuple[dict, bool, LeaseState]:
        """Returns (response body, was_replay, lease state)."""
        if idem_header is not None and not IDEM_KEY_RE.match(idem_header):
            raise ApiError(400, "ENVELOPE_INVALID", "Idempotency-Key must be 1-255 printable ASCII characters")
        extra = set(ev.model_extra or {}) & RESERVED_EVENT_FIELDS
        if extra:
            log.warning("ignoring client-supplied server-stamped fields %s on topic %s", sorted(extra), ev.topic)
        if len(json.dumps(ev.payload, separators=(",", ":")).encode()) > self._s.max_event_bytes:
            raise ApiError(413, "ENVELOPE_INVALID", f"payload exceeds {self._s.max_event_bytes} bytes")

        rec = await self._topics.current(await self._topics.get(ev.topic))
        policy = rec["policy"]
        if self._s.region not in policy["allowed_processing_regions"]:
            raise ApiError(
                403, "POLICY_VIOLATION", "this region may not process the topic", topic=ev.topic, details={"policy_version": rec.get("policy_version")}
            )

        h = body_hash(ev)
        idem_key = f"idem:{idem_header}" if idem_header else None
        evt_key = f"evt:{ev.topic}:{ev.event_id}"
        async with AsyncExitStack() as stack:
            for i in self._stripes(idem_key or evt_key, evt_key):
                await stack.enter_async_context(self._locks[i])
            replay = self._replay(idem_key, evt_key, h, ev.topic)
            lease = self._leases.resolve(ev.topic)
            if replay is not None:
                return replay, True, lease
            self._check_write_allowed(lease, ev, policy)
            bp = await self._topics.backpressure(rec)
            if bp:
                raise ApiError(
                    503,
                    "REPLICATION_BACKPRESSURE",
                    "the unreplicated backlog reached its hard limit; writes are refused until replication catches up",
                    topic=ev.topic,
                    retryable=True,
                    details={"flows": bp["flows"]},
                )
            body = await self._write(ev, rec, lease)
            try:
                await self._idem.put([k for k in (idem_key, evt_key) if k], h, body)
            except ApiError as exc:  # the event IS durable; only the dedupe record is missing
                log.error("idempotency record not persisted for %s: %s", evt_key, exc.message)
                body = {**body, "idempotency_recorded": False}
            return body, False, lease

    def _replay(self, idem_key: str | None, evt_key: str, h: str, topic: str) -> dict | None:
        if idem_key and (e := self._idem.get(idem_key)):
            if e["body_hash"] != h:
                raise ApiError(409, "IDEMPOTENCY_KEY_REUSED", "this Idempotency-Key was used with a different request", topic=topic)
            return {**e["response"], "idempotent_replay": True}
        if e := self._idem.get(evt_key):
            if e["body_hash"] != h:
                raise ApiError(409, "EVENT_ID_CONFLICT", "event_id already used with different content", topic=topic)
            return {**e["response"], "idempotent_replay": True}
        return None

    def _check_write_allowed(self, lease: LeaseState, ev: EventIn, policy: dict) -> None:
        if lease.role != "active":  # 4. active region
            raise ApiError(
                421,
                "NOT_ACTIVE_REGION",
                "this region is not active for the topic",
                topic=ev.topic,
                details={"active_region": lease.active_region, "leadership_epoch": lease.leadership_epoch},
            )
        if not lease.lease_valid:  # 5. lease
            raise ApiError(503, "LEASE_EXPIRED", "write lease is not valid; region is fenced", topic=ev.topic, retryable=True)
        if ev.expected_epoch is not None and ev.expected_epoch != lease.leadership_epoch:  # 6. epoch
            raise ApiError(
                409,
                "STALE_EPOCH",
                "expected_epoch does not match the current leadership epoch",
                topic=ev.topic,
                details={"current_epoch": lease.leadership_epoch, "active_region": lease.active_region},
            )
        mode = policy["durability_mode"]
        if mode not in self._s.supported_durability:  # 8. durability
            raise ApiError(503, "REPLICATION_BACKPRESSURE", f"{mode} needs the replication plane, which is not available", topic=ev.topic, retryable=True)

    async def _write(self, ev: EventIn, rec: dict, lease: LeaseState) -> dict:
        ts = ev.timestamp if ev.timestamp is not None else int(time.time() * 1000)
        policy = rec["policy"]
        envelope = {
            "envelope_version": 1,
            "event_id": ev.event_id,
            "topic": ev.topic,
            "partition_key": ev.partition_key,
            "payload": ev.payload,
            "event_timestamp": ts,
            "origin_region": self._s.region,
            "origin_cluster": self._s.cluster_id,
            "leadership_epoch": lease.leadership_epoch,
            "replication_generation": lease.replication_generation,
            "policy_revision": rec["revision"],
        }
        t0 = time.monotonic()
        try:
            md = await self._producer.send(
                ev.topic,
                ev.partition_key.encode(),
                json.dumps(envelope, separators=(",", ":")).encode(),
                timestamp_ms=ts,
                headers=[("ss-envelope-version", b"1")],
            )
        except Exception:
            metrics.count_write("kafka_error")
            raise
        metrics.observe_ack(time.monotonic() - t0)
        metrics.count_write("acknowledged")
        return {
            "event_id": ev.event_id,
            "topic": ev.topic,
            "origin_region": self._s.region,
            "origin_cluster": self._s.cluster_id,
            "leadership_epoch": lease.leadership_epoch,
            "replication_generation": lease.replication_generation,
            "event_timestamp": ts,
            "source_cluster": self._s.cluster_id,
            "source_partition": md.partition,
            "source_offset": md.offset,
            "durability_mode": policy["durability_mode"],
            "durable_regions": [self._s.region],
        }
