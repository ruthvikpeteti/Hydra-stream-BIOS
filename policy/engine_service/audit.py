"""Append-only, hash-chained decision log. Stores metadata only; callers cannot add free-form fields."""

import hashlib
import json
from typing import Any, Optional

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from ss_policy.models import Decision, EnforcementPoint

GENESIS = "0" * 64
# Whitelist of keys allowed in `subject`. Anything else (for example a payload) is dropped, not stored.
SUBJECT_KEYS = {
    "region",
    "source_region",
    "destination_region",
    "route",
    "failed_regions",
    "capability",
    "required_bytes",
    "replica_status",
    "count",
    "flow_id",
    "record_count",
    "first_offset",
    "last_offset",
    "bytes",
    "partitions",
    "chosen",
    "original_region",
    "current_region",
    "latency_ms",
    "revoked",
    "requested",
    "assignment",
    "flow_id",
    "generation",
    "epoch",
    "path_id",
    "backlog_bytes",
}


def clean_subject(subject: Optional[dict]) -> dict:
    return {k: v for k, v in (subject or {}).items() if k in SUBJECT_KEYS}


class AuditIn(BaseModel):
    """Decision record submitted by a regional agent."""

    model_config = ConfigDict(extra="forbid")
    operation: str = Field(max_length=32)
    enforcement_point: EnforcementPoint
    topic: Optional[str] = Field(None, max_length=201)
    policy_version: Optional[int] = None
    decision: str
    reasons: list[dict] = Field(default_factory=list, max_length=50)
    subject: dict[str, Any] = Field(default_factory=dict)
    requester: Optional[str] = Field(None, max_length=128)
    request_id: Optional[str] = Field(None, max_length=128)


def _digest(prev: str, rec: dict) -> str:
    return hashlib.sha256((prev + json.dumps(rec, sort_keys=True, separators=(",", ":"), default=str)).encode()).hexdigest()


def _reasons(rs) -> list[dict]:
    return [{"code": r["code"], "category": r.get("category"), "region": r.get("region"), "message": str(r.get("message", ""))[:300]} for r in rs]


async def append(
    conn: asyncpg.Connection,
    *,
    source: str,
    operation: str,
    point: str,
    topic: Optional[str],
    policy_version: Optional[int],
    decision: str,
    reasons: list[dict],
    subject: Optional[dict],
    requester: Optional[str],
    request_id: Optional[str],
) -> int:
    """Must run inside a transaction: the advisory lock serialises the chain."""
    await conn.execute("SELECT pg_advisory_xact_lock(7001)")
    prev = await conn.fetchval("SELECT hash FROM policy_decisions ORDER BY id DESC LIMIT 1") or GENESIS
    rec = {
        "source": source,
        "operation": operation,
        "enforcement_point": point,
        "topic": topic,
        "policy_version": policy_version,
        "subject": clean_subject(subject),
        "decision": decision,
        "reasons": _reasons(reasons),
        "requester": requester,
        "request_id": request_id,
    }
    h = _digest(prev, rec)
    return await conn.fetchval(
        """INSERT INTO policy_decisions (source, operation, enforcement_point, topic, policy_version, subject, decision,
                                         reasons, requester, request_id, prev_hash, hash)
           VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8::jsonb,$9,$10,$11,$12) RETURNING id""",
        source,
        operation,
        point,
        topic,
        policy_version,
        json.dumps(rec["subject"]),
        decision,
        json.dumps(rec["reasons"]),
        requester,
        request_id,
        prev,
        h,
    )


async def record(
    conn: asyncpg.Connection, d: Decision, *, point: str, subject: Optional[dict], requester: Optional[str], request_id: Optional[str], source: str = "engine"
) -> int:
    return await append(
        conn,
        source=source,
        operation=d.operation,
        point=point,
        topic=d.topic,
        policy_version=d.policy_version,
        decision=d.decision.value,
        reasons=[r.model_dump() for r in d.reasons],
        subject=subject,
        requester=requester,
        request_id=request_id,
    )


async def verify_chain(conn: asyncpg.Connection) -> dict:
    prev, n = GENESIS, 0
    async with conn.transaction():
        async for r in conn.cursor("SELECT * FROM policy_decisions ORDER BY id"):
            rec = {
                "source": r["source"],
                "operation": r["operation"],
                "enforcement_point": r["enforcement_point"],
                "topic": r["topic"],
                "policy_version": r["policy_version"],
                "subject": json.loads(r["subject"]),
                "decision": r["decision"],
                "reasons": json.loads(r["reasons"]),
                "requester": r["requester"],
                "request_id": r["request_id"],
            }
            if r["prev_hash"] != prev or r["hash"] != _digest(prev, rec):
                return {"ok": False, "checked": n, "first_bad_id": r["id"]}
            prev, n = r["hash"], n + 1
    return {"ok": True, "checked": n, "head": prev}
