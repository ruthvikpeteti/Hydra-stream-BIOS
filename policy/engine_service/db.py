"""PostgreSQL access (asyncpg). Rows <-> Pydantic models."""

import json
from pathlib import Path
from typing import Optional

import asyncpg

from ss_policy.models import Link, NetworkPath, Region, ReplicaAssignment, StorageCapacity, TopicPolicy, TopicPolicyIn

SCHEMA = Path(__file__).with_name("schema.sql")
POLICY_COLS = (
    "topic",
    "classification",
    "legal_zone",
    "home_region",
    "allowed_storage_regions",
    "allowed_processing_regions",
    "allowed_transit_zones",
    "replication_factor",
    "regional_replica_count",
    "preferred_regions",
    "excluded_regions",
    "maximum_replication_lag_ms",
    "durability_mode",
)


async def connect(dsn: str, retries: int = 30) -> asyncpg.Pool:
    import asyncio

    for i in range(retries):
        try:
            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10, command_timeout=10)
            async with pool.acquire() as c:
                await c.execute("SELECT pg_advisory_lock(7000)")  # one process applies the schema at a time
                try:
                    await c.execute(SCHEMA.read_text())
                    from ss_replication.checkpoints import SCHEMA_SQL as CHECKPOINT_SQL

                    await c.execute(CHECKPOINT_SQL)
                finally:
                    await c.execute("SELECT pg_advisory_unlock(7000)")
            return pool
        except (OSError, asyncpg.PostgresError):
            if i == retries - 1:
                raise
            await asyncio.sleep(2)


def region_of(r: asyncpg.Record) -> Region:
    return Region(
        region_id=r["region_id"],
        legal_zone=r["legal_zone"],
        availability_status=r["availability_status"],
        storage_capacity=StorageCapacity(total_bytes=r["storage_total_bytes"], used_bytes=r["storage_used_bytes"]),
        processing_capabilities=list(r["processing_capabilities"]),
        supported_transit_zones=list(r["supported_transit_zones"]),
        failure_domain=r["failure_domain"],
        compute_headroom=r["compute_headroom"],
        max_replication_factor=r["max_replication_factor"],
        cluster_id=r["cluster_id"],
    )


def link_of(r: asyncpg.Record) -> Link:
    return Link(from_region=r["from_region"], to_region=r["to_region"], transit_zones=list(r["transit_zones"]), latency_ms=r["latency_ms"], status=r["status"])


def policy_of(r: asyncpg.Record) -> TopicPolicy:
    d = {k: (list(r[k]) if isinstance(r[k], (list, tuple)) else r[k]) for k in POLICY_COLS}
    return TopicPolicy(**d, policy_version=r["policy_version"])


def assignment_of(r: asyncpg.Record) -> ReplicaAssignment:
    return ReplicaAssignment(
        topic=r["topic"],
        partition=r["partition"],
        source_region=r["source_region"],
        destination_region=r["destination_region"],
        replica_status=r["replica_status"],
        policy_version=r["policy_version"],
        assigned_at=r["assigned_at"].isoformat(),
    )


async def load_world(c: asyncpg.Connection) -> tuple[dict[str, Region], dict[tuple[str, str], Link]]:
    regions = {r["region_id"]: region_of(r) for r in await c.fetch("SELECT * FROM regions")}
    links = {(r["from_region"], r["to_region"]): link_of(r) for r in await c.fetch("SELECT * FROM links")}
    return regions, links


async def get_policy(c: asyncpg.Connection, topic: str) -> Optional[TopicPolicy]:
    r = await c.fetchrow("SELECT * FROM topic_policies WHERE topic=$1", topic)
    return policy_of(r) if r else None


async def all_policies(c: asyncpg.Connection) -> dict[str, TopicPolicy]:
    return {r["topic"]: policy_of(r) for r in await c.fetch("SELECT * FROM topic_policies")}


async def get_assignments(c: asyncpg.Connection, topic: str) -> list[ReplicaAssignment]:
    return [assignment_of(r) for r in await c.fetch("SELECT * FROM replica_assignments WHERE topic=$1 ORDER BY partition, destination_region", topic)]


async def bump_bundle(c: asyncpg.Connection) -> int:
    return await c.fetchval("UPDATE registry_meta SET bundle_version = bundle_version + 1 WHERE id=1 RETURNING bundle_version")


async def bundle_version(c: asyncpg.Connection) -> int:
    return await c.fetchval("SELECT bundle_version FROM registry_meta WHERE id=1")


async def insert_policy(c: asyncpg.Connection, p: TopicPolicyIn, version: int, actor: str, update: bool) -> TopicPolicy:
    vals = [getattr(p, k) for k in POLICY_COLS]
    vals = [v.value if hasattr(v, "value") else v for v in vals]
    if update:
        others = [(k, v) for k, v in zip(POLICY_COLS, vals) if k != "topic"]
        sets = ", ".join(f"{k}=${j + 2}" for j, (k, _) in enumerate(others))
        args = [p.topic] + [v for _, v in others]
        n = len(args)
        await c.execute(
            f"UPDATE topic_policies SET {sets}, policy_version=${n + 1}, updated_at=now(), updated_by=${n + 2} WHERE topic=$1", *args, version, actor
        )
    else:
        ph = ", ".join(f"${i + 1}" for i in range(len(POLICY_COLS)))
        await c.execute(
            f"INSERT INTO topic_policies ({', '.join(POLICY_COLS)}, policy_version, updated_by) VALUES ({ph}, ${len(POLICY_COLS) + 1}, ${len(POLICY_COLS) + 2})",
            *vals,
            version,
            actor,
        )
    pol = await get_policy(c, p.topic)
    await c.execute(
        "INSERT INTO topic_policy_versions (topic, policy_version, body, actor) VALUES ($1,$2,$3::jsonb,$4)",
        p.topic,
        version,
        json.dumps(pol.model_dump(mode="json")),
        actor,
    )
    return pol


def path_of(r: asyncpg.Record) -> NetworkPath:
    return NetworkPath(
        path_id=r["path_id"],
        source_region=r["source_region"],
        target_region=r["target_region"],
        kind=r["kind"],
        via_regions=list(r["via_regions"]),
        transit_zones=list(r["transit_zones"]),
        endpoint_url=r["endpoint_url"],
        encrypted=r["encrypted"],
        cost=r["cost"],
        fallback=r["fallback"],
        enabled=r["enabled"],
    )


async def all_paths(c: asyncpg.Connection) -> list[NetworkPath]:
    return [path_of(r) for r in await c.fetch("SELECT * FROM network_paths ORDER BY path_id")]


async def upsert_path(c: asyncpg.Connection, p: NetworkPath) -> None:
    await c.execute(
        """INSERT INTO network_paths (path_id, source_region, target_region, kind, via_regions, transit_zones, endpoint_url, encrypted, cost, fallback, enabled)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
           ON CONFLICT (path_id) DO UPDATE SET source_region=$2, target_region=$3, kind=$4, via_regions=$5, transit_zones=$6,
               endpoint_url=$7, encrypted=$8, cost=$9, fallback=$10, enabled=$11, updated_at=now()""",
        p.path_id,
        p.source_region,
        p.target_region,
        p.kind,
        p.via_regions,
        p.transit_zones,
        p.endpoint_url,
        p.encrypted,
        p.cost,
        p.fallback,
        p.enabled,
    )
