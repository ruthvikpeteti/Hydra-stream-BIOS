"""Durable replication checkpoints (PostgreSQL). One row per (source cluster, target cluster, topic, partition),
so A->B and A->C progress independently, and B->C (after a promotion) has its own rows.

Contract: a checkpoint is advanced ONLY after the destination acknowledged the batch, and never moves backwards
within a generation (enforced here and, as a last line, by a trigger)."""

import asyncio
from dataclasses import dataclass
from typing import Optional

import asyncpg

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS replication_checkpoints (
    source_cluster                  text   NOT NULL,
    target_cluster                  text   NOT NULL,
    source_topic                    text   NOT NULL,
    source_partition                int    NOT NULL CHECK (source_partition >= 0),
    source_region                   text   NOT NULL,
    target_region                   text   NOT NULL,
    last_acknowledged_source_offset bigint NOT NULL,   -- -1: nothing acknowledged yet; the next offset to read is this + 1
    last_known_destination_offset   bigint NOT NULL,   -- destination offset of the last acknowledged record (-1: unknown)
    replication_generation          int    NOT NULL,
    policy_version                  int    NOT NULL,
    last_success_timestamp          timestamptz,
    updated_at                      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_cluster, target_cluster, source_topic, source_partition)
);
CREATE OR REPLACE FUNCTION ss_guard_checkpoint() RETURNS trigger AS $$
BEGIN
    IF NEW.replication_generation < OLD.replication_generation
       OR (NEW.replication_generation = OLD.replication_generation
           AND NEW.last_acknowledged_source_offset < OLD.last_acknowledged_source_offset) THEN
        RAISE EXCEPTION 'ss_replication: checkpoint may not move backwards (% -> %, generation % -> %)',
            OLD.last_acknowledged_source_offset, NEW.last_acknowledged_source_offset,
            OLD.replication_generation, NEW.replication_generation USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS ss_guard_checkpoint_t ON replication_checkpoints;
CREATE TRIGGER ss_guard_checkpoint_t BEFORE UPDATE ON replication_checkpoints
    FOR EACH ROW EXECUTE FUNCTION ss_guard_checkpoint();
"""

_UPSERT = """
INSERT INTO replication_checkpoints
    (source_cluster, target_cluster, source_topic, source_partition, source_region, target_region,
     last_acknowledged_source_offset, last_known_destination_offset, replication_generation, policy_version,
     last_success_timestamp)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
ON CONFLICT (source_cluster, target_cluster, source_topic, source_partition) DO UPDATE SET
    last_acknowledged_source_offset = EXCLUDED.last_acknowledged_source_offset,
    last_known_destination_offset   = EXCLUDED.last_known_destination_offset,
    replication_generation          = EXCLUDED.replication_generation,
    policy_version                  = EXCLUDED.policy_version,
    last_success_timestamp          = now(), updated_at = now()
WHERE EXCLUDED.replication_generation > replication_checkpoints.replication_generation
   OR (EXCLUDED.replication_generation = replication_checkpoints.replication_generation
       AND EXCLUDED.last_acknowledged_source_offset > replication_checkpoints.last_acknowledged_source_offset)
"""


@dataclass(frozen=True)
class Checkpoint:
    source_cluster: str
    target_cluster: str
    source_topic: str
    source_partition: int
    last_acknowledged_source_offset: int
    last_known_destination_offset: int
    replication_generation: int
    policy_version: int
    last_success_timestamp: Optional[object]

    @property
    def next_offset(self) -> int:
        return self.last_acknowledged_source_offset + 1


class CheckpointStore:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str, retries: int = 30) -> "CheckpointStore":
        for i in range(retries):
            try:
                pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5, command_timeout=10)
                async with pool.acquire() as c:
                    await c.execute("SELECT pg_advisory_lock(7002)")
                    try:
                        await c.execute(SCHEMA_SQL)
                    finally:
                        await c.execute("SELECT pg_advisory_unlock(7002)")
                return cls(pool)
            except (OSError, asyncpg.PostgresError):
                if i == retries - 1:
                    raise
                await asyncio.sleep(2)
        raise RuntimeError("unreachable")

    async def close(self) -> None:
        await self.pool.close()

    async def get(self, source_cluster: str, target_cluster: str, topic: str, partition: int) -> Optional[Checkpoint]:
        r = await self.pool.fetchrow(
            """SELECT * FROM replication_checkpoints WHERE source_cluster=$1 AND target_cluster=$2
               AND source_topic=$3 AND source_partition=$4""",
            source_cluster,
            target_cluster,
            topic,
            partition,
        )
        return self._of(r) if r else None

    async def advance(
        self,
        *,
        source_cluster: str,
        target_cluster: str,
        source_region: str,
        target_region: str,
        topic: str,
        partition: int,
        acked_source_offset: int,
        destination_offset: int,
        generation: int,
        policy_version: int,
    ) -> bool:
        """Call ONLY after the destination acknowledged. True if the stored checkpoint moved."""
        status = await self.pool.execute(
            _UPSERT,
            source_cluster,
            target_cluster,
            topic,
            partition,
            source_region,
            target_region,
            acked_source_offset,
            destination_offset,
            generation,
            policy_version,
        )
        return status.endswith(" 1")

    async def seed(self, rows: list[dict]) -> None:
        """Atomically write the starting checkpoints of a new flow (used by a cutover)."""
        async with self.pool.acquire() as c, c.transaction():
            for r in rows:
                await c.execute(
                    _UPSERT,
                    r["source_cluster"],
                    r["target_cluster"],
                    r["topic"],
                    r["partition"],
                    r["source_region"],
                    r["target_region"],
                    r["acked_source_offset"],
                    r["destination_offset"],
                    r["generation"],
                    r["policy_version"],
                )

    async def for_topic(self, topic: str) -> list[Checkpoint]:
        rows = await self.pool.fetch("SELECT * FROM replication_checkpoints WHERE source_topic=$1 ORDER BY 1,2,4", topic)
        return [self._of(r) for r in rows]

    @staticmethod
    def _of(r) -> Checkpoint:
        return Checkpoint(
            r["source_cluster"],
            r["target_cluster"],
            r["source_topic"],
            r["source_partition"],
            r["last_acknowledged_source_offset"],
            r["last_known_destination_offset"],
            r["replication_generation"],
            r["policy_version"],
            r["last_success_timestamp"],
        )
