"""Kafka consumer manager.

Reads are stateless and offset-addressed: each request creates a short-lived consumer, `assign`s the
partitions (no group rebalancing), seeks, reads, and always stops it. A semaphore bounds concurrent
consumers. Resumability comes from returned `next_offsets` (client-tracked) and, optionally, offsets
committed per `consumer_id` in Kafka (`ss-gw.<consumer_id>` group).
"""

import asyncio
import json
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from .config import Settings
from .errors import ApiError


def group_for(consumer_id: str) -> str:
    return f"ss-gw.{consumer_id}"


class ConsumerManager:
    def __init__(self, s: Settings):
        self._s = s
        self._sem = asyncio.Semaphore(s.max_consumers)

    @asynccontextmanager
    async def _consumer(self, group_id: str | None = None):
        try:
            await asyncio.wait_for(self._sem.acquire(), 5)
        except asyncio.TimeoutError as exc:
            raise ApiError(429, "RATE_LIMITED", "too many concurrent reads", retryable=True) from exc
        c = AIOKafkaConsumer(
            bootstrap_servers=self._s.kafka_bootstrap,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            isolation_level="read_committed",
            **self._s.kafka_client_kwargs,
        )
        try:
            try:
                await asyncio.wait_for(c.start(), 20)
            except (KafkaError, asyncio.TimeoutError) as exc:
                raise ApiError(503, "LOCAL_DURABILITY_UNAVAILABLE", "Kafka is not reachable", retryable=True) from exc
            yield c
        finally:
            try:
                await asyncio.shield(c.stop())
            except Exception:  # noqa: BLE001
                pass
            self._sem.release()

    @staticmethod
    def _tps(topic: str, partitions: list[int]) -> list[TopicPartition]:
        return [TopicPartition(topic, p) for p in partitions]

    async def offsets(self, topic: str, nparts: int) -> dict[int, dict[str, int]]:
        async with self._consumer() as c:
            tps = self._tps(topic, list(range(nparts)))
            b, e = await c.beginning_offsets(tps), await c.end_offsets(tps)
            return {tp.partition: {"start_offset": b[tp], "end_offset": e[tp]} for tp in tps}

    async def read(
        self,
        topic: str,
        nparts: int,
        *,
        partition: int | None,
        offset: int | None,
        limit: int,
        wait_ms: int,
        consumer_id: str | None,
        commit: bool,
        region: str,
    ) -> dict:
        parts = [partition] if partition is not None else list(range(nparts))
        async with self._consumer(group_for(consumer_id) if consumer_id else None) as c:
            tps = self._tps(topic, parts)
            c.assign(tps)
            begins, ends = await c.beginning_offsets(tps), await c.end_offsets(tps)
            for tp in tps:
                start = None
                if offset is not None:
                    start = offset
                elif consumer_id:
                    start = await c.committed(tp)
                if start is None:
                    start = begins[tp]
                if not begins[tp] <= start <= ends[tp]:
                    raise ApiError(
                        400,
                        "OFFSET_OUT_OF_RANGE",
                        "offset is outside the retained range",
                        topic=topic,
                        details={"partition": tp.partition, "start_offset": begins[tp], "end_offset": ends[tp], "requested": start},
                    )
                c.seek(tp, start)
            events: list[dict] = []
            deadline = asyncio.get_running_loop().time() + wait_ms / 1000
            while len(events) < limit:
                positions = {tp: await c.position(tp) for tp in tps}
                caught_up = all(positions[tp] >= ends[tp] for tp in tps)
                if caught_up:
                    if wait_ms <= 0 or asyncio.get_running_loop().time() >= deadline:
                        break
                    ends = await c.end_offsets(tps)  # long-poll: look for new data
                    if all(positions[tp] >= ends[tp] for tp in tps):
                        await asyncio.sleep(0.2)
                        continue
                batch = await c.getmany(*tps, timeout_ms=500, max_records=limit - len(events))
                for tp in tps:  # per-partition order preserved
                    for r in batch.get(tp, []):
                        events.append(self._decode(r, region))
            next_offsets = {tp.partition: await c.position(tp) for tp in tps}
            if consumer_id and commit:
                await c.commit({tp: next_offsets[tp.partition] for tp in tps})
            has_more = any(next_offsets[tp.partition] < ends[tp] for tp in tps)
            return {"topic": topic, "events": events, "next_offsets": next_offsets, "end_offsets": {tp.partition: ends[tp] for tp in tps}, "has_more": has_more}

    async def commit(self, consumer_id: str, topic: str, nparts: int, offsets: dict[int, int]) -> None:
        async with self._consumer(group_for(consumer_id)) as c:
            tps = [tp for tp in self._tps(topic, sorted(offsets))]
            if any(tp.partition >= nparts or tp.partition < 0 for tp in tps):
                raise ApiError(400, "ENVELOPE_INVALID", "unknown partition", topic=topic)
            c.assign(tps)
            ends = await c.end_offsets(tps)
            for tp in tps:
                if not 0 <= offsets[tp.partition] <= ends[tp]:
                    raise ApiError(
                        400, "OFFSET_OUT_OF_RANGE", "offset beyond end of partition", topic=topic, details={"partition": tp.partition, "end_offset": ends[tp]}
                    )
            await c.commit({tp: offsets[tp.partition] for tp in tps})

    def _decode(self, r, region: str) -> dict:
        try:
            ev = json.loads(r.value)
        except (ValueError, TypeError):
            ev = {"corrupt": True}
        ev["source_cluster"] = self._s.cluster_id  # record coordinates in the cluster we read from
        ev["source_partition"] = r.partition
        ev["source_offset"] = r.offset
        return ev
