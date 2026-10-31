import asyncio

from aiokafka import AIOKafkaConsumer, TopicPartition

from .config import Settings


async def read_compacted(s: Settings, topic: str, partitions: int) -> list:
    """Read every record currently in `topic` (used to load small control topics at startup)."""
    c = AIOKafkaConsumer(
        bootstrap_servers=s.kafka_bootstrap,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
        **s.kafka_client_kwargs,
    )
    await c.start()
    try:
        tps = [TopicPartition(topic, p) for p in range(partitions)]
        c.assign(tps)
        await c.seek_to_beginning(*tps)
        ends = await c.end_offsets(tps)
        out: list = []
        deadline = asyncio.get_running_loop().time() + 60
        while any([await c.position(tp) < ends[tp] for tp in tps]):
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(f"timed out reading {topic}")
            for recs in (await c.getmany(timeout_ms=1000)).values():
                out.extend(recs)
        return out
    finally:
        await c.stop()
