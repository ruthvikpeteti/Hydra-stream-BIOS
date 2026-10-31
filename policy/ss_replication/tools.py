"""Operator/test tools that run inside a region's container (they reach that region's Kafka):
python -m ss_replication.tools dump --topic T        JSON lines of every record with headers
python -m ss_replication.tools produce-native --topic T --count N --epoch E --generation G --cluster kafka-b
python -m ss_replication.tools idempotent-demo --topic T   process a topic once per event_id and compare with naive processing"""

import argparse
import asyncio
import json
import os
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

from .idempotent import IdempotentConsumer, find_duplicates

BOOT = os.environ.get("SS_KAFKA_BOOTSTRAP", "localhost:9092")


async def partitions_of(topic: str) -> list[int]:
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOT)
    await admin.start()
    try:
        res = (await admin.describe_topics([topic]))[0]
        return sorted(p["partition"] for p in res.get("partitions", [])) if res.get("error_code", 0) == 0 else []
    finally:
        await admin.close()


async def read_all(topic: str) -> list[dict]:
    parts = await partitions_of(topic)
    if not parts:
        return []  # the topic does not exist in this cluster
    c = AIOKafkaConsumer(bootstrap_servers=BOOT, group_id=None, enable_auto_commit=False, auto_offset_reset="earliest")
    await c.start()
    try:
        tps = [TopicPartition(topic, p) for p in parts]
        c.assign(tps)
        await c.seek_to_beginning(*tps)
        ends = await c.end_offsets(tps)
        out = []
        while any([await c.position(tp) < ends[tp] for tp in tps]):
            for tp, recs in (await c.getmany(*tps, timeout_ms=1000)).items():
                for r in recs:
                    try:
                        val = json.loads(r.value)
                    except ValueError:
                        val = None
                    out.append(
                        {
                            "partition": r.partition,
                            "offset": r.offset,
                            "timestamp": r.timestamp,
                            "key": r.key.decode() if r.key else None,
                            "value": val,
                            "headers": {k: v.decode() for k, v in (r.headers or [])},
                        }
                    )
        return sorted(out, key=lambda x: (x["partition"], x["offset"]))
    finally:
        await c.stop()


async def produce_native(topic: str, count: int, epoch: int, generation: int, cluster: str, region: str, start: int) -> None:
    p = AIOKafkaProducer(bootstrap_servers=BOOT, acks="all", enable_idempotence=True)
    await p.start()
    try:
        parts = sorted(await p.partitions_for(topic))
        for i in range(count):
            eid = str(uuid.uuid4())
            env = {
                "envelope_version": 1,
                "event_id": eid,
                "topic": topic,
                "partition_key": f"native-{i % 7}",
                "payload": {"native": start + i},
                "event_timestamp": 1_700_000_000_000 + i,
                "origin_region": region,
                "origin_cluster": cluster,
                "leadership_epoch": epoch,
                "replication_generation": generation,
                "policy_revision": 1,
            }
            await p.send_and_wait(
                topic, json.dumps(env, separators=(",", ":")).encode(), key=str(env["partition_key"]).encode(), partition=parts[i % len(parts)]
            )
        print(json.dumps({"produced": count}))
    finally:
        await p.stop()


async def idempotent_demo(topic: str) -> dict:
    recs = [r for r in await read_all(topic) if r["value"]]
    naive = sum(r["value"]["payload"].get("amount", 1) for r in recs)
    ledger: list[int] = []
    c = IdempotentConsumer(lambda r: ledger.append(r["value"]["payload"].get("amount", 1)))
    for r in recs:
        c.consume(r)
    dups = find_duplicates(recs)
    return {
        "deliveries": len(recs),
        "unique_events": c.processed,
        "duplicate_deliveries": c.skipped_duplicates,
        "events_delivered_more_than_once": len(dups),
        "naive_total": naive,
        "idempotent_total": sum(ledger),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("dump")
    d.add_argument("--topic", required=True)
    n = sub.add_parser("produce-native")
    n.add_argument("--topic", required=True)
    n.add_argument("--count", type=int, default=10)
    n.add_argument("--epoch", type=int, required=True)
    n.add_argument("--generation", type=int, required=True)
    n.add_argument("--cluster", required=True)
    n.add_argument("--region", required=True)
    n.add_argument("--start", type=int, default=0)
    i = sub.add_parser("idempotent-demo")
    i.add_argument("--topic", required=True)
    a = ap.parse_args()
    if a.cmd == "dump":
        for r in asyncio.run(read_all(a.topic)):
            print(json.dumps(r))
    elif a.cmd == "produce-native":
        asyncio.run(produce_native(a.topic, a.count, a.epoch, a.generation, a.cluster, a.region, a.start))
    else:
        print(json.dumps(asyncio.run(idempotent_demo(a.topic))))


if __name__ == "__main__":
    main()
