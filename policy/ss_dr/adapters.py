"""I/O adapters for the failover controller: policy engine, Kafka clusters, gateways/agents (probes). Thin on purpose;
every decision lives in model.py and controller.py."""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

log = logging.getLogger("ss.dr")


@dataclass
class Config:
    instance: str = "dr-1"
    admin_key: str = ""
    engine_url: str = "http://ss-policy-engine:8090"
    engine_admin_key: str = ""
    service_key: str = ""
    coord_endpoints: list = field(default_factory=list)
    kafka: dict = field(default_factory=dict)
    gateways: dict = field(default_factory=dict)
    agents: dict = field(default_factory=dict)
    order: tuple = ("region-a", "region-b", "region-c")
    expected_brokers: dict = field(default_factory=lambda: {"region-a": 3, "region-b": 1, "region-c": 1})
    min_isr: dict = field(default_factory=lambda: {"region-a": 2, "region-b": 1, "region-c": 1})
    auto: bool = True
    probe_interval_s: float = 1.0
    fail_after: int = 3
    recover_after: int = 3
    lease_ttl_ms: int = 6000
    drift_ms: int = 1000
    leader_ttl_s: int = 6
    allow_degraded_rpo: bool = False
    report_freshness_s: float = 15.0  # a backlog report older than this before the failure boundary is not a measurement
    capacity_limit: float = 0.95
    faults: bool = False
    failback_max_lag_records: int = 200
    failback_catchup_s: float = 90.0
    expected_cluster_ids: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "Config":
        e = os.environ.get
        j = lambda k, d: json.loads(e(k, "") or json.dumps(d))
        return cls(
            instance=e("SS_DR_INSTANCE", "dr-1"),
            admin_key=e("SS_DR_ADMIN_KEY", ""),
            engine_url=e("SS_ENGINE_URL", cls.engine_url).rstrip("/"),
            engine_admin_key=e("SS_ENGINE_ADMIN_KEY", ""),
            service_key=e("SS_SERVICE_KEY", ""),
            coord_endpoints=[x for x in e("SS_COORD_ENDPOINTS", "").split(",") if x],
            kafka=j("SS_KAFKA_BOOTSTRAPS", {}),
            gateways=j("SS_GATEWAY_URLS", {}),
            agents=j("SS_AGENT_URLS", {}),
            auto=e("SS_DR_AUTO", "1") == "1",
            probe_interval_s=float(e("SS_DR_PROBE_INTERVAL_S", "1")),
            fail_after=int(e("SS_DR_FAIL_AFTER", "3")),
            recover_after=int(e("SS_DR_RECOVER_AFTER", "3")),
            lease_ttl_ms=int(e("SS_LEASE_TTL_MS", "6000")),
            drift_ms=int(e("SS_DR_DRIFT_MS", "1000")),
            allow_degraded_rpo=e("SS_DR_ALLOW_DEGRADED_RPO", "0") == "1",
            faults=e("SS_DR_FAULTS", "1") == "1",
            capacity_limit=float(e("SS_DR_CAPACITY_LIMIT", "0.95")),
            report_freshness_s=float(e("SS_DR_REPORT_FRESHNESS_S", "15")),
            expected_cluster_ids=j("SS_EXPECTED_CLUSTER_IDS", {}),
            failback_catchup_s=float(e("SS_DR_FAILBACK_CATCHUP_S", "90")),
        )


class EngineError(Exception):
    def __init__(self, status: int, body):
        super().__init__(f"engine HTTP {status}: {str(body)[:300]}")
        self.status, self.body = status, body


class Engine:
    def __init__(self, cfg: Config):
        self._http = httpx.AsyncClient(base_url=cfg.engine_url, timeout=8.0)
        self._admin, self._service = cfg.engine_admin_key, cfg.service_key

    async def close(self) -> None:
        await self._http.aclose()

    async def _call(self, method: str, path: str, admin: bool = False, ok: tuple = (200, 201, 202), **kw):
        key = self._admin if admin else self._service
        try:
            r = await self._http.request(method, path, headers={"X-Service-Key": key}, **kw)
        except httpx.HTTPError as exc:
            raise EngineError(0, repr(exc)) from exc
        try:
            body = r.json()
        except ValueError:
            body = r.text
        if r.status_code not in ok:
            raise EngineError(r.status_code, body)
        return body

    async def topics(self) -> list[dict]:
        return (await self._call("GET", "/api/v1/replication/topics"))["topics"]

    async def topic(self, topic: str) -> dict:
        return await self._call("GET", f"/api/v1/replication/topics/{topic}")

    async def backlog(self, topic: Optional[str] = None) -> list[dict]:
        return (await self._call("GET", "/api/v1/replication/backlog", params={"topic": topic} if topic else None))["flows"]

    async def regions(self) -> list[dict]:
        return (await self._call("GET", "/api/v1/regions"))["regions"]

    async def put_region(self, region: dict) -> None:
        await self._call("PUT", f"/api/v1/regions/{region['region_id']}", admin=True, json=region)

    async def promote(self, topic: str, region: str, epoch: int, generation: int, reason: str, dry_run: bool = False, reseed: bool = False) -> tuple[int, dict]:
        """(status, body). 200 allowed (or dry-run allowed), 403 rejected with reasons. Anything else raises."""
        body = {"candidate_region": region, "reason": reason[:200], "epoch": epoch, "generation": generation, "dry_run": dry_run, "reseed": reseed}
        r = await self._http.post(f"/api/v1/replication/topics/{topic}/promote", json=body, headers={"X-Service-Key": self._admin})
        if r.status_code not in (200, 403):
            raise EngineError(r.status_code, r.text)
        return r.status_code, r.json()

    async def reseed(self, topic: str, region: str, partitions: int, dry_run: bool = False) -> Optional[dict]:
        """Re-seed `region` as a replica. None when the policy engine refuses (the region may not hold this topic): never an exception for a refusal."""
        r = await self._http.post(
            f"/api/v1/replication/topics/{topic}/reseed",
            json={"region": region, "partitions": partitions, "dry_run": dry_run},
            headers={"X-Service-Key": self._admin},
        )
        if r.status_code == 403:
            return None
        if r.status_code not in (200, 201, 202):
            raise EngineError(r.status_code, r.text)
        return r.json()

    async def replica_regions(self, topic: str) -> set[str]:
        """Regions that hold (or were assigned) a replica of the topic, from the placement records."""
        j = await self._call("GET", f"/api/v1/placement/{topic}")
        return {a["destination_region"] for a in j.get("assignments", []) if a["replica_status"] != "REVOKED"}

    async def audit(self, items: list[dict], source: str) -> None:
        try:
            await self._http.post("/api/v1/audit/decisions", json=items, headers={"X-Service-Key": self._service, "X-Audit-Source": source})
        except httpx.HTTPError as exc:
            log.warning("could not forward audit to the engine: %r", exc)


class Kafkas:
    """Admin clients per region, reused while healthy and dropped on any failure (a dead cluster must not wedge us)."""

    def __init__(self, cfg: Config):
        self._cfg = cfg
        self._admins: dict[str, AIOKafkaAdminClient] = {}
        self._lock = {r: asyncio.Lock() for r in cfg.kafka}

    async def _admin(self, region: str) -> AIOKafkaAdminClient:
        async with self._lock[region]:
            a = self._admins.get(region)
            if a is None:
                a = AIOKafkaAdminClient(bootstrap_servers=self._cfg.kafka[region], request_timeout_ms=3000, client_id=f"ss-dr-{self._cfg.instance}")
                await asyncio.wait_for(a.start(), 6)
                self._admins[region] = a
            return a

    async def drop(self, region: str) -> None:
        a = self._admins.pop(region, None)
        if a is not None:
            try:
                await asyncio.wait_for(a.close(), 3)
            except Exception:  # noqa: BLE001
                pass

    async def call(self, region: str, fn, timeout: float = 5.0):
        try:
            a = await self._admin(region)
            return await asyncio.wait_for(fn(a), timeout)
        except Exception:
            await self.drop(region)
            raise

    async def cluster(self, region: str) -> dict:
        return await self.call(region, lambda a: a.describe_cluster())

    async def partitions(self, region: str, topics: list[str]) -> dict[str, list[dict]]:
        if not topics:
            return {}
        res = await self.call(region, lambda a: a.describe_topics(topics))
        return {t.get("topic") or t.get("name"): t["partitions"] for t in res if t.get("error_code", 0) == 0}

    async def end_offsets(self, region: str, topic: str, n: int) -> dict[int, int]:
        c = AIOKafkaConsumer(bootstrap_servers=self._cfg.kafka[region], enable_auto_commit=False, request_timeout_ms=4000, group_id=None)
        await asyncio.wait_for(c.start(), 8)
        try:
            tps = [TopicPartition(topic, i) for i in range(n)]
            res = await asyncio.wait_for(c.end_offsets(tps), 6)
            return {tp.partition: o for tp, o in res.items()}
        finally:
            await c.stop()

    # ---- cluster-level write fence ----------------------------------------------------------------------------
    @staticmethod
    def _dynamic(entries) -> dict:
        out = {}
        for e in entries:
            name, value = e[0], e[1]
            dyn = (not e[3]) if isinstance(e[3], bool) else (e[3] == 1)  # v0: is_default flag; v1+: config_source == DYNAMIC_TOPIC_CONFIG
            if dyn and value is not None:
                out[name] = value
        return out

    async def topic_config(self, region: str, topic: str) -> dict:
        res = await self.call(region, lambda a: a.describe_configs([ConfigResource(ConfigResourceType.TOPIC, topic)]))
        for r in res:
            for err, _msg, _rtype, name, entries in r.resources:
                if name == topic and err == 0:
                    return self._dynamic(entries)
        return {}

    FENCE_KEY, FENCE_VALUE = "max.message.bytes", "1"

    async def set_fence(self, region: str, topic: str, on: bool) -> bool:
        """Cluster-level write fence: a topic whose max.message.bytes is 1 refuses EVERY produce request (a record batch alone is
        larger), whoever sends it: the gateway, the replication receiver or a producer that bypasses both. (min.insync.replicas
        above the replication factor was tried first and does NOT block acks=all writes in Kafka 3.9: it is not used.)
        Returns True if the topic's config changed."""
        cur = await self.topic_config(region, topic)
        if on:
            if cur.get(self.FENCE_KEY) == self.FENCE_VALUE:
                return False
            cur[self.FENCE_KEY] = self.FENCE_VALUE
        else:
            if self.FENCE_KEY not in cur:
                return False
            cur.pop(self.FENCE_KEY)
        await self.call(region, lambda a: a.alter_configs([ConfigResource(ConfigResourceType.TOPIC, topic, configs=cur)]))
        return True

    async def quarantine(self, region: str, topic: str, partitions: int, acked: dict[int, int]) -> dict:
        """Copy every record of `topic` in `region` beyond the offsets the replication acknowledged (the divergent suffix a restored
        old region holds and nobody else does) into `_ss.quarantine.<topic>` IN THAT REGION, then return what was kept. Nothing is
        merged into the live history (INV-9); reconciliation is a human decision."""
        from aiokafka import AIOKafkaProducer

        qname = f"_ss.quarantine.{topic}"[:240]
        try:
            await self.call(region, lambda a: a.create_topics([NewTopic(qname, 1, 1)]))
        except Exception as exc:  # noqa: BLE001
            if "TopicAlreadyExists" not in repr(exc) and "TOPIC_ALREADY_EXISTS" not in repr(exc):
                raise
        ends = await self.end_offsets(region, topic, partitions)
        kept, ids = 0, []  # type: tuple[int, list[str]]
        c = AIOKafkaConsumer(bootstrap_servers=self._cfg.kafka[region], enable_auto_commit=False, request_timeout_ms=4000, group_id=None)
        prod = AIOKafkaProducer(bootstrap_servers=self._cfg.kafka[region], acks="all", request_timeout_ms=8000)
        await asyncio.wait_for(c.start(), 8)
        await asyncio.wait_for(prod.start(), 8)
        try:
            for p in range(partitions):
                start, end = acked.get(p, -1) + 1, ends.get(p, 0)
                if end <= start:
                    continue
                tp = TopicPartition(topic, p)
                c.assign([tp])
                c.seek(tp, start)
                while await c.position(tp) < end:
                    for _tp, recs in (await c.getmany(tp, timeout_ms=1500)).items():
                        for r in recs:
                            if r.offset >= end:
                                continue
                            hdr = list(r.headers or []) + [
                                ("ss-quarantine-source-partition", str(p).encode()),
                                ("ss-quarantine-source-offset", str(r.offset).encode()),
                            ]
                            await prod.send_and_wait(qname, value=r.value, key=r.key, headers=hdr)
                            kept += 1
                            if len(ids) < 5:
                                try:
                                    ids.append(json.loads(r.value)["event_id"])
                                except Exception:  # noqa: BLE001
                                    pass
        finally:
            await c.stop()
            await prod.stop()
        return {"records": kept, "topic": qname, "sample_event_ids": ids, "acked_through": {str(k): v for k, v in acked.items()}}

    async def recreate_topic(self, region: str, topic: str, partitions: int) -> int:
        """Delete this region's copy of the topic and create it EMPTY again (same partition count and replication factor)."""
        parts = (await self.partitions(region, [topic])).get(topic)
        rf = len(parts[0]["replicas"]) if parts else 1
        if parts:
            await self.call(region, lambda a: a.delete_topics([topic]))
            for _ in range(60):
                await asyncio.sleep(0.5)
                if topic not in await self.partitions(region, [topic]):
                    break
        for attempt in range(20):
            try:
                await self.call(region, lambda a: a.create_topics([NewTopic(topic, partitions, rf)]))
                return rf
            except Exception:  # noqa: BLE001
                if attempt == 19:
                    raise
                await asyncio.sleep(1)
        return rf

    async def tail_ids(self, region: str, topic: str, p: int, k: int) -> list[str]:
        """event_ids of the last k records of a partition (consistency evidence; payloads are never returned)."""
        c = AIOKafkaConsumer(bootstrap_servers=self._cfg.kafka[region], enable_auto_commit=False, request_timeout_ms=4000, group_id=None)
        await asyncio.wait_for(c.start(), 8)
        try:
            tp = TopicPartition(topic, p)
            c.assign([tp])
            end = (await c.end_offsets([tp]))[tp]
            begin = (await c.beginning_offsets([tp]))[tp]
            start = max(begin, end - k)
            c.seek(tp, start)
            out: list[str] = []
            while await c.position(tp) < end:
                for _tp, recs in (await c.getmany(tp, timeout_ms=1500)).items():
                    for r in recs:
                        try:
                            out.append(json.loads(r.value)["event_id"])
                        except Exception:  # noqa: BLE001
                            out.append("?")
            return out
        finally:
            await c.stop()

    async def close(self) -> None:
        for r in list(self._admins):
            await self.drop(r)


async def http_ok(client: httpx.AsyncClient, url: str) -> bool:
    try:
        return (await client.get(url, timeout=2.0)).status_code == 200
    except httpx.HTTPError:
        return False


def now_ms() -> int:
    return int(time.time() * 1000)
