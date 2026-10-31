"""Topic metadata service: policy records (compacted topic `_ss.topic-policy`) + Kafka topic administration."""

import asyncio
import hashlib
import json
import logging
import time
from typing import Any

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError, TopicAlreadyExistsError

from .config import Settings
from .errors import ApiError
from .policy_client import PolicyClient, unavailable
from .kafka_util import read_compacted
from .producer import ProducerManager
from .registry import Registry, apply_defaults, validate_policy

POLICY_TOPIC = "_ss.topic-policy"
POLICY_PARTITIONS = 1
log = logging.getLogger("ss.topics")


class TopicService:
    def __init__(self, s: Settings, reg: Registry, producer: ProducerManager, policy: PolicyClient | None = None):
        self._s, self._reg, self._producer, self._policy = s, reg, producer, policy
        self._confirmed: dict[str, float] = {}  # topic -> monotonic time the engine last confirmed its policy
        self._bp: dict[str, tuple[float, dict | None]] = {}  # topic -> (checked at, blocking info or None)
        self._admin: Any = None  # AIOKafkaAdminClient, set by start()
        self._cache: dict[str, dict] = {}
        self._reload_lock = asyncio.Lock()
        self._last_reload = 0.0
        self._create_lock = asyncio.Lock()

    # ---- lifecycle -------------------------------------------------------------------------------
    async def start(self) -> None:
        self._admin = AIOKafkaAdminClient(bootstrap_servers=self._s.kafka_bootstrap, **self._s.kafka_client_kwargs)
        await self._admin.start()
        rf = min(3, self._reg.regions[self._s.region]["capacity"]["max_local_rf"])
        isr = rf // 2 + 1
        await self._ensure(POLICY_TOPIC, POLICY_PARTITIONS, rf, {"cleanup.policy": "compact", "min.insync.replicas": str(isr)})
        from .idempotency import PARTITIONS as IP, TOPIC as IT

        await self._ensure(
            IT, IP, rf, {"cleanup.policy": "compact,delete", "min.insync.replicas": str(isr), "retention.ms": str(self._s.idempotency_ttl_s * 1000)}
        )
        await self.reload(force=True)

    async def stop(self) -> None:
        if self._admin:
            await self._admin.close()

    async def _ensure(self, name: str, partitions: int, rf: int, cfg: dict) -> None:
        for attempt in range(30):
            try:
                await self._admin.create_topics([NewTopic(name, partitions, rf, topic_configs=cfg)])
                log.info("created internal topic %s", name)
                break
            except TopicAlreadyExistsError:
                break
            except KafkaError as exc:
                if attempt == 29:
                    raise
                log.warning("waiting for Kafka (%r)", exc)
                await asyncio.sleep(2)
        await self._producer.warm(name)

    async def kafka_reachable(self) -> bool:
        try:
            await asyncio.wait_for(self._admin.describe_cluster(), 3)
            return True
        except Exception:  # noqa: BLE001
            return False

    # ---- policy cache ----------------------------------------------------------------------------
    async def reload(self, force: bool = False) -> None:
        async with self._reload_lock:
            if not force and time.monotonic() - self._last_reload < 1.0:
                return
            fresh: dict[str, dict] = {}
            for r in await read_compacted(self._s, POLICY_TOPIC, POLICY_PARTITIONS):
                if r.value is not None:
                    fresh[r.key.decode()] = json.loads(r.value)
            self._cache = fresh
            self._last_reload = time.monotonic()

    async def get(self, name: str) -> dict:
        rec = self._cache.get(name)
        if rec is None:
            await self.reload()  # another gateway instance may have created it
            rec = self._cache.get(name)
        if rec is None:
            rec = await self._adopt(name)
        if rec is None:
            raise ApiError(404, "NOT_FOUND", "unknown topic", topic=name)
        return rec

    async def _adopt(self, name: str) -> dict | None:
        """A topic that was created through another region's gateway and replicated here (Phase 6): its gateway record lives in
        the OTHER region's `_ss.topic-policy`, so rebuild it from the policy engine (the authority) plus the replica topic
        that really exists in this cluster. Only for a region the policy allows to store AND process the topic."""
        if self._policy is None:
            return None
        try:
            status, body = await self._policy.get_policy(name)
            if status != 200:
                return None
            ep = body["policy"]
            if self._s.region not in ep["allowed_storage_regions"] or self._s.region not in ep["allowed_processing_regions"]:
                return None
            desc = (await self._admin.describe_topics([name]))[0]
            if desc.get("error_code", 0) != 0 or not desc.get("partitions"):
                return None
        except (ApiError, KafkaError, asyncio.TimeoutError):
            return None
        replicas = ep["regional_replica_count"]
        others = [r for r in ep.get("preferred_regions", []) if r != self._s.region][:replicas]
        pol = apply_defaults(
            {
                "classification": ep["classification"],
                "legal_zone": ep["legal_zone"],
                "allowed_storage_regions": ep["allowed_storage_regions"],
                "allowed_processing_regions": ep["allowed_processing_regions"],
                "allowed_transit_zones": ep.get("allowed_transit_zones", []),
                "durability_mode": ep["durability_mode"],
                "regional_replica_count": replicas,
                "local_replication_factor": len(desc["partitions"][0]["replicas"]),
                "failover_priority": [self._s.region] + others[:replicas],
                "max_tolerated_replication_lag_ms": ep.get("maximum_replication_lag_ms"),
            },
            self._reg,
            self._s.region,
        )
        rec = {
            "topic": name,
            "partitions": len(desc["partitions"]),
            "revision": 1,
            "policy": pol,
            "policy_version": body["policy_version"],
            "replica_plan": [],
            "adopted_from_engine": True,
            "policy_hash": hashlib.sha256(json.dumps(pol, sort_keys=True).encode()).hexdigest()[:16],
            "created_at": int(time.time() * 1000),
            "origin_region": ep["home_region"],
        }
        self._cache[name] = rec
        self._confirmed[name] = time.monotonic()
        log.info("adopted replicated topic %s from the policy engine (region %s)", name, self._s.region)
        return rec

    # ---- create ----------------------------------------------------------------------------------
    async def create(self, req: dict) -> dict:
        name, partitions = req.pop("name"), req.pop("partitions")
        policy = apply_defaults(req, self._reg, self._s.region)
        validate_policy(policy, self._reg, self._s.region)
        async with self._create_lock:
            await self.reload(force=True)
            if name in self._cache:
                raise ApiError(409, "TOPIC_EXISTS", "topic already exists", topic=name)
            engine = await self._engine_create(name, policy)  # the engine decides BEFORE anything is created
            try:
                await self._admin.create_topics(
                    [
                        NewTopic(
                            name,
                            partitions,
                            policy["local_replication_factor"],
                            topic_configs={"min.insync.replicas": str(policy["local_min_insync_replicas"])},
                        )
                    ]
                )
            except TopicAlreadyExistsError as exc:
                raise ApiError(
                    409, "TOPIC_EXISTS", "a Kafka topic with this name exists without a policy record; remove it manually before reusing the name", topic=name
                ) from exc
            except KafkaError as exc:
                raise ApiError(503, "LOCAL_DURABILITY_UNAVAILABLE", f"could not create topic: {exc!r}", topic=name, retryable=True) from exc
            canonical = json.dumps(policy, sort_keys=True)
            rec = {
                "topic": name,
                "partitions": partitions,
                "revision": 1,
                "policy": policy,
                **engine,
                "policy_hash": hashlib.sha256(canonical.encode()).hexdigest()[:16],
                "created_at": int(time.time() * 1000),
                "origin_region": self._s.region,
            }
            await self._producer.send(POLICY_TOPIC, name.encode(), json.dumps(rec).encode())
            self._cache[name] = rec
            self._confirmed[name] = time.monotonic()
        await self._producer.warm(name)
        return rec

    # ---- policy engine ---------------------------------------------------------------------------------
    async def _engine_create(self, name: str, policy: dict) -> dict:
        if self._policy is None:
            return {}
        payload = {
            "topic": name,
            "classification": policy["classification"],
            "legal_zone": policy["legal_zone"],
            "home_region": self._s.region,
            "allowed_storage_regions": policy["allowed_storage_regions"],
            "allowed_processing_regions": policy["allowed_processing_regions"],
            "allowed_transit_zones": policy["allowed_transit_zones"],
            "replication_factor": policy["local_replication_factor"],
            "regional_replica_count": policy["regional_replica_count"],
            "preferred_regions": policy["failover_priority"][1:],
            "excluded_regions": [],
            "maximum_replication_lag_ms": policy["max_tolerated_replication_lag_ms"],
            "durability_mode": policy["durability_mode"],
        }
        status, body = await self._policy.create_policy(payload)
        if status == 201:
            return {
                "policy_version": body["policy_version"],
                "replica_plan": [c["region"] for c in body.get("details", {}).get("placement", {}).get("chosen", [])],
            }
        kind, reasons = body.get("decision"), body.get("reasons", [])
        details = {"decision": kind, "policy_version": body.get("policy_version"), "reasons": reasons}
        if any(r.get("code") == "POLICY_EXISTS" for r in reasons):
            raise ApiError(409, "TOPIC_EXISTS", "a policy already exists for this topic name", topic=name, details=details)
        if kind == "INSUFFICIENT_ELIGIBLE_REGIONS":
            raise ApiError(409, "INSUFFICIENT_ELIGIBLE_REGIONS", "not enough regions pass the policy's hard constraints", topic=name, details=details)
        raise ApiError(403 if status == 403 else 422, "POLICY_VIOLATION", "the policy engine rejected this topic policy", topic=name, details=details)

    async def backpressure(self, rec: dict) -> dict | None:
        """Blocking info if a replication flow of this topic reports a HARD unreplicated backlog and the topic's policy says
        REJECT_WRITES; otherwise None. Checked at most once a second per topic; if the engine cannot be reached the last
        known answer is kept (the policy-staleness rules of current() still apply to restricted topics)."""
        pol = rec["policy"]
        if self._policy is None or pol.get("regional_replica_count", 0) < 1 or pol.get("lag_breach_action") != "REJECT_WRITES":
            return None
        topic, now = rec["topic"], time.monotonic()
        cached = self._bp.get(topic)
        if cached and now - cached[0] < 1.0:
            return cached[1]
        try:
            status, body = await self._policy.backpressure(topic)
            info = body if status == 200 and body.get("blocked") else None
        except ApiError:
            return cached[1] if cached else None
        self._bp[topic] = (now, info)
        return info

    async def current(self, rec: dict) -> dict:
        """The topic record with its policy confirmed against the engine as often as the classification demands.
        RESTRICTED: confirmed within `restricted_policy_ttl_s`, otherwise POLICY_UNAVAILABLE (fail closed).
        Other classes use the cached policy and refresh opportunistically."""
        topic, restricted = rec["topic"], rec["policy"]["classification"] == "RESTRICTED"
        if self._policy is None:
            return rec
        age = time.monotonic() - self._confirmed.get(topic, float("-inf"))
        if age <= (self._s.restricted_policy_ttl_s if restricted else 300.0):
            return rec
        if "policy_version" not in rec:
            if restricted:
                raise unavailable("restricted topic has no engine policy record")
            return rec  # legacy non-restricted topic created before the engine existed
        try:
            status, body = await self._policy.get_policy(topic)
            if status != 200:
                raise unavailable(f"policy lookup returned {status}")
        except ApiError:
            if restricted:
                raise
            return rec
        self._confirmed[topic] = time.monotonic()
        ep = body["policy"]
        if body["policy_version"] != rec.get("policy_version"):
            log.warning("policy for %s changed v%s -> v%s", topic, rec.get("policy_version"), body["policy_version"])
            rec = {
                **rec,
                "policy_version": body["policy_version"],
                "engine_policy": ep,
                "policy": {
                    **rec["policy"],
                    "classification": ep["classification"],
                    "allowed_storage_regions": ep["allowed_storage_regions"],
                    "allowed_processing_regions": ep["allowed_processing_regions"],
                    "durability_mode": ep["durability_mode"],
                },
            }
            self._cache[topic] = rec
        return rec
