"""Kafka producer manager: one long-lived idempotent producer, acks=all, started/stopped with the app."""

import asyncio
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import (
    KafkaConnectionError,
    KafkaError,
    KafkaTimeoutError,
    NotEnoughReplicasAfterAppendError,
    NotEnoughReplicasError,
    RequestTimedOutError,
    UnknownTopicOrPartitionError,
)

from .config import Settings
from .errors import ApiError

log = logging.getLogger("ss.producer")


class ProducerManager:
    """acks=all is used for EVERY topic. The topic's own `min.insync.replicas` (from its policy:
    A defaults to RF 3 / min ISR 2) then decides how many replicas must hold a record before the ack.
    Idempotence keeps per-partition order and drops broker-side duplicates on internal retries.
    """

    def __init__(self, s: Settings):
        self._s = s
        self._p: AIOKafkaProducer | None = None

    @property
    def started(self) -> bool:
        return self._p is not None

    async def start(self) -> None:
        p = AIOKafkaProducer(
            bootstrap_servers=self._s.kafka_bootstrap,
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
            request_timeout_ms=self._s.produce_timeout_ms,
            **self._s.kafka_client_kwargs,
        )
        await p.start()
        self._p = p

    async def stop(self) -> None:
        if self._p:
            await self._p.stop()  # flushes outstanding batches
            self._p = None

    async def warm(self, topic: str) -> None:
        if self._p:
            await asyncio.wait_for(self._p.partitions_for(topic), 30)

    async def send(self, topic: str, key: bytes | None, value: bytes, *, timestamp_ms: int | None = None, headers: list[tuple[str, bytes]] | None = None):
        if self._p is None:
            raise ApiError(503, "LOCAL_DURABILITY_UNAVAILABLE", "producer is not running", retryable=True)
        timeout = self._s.produce_timeout_ms / 1000 + 5
        try:
            fut = await asyncio.wait_for(self._p.send(topic, value, key=key, timestamp_ms=timestamp_ms, headers=headers), timeout)
            return await asyncio.wait_for(fut, timeout)
        except NotEnoughReplicasError as exc:
            raise ApiError(
                503,
                "LOCAL_DURABILITY_UNAVAILABLE",
                "fewer in-sync replicas than min.insync.replicas; write rejected",
                topic=topic,
                retryable=True,
                details={"outcome": "not_written"},
            ) from exc
        except NotEnoughReplicasAfterAppendError as exc:
            raise ApiError(
                503,
                "LOCAL_DURABILITY_UNAVAILABLE",
                "write may have been appended but was not acknowledged",
                topic=topic,
                retryable=True,
                details={"outcome": "unknown"},
            ) from exc
        except UnknownTopicOrPartitionError as exc:
            raise ApiError(404, "NOT_FOUND", "topic does not exist in Kafka", topic=topic) from exc
        except (KafkaTimeoutError, RequestTimedOutError, KafkaConnectionError, asyncio.TimeoutError) as exc:
            raise ApiError(
                503, "LOCAL_DURABILITY_UNAVAILABLE", "Kafka did not acknowledge the write in time", topic=topic, retryable=True, details={"outcome": "unknown"}
            ) from exc
        except KafkaError as exc:
            log.error("kafka error on %s: %r", topic, exc)
            raise ApiError(502, "KAFKA_ERROR", "Kafka rejected the write", topic=topic, retryable=False) from exc
