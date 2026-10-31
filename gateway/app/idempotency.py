"""Request idempotency (see GATEWAY.md for the exact contract).

Records are kept in memory for speed and persisted to the compacted Kafka topic `_ss.idempotency`, so they
survive a gateway restart. Entries expire after `ttl_s`.
"""

import json
import logging
import time
from collections import OrderedDict

from .config import Settings
from .kafka_util import read_compacted
from .producer import ProducerManager

TOPIC = "_ss.idempotency"
PARTITIONS = 3
log = logging.getLogger("ss.idem")


class IdempotencyStore:
    def __init__(self, s: Settings, producer: ProducerManager):
        self._s, self._producer = s, producer
        self._mem: OrderedDict[str, dict] = OrderedDict()

    async def load(self) -> None:
        now = time.time() * 1000
        for r in await read_compacted(self._s, TOPIC, PARTITIONS):
            if r.value is None:
                continue
            e = json.loads(r.value)
            if now - e["ts"] < self._s.idempotency_ttl_s * 1000:
                self._mem[r.key.decode()] = e
        log.info("loaded %d idempotency records", len(self._mem))

    def get(self, key: str) -> dict | None:
        e = self._mem.get(key)
        if e and time.time() * 1000 - e["ts"] >= self._s.idempotency_ttl_s * 1000:
            del self._mem[key]
            return None
        return e

    async def put(self, keys: list[str], body_hash: str, response: dict) -> None:
        entry = {"body_hash": body_hash, "response": response, "ts": int(time.time() * 1000)}
        raw = json.dumps(entry, separators=(",", ":")).encode()
        for k in keys:
            await self._producer.send(TOPIC, k.encode(), raw)
            self._mem[k] = entry
            self._mem.move_to_end(k)
        while len(self._mem) > self._s.idempotency_max_entries:
            self._mem.popitem(last=False)
