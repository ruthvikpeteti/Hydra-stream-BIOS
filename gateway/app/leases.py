"""Active-region resolver and write-lease checks.

Phase 2 STAND-IN for the control plane (ARCHITECTURE §4.3): the authority is a JSON file (mounted
read-only) that an operator edits. The gateway re-reads it every `renew_ms`; each successful read
renews the local lease for `ttl_ms`, measured on the MONOTONIC clock from before the read (SECURITY §4).
If reads keep failing, the lease expires and the gateway self-fences (writes -> LEASE_EXPIRED).
Epochs and generations never move backwards in memory (INV-4); a file that regresses is ignored.

The interface (`resolve`) is what the real control-plane client will implement.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass

log = logging.getLogger("ss.lease")


@dataclass(frozen=True)
class LeaseState:
    topic: str | None
    active_region: str
    role: str  # "active" if this gateway's region is the active region, else "passive"
    leadership_epoch: int
    replication_generation: int
    lease_valid: bool
    ttl_remaining_ms: int


class DevFileLeaseSource:
    def __init__(self, path: str, region: str, ttl_ms: int, renew_ms: int, clock=time.monotonic):
        self._path, self._region, self._ttl, self._renew = path, region, ttl_ms / 1000, renew_ms / 1000
        self._clock = clock
        self._state: dict = {}
        self._expires = float("-inf")
        self._max_seen: dict[str, tuple[int, int]] = {}
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        await self.renew_once()
        self._task = asyncio.create_task(self._loop(), name="lease-renewal")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._renew)
            await self.renew_once()

    def _read(self) -> dict:
        with open(self._path, encoding="utf-8") as f:
            return json.load(f)

    async def renew_once(self) -> bool:
        started = self._clock()
        try:
            data = await asyncio.to_thread(self._read)
            self._ingest(data)
        except Exception as exc:  # noqa: BLE001 - any failure means "cannot renew"
            log.warning("lease renewal failed: %s", exc)
            return False
        self._expires = started + self._ttl
        return True

    def _ingest(self, data: dict) -> None:
        entries = {"*": data["default"], **data.get("topics", {})}
        for name in self._max_seen:
            if name not in entries:  # dropping an override would silently lower that topic's epoch
                raise ValueError(f"epoch override for {name} was removed; ignored (restart to reset)")
        parsed: dict[str, dict] = {}
        for name, e in entries.items():
            epoch, gen = int(e["leadership_epoch"]), int(e["replication_generation"])
            if epoch < 1 or gen < epoch:  # G >= E always (DATA_MODEL §4)
                raise ValueError(f"bad epoch/generation for {name}")
            prev = self._max_seen.get(name)
            if prev and (epoch < prev[0] or gen < prev[1]):
                raise ValueError(f"epoch/generation regression for {name}: {prev} -> {(epoch, gen)} ignored")
            parsed[name] = {"active_region": str(e["active_region"]), "leadership_epoch": epoch, "replication_generation": gen}
        for name, e in parsed.items():
            self._max_seen[name] = (e["leadership_epoch"], e["replication_generation"])
        self._state = parsed

    def resolve(self, topic: str | None = None) -> LeaseState:
        e = self._state.get(topic) if topic else None
        e = e or self._state.get("*")
        remaining = max(0, int((self._expires - self._clock()) * 1000))
        if e is None:
            return LeaseState(topic, "unknown", "passive", 0, 0, False, 0)
        role = "active" if e["active_region"] == self._region else "passive"
        return LeaseState(topic, e["active_region"], role, e["leadership_epoch"], e["replication_generation"], remaining > 0 and role == "active", remaining)

    def lease_fresh(self) -> bool:
        return self._clock() < self._expires
