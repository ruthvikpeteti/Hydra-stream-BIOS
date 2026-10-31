"""Lease source backed by the etcd coordinator (Phase 6).

The authoritative record is `/ss/dr/state`, written only by compare-and-swap from the failover controller.
A gateway renews its local lease by a LINEARIZABLE read of that key (etcd serves it only through the quorum):
no quorum -> no read -> the lease runs out on the local MONOTONIC clock -> writes are refused (LEASE_EXPIRED).
A region that is listed in `fenced`, or that is not `active_region`, is passive: it can never write.
Everything else (monotonic expiry, epochs never regress) is inherited from DevFileLeaseSource."""

import base64
import json

import httpx

from .leases import DevFileLeaseSource

STATE_KEY = "/ss/dr/state"


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


class EtcdLeaseSource(DevFileLeaseSource):
    def __init__(self, endpoints: list[str], region: str, ttl_ms: int, renew_ms: int, timeout_s: float = 2.0, **kw):
        super().__init__("etcd", region, ttl_ms, renew_ms, **kw)
        self._endpoints, self._timeout = [e.rstrip("/") for e in endpoints], timeout_s
        self._http = httpx.Client(timeout=timeout_s)
        self._last_ok = 0

    def _read(self) -> dict:
        last: Exception | None = None
        for i in range(len(self._endpoints)):
            ep = self._endpoints[(self._last_ok + i) % len(self._endpoints)]
            try:
                r = self._http.post(f"{ep}/v3/kv/range", json={"key": _b64(STATE_KEY)})
                r.raise_for_status()
                kvs = r.json().get("kvs") or []
                if not kvs:
                    raise RuntimeError("coordinator holds no state yet")
                st = json.loads(base64.b64decode(kvs[0]["value"]))
                self._last_ok = (self._last_ok + i) % len(self._endpoints)
                active = "" if self._region in st.get("fenced", {}) else str(st.get("active_region") or "")
                return {
                    "default": {
                        "active_region": active or "none",
                        "leadership_epoch": st["leadership_epoch"],
                        "replication_generation": st["replication_generation"],
                    },
                    "topics": {},
                }
            except Exception as exc:  # noqa: BLE001 - try the next member
                last = exc
        raise RuntimeError(f"no coordinator member answered a linearizable read: {last!r}")

    async def stop(self) -> None:
        await super().stop()
        self._http.close()
