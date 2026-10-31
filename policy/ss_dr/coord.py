"""Client for the etcd coordinator (v3 JSON gateway). The coordinator, not a database row, is the authority for the active
region, the leadership epoch and fencing.

* Reads are linearizable (etcd default): a member without quorum cannot answer, so "unreadable" means "no authority".
* State changes are compare-and-swap on the key's mod_revision, in one txn together with an audit record.
* A write whose outcome is unknown (timeout) raises CoordinationUnavailable; callers re-read and decide by the
  transition id, never by assuming it did or did not happen."""

import asyncio
import base64
import json
import time
from typing import Optional

import httpx

STATE_KEY = "/ss/dr/state"
AUDIT_PREFIX = "/ss/dr/audit/"
LEADER_KEY = "/ss/dr/controller"


class CoordinationUnavailable(Exception):
    pass


def b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def unb64(s: str) -> str:
    return base64.b64decode(s).decode()


class Coordinator:
    def __init__(self, endpoints: list[str], timeout_s: float = 2.0):
        self.endpoints = [e.rstrip("/") for e in endpoints]
        self._http = httpx.AsyncClient(timeout=timeout_s)
        self._pref = 0

    async def close(self) -> None:
        await self._http.aclose()

    async def _post(self, path: str, body: dict, *, retry_other: bool = True) -> dict:
        last: Optional[Exception] = None
        n = len(self.endpoints) if retry_other else 1
        for i in range(n):
            idx = (self._pref + i) % len(self.endpoints)
            try:
                r = await self._http.post(f"{self.endpoints[idx]}{path}", json=body)
                if r.status_code == 200:
                    self._pref = idx
                    return r.json()
                last = RuntimeError(f"{self.endpoints[idx]} -> HTTP {r.status_code} {r.text[:120]}")
            except httpx.HTTPError as exc:
                last = exc
        raise CoordinationUnavailable(f"no coordinator member could serve {path}: {last!r}")

    # ---- reads -----------------------------------------------------------------------------------------------
    async def get(self, key: str) -> tuple[Optional[dict], int]:
        """(JSON value or None, mod_revision; 0 when absent). Linearizable."""
        j = await self._post("/v3/kv/range", {"key": b64(key)})
        kvs = j.get("kvs") or []
        if not kvs:
            return None, 0
        return json.loads(unb64(kvs[0]["value"])), int(kvs[0]["mod_revision"])

    async def state(self) -> tuple[dict, int]:
        v, rev = await self.get(STATE_KEY)
        if v is None:
            raise CoordinationUnavailable("the coordinator holds no DR state (not bootstrapped)")
        return v, rev

    async def reachable(self) -> bool:
        try:
            await self.get(STATE_KEY)
            return True
        except CoordinationUnavailable:
            return False

    async def members_alive(self) -> dict[str, bool]:
        async def one(ep):
            try:
                r = await self._http.get(f"{ep}/health", timeout=1.5)
                return ep, r.status_code == 200 and r.json().get("health") == "true"
            except (httpx.HTTPError, ValueError):
                return ep, False

        return dict(await asyncio.gather(*(one(e) for e in self.endpoints)))

    async def audit(self, limit: int = 200) -> list[dict]:
        end = AUDIT_PREFIX[:-1] + chr(ord(AUDIT_PREFIX[-1]) + 1)
        j = await self._post(
            "/v3/kv/range", {"key": b64(AUDIT_PREFIX), "range_end": b64(end), "limit": str(limit), "sort_order": "ASCEND", "sort_target": "KEY"}
        )
        return [json.loads(unb64(kv["value"])) for kv in j.get("kvs") or []]

    # ---- writes ----------------------------------------------------------------------------------------------
    async def bootstrap(self, initial: dict) -> bool:
        """Create the state if (and only if) it does not exist. True if this call created it."""
        j = await self._post(
            "/v3/kv/txn",
            {
                "compare": [{"key": b64(STATE_KEY), "target": "MOD", "result": "EQUAL", "mod_revision": "0"}],
                "success": [{"request_put": {"key": b64(STATE_KEY), "value": b64(json.dumps(initial))}}],
                "failure": [],
            },
            retry_other=False,
        )
        return bool(j.get("succeeded"))

    async def cas_state(self, expected_rev: int, new_state: dict, audit: dict) -> bool:
        """True: applied. False: somebody else changed the state first (compare failed). Raises CoordinationUnavailable
        when the outcome is unknown. The audit record is written in the same transaction."""
        new_state = {**new_state, "seq": int(new_state.get("seq", 0))}
        rec = {**audit, "seq": new_state["seq"], "wall_ms": int(time.time() * 1000)}
        j = await self._post(
            "/v3/kv/txn",
            {
                "compare": [{"key": b64(STATE_KEY), "target": "MOD", "result": "EQUAL", "mod_revision": str(expected_rev)}],
                "success": [
                    {"request_put": {"key": b64(STATE_KEY), "value": b64(json.dumps(new_state))}},
                    {"request_put": {"key": b64(f"{AUDIT_PREFIX}{new_state['seq']:012d}"), "value": b64(json.dumps(rec))}},
                ],
                "failure": [],
            },
            retry_other=False,
        )
        return bool(j.get("succeeded"))

    # ---- controller leader election --------------------------------------------------------------------------
    async def lease_grant(self, ttl_s: int) -> int:
        j = await self._post("/v3/lease/grant", {"TTL": str(ttl_s)}, retry_other=False)
        return int(j["ID"])

    async def lease_keepalive(self, lease_id: int) -> bool:
        j = await self._post("/v3/lease/keepalive", {"ID": str(lease_id)}, retry_other=False)
        return int((j.get("result") or {}).get("TTL", 0)) > 0

    async def try_lead(self, instance: str, lease_id: int) -> tuple[bool, Optional[str]]:
        """Create the leader key attached to our lease if nobody holds it. Returns (we lead, current leader)."""
        me = json.dumps({"id": instance})
        j = await self._post(
            "/v3/kv/txn",
            {
                "compare": [{"key": b64(LEADER_KEY), "target": "CREATE", "result": "EQUAL", "create_revision": "0"}],
                "success": [{"request_put": {"key": b64(LEADER_KEY), "value": b64(me), "lease": str(lease_id)}}],
                "failure": [{"request_range": {"key": b64(LEADER_KEY)}}],
            },
            retry_other=False,
        )
        if j.get("succeeded"):
            return True, instance
        kvs = (j.get("responses") or [{}])[0].get("response_range", {}).get("kvs") or []
        holder = json.loads(unb64(kvs[0]["value"]))["id"] if kvs else None
        return holder == instance, holder

    async def leader(self) -> Optional[str]:
        v, _ = await self.get(LEADER_KEY)
        return v["id"] if v else None
