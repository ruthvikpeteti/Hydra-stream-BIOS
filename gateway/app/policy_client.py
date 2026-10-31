"""Client for the policy engine. Anything other than a clear answer is treated as POLICY_UNAVAILABLE (fail closed)."""

import logging

import httpx

from .config import Settings
from .errors import ApiError

log = logging.getLogger("ss.policy")


def unavailable(why: str) -> ApiError:
    return ApiError(
        503, "POLICY_UNAVAILABLE", f"policy engine could not be consulted ({why}); failing closed", retryable=True, details={"decision": "POLICY_UNAVAILABLE"}
    )


class PolicyClient:
    def __init__(self, s: Settings):
        self._http = httpx.AsyncClient(base_url=s.policy_url, timeout=s.policy_timeout_s, headers={"X-Service-Key": s.policy_key})

    async def close(self) -> None:
        await self._http.aclose()

    async def _call(self, method: str, path: str, **kw) -> tuple[int, dict]:
        try:
            r = await self._http.request(method, path, **kw)
            body = r.json()
        except (httpx.HTTPError, ValueError) as exc:
            log.warning("policy engine call %s %s failed: %r", method, path, exc)
            raise unavailable(type(exc).__name__) from exc
        if r.status_code >= 500 or (r.status_code == 401):
            raise unavailable(f"HTTP {r.status_code}")
        return r.status_code, body

    async def create_policy(self, payload: dict) -> tuple[int, dict]:
        return await self._call("POST", "/api/v1/policies", json=payload)

    async def get_policy(self, topic: str) -> tuple[int, dict]:
        return await self._call("GET", f"/api/v1/policies/{topic}")

    async def backpressure(self, topic: str) -> tuple[int, dict]:
        return await self._call("GET", f"/api/v1/replication/topics/{topic}/backpressure")
