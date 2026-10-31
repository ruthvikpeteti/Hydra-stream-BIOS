import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[2]
INFRA = ROOT / "infrastructure"
STATE_FILE = INFRA / "config" / "control" / "dev-control-state.json"
BASE = os.environ.get("SS_GATEWAY_URL", "http://localhost:8080")
sys.path.insert(0, str(ROOT / "gateway"))  # lets unit tests import `app.*` (pure modules only)


def _env() -> dict:
    out = {}
    for line in (INFRA / ".env").read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k] = v
    return out


API_KEY = _env()["SS_GATEWAY_API_KEY"]
LEASE_TTL_S = int(os.environ.get("SS_LEASE_TTL_MS", "10000")) / 1000


def sh(*args: str, check: bool = True, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, check=check, timeout=timeout)


def wait_health(timeout: int = 150) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            if httpx.get(f"{BASE}/api/v1/health", timeout=3).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(1)
    raise TimeoutError("gateway did not become healthy")


@pytest.fixture(scope="session")
def client():
    wait_health()
    with httpx.Client(base_url=f"{BASE}/api/v1", headers={"X-API-Key": API_KEY}, timeout=30) as c:
        yield c


@pytest.fixture(scope="session")
def anon():
    with httpx.Client(base_url=f"{BASE}/api/v1", timeout=30) as c:
        yield c


def new_topic(client, prefix: str, partitions: int = 3, **policy) -> str:
    name = f"{prefix}-{uuid.uuid4().hex[:8]}"
    r = client.post("/topics", json={"name": name, "partitions": partitions, **policy})
    assert r.status_code == 201, r.text
    return name


def event(topic: str, key: str, payload=None, **kw) -> dict:
    return {"topic": topic, "event_id": str(uuid.uuid4()), "partition_key": key, "payload": payload if payload is not None else {"k": key}, **kw}


def read_all(client, topic: str, **params) -> list[dict]:
    """Page through all events, following next_offsets (also exercises resumable offsets)."""
    t = client.get(f"/topics/{topic}").json()
    out = []
    for part in t["partitions"]:
        p, off = part["partition"], 0
        while True:
            r = client.get(f"/events/{topic}", params={"partition": p, "offset": off, "limit": 500, **params})
            assert r.status_code == 200, r.text
            body = r.json()
            out.extend(body["events"])
            off = body["next_offsets"][str(p)]
            if not body["has_more"]:
                break
    return out


def murmur2(data: bytes) -> int:
    """Kafka's default-partitioner hash (Java Utils.murmur2)."""
    m, r = 0x5BD1E995, 24
    h = (0x9747B28C ^ len(data)) & 0xFFFFFFFF
    n4 = len(data) // 4
    for i in range(n4):
        k = int.from_bytes(data[i * 4 : i * 4 + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = ((h * m) & 0xFFFFFFFF) ^ k
    rem, base = len(data) & 3, len(data) & ~3
    if rem == 3:
        h ^= data[base + 2] << 16
    if rem >= 2:
        h ^= data[base + 1] << 8
    if rem >= 1:
        h ^= data[base]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def expected_partition(key: str, n: int) -> int:
    return (murmur2(key.encode()) & 0x7FFFFFFF) % n


def set_topic_control(topic: str, **entry) -> None:
    """Edit the dev control-plane stand-in (the file the gateway re-reads)."""
    data = json.loads(STATE_FILE.read_text())
    cur = data["topics"].get(topic, dict(data["default"]))
    data["topics"][topic] = {**cur, **entry}
    STATE_FILE.write_text(json.dumps(data, indent=2))


def wait_epoch(client, topic: str, epoch: int, timeout: int = 20) -> dict:
    end = time.time() + timeout
    while time.time() < end:
        r = client.get("/active-region", params={"topic": topic}).json()
        if r["leadership_epoch"] == epoch:
            return r
        time.sleep(0.5)
    raise TimeoutError(f"gateway never saw epoch {epoch}")


@pytest.fixture(scope="session", autouse=True)
def restore_control_state():
    base = STATE_FILE.read_text()
    yield
    STATE_FILE.write_text(base)  # overrides are append-only in a running gateway: restart clears them
    sh("docker", "restart", "ss-a-gateway", check=False)
