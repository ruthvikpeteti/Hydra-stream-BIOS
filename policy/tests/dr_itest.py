"""Helpers for the Phase 6 disaster-recovery tests (live stack started with scripts/up.sh --dr)."""

import asyncio
import uuid

import httpx

from itest import ENV, KEYS, gw_topic, sh, start_replication, uid, wait_for, dump, uniq, api, topology, ckpt  # noqa: F401
from ss_dr.coord import Coordinator

DR_URL = {1: "http://localhost:8101/dr/v1", 2: "http://localhost:8102/dr/v1"}
GW_URL = {"region-a": "http://localhost:8080/api/v1", "region-b": "http://localhost:8081/api/v1", "region-c": "http://localhost:8082/api/v1"}
ETCD_PORTS = {"a": 12379, "b": 12380, "c": 12381, "w1": 12382, "w2": 12383}
ETCD_URLS = [f"http://localhost:{p}" for p in ETCD_PORTS.values()]
GW_KEY = ENV["SS_GATEWAY_API_KEY"]
LEASE_TTL_S = 6
FENCE_WAIT_S = LEASE_TTL_S + 1

REGION_CONTAINERS = {
    "region-a": [
        "ss-a-gateway",
        "ss-a-agent",
        "ss-a-replicator",
        "ss-a-exporter",
        "ss-a-broker-1",
        "ss-a-broker-2",
        "ss-a-broker-3",
        "ss-a-controller-1",
        "ss-a-controller-2",
        "ss-a-controller-3",
        "ss-etcd-a",
    ],
    "region-b": ["ss-b-gateway", "ss-b-agent", "ss-b-replicator", "ss-b-exporter", "ss-b-kafka-1", "ss-etcd-b"],
    "region-c": ["ss-c-gateway", "ss-c-agent", "ss-c-replicator", "ss-c-exporter", "ss-c-kafka-1", "ss-etcd-c"],
}
START_ORDER = lambda names: sorted(names, key=lambda n: (0 if ("controller" in n or "kafka" in n or "etcd" in n) else 1 if "broker" in n else 2, n))


def run(coro):
    return asyncio.run(coro)


def coord_call(fn):
    async def go():
        c = Coordinator(ETCD_URLS, timeout_s=3.0)
        try:
            return await fn(c)
        finally:
            await c.close()

    return run(go())


def dr_state():
    """(state, mod_revision) straight from the coordinator, or None when no quorum answers."""
    from ss_dr.coord import CoordinationUnavailable

    try:
        return coord_call(lambda c: c.state())
    except CoordinationUnavailable:
        return None


def dr_audit():
    return coord_call(lambda c: c.audit(500))


def dr_status(n=1):
    return httpx.get(f"{DR_URL[n]}/status", timeout=10).json()


def dr_failover(n=1, **body):
    return httpx.post(f"{DR_URL[n]}/failover", json=body, headers={"X-Admin-Key": ENV["SS_POLICY_ADMIN_KEY"]}, timeout=120).json()


def arm_crash(step, n=None):
    for i in [n] if n else [1, 2]:
        httpx.post(f"{DR_URL[i]}/debug/crash", json={"step": step}, headers={"X-Admin-Key": ENV["SS_POLICY_ADMIN_KEY"]}, timeout=10)


def wait_phase(*phases, timeout=90):
    def check():
        s = dr_state()
        return s[0] if s and s[0]["phase"] in phases else None

    try:
        return wait_for(check, timeout, 0.5, f"DR phase in {phases}")
    except TimeoutError as exc:
        s = dr_state()
        try:
            h = {r: (x["declared"], {k: v for k, v in x["sample"].items() if k != "cluster_id"}) for r, x in dr_status(1)["health"].items()}
        except Exception:  # noqa: BLE001
            h = "controller status unavailable"
        raise TimeoutError(f"{exc}; coordinator state={(s[0]['phase'], s[0]['active_region'], s[0]['leadership_epoch']) if s else None}; health={h}") from None


def gwr(region, method, path, **kw):
    kw.setdefault("timeout", 20)
    return httpx.request(method, f"{GW_URL[region]}{path}", headers={"X-API-Key": GW_KEY}, **kw)


def event_body(topic, seq, eid=None, key=None):
    return {"topic": topic, "event_id": eid or str(uuid.uuid4()), "partition_key": key or f"k-{seq % 10}", "payload": {"seq": seq}}


def produce_at(region, topic, n, start=0):
    """Returns (acked_event_ids, [error codes]). Only a 200 counts as acknowledged."""
    acked, errs = [], []
    for i in range(start, start + n):
        b = event_body(topic, i)
        try:
            r = gwr(region, "POST", "/events", json=b, timeout=15)
        except httpx.HTTPError as exc:
            errs.append(type(exc).__name__)
            continue
        if r.status_code == 200:
            acked.append(b["event_id"])
        else:
            errs.append((r.json().get("error") or {}).get("code", r.status_code))
    return acked, errs


def stop_containers(names):
    """Abrupt stop (SIGKILL after 1 s). `docker kill` is not used: the restart policy would bring the container back."""
    for n in names:
        sh("docker", "stop", "-t", "1", n, check=False)


def start_containers(names):
    for n in START_ORDER(names):
        sh("docker", "start", n, check=False)


def kill_region(region):
    stop_containers(REGION_CONTAINERS[region])


def restore_region(region, wait_gateway=True):
    start_containers(REGION_CONTAINERS[region])
    if wait_gateway:
        wait_for(lambda: gw_role(region) is not None, 180, 2, f"gateway {region} to answer")


def gw_role(region):
    try:
        r = httpx.get(f"{GW_URL[region]}/active-region", headers={"X-API-Key": GW_KEY}, timeout=4)
        return r.json()
    except (httpx.HTTPError, ValueError):
        return None


def write_status(region, topic):
    """HTTP status + error code of one probe write at `region` (does not count as acknowledged unless 200)."""
    try:
        r = gwr(region, "POST", "/events", json=event_body(topic, 0), timeout=10)
        return r.status_code, (r.json().get("error") or {}).get("code") if r.status_code != 200 else None
    except httpx.HTTPError as exc:
        return 0, type(exc).__name__


def active_by_gateways():
    """Regions whose gateway currently says it is the active, lease-valid writer."""
    out = []
    for r in GW_URL:
        j = gw_role(r)
        if j and j.get("role") == "active" and j.get("lease", {}).get("valid"):
            out.append(r)
    return out
