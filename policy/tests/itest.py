"""Helpers for integration tests against the live stack."""

import json
import subprocess
import time
import uuid
from pathlib import Path

import httpx

INFRA = Path(__file__).resolve().parents[2] / "infrastructure"
ENV = dict(l.split("=", 1) for l in (INFRA / ".env").read_text().splitlines() if "=" in l)
KEYS = {"admin": ENV["SS_POLICY_ADMIN_KEY"], "writer": ENV["SS_POLICY_WRITER_KEY"], "service": ENV["SS_POLICY_SERVICE_KEY"]}
ENGINE = "http://localhost:8090/api/v1"
AGENTS = {"region-a": "http://localhost:8091/agent/v1", "region-b": "http://localhost:8092/agent/v1", "region-c": "http://localhost:8093/agent/v1"}
GATEWAY = "http://localhost:8080/api/v1"
GW_KEY = ENV["SS_GATEWAY_API_KEY"]
GB = 10**9


def sh(*args, check=True, timeout=180):
    return subprocess.run(args, capture_output=True, text=True, check=check, timeout=timeout)


def api(method, path, role="service", **kw):
    h = {"X-Service-Key": KEYS[role], **kw.pop("headers", {})}
    return httpx.request(method, f"{ENGINE}{path}", headers=h, timeout=30, **kw)


def agent(region, method, path, **kw):
    return httpx.request(method, f"{AGENTS[region]}{path}", headers={"X-Service-Key": KEYS["service"]}, timeout=30, **kw)


def gw(method, path, **kw):
    return httpx.request(method, f"{GATEWAY}{path}", headers={"X-API-Key": GW_KEY}, timeout=30, **kw)


def uid(prefix="t"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def region_body(rid, zone, fd, *, avail="AVAILABLE", used=100 * GB, total=1000 * GB, headroom=0.5, max_rf=3):
    return {
        "region_id": rid,
        "legal_zone": zone,
        "availability_status": avail,
        "storage_capacity": {"total_bytes": total, "used_bytes": used},
        "processing_capabilities": ["gateway", "replication"],
        "supported_transit_zones": ["EU", "US", "ATLANTIC"],
        "failure_domain": fd,
        "compute_headroom": headroom,
        "max_replication_factor": max_rf,
    }


def put_region(**kw):
    r = api("PUT", f"/regions/{kw['rid']}", "admin", json=region_body(**kw))
    assert r.status_code == 200, r.text
    return r.json()


def put_link(a, b, zones, ms, status="UP"):
    r = api("PUT", f"/links/{a}/{b}", "admin", json={"from_region": a, "to_region": b, "transit_zones": zones, "latency_ms": ms, "status": status})
    assert r.status_code == 200, r.text


def mkworld():
    """A private EU/US world with unique ids so tests never interfere. Returns names."""
    p = uid("w")
    n = {k: f"{p}-{k}" for k in ("central", "west", "south", "us")}
    put_region(rid=n["central"], zone="EU", fd=f"{p}-d1")
    put_region(rid=n["west"], zone="EU", fd=f"{p}-d2")
    put_region(rid=n["south"], zone="EU", fd=f"{p}-d3")
    put_region(rid=n["us"], zone="US", fd=f"{p}-d4", headroom=1.0, used=0)
    for a, b, ms in ((n["central"], n["west"], 20), (n["central"], n["south"], 900), (n["west"], n["south"], 900)):
        put_link(a, b, ["EU"], ms)
        put_link(b, a, ["EU"], ms)
    for eu in (n["central"], n["west"], n["south"]):
        put_link(eu, n["us"], ["EU", "ATLANTIC", "US"], 1)
        put_link(n["us"], eu, ["US", "ATLANTIC", "EU"], 1)
    return n


def policy_body(topic, home, storage, processing=None, **kw):
    b = {
        "topic": topic,
        "classification": "INTERNAL",
        "legal_zone": "EU",
        "home_region": home,
        "allowed_storage_regions": storage,
        "allowed_processing_regions": processing or storage,
        "allowed_transit_zones": ["EU"],
        "replication_factor": 1,
        "regional_replica_count": 1,
        "preferred_regions": [],
        "excluded_regions": [],
        "maximum_replication_lag_ms": 5000,
        "durability_mode": "ASYNC_REGIONAL",
    }
    b.update(kw)
    return b


def create_policy(body, expect=201):
    r = api("POST", "/policies", "writer", json=body)
    assert r.status_code == expect, r.text
    return r.json()


def evaluate(topic, operation, **kw):
    rid = kw.setdefault("request_id", uid("req"))
    r = api("POST", "/placement/evaluate", json={"topic": topic, "operation": operation, **kw})
    return r, rid


def codes(j):
    return {x["code"] for x in j["reasons"]}


def audit_by_request(rid):
    rows = api("GET", "/audit", params={"request_id": rid}).json()["decisions"]
    return rows


def wait_for(fn, timeout=30, every=1.0, what="condition"):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        last = fn()
        if last:
            return last
        time.sleep(every)
    raise TimeoutError(f"timed out waiting for {what} (last={last!r})")


def engine_up():
    try:
        return httpx.get(f"{ENGINE}/health", timeout=3).status_code == 200
    except httpx.HTTPError:
        return False


def kafka_topics(container):
    out = sh("docker", "exec", container, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", f"{container}:9092", "--list").stdout
    return set(out.split())


# ===== Phase 4: replication helpers ===================================================================================
REPL_KEYS = {"region-a": ENV["SS_REPL_KEY_A"], "region-b": ENV["SS_REPL_KEY_B"], "region-c": ENV["SS_REPL_KEY_C"]}
RECEIVER = {"region-a": "http://localhost:8091", "region-b": "http://localhost:8092", "region-c": "http://localhost:8093"}
REPLICATOR = {"region-a": "http://localhost:8095", "region-b": "http://localhost:8096", "region-c": "http://localhost:8097"}
CLUSTER = {"region-a": "kafka-a", "region-b": "kafka-b", "region-c": "kafka-c"}
ABC = ["region-a", "region-b", "region-c"]


def psql(sql):
    out = sh("docker", "exec", "ss-postgres", "psql", "-U", "sspolicy", "-d", "sspolicy", "-tA", "-F", "|", "-c", sql, check=False)
    return out


def ckpt(topic):
    """{(source_cluster, target_cluster, partition): (acked_source_offset, destination_offset, generation)}"""
    out = psql(
        f"select source_cluster,target_cluster,source_partition,last_acknowledged_source_offset,last_known_destination_offset,"
        f"replication_generation from replication_checkpoints where source_topic='{topic}' order by 1,2,3"
    )
    rows = {}
    for line in out.stdout.strip().splitlines():
        a, b, p, ack, dst, gen = line.split("|")
        rows[(a, b, int(p))] = (int(ack), int(dst), int(gen))
    return rows


def acked_count(topic, src, dst):
    return sum(v[0] + 1 for (s, t, _), v in ckpt(topic).items() if s == CLUSTER[src] and t == CLUSTER[dst])


def dump(region, topic):
    import json as _j

    out = sh("docker", "exec", f"ss-{region[-1]}-agent", "python", "-m", "ss_replication.tools", "dump", "--topic", topic, timeout=120)
    return [_j.loads(l) for l in out.stdout.splitlines() if l.strip()]


def uniq(recs):
    return {r["value"]["event_id"] for r in recs if r["value"]}


def gw_topic(
    prefix, partitions=3, regions=ABC, replicas=2, zone="GLOBAL", transit=("EU", "ATLANTIC", "US"), classification="PUBLIC", failover=None, max_lag_ms=None
):
    name = uid(prefix)
    body = {
        "name": name,
        "partitions": partitions,
        "legal_zone": zone,
        "classification": classification,
        "allowed_storage_regions": list(regions),
        "allowed_processing_regions": list(regions),
        "allowed_transit_zones": list(transit),
        "failover_priority": list(failover or list(regions)[: 1 + replicas]),
        "regional_replica_count": replicas,
        "durability_mode": "ASYNC_REGIONAL",
        "local_replication_factor": 1,
        "local_min_insync_replicas": 1,
    }
    if max_lag_ms is not None:
        body["max_tolerated_replication_lag_ms"] = max_lag_ms
    r = gw("POST", "/topics", json=body)
    assert r.status_code == 201, r.text
    return name


def start_replication(topic, partitions):
    r = api("POST", f"/replication/topics/{topic}/start", "writer", json={"partitions": partitions})
    assert r.status_code == 200 and r.json()["decision"] == "ALLOWED", r.text
    return r.json()


def produce(topic, n, keys=20, amount=None, start=0):
    ids = []
    for i in range(start, start + n):
        eid = str(uuid.uuid4())
        payload = {"seq": i} if amount is None else {"seq": i, "amount": amount}
        r = gw("POST", "/events", json={"topic": topic, "event_id": eid, "partition_key": f"key-{i % keys}", "payload": payload})
        assert r.status_code == 200, r.text
        ids.append(eid)
    return ids


def wait_acked(topic, src, dst, n, timeout=120):
    return wait_for(lambda: acked_count(topic, src, dst) >= n, timeout, 1, f"{src}->{dst} to acknowledge {n} events")


def wait_unique(region, topic, n, timeout=150):
    return wait_for(lambda: (lambda d: d if len(uniq(d)) >= n else None)(dump(region, topic)), timeout, 2, f"{region} to hold {n} unique events")


def fault(region, **kw):
    r = httpx.post(f"{RECEIVER[region]}/internal/v1/replication/_fault", headers={"X-Service-Key": KEYS["service"]}, json=kw, timeout=10)
    assert r.status_code == 200, r.text


def written(region):
    """Records this region's receiver has written to Kafka so far (fast; test-only endpoint)."""
    return httpx.get(f"{RECEIVER[region]}/internal/v1/replication/_stats", headers={"X-Service-Key": KEYS["service"]}, timeout=5).json()["records_written"]


def topology(topic):
    return api("GET", f"/replication/topics/{topic}").json()


def flow_of(topic, source, target, states=("ENABLED", "PENDING_CUTOVER")):
    return next((f for f in topology(topic)["flows"] if f["source_region"] == source and f["target_region"] == target and f["state"] in states), None)


def policy_version(topic):
    return api("GET", f"/policies/{topic}").json()["policy_version"]


def fake_batch(topic, *, source="region-a", dest="region-b", generation=1, epoch=1, version=None, first=0, attempt=1):
    import base64 as b64

    env = {"event_id": str(uuid.uuid4()), "topic": topic, "payload": {"fake": True}}
    return {
        "batch_id": f"fake-{uuid.uuid4().hex[:8]}",
        "attempt": attempt,
        "stream_session": "fake",
        "source_region": source,
        "source_cluster": CLUSTER[source],
        "destination_region": dest,
        "topic": topic,
        "partition": 0,
        "policy_version": version if version is not None else (policy_version(topic) or 1),
        "replication_generation": generation,
        "leadership_epoch": epoch,
        "prev_offset": first - 1,
        "first_offset": first,
        "last_offset": first,
        "records": [
            {
                "offset": first,
                "timestamp": 1,
                "key": None,
                "value": b64.b64encode(json.dumps(env).encode()).decode(),
                "origin": {"cluster": CLUSTER[source], "topic": topic, "partition": 0, "offset": first},
                "hop_path": CLUSTER[source],
            }
        ],
    }


def send_batch(dest, batch, *, as_region="region-a", key=None, path=None):
    h = {"Authorization": f"Bearer {key or REPL_KEYS[as_region]}", "X-Replication-Agent": as_region}
    if path:
        h["X-Replication-Path"] = path
    return httpx.post(f"{RECEIVER[dest]}/internal/v1/replication/batches", json=batch, timeout=20, headers=h)


# ===== Phase 5: routing helpers =====================================================================================
CHAOS = INFRA / "chaos"


def chaos(script, *args):
    return sh(str(CHAOS / script), *args)


def chaos_reset():
    chaos("recover.sh", "--all")


def route_state(topic, source="region-a", target="region-b"):
    st = httpx.get(f"{REPLICATOR[source]}/status", timeout=10).json()
    f = next((f for f in st["flows"] if f["topic"] == topic and f["target"] == target), None)
    return f


def current_route(topic, **kw):
    f = route_state(topic, **kw)
    return f["route"]["current"] if f else None


def wait_route(topic, expect, timeout=60, **kw):
    return wait_for(lambda: current_route(topic, **kw) == expect, timeout, 0.5, f"route of {topic} to become {expect}")


def route_audit(topic):
    rows = api("GET", "/audit", params={"topic": topic, "limit": 500}).json()["decisions"]
    return [r for r in rows if r["operation"] == "ROUTE_SWITCH"]


def backlog(topic):
    rows = api("GET", "/replication/backlog", params={"topic": topic}).json()["flows"]
    return rows[0] if rows else None


def first_occurrence_order(recs):
    seen, out = set(), []
    for r in recs:
        e = r["value"]["event_id"]
        if e not in seen:
            seen.add(e)
            out.append((r["partition"], e))
    return out
