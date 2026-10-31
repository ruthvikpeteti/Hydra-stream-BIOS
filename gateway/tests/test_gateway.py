"""Integration tests against the running stack (gateway in Docker + Region A Kafka). Run in file order:
later tests reuse the topic holding 1,000 same-key events and restart components."""

import socket
import subprocess
import threading
import time
import uuid

import httpx
import pytest

from conftest import BASE, LEASE_TTL_S, STATE_FILE, event, expected_partition, new_topic, read_all, set_topic_control, sh, wait_epoch, wait_health

N_PARTS = 3


@pytest.fixture(scope="module")
def spread(client):
    """1,000 events over 20 keys, 3 partitions."""
    topic = new_topic(client, "t-spread", N_PARTS)
    sent = []
    for i in range(1000):
        key = f"user-{i % 20}"
        ev = event(topic, key, {"seq": i})
        r = client.post("/events", json=ev)
        assert r.status_code == 200, r.text
        sent.append((ev, r.json()))
    return topic, sent


@pytest.fixture(scope="module")
def same_key(client):
    """1,000 events, one partition key: all land in one partition in send order."""
    topic = new_topic(client, "t-samekey", N_PARTS)
    sent = []
    for i in range(1000):
        ev = event(topic, "account-42", {"seq": i})
        r = client.post("/events", json=ev)
        assert r.status_code == 200, r.text
        sent.append(r.json())
    return topic, sent


# ---- publish ----------------------------------------------------------------------------------------
def test_01_publish_one_event(client):
    topic = new_topic(client, "t-one")
    ev = event(topic, "k1", {"hello": "world"}, timestamp=1_700_000_000_000)
    r = client.post("/events", json=ev)
    assert r.status_code == 200, r.text
    b = r.json()
    assert (b["event_id"], b["topic"], b["origin_region"], b["origin_cluster"]) == (ev["event_id"], topic, "region-a", "kafka-a")
    assert (b["leadership_epoch"], b["replication_generation"], b["event_timestamp"]) == (1, 1, 1_700_000_000_000)
    assert b["source_cluster"] == "kafka-a" and b["source_offset"] == 0 and b["durable_regions"] == ["region-a"]
    assert r.headers["x-ss-region-role"] == "active" and r.headers["x-ss-epoch"] == "1"

    got = client.get(f"/events/{topic}", params={"partition": b["source_partition"], "offset": 0}).json()["events"]
    assert len(got) == 1
    e = got[0]
    assert e["event_id"] == ev["event_id"] and e["partition_key"] == "k1" and e["payload"] == {"hello": "world"}
    assert e["event_timestamp"] == 1_700_000_000_000 and e["topic"] == topic
    assert (e["origin_cluster"], e["source_offset"], e["leadership_epoch"], e["replication_generation"]) == ("kafka-a", 0, 1, 1)


def test_01b_topic_is_rf3_minisr2_and_describes(client):
    topic = new_topic(client, "t-policy")
    t = client.get(f"/topics/{topic}").json()
    assert t["policy"]["local_replication_factor"] == 3 and t["policy"]["local_min_insync_replicas"] == 2
    assert t["active_region"] == "region-a" and t["role"] == "active"
    d = sh(
        "docker", "exec", "ss-a-broker-1", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "ss-a-broker-1:9092", "--describe", "--topic", topic
    ).stdout
    assert "ReplicationFactor: 3" in d and "min.insync.replicas=2" in d


def test_validation_errors(client, anon):
    topic = new_topic(client, "t-val")
    good = event(topic, "k")
    assert client.post("/events", json={**good, "event_id": "not-a-uuid"}).status_code == 400
    assert client.post("/events", json={**good, "partition_key": ""}).status_code == 400
    bad = dict(good)
    del bad["partition_key"]
    assert client.post("/events", json=bad).status_code == 400
    assert client.post("/events", json={**good, "payload": None}).status_code == 400
    assert client.post("/events", json={**event("no-such-topic-x", "k")}).json()["error"]["code"] == "NOT_FOUND"
    assert anon.post("/events", json=good).status_code == 401
    assert client.post("/topics", json={"name": "_internal"}).status_code == 400
    r = client.post("/topics", json={"name": topic})
    assert r.status_code == 409 and r.json()["error"]["code"] == "TOPIC_EXISTS"
    r = client.post(
        "/topics",
        json={
            "name": f"t-bad-{uuid.uuid4().hex[:6]}",
            "allowed_storage_regions": ["region-a", "region-c"],
            "allowed_processing_regions": ["region-a", "region-c"],
            "failover_priority": ["region-a", "region-c"],
            "regional_replica_count": 1,
            "durability_mode": "ASYNC_REGIONAL",
        },
    )
    assert r.status_code == 422 and r.json()["error"]["details"]["rule"] in ("V2", "V6")  # AS-1 at the API


def test_server_stamped_fields_cannot_be_spoofed(client):
    topic = new_topic(client, "t-spoof", 1)
    ev = event(topic, "k", leadership_epoch=99, replication_generation=99, origin_region="region-c", source_offset=777)
    b = client.post("/events", json=ev).json()
    assert (b["leadership_epoch"], b["replication_generation"], b["origin_region"], b["source_offset"]) == (1, 1, "region-a", 0)
    e = client.get(f"/events/{topic}", params={"partition": 0, "offset": 0}).json()["events"][0]
    assert (e["leadership_epoch"], e["origin_region"]) == (1, "region-a")


# ---- 1,000 events, partitioning, ordering ------------------------------------------------------------
def test_02_publish_1000_events_partitioned_by_key(client, spread):
    topic, sent = spread
    per_key = {}
    for ev, resp in sent:  # same key -> same partition == Kafka's murmur2 mapping
        assert resp["source_partition"] == expected_partition(ev["partition_key"], N_PARTS)
        per_key.setdefault(ev["partition_key"], set()).add(resp["source_partition"])
    assert all(len(v) == 1 for v in per_key.values())
    assert len({p for v in per_key.values() for p in v}) > 1, "keys should spread over several partitions"

    got = read_all(client, topic)
    assert len(got) == 1000 and len({e["event_id"] for e in got}) == 1000
    assert {e["event_id"] for e in got} == {ev["event_id"] for ev, _ in sent}


def test_03_same_key_ordering_within_partition(client, spread, same_key):
    topic, sent = spread  # per key, offsets and seq are strictly increasing
    got = read_all(client, topic)
    last = {}
    for e in got:
        k = e["partition_key"]
        cur = (e["source_offset"], e["payload"]["seq"])
        assert k not in last or (cur[0] > last[k][0] and cur[1] > last[k][1]), f"order broken for {k}"
        last[k] = cur

    topic, sent = same_key  # 1,000 identical keys: one partition, contiguous offsets
    assert len({s["source_partition"] for s in sent}) == 1
    assert [s["source_offset"] for s in sent] == list(range(1000))
    got = read_all(client, topic)
    assert [e["payload"]["seq"] for e in got] == list(range(1000))


# ---- consume / resume ----------------------------------------------------------------------------------
def test_04_consume_from_zero_and_resume_from_500(client, same_key):
    topic, sent = same_key
    part = sent[0]["source_partition"]
    r = client.get(f"/events/{topic}", params={"partition": part, "offset": 0, "limit": 1000}).json()
    assert len(r["events"]) == 1000 and r["events"][0]["source_offset"] == 0 and r["next_offsets"][str(part)] == 1000
    assert not r["has_more"]

    r = client.get(f"/events/{topic}", params={"partition": part, "offset": 500, "limit": 1000}).json()
    assert len(r["events"]) == 500
    assert r["events"][0]["source_offset"] == 500 and r["events"][0]["payload"]["seq"] == 500

    # paging resumes exactly where the previous page ended, with no gaps or repeats
    seqs, off = [], 0
    while True:
        r = client.get(f"/events/{topic}", params={"partition": part, "offset": off, "limit": 300}).json()
        seqs += [e["payload"]["seq"] for e in r["events"]]
        off = r["next_offsets"][str(part)]
        if not r["has_more"]:
            break
    assert seqs == list(range(1000))

    assert client.get(f"/events/{topic}", params={"partition": part, "offset": 5000}).json()["error"]["code"] == "OFFSET_OUT_OF_RANGE"
    assert client.get(f"/events/{topic}", params={"offset": 5}).status_code == 400


def test_04b_server_side_consumer_offsets(client, same_key):
    topic, sent = same_key
    part = sent[0]["source_partition"]
    cid = f"c-{uuid.uuid4().hex[:8]}"
    r = client.get(f"/events/{topic}", params={"partition": part, "consumer_id": cid, "limit": 200}).json()
    assert r["events"][0]["payload"]["seq"] == 0 and len(r["events"]) == 200
    # nothing was acknowledged yet: a re-read starts from the beginning again
    assert client.get(f"/events/{topic}", params={"partition": part, "consumer_id": cid, "limit": 1}).json()["events"][0]["payload"]["seq"] == 0
    assert client.put(f"/consumers/{cid}/offsets", json={"topic": topic, "offsets": {part: 200}}).status_code == 204
    r = client.get(f"/events/{topic}", params={"partition": part, "consumer_id": cid, "limit": 100, "commit": "true"}).json()
    assert r["events"][0]["payload"]["seq"] == 200 and r["next_offsets"][str(part)] == 300
    r = client.get(f"/events/{topic}", params={"partition": part, "consumer_id": cid, "limit": 1}).json()
    assert r["events"][0]["payload"]["seq"] == 300  # commit=true advanced the stored offset


# ---- idempotency ---------------------------------------------------------------------------------------
def test_05_idempotency_contract(client):
    topic = new_topic(client, "t-idem", 1)
    key = f"req-{uuid.uuid4()}"
    ev = event(topic, "k", {"n": 1})
    first = client.post("/events", json=ev, headers={"Idempotency-Key": key})
    again = client.post("/events", json=ev, headers={"Idempotency-Key": key})
    assert first.status_code == again.status_code == 200
    assert "idempotent-replay" not in first.headers and again.headers["idempotent-replay"] == "true"
    assert again.json()["source_offset"] == first.json()["source_offset"] == 0
    assert again.json()["idempotent_replay"] is True

    # same key + different content: rejected, nothing written
    other = event(topic, "k", {"n": 2})
    r = client.post("/events", json=other, headers={"Idempotency-Key": key})
    assert r.status_code == 409 and r.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"
    # same event_id, no key, same content: replay;  different content: conflict
    assert client.post("/events", json=ev).headers["idempotent-replay"] == "true"
    r = client.post("/events", json={**ev, "payload": {"n": 3}})
    assert r.status_code == 409 and r.json()["error"]["code"] == "EVENT_ID_CONFLICT"
    # event_id alone is the dedupe key when no header is sent
    solo = event(topic, "k", {"n": 9})
    assert "idempotent-replay" not in client.post("/events", json=solo).headers
    assert client.post("/events", json=solo).headers["idempotent-replay"] == "true"

    stored = read_all(client, topic)
    assert [e["payload"]["n"] for e in stored] == [1, 9]  # exactly one record per accepted event

    # concurrent duplicates: one write
    ev2 = event(topic, "k", {"n": 5})
    results = []

    def go():
        results.append(client.post("/events", json=ev2, headers={"Idempotency-Key": "race-" + ev2["event_id"]}))

    ts = [threading.Thread(target=go) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert all(r.status_code == 200 for r in results)
    assert len({r.json()["source_offset"] for r in results}) == 1
    assert [e["payload"]["n"] for e in read_all(client, topic)].count(5) == 1


# ---- restarts: gateway, then broker ---------------------------------------------------------------------
def test_06_gateway_restart_keeps_stream_and_idempotency(client, same_key):
    topic, sent = same_key
    part = sent[0]["source_partition"]
    itopic = new_topic(client, "t-idem-restart", 1)
    ev = event(itopic, "k", {"n": 1})
    key = f"restart-{uuid.uuid4()}"
    first = client.post("/events", json=ev, headers={"Idempotency-Key": key}).json()

    sh("docker", "restart", "ss-a-gateway")
    wait_health()

    r = client.get(f"/events/{topic}", params={"partition": part, "offset": 500, "limit": 1000}).json()
    assert len(r["events"]) == 500 and r["events"][0]["payload"]["seq"] == 500
    assert len(read_all(client, topic)) == 1000
    assert client.get(f"/topics/{topic}").json()["policy"]["local_replication_factor"] == 3  # policy reloaded from Kafka

    again = client.post("/events", json=ev, headers={"Idempotency-Key": key})
    assert again.headers["idempotent-replay"] == "true" and again.json()["source_offset"] == first["source_offset"]
    assert len(read_all(client, itopic)) == 1
    # still writable, and offsets continue where they were
    nxt = client.post("/events", json=event(topic, "account-42", {"seq": 1000})).json()
    assert nxt["source_offset"] == 1000


def test_07_kafka_broker_restart_stream_persists(client, same_key, spread):
    topic, sent = same_key
    stopper = threading.Thread(target=lambda: sh("docker", "restart", "ss-a-broker-1", timeout=240))
    stopper.start()
    acked, failed = [], []
    t_end = time.time() + 90
    while (stopper.is_alive() or len(acked) < 40) and time.time() < t_end:
        ev = event(topic, f"during-{len(acked) + len(failed)}", {"seq": -1})
        r = client.post("/events", json=ev)
        if r.status_code == 200:
            acked.append(ev["event_id"])
        else:
            assert r.status_code == 503 and r.json()["error"]["retryable"], r.text  # clean, retryable refusal
            failed.append(ev["event_id"])
        time.sleep(0.1)
    stopper.join()
    assert acked, "writes should keep succeeding with 2 of 3 brokers (min ISR 2)"

    deadline = time.time() + 120  # wait for ISR to recover to 3
    while time.time() < deadline:
        out = sh(
            "docker",
            "exec",
            "ss-a-broker-2",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "ss-a-broker-2:9092",
            "--describe",
            "--topic",
            topic,
            check=False,
        ).stdout
        lines = [l for l in out.splitlines() if "Partition:" in l]
        if lines and all(len(l.split("Isr:")[1].strip().split(",")) == 3 for l in lines):
            break
        time.sleep(2)
    else:
        pytest.fail("ISR did not recover to 3")

    wait_health()
    got = read_all(client, topic)
    ids = [e["event_id"] for e in got]
    assert len(ids) == len(set(ids)), "duplicate events after restart"
    assert set(acked) <= set(ids), "an acknowledged event was lost"
    assert [e["payload"]["seq"] for e in got if e["partition_key"] == "account-42"] == list(range(1001))
    assert len(read_all(client, spread[0])) == 1000
    r = client.post("/events", json=event(topic, "after-recovery"))
    assert r.status_code == 200


# ---- epochs, leases, active region ----------------------------------------------------------------------
def test_08_stale_leadership_epoch_rejected(client):
    topic = new_topic(client, "t-epoch", 1)
    assert client.post("/events", json=event(topic, "k", expected_epoch=1)).status_code == 200
    set_topic_control(topic, leadership_epoch=2, replication_generation=2)  # control plane bumps the epoch
    st = wait_epoch(client, topic, 2)
    assert st["role"] == "active" and st["lease"]["valid"]

    r = client.post("/events", json=event(topic, "k", expected_epoch=1))
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "STALE_EPOCH" and err["details"]["current_epoch"] == 2
    assert client.post("/events", json=event(topic, "k", expected_epoch=3)).status_code == 409  # not a future epoch either
    assert len(read_all(client, topic)) == 1  # nothing from rejected writes

    ok = client.post("/events", json=event(topic, "k", expected_epoch=2))
    assert ok.status_code == 200 and ok.json()["leadership_epoch"] == 2 and ok.json()["replication_generation"] == 2
    assert client.post("/events", json=event(topic, "k")).json()["leadership_epoch"] == 2  # stamped by the gateway


def test_08b_not_active_region_rejected_reads_allowed(client):
    topic = new_topic(client, "t-passive", 1)
    assert client.post("/events", json=event(topic, "k")).status_code == 200
    set_topic_control(topic, active_region="region-b", leadership_epoch=2, replication_generation=2)
    wait_epoch(client, topic, 2)
    r = client.post("/events", json=event(topic, "k"))
    assert r.status_code == 421 and r.json()["error"]["code"] == "NOT_ACTIVE_REGION"
    assert r.json()["error"]["details"]["active_region"] == "region-b"
    g = client.get(f"/events/{topic}", params={"partition": 0, "offset": 0})
    assert g.status_code == 200 and g.headers["x-ss-region-role"] == "passive" and len(g.json()["events"]) == 1
    assert client.get("/active-region", params={"topic": topic}).json()["role"] == "passive"


def test_08c_lease_expiry_self_fences_writes(client):
    topic = new_topic(client, "t-lease", 1)
    assert client.post("/events", json=event(topic, "k")).status_code == 200
    good = STATE_FILE.read_text()
    STATE_FILE.write_text("{ control plane unreachable")  # renewals now fail
    try:
        time.sleep(LEASE_TTL_S + 3)
        r = client.post("/events", json=event(topic, "k"))
        assert r.status_code == 503 and r.json()["error"]["code"] == "LEASE_EXPIRED"
        g = client.get(f"/events/{topic}", params={"partition": 0, "offset": 0})
        assert g.status_code == 200 and g.headers.get("x-ss-stale") == "true"  # reads continue, flagged stale
        assert httpx.get(f"{BASE}/api/v1/health").status_code == 503
    finally:
        STATE_FILE.write_text(good)
    deadline = time.time() + 20
    while time.time() < deadline and client.post("/events", json=event(topic, "k")).status_code != 200:
        time.sleep(1)
    assert client.post("/events", json=event(topic, "k")).status_code == 200


def test_09_health_and_active_region(client, anon):
    wait_health()
    h = anon.get("/health")
    assert h.status_code == 200 and h.json()["status"] == "ok" and h.json()["kafka_reachable"]
    ar = client.get("/active-region").json()
    assert ar["active_region"] == "region-a" and ar["role"] == "active" and ar["lease"]["valid"]


# ---- gateway-only security model ------------------------------------------------------------------------
def tcp_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def in_client_net(script: str) -> subprocess.CompletedProcess:
    return sh("docker", "run", "--rm", "--network", "ss-client-net", "--entrypoint", "sh", "apache/kafka:3.9.0", "-c", script, check=False)


def test_10_brokers_unreachable_from_client_side():
    exposed = bool(sh("docker", "port", "ss-a-broker-1", check=False).stdout.strip())
    if exposed:
        pytest.skip("stack was started with --expose-kafka (debug override); gateway-only model is OFF")
    for port in (29092, 29093, 29094, 39092, 49092, 9092, 9093):  # from the host
        assert not tcp_open("localhost", port), f"Kafka port {port} reachable from the host"

    ip = sh("docker", "inspect", "-f", '{{(index .NetworkSettings.Networks "ss-region-a-net").IPAddress}}', "ss-a-broker-1").stdout.strip()
    assert ip
    # positive control: the client network can reach the gateway
    assert in_client_net("nc -z -w3 ss-a-gateway 8080").returncode == 0
    # negative: from the client network, brokers can neither be resolved nor connected to by IP
    assert in_client_net("getent hosts ss-a-broker-1").returncode != 0
    for port in (9092, 29092, 9093):
        assert in_client_net(f"nc -z -w3 {ip} {port}").returncode != 0, f"broker {ip}:{port} reachable from client network"
    assert in_client_net("nc -z -w3 host.docker.internal 29092").returncode != 0

    nets = set(sh("docker", "inspect", "-f", "{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}", "ss-a-gateway").stdout.split())
    assert nets == {"ss-client-net", "ss-region-a-net", "ss-control-net"}, nets  # control-net: policy traffic only
    for b in ("ss-a-broker-1", "ss-a-controller-1", "ss-b-kafka-1", "ss-c-kafka-1"):
        bn = set(sh("docker", "inspect", "-f", "{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}", b).stdout.split())
        assert not bn & {"ss-client-net", "ss-control-net"}, f"{b} is on a client-reachable or control network: {bn}"
    # the gateway itself cannot reach other regions
    r = sh("docker", "exec", "ss-a-gateway", "python", "-c", "import socket;socket.create_connection(('ss-b-kafka-1',9092),2)", check=False)
    assert r.returncode != 0


def test_11_api_never_reveals_broker_endpoints(client):
    topic = new_topic(client, "t-leak", 1)
    texts = [
        client.get("/health").text,
        client.get(f"/topics/{topic}").text,
        client.get("/active-region").text,
        client.post("/events", json=event(topic, "k")).text,
        client.get(f"/events/{topic}").text,
        client.post("/topics", json={"name": topic}).text,
    ]
    for t in texts:
        assert "ss-a-broker" not in t and ":9092" not in t and "bootstrap" not in t.lower() and "password" not in t.lower()
    assert client.get("/topics/does-not-exist").status_code == 404
