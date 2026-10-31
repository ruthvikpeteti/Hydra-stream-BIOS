"""Phase 8 end-to-end scenarios 1 and 2 (they need only the normal stack: scripts/up.sh, or the DR stack).

Scenario 1 interpretation, stated plainly: an EU-restricted topic can only live in regions whose legal zones include EU. In this deployment
region C is US-only, so an EU-restricted topic replicates A->B and C is PROHIBITED (that is scenario 2). Replication to both B and C is
therefore shown with a second topic in the GLOBAL zone."""

import httpx

from itest import *  # noqa: F403
from itest import (
    api,
    dump,
    first_occurrence_order,
    gw,
    gw_topic,
    produce,
    send_batch,
    start_replication,
    sh,
    uid,
    uniq,
    wait_unique,
    fake_batch,
)


def per_partition_order(recs):
    """{partition: [event_id in log order]} (duplicates collapsed to their first occurrence)."""
    out: dict[int, list[str]] = {}
    for r in sorted(recs, key=lambda x: (x["partition"], x["offset"])):
        if r["value"]:
            ids = out.setdefault(r["partition"], [])
            if r["value"]["event_id"] not in ids:
                ids.append(r["value"]["event_id"])
    return out


def topic_exists(region, topic):
    c = {"region-a": "ss-a-broker-1", "region-b": "ss-b-kafka-1", "region-c": "ss-c-kafka-1"}[region]
    out = sh("docker", "exec", c, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", f"{c}:9092", "--list").stdout.split()
    return topic in out


def eu_restricted_topic(prefix="s-eu"):
    name = uid(prefix)
    body = {
        "name": name,
        "partitions": 3,
        "legal_zone": "EU",
        "classification": "RESTRICTED",
        "allowed_storage_regions": ["region-a", "region-b"],
        "allowed_processing_regions": ["region-a", "region-b"],
        "allowed_transit_zones": ["EU"],
        "failover_priority": ["region-a", "region-b"],
        "regional_replica_count": 1,
        "durability_mode": "ASYNC_REGIONAL",
        "local_replication_factor": 1,
        "local_min_insync_replicas": 1,
    }
    r = gw("POST", "/topics", json=body)
    assert r.status_code == 201, r.text
    return name


def test_scenario_1_normal_operation_eu_restricted_topic_replicates_to_b_and_global_topic_to_b_and_c():
    t = eu_restricted_topic()
    start_replication(t, 3)
    ids = produce(t, 300, keys=12)
    a = dump("region-a", t)
    assert uniq(a) == set(ids), "every acknowledged event is persisted locally in A"
    b = wait_unique("region-b", t, 300)
    assert uniq(b) == set(ids)
    assert per_partition_order(b) == per_partition_order(a), "B holds every partition in the same order as A"
    assert not topic_exists("region-c", t), "the US-only region never receives the EU-restricted topic"
    g = gw_topic("s-glob", 3)
    start_replication(g, 3)
    gids = produce(g, 300, keys=12)
    ga = dump("region-a", g)
    assert uniq(ga) == set(gids)
    for r in ("region-b", "region-c"):
        d = wait_unique(r, g, 300)
        assert uniq(d) == set(gids) and per_partition_order(d) == per_partition_order(ga) and first_occurrence_order(d) == first_occurrence_order(ga), r


def test_scenario_2_residency_rejection_at_the_source_the_receiver_and_in_the_audit_log():
    t = eu_restricted_topic("s-res")
    start_replication(t, 3)
    produce(t, 20)
    wait_unique("region-b", t, 20)
    # (1) source side: the policy engine refuses to create a flow into the prohibited region (and says why)
    r = api("POST", f"/replication/topics/{t}/flows", "admin", json={"source_region": "region-a", "target_region": "region-c"})
    assert r.status_code == 403 and r.json()["decision"] == "REJECTED"
    codes = {x["code"] for x in r.json()["reasons"]}
    assert codes & {"LEGAL_STORAGE_REGION", "REGION_NOT_ALLOWED", "LEGAL_ZONE", "STORAGE_NOT_ALLOWED"} or any(
        "LEGAL" in x["category"] for x in r.json()["reasons"]
    ), r.json()
    assert flow_exists(t, "region-c") is False
    # (2) receiver side: a batch for the prohibited region is refused by the REGION'S OWN agent even with valid source credentials
    before = httpx.get("http://localhost:8093/internal/v1/replication/_stats", headers={"X-Service-Key": KEYS["service"]}, timeout=5).json()["records_written"]
    resp = send_batch("region-c", fake_batch(t, source="region-a", dest="region-c"), path="a-c-direct")
    assert resp.status_code in (403, 409), resp.text
    assert resp.json()["error"]["code"] in ("POLICY_REJECTED", "FLOW_NOT_AUTHORIZED", "POLICY_UNAVAILABLE", "UNKNOWN_FLOW"), resp.text
    after = httpx.get("http://localhost:8093/internal/v1/replication/_stats", headers={"X-Service-Key": KEYS["service"]}, timeout=5).json()["records_written"]
    assert after == before and not topic_exists("region-c", t), "nothing was written to the prohibited region"
    # (3) audit: both refusals are recorded with reasons, a policy version and the enforcement point; the chain is intact
    rows = [
        a
        for a in api("GET", "/audit", params={"topic": t, "decision": "REJECTED", "limit": 200}).json()["decisions"]
        if a["operation"] == "FLOW_CREATE" and a["source"] == "engine"
    ]
    assert rows and rows[-1]["reasons"] and rows[-1]["policy_version"] is not None and rows[-1]["enforcement_point"] == "FLOW_INIT"
    wait_for(
        lambda: [
            a
            for a in api("GET", "/audit", params={"topic": t, "decision": "REJECTED", "limit": 200}).json()["decisions"]
            if a["source"].startswith("agent:region-c")
        ],
        30,
        1,
        "the receiver's rejection in the audit log",
    )
    assert api("GET", "/audit/verify").json()["ok"] is True


def flow_exists(topic, target):
    return any(f["target_region"] == target and f["state"] != "DISABLED" for f in topology(topic)["flows"])
