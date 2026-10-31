"""Integration tests against the running stack: engine + PostgreSQL + three regional agents + gateway.
Each test builds its own uniquely-named regions/topics, so runs do not interfere (the DB keeps them afterwards)."""

import time
import uuid

import httpx
import pytest

from itest import *  # noqa: F401,F403

pytestmark = pytest.mark.skipif(not engine_up(), reason="stack is not running (infrastructure/scripts/up.sh)")


@pytest.fixture(scope="module")
def world():
    return mkworld()


def eu(world, topic=None, **kw):
    """EU topic homed in central; storage/processing = the three EU regions (never us)."""
    t = topic or uid("eu")
    eu_regions = [world["central"], world["west"], world["south"]]
    return policy_body(t, world["central"], eu_regions, **kw)


# ===== The twelve required scenarios, through the engine API =====================================================
def test_01_eu_topic_to_eu_west_allowed(world):
    b = eu(world)
    create_policy(b)
    r, rid = evaluate(b["topic"], "ASSIGN", source_region=world["central"], destination_region=world["west"])
    j = r.json()
    assert r.status_code == 200 and j["decision"] == "ALLOWED" and j["policy_version"] == 1


def test_02_eu_topic_to_eu_central_allowed(world):
    b = eu(world)
    create_policy(b)
    r, _ = evaluate(b["topic"], "STORE", region=world["central"])
    assert r.json()["decision"] == "ALLOWED"


def test_03_eu_topic_to_us_east_rejected_with_reason_and_version(world):
    b = eu(world)
    create_policy(b)
    r, rid = evaluate(b["topic"], "ASSIGN", source_region=world["central"], destination_region=world["us"])
    j = r.json()
    assert r.status_code == 403 and j["decision"] == "REJECTED" and j["policy_version"] == 1
    assert {"LEGAL_ZONE_MISMATCH", "REGION_NOT_ALLOWED_STORAGE", "TRANSIT_ZONE_NOT_ALLOWED"} <= codes(j)
    assert all(x["message"] for x in j["reasons"])


def test_04_unauthorized_transit_zone_rejected(world):
    n = mkworld()
    put_link(n["central"], n["west"], ["EU", "ATLANTIC"], 5)  # the only direct link crosses the Atlantic
    for k in ("south",):
        for a, b in ((n["central"], n[k]), (n[k], n["central"]), (n["west"], n[k]), (n[k], n["west"])):
            put_link(a, b, ["EU"], 50, "DOWN")  # and no usable detour
    b = policy_body(uid("tz"), n["central"], [n["central"], n["west"]])
    create_policy({**b, "regional_replica_count": 0, "maximum_replication_lag_ms": None, "durability_mode": "LOCAL_DURABLE"})
    r, _ = evaluate(b["topic"], "ROUTE", route=[n["central"], n["west"]])
    assert r.json()["decision"] == "REJECTED" and "TRANSIT_ZONE_NOT_ALLOWED" in codes(r.json())
    r, _ = evaluate(b["topic"], "ROUTE", route=[n["central"], n["us"], n["west"]])
    assert r.json()["decision"] == "REJECTED" and "RELAY_REGION_NOT_ALLOWED" in codes(r.json())
    r, _ = evaluate(b["topic"], "ASSIGN", source_region=n["central"], destination_region=n["west"])
    assert r.json()["decision"] == "REJECTED" and "TRANSIT_ZONE_NOT_ALLOWED" in codes(r.json())


def test_05_healthy_but_prohibited_destination_rejected(world):
    assert next(r for r in api("GET", "/regions").json()["regions"] if r["region_id"] == world["us"])["availability_status"] == "AVAILABLE"
    b = eu(world)
    create_policy(b)
    r, _ = evaluate(b["topic"], "PLAN", count=4)
    j = r.json()
    assert j["decision"] == "INSUFFICIENT_ELIGIBLE_REGIONS" and r.status_code == 409 and j["policy_version"] == 1
    assert world["us"] not in {c["region"] for c in j["details"]["eligible"]}
    assert world["us"] in {x["region"] for x in j["details"]["rejected"]}


def test_06_slow_but_permitted_destination_remains_eligible(world):
    b = eu(world, regional_replica_count=2)
    create_policy(b)
    r, _ = evaluate(b["topic"], "PLAN", count=2)
    j = r.json()
    assert j["decision"] == "ALLOWED"
    assert world["south"] in {c["region"] for c in j["details"]["eligible"]}  # 900 ms but legal
    r, _ = evaluate(b["topic"], "ASSIGN", source_region=world["central"], destination_region=world["south"])
    assert r.json()["decision"] == "ALLOWED"


def test_07_unknown_region_rejected(world):
    b = eu(world)
    create_policy(b)
    for op, kw in (
        ("STORE", {"region": "atlantis"}),
        ("PROCESS", {"region": "atlantis"}),
        ("ASSIGN", {"source_region": world["central"], "destination_region": "atlantis"}),
        ("PROMOTE", {"region": "atlantis", "replica_status": "IN_SYNC"}),
        ("BACKUP", {"source_region": world["central"], "destination_region": "atlantis"}),
    ):
        r, _ = evaluate(b["topic"], op, **kw)
        assert r.json()["decision"] == "REJECTED" and "UNKNOWN_REGION" in codes(r.json()), op
    bad = policy_body(uid("u"), world["central"], [world["central"], "atlantis"])
    r = api("POST", "/policies", "writer", json=bad)
    assert r.status_code == 422 and "UNKNOWN_REGION" in codes(r.json()) and r.json()["policy_version"] == 1


def test_08a_unknown_topic_is_policy_unavailable():
    r, _ = evaluate("no-such-topic-xyz", "STORE", region="region-a")
    assert r.status_code == 404 and r.json()["decision"] == "POLICY_UNAVAILABLE"
    assert agent("region-a", "POST", "/authorize", json={"topic": "no-such-topic-xyz", "operation": "PROCESS"}).json()["decision"] == "POLICY_UNAVAILABLE"


def test_10_promotion_into_prohibited_region_rejected():
    t = uid("promo")
    create_policy(policy_body(t, "region-a", ["region-a", "region-b"], replication_factor=1))
    r, _ = evaluate(t, "PROMOTE", region="region-c", replica_status="IN_SYNC")
    assert r.json()["decision"] == "REJECTED" and "REGION_NOT_ALLOWED_PROCESSING" in codes(r.json()) and r.json()["policy_version"] == 1
    # the receiving region's own agent refuses independently of what the sender decided
    j = agent("region-c", "POST", "/authorize", json={"topic": t, "operation": "PROMOTE", "replica_status": "IN_SYNC", "enforcement_point": "PROMOTION"}).json()
    assert j["decision"] == "REJECTED" and "LEGAL_ZONE_MISMATCH" in codes(j)
    j = agent("region-b", "POST", "/authorize", json={"topic": t, "operation": "PROMOTE", "replica_status": "IN_SYNC", "enforcement_point": "PROMOTION"}).json()
    assert j["decision"] == "ALLOWED"
    j = agent("region-b", "POST", "/authorize", json={"topic": t, "operation": "PROMOTE", "enforcement_point": "PROMOTION"}).json()
    assert j["decision"] == "REJECTED" and "NO_REPLICA" in codes(j)


def test_11_backup_destination_outside_permitted_set_rejected():
    t = uid("bk")
    create_policy(policy_body(t, "region-a", ["region-a", "region-b"], replication_factor=1))
    r, _ = evaluate(t, "BACKUP", source_region="region-a", destination_region="region-c")
    assert r.json()["decision"] == "REJECTED" and "REGION_NOT_ALLOWED_STORAGE" in codes(r.json())
    j = agent("region-c", "POST", "/authorize", json={"topic": t, "operation": "BACKUP", "source_region": "region-a", "enforcement_point": "BACKUP"}).json()
    assert j["decision"] == "REJECTED"
    assert (
        agent("region-b", "POST", "/authorize", json={"topic": t, "operation": "BACKUP", "source_region": "region-a", "enforcement_point": "BACKUP"}).json()[
            "decision"
        ]
        == "ALLOWED"
    )


def test_12_processing_request_from_unauthorized_region_rejected():
    t = uid("proc")
    create_policy(policy_body(t, "region-a", ["region-a", "region-b"], ["region-a"], replication_factor=1))
    for region in ("region-c", "region-b"):  # c: wrong zone; b: stores but may not process
        r, _ = evaluate(t, "PROCESS", region=region)
        j = r.json()
        assert j["decision"] == "REJECTED" and "REGION_NOT_ALLOWED_PROCESSING" in codes(j) and j["policy_version"] == 1
        assert (
            agent(region, "POST", "/authorize", json={"topic": t, "operation": "PROCESS", "enforcement_point": "PROCESSING"}).json()["decision"] == "REJECTED"
        )
    assert evaluate(t, "PROCESS", region="region-a")[0].json()["decision"] == "ALLOWED"


# ===== Policy CRUD, versions, revocation ===========================================================================
def test_policy_crud_and_versioning(world):
    b = eu(world)
    j = create_policy(b)
    assert j["decision"] == "ALLOWED" and j["policy_version"] == 1 and j["details"]["placement"]["chosen"]
    assert api("POST", "/policies", "writer", json=b).status_code == 409  # already exists
    got = api("GET", f"/policies/{b['topic']}").json()
    assert got["policy_version"] == 1 and got["policy"]["legal_zone"] == "EU"
    assert api("PUT", f"/policies/{b['topic']}", "admin", json=b).status_code == 412  # no If-Match
    assert api("PUT", f"/policies/{b['topic']}", "admin", json=b, headers={"If-Match": "7"}).status_code == 412
    upd = {**b, "maximum_replication_lag_ms": 1000}
    r = api("PUT", f"/policies/{b['topic']}", "admin", json=upd, headers={"If-Match": "1"})
    assert r.status_code == 200 and r.json()["policy_version"] == 2
    assert api("PUT", f"/policies/{b['topic']}", "admin", json=upd, headers={"If-Match": "1"}).status_code == 412  # stale writer
    assert [h["policy_version"] for h in api("GET", f"/policies/{b['topic']}").json()["history"]] == [1, 2]
    # roles: a writer may create but not update; a service credential may not create
    assert api("PUT", f"/policies/{b['topic']}", "writer", json=upd, headers={"If-Match": "2"}).status_code == 403
    assert api("POST", "/policies", "service", json=eu(world)).status_code == 403
    assert httpx.get(f"{ENGINE}/policies/{b['topic']}").status_code == 401


def test_policy_rejections_carry_reason_and_proposed_version(world):
    cases = {
        "LEGAL_ZONE_MISMATCH": eu(world, allowed_storage_regions=[world["central"], world["us"]]),
        "HOME_REGION_NOT_ALLOWED": eu(world, home_region=world["west"], allowed_storage_regions=[world["central"]]),
        "PREFERRED_NOT_ALLOWED": eu(world, preferred_regions=[world["us"]]),
        "HOME_REGION_EXCLUDED": eu(world, excluded_regions=[world["central"]]),
    }
    for code, body in cases.items():
        r = api("POST", "/policies", "writer", json=body)
        assert r.status_code == 422, (code, r.text)
        assert code in codes(r.json()) and r.json()["policy_version"] == 1 and r.json()["details"]["policy_version_kind"] == "proposed"
        assert api("GET", f"/policies/{body['topic']}").status_code == 404  # nothing stored
    over = eu(world, regional_replica_count=3)  # only 2 other EU regions exist
    r = api("POST", "/policies", "writer", json=over)
    assert r.status_code == 409 and r.json()["decision"] == "INSUFFICIENT_ELIGIBLE_REGIONS"
    assert api("GET", f"/policies/{over['topic']}").status_code == 404


def test_policy_narrowing_revokes_assignments_and_db_guard_blocks_illegal_rows(world):
    b = eu(world, regional_replica_count=1, preferred_regions=[world["west"]])
    create_policy(b)
    r = api("POST", f"/placement/{b['topic']}/assignments", "writer", json={"partitions": 2})
    j = r.json()
    assert r.status_code == 200 and j["decision"] == "ALLOWED" and {a["destination_region"] for a in j["assignments"]} == {world["west"]}
    assert all(a["replica_status"] == "PENDING" and a["policy_version"] == 1 for a in j["assignments"])
    for part in (0, 1):
        assert api("PUT", f"/placement/{b['topic']}/assignments/{part}/{world['west']}/status", json={"replica_status": "IN_SYNC"}).status_code == 200
    assert evaluate(b["topic"], "PROMOTE", region=world["west"])[0].json()["decision"] == "ALLOWED"  # status from DB
    # the organisation removes eu-west from the policy
    narrow = {
        **b,
        "allowed_storage_regions": [world["central"], world["south"]],
        "allowed_processing_regions": [world["central"], world["south"]],
        "preferred_regions": [],
    }
    r = api("PUT", f"/policies/{b['topic']}", "admin", json=narrow, headers={"If-Match": "1"})
    j = r.json()
    assert r.status_code == 200 and j["policy_version"] == 2 and len(j["revoked_assignments"]) == 2
    assert all(x["destination_region"] == world["west"] for x in j["revoked_assignments"])
    pl = api("GET", f"/placement/{b['topic']}").json()
    assert {a["replica_status"] for a in pl["assignments"]} == {"REVOKED"}
    # a revoked replica can neither be promoted nor re-activated
    r, _ = evaluate(b["topic"], "PROMOTE", region=world["west"])
    assert r.json()["decision"] == "REJECTED" and "REGION_NOT_ALLOWED_STORAGE" in codes(r.json())
    r = api("PUT", f"/placement/{b['topic']}/assignments/0/{world['west']}/status", json={"replica_status": "IN_SYNC"})
    assert r.status_code == 403
    # DB-level guard: raw SQL cannot create a live assignment in a prohibited region either
    sql = (
        f"INSERT INTO replica_assignments (topic,partition,source_region,destination_region,replica_status,policy_version) "
        f"VALUES ('{b['topic']}', 7, '{world['central']}', '{world['us']}', 'PENDING', 2)"
    )
    out = sh("docker", "exec", "ss-postgres", "psql", "-U", "sspolicy", "-d", "sspolicy", "-c", sql, check=False)
    assert out.returncode != 0 and "may not store topic" in out.stderr


def test_region_change_that_makes_assignment_illegal_revokes_it():
    n = mkworld()
    b = policy_body(uid("rz"), n["central"], [n["central"], n["west"]])
    create_policy(b)
    assert api("POST", f"/placement/{b['topic']}/assignments", "writer", json={"partitions": 1}).status_code == 200
    j = put_region(rid=n["west"], zone="US", fd="x")  # the registry now says west is in the US
    assert [x["destination_region"] for x in j["revoked_assignments"]] == [n["west"]]
    assert "LEGAL_ZONE_MISMATCH" in j["revoked_assignments"][0]["reasons"]


def test_recovery_destination_and_failback(world):
    b = eu(world, regional_replica_count=1)
    create_policy(b)
    r, _ = evaluate(b["topic"], "RECOVERY", source_region=world["central"], failed_regions=[world["west"]])
    j = r.json()
    assert j["decision"] == "ALLOWED" and j["details"]["chosen"][0]["region"] == world["south"]
    r, _ = evaluate(b["topic"], "RECOVERY", source_region=world["central"], failed_regions=[world["west"], world["south"]])
    assert r.json()["decision"] == "INSUFFICIENT_ELIGIBLE_REGIONS"  # the US region is healthy but illegal
    assert world["us"] not in {c["region"] for c in r.json()["details"]["eligible"]}
    r, _ = evaluate(b["topic"], "FAILBACK", region=world["central"], source_region=world["west"])
    assert r.json()["decision"] == "ALLOWED"
    # tighten the policy so the original region is no longer permitted: failback must refuse
    narrow = {
        **b,
        "home_region": world["west"],
        "allowed_storage_regions": [world["west"], world["south"]],
        "allowed_processing_regions": [world["west"], world["south"]],
    }
    assert api("PUT", f"/policies/{b['topic']}", "admin", json=narrow, headers={"If-Match": "1"}).status_code == 200
    r, _ = evaluate(b["topic"], "FAILBACK", region=world["central"], source_region=world["west"], enforcement_point="FAILBACK")
    j = r.json()
    assert j["decision"] == "REJECTED" and "REGION_NOT_ALLOWED_STORAGE" in codes(j) and j["policy_version"] == 2
    assert (
        evaluate(b["topic"], "ASSIGN", source_region=world["west"], destination_region=world["central"], policy_version=1)[0].json()["decision"] == "REJECTED"
    )  # stale caller


# ===== Receiving-side enforcement (regional agents) ================================================================
def test_regional_replica_creation_is_enforced_by_the_receiving_agent():
    t = uid("rep")
    create_policy(policy_body(t, "region-a", ["region-a", "region-b"], replication_factor=1))
    r = api("POST", f"/placement/{t}/provision", "writer", json={"partitions": 3}, headers={"X-Request-Id": "prov-" + t})
    j = r.json()
    assert r.status_code == 200 and j["decision"] == "ALLOWED" and [x["destination_region"] for x in j["results"]] == ["region-b"], j
    assert t in kafka_topics("ss-b-kafka-1") and t not in kafka_topics("ss-c-kafka-1")
    desc = sh("docker", "exec", "ss-b-kafka-1", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "ss-b-kafka-1:9092", "--describe", "--topic", t).stdout
    assert "PartitionCount: 3" in desc
    st = api("GET", f"/placement/{t}").json()
    assert {a["replica_status"] for a in st["assignments"]} == {"PROVISIONED"}
    # a sender that bypasses the engine and asks region-c's agent directly is refused: C re-validates on its own
    r = agent("region-c", "POST", "/replicas", json={"topic": t, "partition_count": 3, "source_region": "region-a", "policy_version": 1})
    assert r.status_code == 403 and r.json()["decision"] == "REJECTED" and r.json()["policy_version"] == 1
    assert {"LEGAL_ZONE_MISMATCH", "REGION_NOT_ALLOWED_STORAGE"} <= codes(r.json())
    assert t not in kafka_topics("ss-c-kafka-1")
    # and a stale sender is refused too
    r = agent("region-b", "POST", "/replicas", json={"topic": t, "partition_count": 3, "source_region": "region-a", "policy_version": 0})
    assert r.json()["decision"] == "REJECTED" and "STALE_POLICY_VERSION" in codes(r.json())


def test_provision_with_no_eligible_destination_creates_nothing():
    t = uid("none")
    create_policy(policy_body(t, "region-a", ["region-a", "region-b"], replication_factor=1))
    assert (
        api("PUT", "/regions/region-b", "admin", json=region_body("region-b", "EU", "site-b", avail="UNAVAILABLE", total=200 * GB, max_rf=1)).status_code == 200
    )
    try:
        r = api("POST", f"/placement/{t}/provision", "writer", json={"partitions": 1})
        assert r.status_code == 409 and r.json()["decision"] == "INSUFFICIENT_ELIGIBLE_REGIONS"
        assert t not in kafka_topics("ss-b-kafka-1")
    finally:
        assert api("PUT", "/regions/region-b", "admin", json=region_body("region-b", "EU", "site-b", total=200 * GB, max_rf=1)).status_code == 200
        wait_for(lambda: agent("region-b", "GET", "/status").json()["bundle_version"] and True, 10)


def test_policy_version_change_stops_a_stale_replication_worker():
    t, flow = uid("ver"), uid("flow")
    create_policy(policy_body(t, "region-a", ["region-a", "region-b"], replication_factor=1))
    init = agent(
        "region-b",
        "POST",
        "/authorize",
        json={"topic": t, "operation": "ASSIGN", "source_region": "region-a", "policy_version": 1, "flow_id": flow, "enforcement_point": "FLOW_INIT"},
    ).json()
    assert init["decision"] == "ALLOWED"
    batch = {
        "topic": t,
        "flow_id": flow,
        "policy_version": 1,
        "source_region": "region-a",
        "record_count": 100,
        "first_offset": 0,
        "last_offset": 99,
        "bytes": 5000,
    }
    assert agent("region-b", "POST", "/batches", json=batch).json()["decision"] == "ALLOWED"
    # policy v2: still legal but changed; the flow was admitted under v1
    upd = policy_body(t, "region-a", ["region-a", "region-b"], replication_factor=1, maximum_replication_lag_ms=1000)
    assert api("PUT", f"/policies/{t}", "admin", json=upd, headers={"If-Match": "1"}).json()["policy_version"] == 2

    def stale():
        j = agent("region-b", "POST", "/batches", json=batch).json()
        return j if j["decision"] == "REJECTED" else None

    j = wait_for(stale, 20, 1, "agent to pick up policy v2")  # refresh interval 5 s
    assert j["reasons"][0]["code"] == "STALE_POLICY_VERSION" and j["policy_version"] == 2
    b2 = {**batch, "policy_version": 2}
    j = agent("region-b", "POST", "/batches", json=b2).json()
    assert j["decision"] == "REJECTED" and j["reasons"][0]["code"] == "STALE_FLOW"  # must re-initialise under v2
    re = agent(
        "region-b",
        "POST",
        "/authorize",
        json={"topic": t, "operation": "ASSIGN", "source_region": "region-a", "policy_version": 2, "flow_id": flow, "enforcement_point": "FLOW_INIT"},
    ).json()
    assert re["decision"] == "ALLOWED"
    assert agent("region-b", "POST", "/batches", json=b2).json()["decision"] == "ALLOWED"
    # v3 removes region-b entirely: even a worker on the newest version is refused
    v3 = policy_body(
        t, "region-a", ["region-a"], regional_replica_count=0, maximum_replication_lag_ms=None, durability_mode="LOCAL_DURABLE", replication_factor=1
    )
    assert api("PUT", f"/policies/{t}", "admin", json=v3, headers={"If-Match": "2"}).json()["policy_version"] == 3
    j = wait_for(
        lambda: (lambda x: x if x["decision"] == "REJECTED" and x["policy_version"] == 3 else None)(
            agent("region-b", "POST", "/batches", json={**batch, "policy_version": 3}).json()
        ),
        20,
        1,
        "agent to pick up v3",
    )
    assert "REGION_NOT_ALLOWED_STORAGE" in codes(j)


def test_agents_reject_extra_fields_so_payloads_cannot_enter_the_audit_log():
    t = uid("pl")
    create_policy(policy_body(t, "region-a", ["region-a", "region-b"], replication_factor=1))
    assert agent("region-b", "POST", "/authorize", json={"topic": t, "operation": "PROCESS", "payload": {"ssn": "123"}}).status_code == 422
    assert (
        agent(
            "region-b",
            "POST",
            "/batches",
            json={
                "topic": t,
                "flow_id": "f",
                "policy_version": 1,
                "source_region": "region-a",
                "record_count": 1,
                "first_offset": 0,
                "last_offset": 0,
                "payload": "secret",
            },
        ).status_code
        == 422
    )
    r = api("POST", "/placement/evaluate", json={"topic": t, "operation": "PROCESS", "region": "region-a", "payload": "secret"})
    assert r.status_code == 422


# ===== Audit trail ================================================================================================
def test_every_decision_is_audited_with_reason_version_and_enforcement_point(world):
    b = eu(world)
    create_policy(b)
    r1, rid1 = evaluate(
        b["topic"], "ASSIGN", source_region=world["central"], destination_region=world["west"], enforcement_point="FLOW_INIT", requester="svc-x"
    )
    r2, rid2 = evaluate(b["topic"], "ASSIGN", source_region=world["central"], destination_region=world["us"], enforcement_point="REPLICA_CREATION")
    r3, rid3 = evaluate(b["topic"], "PLAN", count=9)
    r4, rid4 = evaluate("nope-" + uid(), "STORE", region="x")
    rows = {rid: audit_by_request(rid) for rid in (rid1, rid2, rid3, rid4)}
    assert all(len(v) == 1 for v in rows.values())
    a1, a2, a3, a4 = (rows[r][0] for r in (rid1, rid2, rid3, rid4))
    assert (a1["decision"], a1["enforcement_point"], a1["requester"], a1["policy_version"], a1["source"]) == ("ALLOWED", "FLOW_INIT", "svc-x", 1, "engine")
    assert a2["decision"] == "REJECTED" and a2["enforcement_point"] == "REPLICA_CREATION" and a2["policy_version"] == 1
    assert {"LEGAL_ZONE_MISMATCH"} <= {x["code"] for x in a2["reasons"]} and all(x["message"] for x in a2["reasons"])
    assert a3["decision"] == "INSUFFICIENT_ELIGIBLE_REGIONS" and a4["decision"] == "POLICY_UNAVAILABLE"
    assert a2["subject"]["destination_region"] == world["us"]
    # policy lifecycle is audited too
    life = api("GET", "/audit", params={"topic": b["topic"], "limit": 1000}).json()["decisions"]
    assert "POLICY_CREATE" in {x["operation"] for x in life}
    # no field of any record can hold payload-like data
    import json

    allowed_subject = {
        "region",
        "source_region",
        "destination_region",
        "route",
        "failed_regions",
        "capability",
        "required_bytes",
        "replica_status",
        "count",
        "flow_id",
        "record_count",
        "first_offset",
        "last_offset",
        "bytes",
        "partitions",
        "chosen",
        "original_region",
        "current_region",
        "latency_ms",
        "revoked",
        "requested",
        "assignment",
    }
    for x in life:
        assert set(x["subject"]) <= allowed_subject
        assert "payload" not in json.dumps(x).lower()


def test_agent_decisions_reach_the_central_audit_log_including_batches():
    t, flow = uid("aud"), uid("flow")
    create_policy(policy_body(t, "region-a", ["region-a", "region-b"], replication_factor=1))
    rid = uid("fi")
    agent(
        "region-b",
        "POST",
        "/authorize",
        json={
            "topic": t,
            "operation": "ASSIGN",
            "source_region": "region-a",
            "policy_version": 1,
            "flow_id": flow,
            "enforcement_point": "FLOW_INIT",
            "request_id": rid,
        },
    )
    brid = uid("b")
    agent(
        "region-b",
        "POST",
        "/batches",
        json={
            "topic": t,
            "flow_id": flow,
            "policy_version": 1,
            "source_region": "region-a",
            "record_count": 42,
            "first_offset": 10,
            "last_offset": 51,
            "bytes": 1234,
            "request_id": brid,
        },
    )
    agent(
        "region-c",
        "POST",
        "/authorize",
        json={"topic": t, "operation": "ASSIGN", "source_region": "region-a", "enforcement_point": "FLOW_INIT", "request_id": uid("c")},
    )
    row = wait_for(lambda: audit_by_request(brid), 15, 1, "queued batch decision to be flushed")[0]
    assert row["source"] == "agent:region-b" and row["enforcement_point"] == "BATCH_ADMISSION" and row["decision"] == "ALLOWED"
    assert row["subject"]["record_count"] == 42 and row["subject"]["first_offset"] == 10 and row["policy_version"] == 1
    assert audit_by_request(rid)[0]["enforcement_point"] == "FLOW_INIT"
    denied = [x for x in api("GET", "/audit", params={"topic": t, "source": "agent:region-c"}).json()["decisions"]]
    assert denied and denied[0]["decision"] == "REJECTED" and denied[0]["reasons"]


def test_audit_log_is_append_only_and_tampering_is_detectable():
    assert api("GET", "/audit/verify").json()["ok"] is True
    last = api("GET", "/audit", params={"limit": 1000, "from_id": 0}).json()["decisions"][-1]
    psql = lambda sql: sh("docker", "exec", "ss-postgres", "psql", "-U", "sspolicy", "-d", "sspolicy", "-tA", "-c", sql, check=False)  # noqa: E731
    for sql in (
        f"UPDATE policy_decisions SET decision='ALLOWED' WHERE id={last['id']}",
        f"DELETE FROM policy_decisions WHERE id={last['id']}",
        "TRUNCATE policy_decisions",
    ):
        out = psql(sql)
        assert out.returncode != 0 and "append-only" in out.stderr, sql
    # a database superuser CAN bypass the trigger; the hash chain is what exposes that
    other = "REJECTED" if last["decision"] == "ALLOWED" else "ALLOWED"
    try:
        out = psql(
            f"ALTER TABLE policy_decisions DISABLE TRIGGER ss_audit_no_update; UPDATE policy_decisions SET decision='{other}' WHERE id={last['id']}; ALTER TABLE policy_decisions ENABLE TRIGGER ss_audit_no_update;"
        )
        assert out.returncode == 0, out.stderr
        v = api("GET", "/audit/verify").json()
        assert v["ok"] is False and v["first_bad_id"] == last["id"]
    finally:
        psql(
            f"ALTER TABLE policy_decisions DISABLE TRIGGER ss_audit_no_update; UPDATE policy_decisions SET decision='{last['decision']}' WHERE id={last['id']}; ALTER TABLE policy_decisions ENABLE TRIGGER ss_audit_no_update;"
        )
    assert api("GET", "/audit/verify").json()["ok"] is True


# ===== Gateway integration (topic creation, processing region, versions) ===========================================
def test_gateway_topic_creation_goes_through_the_engine():
    name = uid("gw-pol")
    r = gw("POST", "/topics", json={"name": name, "partitions": 1})
    assert r.status_code == 201 and r.json()["policy_version"] == 1
    assert api("GET", f"/policies/{name}").json()["policy"]["home_region"] == "region-a"
    assert any(
        x["topic"] == name and x["decision"] == "ALLOWED" and x["enforcement_point"] == "TOPIC_CREATION"
        for x in api("GET", "/audit", params={"topic": name}).json()["decisions"]
    )
    # a technical hard constraint at the engine stops a topic the gateway's own rules would accept
    assert (
        api("PUT", "/regions/region-b", "admin", json=region_body("region-b", "EU", "site-b", avail="UNAVAILABLE", total=200 * GB, max_rf=1)).status_code == 200
    )
    try:
        n2 = uid("gw-ins")
        r = gw(
            "POST",
            "/topics",
            json={
                "name": n2,
                "partitions": 1,
                "allowed_storage_regions": ["region-a", "region-b"],
                "allowed_processing_regions": ["region-a", "region-b"],
                "failover_priority": ["region-a", "region-b"],
                "regional_replica_count": 1,
                "durability_mode": "ASYNC_REGIONAL",
                "allowed_transit_zones": ["EU"],
                "local_replication_factor": 1,
                "local_min_insync_replicas": 1,
            },
        )
        assert r.status_code == 409 and r.json()["error"]["code"] == "INSUFFICIENT_ELIGIBLE_REGIONS"
        assert r.json()["error"]["details"]["policy_version"] == 1 and r.json()["error"]["details"]["reasons"]
        assert gw("GET", f"/topics/{n2}").status_code == 404  # nothing was created
        assert api("GET", f"/policies/{n2}").status_code == 404
    finally:
        assert api("PUT", "/regions/region-b", "admin", json=region_body("region-b", "EU", "site-b", total=200 * GB, max_rf=1)).status_code == 200
    ok = uid("gw-rep")
    r = gw(
        "POST",
        "/topics",
        json={
            "name": ok,
            "partitions": 1,
            "allowed_storage_regions": ["region-a", "region-b"],
            "allowed_processing_regions": ["region-a", "region-b"],
            "failover_priority": ["region-a", "region-b"],
            "regional_replica_count": 1,
            "durability_mode": "ASYNC_REGIONAL",
            "allowed_transit_zones": ["EU"],
            "local_replication_factor": 1,
            "local_min_insync_replicas": 1,
        },
    )
    assert r.status_code == 201 and r.json()["replica_plan"] == ["region-b"]


def test_gateway_enforces_policy_changes_on_restricted_topics():
    name = uid("gw-res")
    assert gw("POST", "/topics", json={"name": name, "partitions": 1, "classification": "RESTRICTED", "local_replication_factor": 3}).status_code == 201
    ev = lambda: gw("POST", "/events", json={"topic": name, "event_id": str(uuid.uuid4()), "partition_key": "k", "payload": {"x": 1}})  # noqa: E731
    assert ev().status_code == 200
    # the organisation moves the topic out of region-a: v2 no longer lets region-a process it
    cur = api("GET", f"/policies/{name}").json()["policy"]
    v2 = {k: v for k, v in cur.items() if k != "policy_version"}
    v2.update(home_region="region-b", allowed_storage_regions=["region-b"], allowed_processing_regions=["region-b"], replication_factor=1)
    assert api("PUT", f"/policies/{name}", "admin", json=v2, headers={"If-Match": "1"}).status_code == 200
    r = wait_for(lambda: (lambda x: x if x.status_code == 403 else None)(ev()), 25, 1, "gateway to notice policy v2")  # TTL 10 s
    e = r.json()["error"]
    assert e["code"] == "POLICY_VIOLATION" and e["details"]["policy_version"] == 2
    assert gw("GET", f"/events/{name}", params={"partition": 0, "offset": 0}).status_code == 403  # reads too


# ===== Fail closed when policy is unavailable or stale =============================================================
def test_policy_store_outage_fails_closed_then_recovers():
    t = uid("out")
    create_policy(policy_body(t, "region-a", ["region-a", "region-b"], replication_factor=1))
    sh("docker", "stop", "-t", "5", "ss-postgres")
    try:
        r, _ = (
            lambda: (
                httpx.post(
                    f"{ENGINE}/placement/evaluate",
                    headers={"X-Service-Key": KEYS["service"]},
                    json={"topic": t, "operation": "STORE", "region": "region-b"},
                    timeout=30,
                ),
                None,
            )
        )()
        assert r.status_code == 503 and r.json()["decision"] == "POLICY_UNAVAILABLE"
        assert httpx.get(f"{ENGINE}/health").status_code == 503
        r = gw("POST", "/topics", json={"name": uid("gw-down"), "partitions": 1})
        assert r.status_code == 503 and r.json()["error"]["code"] == "POLICY_UNAVAILABLE"
    finally:
        sh("docker", "start", "ss-postgres")
    wait_for(lambda: engine_up(), 60, 2, "engine to recover")
    assert evaluate(t, "STORE", region="region-b")[0].json()["decision"] == "ALLOWED"
    assert gw("POST", "/topics", json={"name": uid("gw-up"), "partitions": 1}).status_code == 201


def test_stale_policy_fails_closed_for_restricted_data_only():
    res, internal = uid("rs"), uid("in")
    for name, cls in ((res, "RESTRICTED"), (internal, "INTERNAL")):
        create_policy(policy_body(name, "region-a", ["region-a", "region-b"], replication_factor=1, classification=cls))
    gres, gin = uid("gres"), uid("gin")
    assert gw("POST", "/topics", json={"name": gres, "partitions": 1, "classification": "RESTRICTED", "local_replication_factor": 3}).status_code == 201
    assert gw("POST", "/topics", json={"name": gin, "partitions": 1}).status_code == 201
    ev = lambda tp: gw("POST", "/events", json={"topic": tp, "event_id": str(uuid.uuid4()), "partition_key": "k", "payload": 1})  # noqa: E731
    assert ev(gres).status_code == 200 and ev(gin).status_code == 200
    ok_req = {"operation": "PROCESS", "enforcement_point": "PROCESSING"}
    assert agent("region-b", "POST", "/authorize", json={"topic": res, **ok_req}).json()["decision"] == "ALLOWED"
    sh("docker", "stop", "-t", "5", "ss-policy-engine")
    try:
        time.sleep(36)  # agent RESTRICTED limit 30 s, gateway TTL 10 s
        j = agent("region-b", "POST", "/authorize", json={"topic": res, **ok_req}).json()
        assert j["decision"] == "POLICY_UNAVAILABLE" and j["reasons"][0]["code"] == "POLICY_STALE"
        assert agent("region-b", "POST", "/authorize", json={"topic": internal, **ok_req}).json()["decision"] == "ALLOWED"  # INTERNAL tolerates 10 min
        r = ev(gres)
        assert r.status_code == 503 and r.json()["error"]["code"] == "POLICY_UNAVAILABLE"
        assert gw("GET", f"/events/{gres}", params={"partition": 0, "offset": 0}).status_code == 503
        assert ev(gin).status_code == 200  # cached, non-restricted
        assert agent("region-b", "GET", "/status").json()["audit_queue"] >= 0
    finally:
        sh("docker", "start", "ss-policy-engine")
    wait_for(engine_up, 60, 2, "engine to recover")
    wait_for(lambda: agent("region-b", "POST", "/authorize", json={"topic": res, **ok_req}).json()["decision"] == "ALLOWED", 30, 2, "agent to refresh")
    wait_for(lambda: ev(gres).status_code == 200, 30, 2, "gateway to recover")
    wait_for(lambda: agent("region-b", "GET", "/status").json()["audit_queue"] == 0, 30, 1, "agent audit queue to drain")
