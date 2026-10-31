"""Phase 7 dashboard backend tests (no infrastructure needed): accuracy of the views, staleness, metrics label policy, permissions and the
rule that payloads and credentials never leave the API."""

import json
import re

import httpx
import pytest
from starlette.testclient import TestClient

import obs_fixtures as F
from ss_obs import collect, main
from ss_obs.metrics import LabelPolicyError, Registry
from ss_obs.sanitize import find_violations, scrub

INTERVAL = 2.0


def snap(**kw):
    return collect.build_snapshot(F.raw(**kw), INTERVAL, now=F.NOW)


# ---- accuracy ---------------------------------------------------------------------------------------------------------------
def test_broker_health_is_reported_as_measured():
    full, degraded = snap(), snap(brokers_up=2)
    a = lambda s: next(r for r in s["topology"]["regions"] if r["region"] == "region-a")
    assert (a(full)["brokers_up"], a(full)["declared_health"]) == (3, "UP")
    assert (a(degraded)["brokers_up"], a(degraded)["brokers_expected"], a(degraded)["declared_health"]) == (2, 3, "DEGRADED")
    c = next(r for r in full["topology"]["regions"] if r["region"] == "region-c")
    assert c["declared_health"] == "DOWN" and c["kafka_reachable"] is False and c["gateway_healthy"] is False


def test_active_region_and_epoch_come_from_the_coordinator_not_from_anything_else():
    s = snap(active="region-b", phase="HEALTHY_B", fenced={"region-a": 2})
    roles = {r["region"]: r["role"] for r in s["topology"]["regions"]}
    assert roles == {"region-a": "fenced", "region-b": "active", "region-c": "standby"}
    assert s["topology"]["active_region"] == s["dr"]["active_region"] == "region-b" and s["topology"]["leadership_epoch"] == 4
    # the DR controller's copy disagreeing must not change the answer: the coordinator wins
    r = F.raw(active="region-a")
    r["dr"]["data"]["state"] = {**r["dr"]["data"]["state"], "active_region": "region-c"}
    assert collect.build_snapshot(r, INTERVAL, now=F.NOW)["topology"]["active_region"] == "region-a"


def test_unknown_state_is_unknown_not_guessed():
    r = F.raw()
    r["coordinator"] = {"ok": False, "data": None, "fetched_at": None, "error": "down"}
    r["dr"] = {"ok": False, "data": None, "fetched_at": None, "error": "down"}
    s = collect.build_snapshot(r, INTERVAL, now=F.NOW)
    assert s["topology"]["active_region"] is None and {x["role"] for x in s["topology"]["regions"]} == {"unknown"}
    assert s["dr"]["phase"] is None and s["dr"]["coordination_reachable"] is False


def test_replication_view_matches_backend_numbers():
    rows = {r["flow_id"]: r for r in snap()["replication"]}
    r1 = rows[1]
    assert (r1["lag_records"], r1["backlog_bytes"], r1["lag_seconds"], r1["throughput_bps"]) == (40, 8000, 2.5, 3200.0)
    assert (r1["last_source_offset"], r1["last_destination_offset"], r1["last_ack_at"]) == (120, 120, "2026-01-01T00:00:02+00:00")
    assert rows[2]["lag_seconds"] is None and rows[2]["backlog_state"] == "HARD"  # unknown stays unknown


def test_metrics_equal_the_snapshot():
    r, s = F.raw(), snap()
    reg = Registry()
    collect.build_metrics(r, s, reg)
    text = reg.render()
    assert 'ss_replication_backlog_records{source="a",target="b"} 40' in text and 'ss_replication_backlog_bytes{source="a",target="b"} 8000' in text
    assert 'ss_replication_lag_seconds{source="a",target="b"} 2.5' in text
    assert 'ss_active_region{region="region-a"} 1' in text and 'ss_active_region{region="region-b"} 0' in text and "ss_leadership_epoch 4" in text
    assert 'ss_path_health{path="a-b-peering"} 0.0' in text and 'ss_route_switches_total{source="a",target="b"} 3' in text
    assert 'ss_failover_duration_seconds{from="region-a",to="region-b"} 10.0' in text
    assert 'ss_policy_rejections_total{point="TOPIC_CREATION"} 3' in text and 'ss_replica_placements{status="REVOKED"} 1' in text
    assert 'ss_dr_state{state="HEALTHY_A"} 1' in text and 'ss_region_health{region="region-c"} 0' in text
    assert 'ss_producer_ack_latency_seconds_count{region="region-a"} 5' in text


def test_routing_view_shows_selected_path_scores_breaker_and_last_change():
    (pair,) = snap()["routing"]
    assert (pair["source_region"], pair["target_region"], pair["selected_path"], pair["route_changes"]) == ("region-a", "region-b", "a-b-direct", 3)
    p = {x["path_id"]: x for x in pair["paths"]}
    assert p["a-b-direct"]["selected"] and p["a-b-direct"]["rtt_ms"] == 7.4 and p["a-b-direct"]["score"] == 0.04
    assert p["a-b-peering"]["breaker"] == "OPEN" and p["a-b-peering"]["loss"] == 0.4 and p["a-b-relay"]["excluded"] == "RELAY_REGION_NOT_ALLOWED"
    assert pair["last_route_change"] == collect.iso(F.NOW - 90)


def test_dr_view_has_eligibility_rto_and_expected_rpo_but_no_invented_actual_rpo():
    d = snap()["dr"]
    assert (
        d["phase"] == "HEALTHY_A"
        and d["candidate_eligibility"]["region-b"]["eligible"] is False
        and d["candidate_eligibility"]["region-b"]["reasons"] == ["LAG_EXCEEDS_RPO"]
    )
    lf = d["last_failover"]
    assert (lf["measured_rto_s"], lf["fence_wait_s"], lf["replication_reconfigured_s"], lf["expected_rpo_bound_s"]) == (10.0, 7.1, 5.0, 3.4)
    assert d["actual_rpo"] is None  # no failback has measured it: the view must not make one up


def test_policy_view_and_blocked_operations():
    p = snap()["policies"]
    assert (
        p["policies"][0]["prohibited_regions"] == ["region-c"]
        and p["policies"][0]["policy_version"] == 3
        and p["policies"][0]["replicas"][0]["status"] == "IN_SYNC"
    )
    assert [b["reasons"] for b in p["recent_blocked"]] == [["LEGAL_STORAGE"]]


def test_audit_merges_policy_and_coordinator_events_with_categories():
    cats = {(a["source"], a["operation"]): a["category"] for a in snap()["audit"]}
    assert cats[("coordinator", "PROMOTED")] == "failover" and cats[("policy", "ROUTE_SWITCH")] == "route" and cats[("policy", "PLACEMENT")] == "policy"
    assert (
        collect.categorize("DR_FAILBACK_COMPLETED") == "failback"
        and collect.categorize("DASH_FAILOVER") == "admin"
        and collect.categorize("FLOW_CREATE") == "replication"
    )


# ---- staleness ----------------------------------------------------------------------------------------------------------------------
def test_stale_and_failed_sources_are_flagged_and_keep_their_last_data():
    s = snap(stale=("engine", "dr"))
    assert s["sources"]["engine"]["stale"] and not s["sources"]["engine"]["ok"] and "ConnectError" in s["sources"]["engine"]["error"]
    assert not s["sources"]["coordinator"]["stale"] and s["sources"]["coordinator"]["ok"]
    assert s["replication"], "the last known data is still served, but marked stale: nothing is silently dropped or refreshed"


def test_a_source_never_fetched_is_stale():
    r = F.raw()
    r["replicators"] = {"ok": False, "data": None, "fetched_at": None, "error": "never"}
    assert collect.build_snapshot(r, INTERVAL, now=F.NOW)["sources"]["replicators"]["stale"]


# ---- payloads and secrets ---------------------------------------------------------------------------------------------------------------
def test_no_payload_or_credential_field_survives_the_snapshot():
    raw = F.raw()
    raw["engine"]["data"]["recent"][0]["reasons"][0]["message"] = f"leaked {F.SECRET}"
    s = scrub(collect.build_snapshot(raw, INTERVAL, now=F.NOW), [F.SECRET])
    assert find_violations(s, [F.SECRET]) == []
    assert "LEAK-ME" not in json.dumps(s) and F.SECRET not in json.dumps(s)


def test_scrub_removes_forbidden_keys_everywhere_and_redacts_secret_values():
    dirty = {"a": [{"payload": {"x": 1}, "ok": 1}], "n": {"api_key": "k", "value": 3, "msg": f"bad {F.SECRET} bad"}, "records": [1], "partition_key": "p"}
    clean = scrub(dirty, [F.SECRET])
    assert clean == {"a": [{"ok": 1}], "n": {"msg": "bad [redacted] bad"}}
    assert find_violations(dirty, [F.SECRET])


# ---- metric label policy ---------------------------------------------------------------------------------------------------------------------
def test_metrics_never_carry_ids_topics_or_free_text():
    reg = Registry()
    collect.build_metrics(F.raw(), snap(), reg)
    text = reg.render()
    assert "t1" not in re.findall(r'="([^"]*)"', text) and "LEAK" not in text
    for k in re.findall(r'(\w+)="', text):
        assert k in {"region", "source", "target", "path", "state", "operation", "decision", "worker", "status", "from", "to", "le", "point", "outcome"}


@pytest.mark.parametrize(
    "labels",
    [
        {"event_id": "x"},
        {"topic": "orders"},
        {"region": "3f2504e0-4f89-11d3-9a0c-0305e82c3301"},
        {"region": "has space"},
        {"region": "a" * 60},
        {"region": "customer-1234567890abcdef12345678"},
    ],
)
def test_label_policy_rejects_high_cardinality_and_sensitive_labels(labels):
    with pytest.raises(LabelPolicyError):
        Registry().add("m", "gauge", "h", 1, **labels)


# ---- permissions -------------------------------------------------------------------------------------------------------------------------------
@pytest.fixture
def client(monkeypatch):
    cfg = {
        "engine_url": "http://engine",
        "service_key": F.SECRET,
        "coord_endpoints": [],
        "dr_urls": ["http://dr1"],
        "dr_admin_key": "admin-key-very-secret",
        "replicator_urls": {},
        "gateway_urls": {},
        "tokens": {"viewer-token-1": {"user": "vera", "role": "viewer"}, "operator-token-1": {"user": "olga", "role": "operator"}},
        "interval_s": 2.0,
        "static_dir": "/nonexistent",
    }
    calls = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append((req.method, str(req.url), dict(req.headers), req.content))
        if "/failover" in str(req.url):
            return httpx.Response(200, json={"result": "REFUSED", "reason": "ACTIVE_REGION_NOT_FAILED", "detail": f"echo {'admin-key-very-secret'}"})
        return httpx.Response(202, json={"accepted": 1})

    class S:
        http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    main.app.state.cfg, main.app.state.sources = cfg, S()
    main.app.state.snapshot, main.app.state.raw = collect.build_snapshot(F.raw(), 2.0, now=F.NOW), F.raw()
    main.app.state.secrets = [cfg["service_key"], cfg["dr_admin_key"], *cfg["tokens"]]
    c = TestClient(main.app)  # no `with`: the lifespan (and its network collectors) is not started
    c.calls = calls
    return c


H = lambda t: {"Authorization": f"Bearer {t}"}


def test_unauthenticated_and_unknown_tokens_are_refused(client):
    assert client.get("/api/v1/snapshot").status_code == 401
    assert client.get("/api/v1/snapshot", headers=H("nope")).status_code == 401
    assert client.get("/api/v1/me", headers=H("nope")).status_code == 401


def test_viewer_reads_but_cannot_trigger_anything_administrative(client):
    assert client.get("/api/v1/snapshot", headers=H("viewer-token-1")).status_code == 200
    me = client.get("/api/v1/me", headers=H("viewer-token-1")).json()
    assert me == {"user": "vera", "role": "viewer", "can_administer": False}
    for path, body in (
        ("/api/v1/admin/failover", {}),
        ("/api/v1/admin/failback/start", {"region": "region-a"}),
        ("/api/v1/admin/failback/approve", None),
        ("/api/v1/admin/failback/abort", None),
    ):
        r = client.post(path, headers=H("viewer-token-1"), json=body)
        assert r.status_code == 403, path
        assert client.post(path, json=body).status_code == 401
    assert client.calls == [], "a refused request must never reach the DR controller"
    # a forged role in the body or header changes nothing
    assert client.post("/api/v1/admin/failover", headers={**H("viewer-token-1"), "X-Role": "operator"}, json={"role": "operator"}).status_code in (403, 422)


def test_operator_request_is_forwarded_and_the_controllers_answer_is_returned_verbatim_without_secrets(client):
    r = client.post("/api/v1/admin/failover", headers=H("operator-token-1"), json={})
    assert r.status_code == 200
    body = r.json()
    assert (
        body["requested_by"] == "olga"
        and body["controller_answer"]["result"] == "REFUSED"
        and body["controller_answer"]["reason"] == "ACTIVE_REGION_NOT_FAILED"
    )
    assert "admin-key-very-secret" not in json.dumps(body)  # the controller's key (echoed by a hostile answer) is redacted
    method, url, headers, content = client.calls[0]
    assert url == "http://dr1/dr/v1/failover" and headers["x-admin-key"] == "admin-key-very-secret"
    assert any("/audit/decisions" in c[1] for c in client.calls)  # the request was audited with the requester
    assert b"olga" in [c for c in client.calls if "/audit/decisions" in c[1]][0][3]


def test_admin_input_is_validated(client):
    assert client.post("/api/v1/admin/failback/start", headers=H("operator-token-1"), json={"region": "../etc"}).status_code == 422
    assert client.post("/api/v1/admin/failover", headers=H("operator-token-1"), json={"unknown": 1}).status_code == 422


def test_snapshot_endpoint_never_returns_payloads_or_credentials(client):
    r = client.get("/api/v1/snapshot", headers=H("viewer-token-1"))
    text = r.text
    assert "LEAK-ME" not in text and F.SECRET not in text and "admin-key-very-secret" not in text and "viewer-token-1" not in text
    assert find_violations(r.json(), main.app.state.secrets) == []


def test_metrics_endpoint_is_unauthenticated_but_clean(client):
    r = client.get("/metrics")
    assert r.status_code == 200 and "ss_active_region" in r.text and "LEAK" not in r.text
