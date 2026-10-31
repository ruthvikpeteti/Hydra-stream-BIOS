"""Phase 7 integration tests: the dashboard API against the LIVE DR stack (scripts/up.sh --dr, which includes ss-dashboard).
Skipped when the dashboard is not running. Each test that disturbs the stack restores it."""

import re
import time

import httpx
import pytest

from dr_itest import *  # noqa: F403
from dr_itest import dr_state, gwr, produce_at, wait_for, wait_phase
from itest import ENV, INFRA, api, gw_topic, start_replication, wait_acked, sh

DASH = "http://localhost:8200"
VIEWER, OPERATOR = ENV.get("SS_DASH_VIEWER_TOKEN"), ENV.get("SS_DASH_OPERATOR_TOKEN")
CHAOS = INFRA / "chaos"
ALLOWED_LABELS = {
    "region",
    "source",
    "target",
    "path",
    "state",
    "operation",
    "decision",
    "worker",
    "status",
    "from",
    "to",
    "le",
    "point",
    "outcome",
    "classification",
}

try:
    _up = httpx.get(f"{DASH}/api/v1/health", timeout=3).status_code == 200
except httpx.HTTPError:
    _up = False
pytestmark = pytest.mark.skipif(not (_up and VIEWER and OPERATOR), reason="the dashboard (scripts/up.sh --dr) is not running")


def snap(token=None):
    r = httpx.get(f"{DASH}/api/v1/snapshot", headers={"Authorization": f"Bearer {token or VIEWER}"}, timeout=10)
    assert r.status_code == 200, r.text
    return r.json()


def region(s, name):
    return next(r for r in s["topology"]["regions"] if r["region"] == name)


def metrics():
    return httpx.get(f"{DASH}/metrics", timeout=10).text


def metric(text, name, **labels):
    for ln in text.splitlines():
        if ln.startswith(name + "{") or ln.startswith(name + " "):
            if all(f'{k}="{v}"' in ln for k, v in labels.items()):
                return float(ln.rsplit(" ", 1)[1])
    return None


@pytest.fixture(autouse=True)
def healthy_start():
    wait_phase("HEALTHY_A", timeout=120)
    wait_for(lambda: snap()["sources"]["dr"]["ok"] and not any(v["stale"] for v in snap()["sources"].values()), 60, 1, "dashboard sources fresh")
    yield
    sh(str(CHAOS / "recover.sh"), "--all", check=False)


def test_dashboard_active_region_and_epoch_equal_the_coordinator():
    st, _ = dr_state()
    s = snap()
    assert s["topology"]["active_region"] == st["active_region"] and s["topology"]["leadership_epoch"] == st["leadership_epoch"]
    assert s["dr"]["active_region"] == st["active_region"] and s["dr"]["phase"] == st["phase"] and s["dr"]["fenced"] == st["fenced"]
    assert {r["region"]: r["role"] for r in s["topology"]["regions"]}["region-a"] == "active"
    assert metric(metrics(), "ss_active_region", region="region-a") == 1 and metric(metrics(), "ss_leadership_epoch") == st["leadership_epoch"]


def test_dashboard_reflects_a_lost_broker_and_its_recovery():
    assert region(snap(), "region-a")["brokers_up"] == 3
    stop_containers(["ss-a-broker-3"])
    try:
        wait_for(lambda: region(snap(), "region-a")["brokers_up"] == 2, 40, 1, "the dashboard to show 2 of 3 brokers")
        r = region(snap(), "region-a")
        assert (r["brokers_expected"], r["declared_health"]) == (3, "DEGRADED") and r["role"] == "active"
        assert metric(metrics(), "ss_region_brokers_up", region="region-a") == 2 and metric(metrics(), "ss_region_health", region="region-a") == 1
    finally:
        start_containers(["ss-a-broker-3"])
    wait_for(lambda: region(snap(), "region-a")["brokers_up"] == 3 and region(snap(), "region-a")["declared_health"] == "UP", 120, 2, "recovery to show")
    wait_phase("HEALTHY_A", timeout=120)


def test_route_change_shows_up_only_after_the_replicator_reports_it():
    t = gw_topic("obs-route", 3)
    start_replication(t, 3)
    produce_at("region-a", t, 20)
    wait_acked(t, "region-a", "region-b", 20)

    def dash_route():
        return next((r for r in snap()["routing"] if (r["source_region"], r["target_region"]) == ("region-a", "region-b")), None)

    wait_for(lambda: dash_route() and dash_route()["selected_path"] == "a-b-direct", 60, 1, "the baseline route")
    changes0 = dash_route()["route_changes"]
    sh(str(CHAOS / "disconnect.sh"), "a-b-direct")
    seen = set()

    def switched():
        r = dash_route()
        seen.add(r["selected_path"])
        rep = httpx.get("http://localhost:8095/status", timeout=10).json()
        backend = next(f["route"]["current"] for f in rep["flows"] if f["target"] == "region-b")
        return r["selected_path"] != "a-b-direct" and r["selected_path"] == backend

    wait_for(switched, 90, 1, "the dashboard to show the new path the replicator confirms")
    r = dash_route()
    assert r["route_changes"] > changes0 and r["selected_path"] in ("a-b-peering", "a-b-relay")

    def direct_failed():  # the breaker of the dead path opens a moment after the router has already moved away from it
        d = next(p for p in dash_route()["paths"] if p["path_id"] == "a-b-direct")
        return d["breaker"] in ("OPEN", "HALF_OPEN") and d["loss"] and d["loss"] > 0

    wait_for(direct_failed, 60, 1, "the dead path to show an open breaker and loss")
    assert any(c["operation"] == "ROUTE_SWITCH" for c in snap()["audit"])  # and it is in the audit view
    assert metric(metrics(), "ss_path_health", path="a-b-direct") in (0.0, 0.5)


def test_replication_numbers_equal_the_engine_and_the_metrics():
    t = gw_topic("obs-lag", 3)
    start_replication(t, 3)
    produce_at("region-a", t, 20)
    wait_acked(t, "region-a", "region-b", 20)
    sh(str(CHAOS / "disconnect.sh"), "--pair", "a", "b")
    produce_at("region-a", t, 33, start=100)

    def backlog_visible():
        row = next((r for r in snap()["replication"] if r["topic"] == t and r["target_region"] == "region-b"), None)
        return row and row["lag_records"] == 33

    wait_for(backlog_visible, 60, 1, "the 33-record backlog to show")
    time.sleep(1)

    def engine_row():
        return next(f for f in api("GET", "/replication/backlog", params={"topic": t}).json()["flows"] if f["target_region"] == "region-b")

    def dash_row():
        return next(r for r in snap()["replication"] if r["topic"] == t and r["target_region"] == "region-b")

    def equal_to_engine():
        # the dashboard is up to one refresh behind and the byte figure is an estimate that the replicator re-computes every 2 s:
        # the dashboard value must equal the engine's value read just before or just after it
        e0, row, e1 = engine_row(), dash_row(), engine_row()
        return row["lag_records"] == e0["backlog_records"] == e1["backlog_records"] == 33 and row["backlog_bytes"] in (e0["backlog_bytes"], e1["backlog_bytes"])

    wait_for(equal_to_engine, 30, 0.5, "the dashboard's backlog to equal the engine's")
    row = dash_row()
    pair_total = sum(r["lag_records"] or 0 for r in snap()["replication"] if (r["source_region"], r["target_region"]) == ("region-a", "region-b"))
    assert metric(metrics(), "ss_replication_backlog_records", source="a", target="b") == pair_total
    assert row["last_source_offset"] is not None and row["last_ack_at"]


def test_stale_data_is_flagged_and_last_values_are_kept_not_refreshed():
    before = snap()
    assert before["dr"]["phase"] == "HEALTHY_A"
    stop_containers(["ss-dr-1", "ss-dr-2"])
    try:
        wait_for(lambda: snap()["sources"]["dr"]["stale"], 40, 1, "the dr source to be flagged stale")
        s = snap()
        assert not s["sources"]["dr"]["ok"] and s["sources"]["dr"]["error"] and s["sources"]["dr"]["age_s"] > 10
        assert s["dr"]["leadership_epoch"] == before["dr"]["leadership_epoch"]  # the last known data, labelled stale
        assert not s["sources"]["engine"]["stale"]  # other sources are unaffected
        assert metric(metrics(), "ss_worker_up", worker="dr_controller", region="control") == 0
    finally:
        start_containers(["ss-dr-1", "ss-dr-2"])
    wait_for(lambda: not snap()["sources"]["dr"]["stale"] and snap()["sources"]["dr"]["ok"], 90, 1, "the dr source to be fresh again")


def test_unauthorised_users_cannot_trigger_failover_and_nothing_changes():
    st0, _ = dr_state()
    n0 = len(dr_audit())
    for token, code in ((VIEWER, 403), ("wrong-token", 401), (None, 401)):
        h = {"Authorization": f"Bearer {token}"} if token else {}
        for path, body in (("/api/v1/admin/failover", {}), ("/api/v1/admin/failback/start", {"region": "region-a"}), ("/api/v1/admin/failback/approve", None)):
            r = httpx.post(f"{DASH}{path}", headers=h, json=body, timeout=10)
            assert r.status_code == code, (token, path, r.status_code)
    st1, _ = dr_state()
    assert (st1["seq"], st1["phase"], st1["leadership_epoch"]) == (st0["seq"], st0["phase"], st0["leadership_epoch"]) and len(dr_audit()) == n0
    # an operator asking while the active region is healthy is refused BY THE CONTROLLER, and the state does not move
    r = httpx.post(f"{DASH}/api/v1/admin/failover", headers={"Authorization": f"Bearer {OPERATOR}"}, json={}, timeout=60)
    assert (
        r.status_code == 200 and r.json()["controller_answer"]["result"] == "REFUSED" and r.json()["controller_answer"]["reason"] == "ACTIVE_REGION_NOT_FAILED"
    )
    st2, _ = dr_state()
    assert (st2["seq"], st2["phase"], st2["leadership_epoch"]) == (st0["seq"], st0["phase"], st0["leadership_epoch"])
    time.sleep(2)
    rows = [a for a in snap()["audit"] if a["operation"] == "DASH_FAILOVER"]
    assert rows and rows[0]["requester"] == "operator" and rows[0]["decision"] == "REJECTED"  # the request is in the audit trail, with who asked


def test_payloads_and_credentials_never_appear_in_the_api_or_the_metrics():
    marker = "PAYLOAD-MARKER-" + uid("x")
    t = gw_topic("obs-pii", 1)
    start_replication(t, 1)
    r = gwr(
        "region-a", "POST", "/events", json={"topic": t, "event_id": str(uuid.uuid4()), "partition_key": "customer-4711-secret-key", "payload": {"ssn": marker}}
    )
    assert r.status_code == 200
    time.sleep(4)
    texts = [httpx.get(f"{DASH}/api/v1/snapshot", headers={"Authorization": f"Bearer {OPERATOR}"}, timeout=10).text, metrics()]
    secrets = [v for k, v in ENV.items() if re.search(r"(KEY|PASSWORD|TOKEN)", k) and len(v) >= 8]
    assert secrets
    for text in texts:
        assert marker not in text and "customer-4711" not in text
        for sec in secrets:
            assert sec not in text, "a credential leaked"


def test_metrics_follow_the_label_policy_on_the_live_system():
    text = metrics()
    assert metric(text, "ss_replication_lag_seconds", source="a", target="b") is not None
    names = {n for n in re.findall(r"(\w+)=\"", text)}
    assert names <= ALLOWED_LABELS, names - ALLOWED_LABELS
    for v in re.findall(r"\w+=\"([^\"]*)\"", text):
        assert not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-|[0-9a-f]{24,}", v) and len(v) <= 48
    for needed in (
        "ss_active_region",
        "ss_leadership_epoch",
        "ss_region_health",
        "ss_worker_up",
        "ss_replication_backlog_bytes",
        "ss_replication_throughput_bytes_per_second",
        "ss_path_health",
        "ss_route_switches_total",
        "ss_policy_rejections_total",
        "ss_replica_placements",
        "ss_dr_state",
    ):
        assert needed in text, needed
    assert "ss_producer_ack_latency_seconds_count" in text  # produced by the gateways, passed through
