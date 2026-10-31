"""Phase 5 integration tests: adaptive routing against the live stack with Toxiproxy-injected faults.
Fixed toxics only (deterministic); randomized loss is confined to the opt-in stress test at the end."""

import os
import uuid
import time

import pytest

from itest import *  # noqa: F401,F403

pytestmark = pytest.mark.skipif(not engine_up(), reason="stack is not running (infrastructure/scripts/up.sh)")
AB = dict(source="region-a", target="region-b")


@pytest.fixture(autouse=True)
def clean_network():
    chaos_reset()
    yield
    chaos_reset()
    for pid, original in list(RESTORE.items()):
        api("PUT", f"/network/paths/{pid}", "admin", json=original)
        RESTORE.pop(pid)


RESTORE = {}


def two_region_topic(prefix="rt", classification="PUBLIC", partitions=2, **extra):
    t = gw_topic(prefix, partitions=partitions, regions=["region-a", "region-b"], replicas=1, transit=("EU",), classification=classification)
    start_replication(t, partitions)
    return t


def abc_topic(prefix="rt-abc", partitions=2):
    t = gw_topic(prefix, partitions=partitions, regions=ABC, replicas=1)
    start_replication(t, partitions)
    return t


# ===== path eligibility is decided by policy, never by performance ===========================================================
def test_a_prohibited_route_is_never_selected_even_with_the_best_network_score():
    t = two_region_topic("rt-legal")
    produce(t, 10)
    wait_acked(t, "region-a", "region-b", 10)
    wait_for(lambda: current_route(t) is not None, 30, 1, "an initial route")
    orig = next(p for p in api("GET", "/network/paths").json()["paths"] if p["path_id"] == "a-b-peering")
    RESTORE["a-b-peering"] = orig
    api("PUT", "/network/paths/a-b-peering", "admin", json={**orig, "transit_zones": ["EU", "ATLANTIC"]})  # now crosses a zone the topic forbids
    chaos("latency.sh", "a-b-direct", "600")  # the legal path gets slow; the illegal one is the fastest
    time.sleep(12)
    f = route_state(t)
    assert f["route"]["current"] == "a-b-direct"  # stays on the only legal path
    assert f["route"]["policy_rejected"].get("a-b-peering") == ["PATH_TRANSIT_ZONE_NOT_ALLOWED"]
    assert "a-b-peering" not in f["route"]["scores"]
    assert f["route"]["measurements"]["a-b-peering"]["rtt_ms"] < f["route"]["measurements"]["a-b-direct"]["rtt_ms"]  # it WOULD have won on performance
    assert all("a-b-peering" not in r["subject"]["route"] for r in route_audit(t))
    # the receiver enforces it independently: a sender that tries the prohibited path is refused
    r = send_batch("region-b", fake_batch(t, dest="region-b"), path="a-b-peering")
    assert r.status_code == 403 and r.json()["error"]["code"] == "PATH_NOT_AUTHORIZED"
    assert send_batch("region-b", fake_batch(t, dest="region-b")).json()["error"]["code"] == "PATH_NOT_AUTHORIZED"  # no path declared
    assert send_batch("region-b", fake_batch(t, dest="region-b"), path="a-c-direct").json()["error"]["code"] == "PATH_NOT_AUTHORIZED"
    # replication still works over the legal (slow) path
    produce(t, 20, start=10)
    wait_acked(t, "region-a", "region-b", 30, timeout=90)


def test_security_requirement_excludes_the_unencrypted_path_for_restricted_topics():
    t = two_region_topic("rt-sec", classification="RESTRICTED")
    produce(t, 5)
    wait_acked(t, "region-a", "region-b", 5)
    wait_for(lambda: current_route(t) is not None, 30, 1, "route")
    f = route_state(t)
    assert f["route"]["policy_rejected"].get("a-b-direct") == ["PATH_NOT_ENCRYPTED"]
    assert f["route"]["current"] == "a-b-peering"  # the fast plaintext direct path is not allowed
    assert f["route"]["measurements"]["a-b-direct"]["rtt_ms"] is not None  # it is healthy, but ineligible
    r = send_batch("region-b", fake_batch(t, dest="region-b"), path="a-b-direct")
    assert r.status_code == 403 and r.json()["error"]["code"] == "PATH_NOT_AUTHORIZED"


# ===== degradation, switching, flapping, recovery ==============================================================================
def test_path_b_is_selected_when_a_degrades_and_replication_and_checkpoints_stay_correct():
    t = two_region_topic("rt-switch")
    ids = produce(t, 100)
    wait_acked(t, "region-a", "region-b", 100)
    wait_route(t, "a-b-direct")
    before = ckpt(t)
    t_inject = time.time()
    chaos("latency.sh", "a-b-direct", "400")  # Path A: +400 ms ...
    chaos("bandwidth.sh", "a-b-direct", "20")  # ... and 20 KB/s
    got_switch = None
    cps = []
    n = 100
    while time.time() - t_inject < 60:
        ids += produce(t, 10, start=n)
        n += 10  # replication keeps flowing while we degrade
        cps.append(ckpt(t))
        if current_route(t) == "a-b-peering":
            got_switch = time.time() - t_inject
            break
        time.sleep(0.5)
    assert got_switch is not None, route_state(t)["route"]
    print(f"[measured] route switch took {got_switch:.1f}s after injection")
    # checkpoints never went backwards and were not reset by the switch
    seq = [before] + cps
    for a, b in zip(seq, seq[1:]):
        assert all(b[k][0] >= a[k][0] and b[k][2] == a[k][2] for k in a), (a, b)
    wait_acked(t, "region-a", "region-b", n, timeout=120)
    d = wait_unique("region-b", t, n)
    assert uniq(d) == set(ids)
    assert len(d) == n, "a latency/bandwidth change must not cause duplicates"  # nothing timed out, nothing was re-sent
    assert all(v[2] == 1 for v in ckpt(t).values())  # still generation 1: the checkpoint was kept
    # the change is audited
    sw = [r for r in route_audit(t) if r["subject"]["route"] == ["a-b-direct", "a-b-peering"]]
    assert sw and sw[0]["enforcement_point"] == "ROUTING" and sw[0]["source"] == "agent:region-a" and sw[0]["policy_version"]
    # sustained improvement: heal the old path, and the router moves back to it (cheaper) after the hold and cooldown
    chaos("recover.sh", "a-b-direct")
    wait_route(t, "a-b-direct", timeout=90)
    ids += produce(t, 20, start=n)
    wait_acked(t, "region-a", "region-b", n + 20, timeout=60)
    assert uniq(dump("region-b", t)) == set(ids)


def test_a_small_latency_fluctuation_does_not_cause_repeated_switching():
    t = two_region_topic("rt-flap")
    produce(t, 5)
    wait_route(t, "a-b-direct", timeout=40)
    time.sleep(6)
    base = route_state(t)["route"]["changes"]
    chaos("jitter.sh", "a-b-direct", "20", "20")  # 0-40 ms of jitter on the preferred path
    chaos("jitter.sh", "a-b-peering", "20", "20")
    time.sleep(25)
    f = route_state(t)["route"]
    assert f["current"] == "a-b-direct" and f["changes"] == base, f["reason"]
    assert len([r for r in route_audit(t)]) == 1  # only the initial selection was ever recorded


def test_circuit_breaker_opens_on_failures_and_recovery_probing_closes_it():
    t = abc_topic("rt-breaker")
    produce(t, 5)
    target = next(f for f in topology(t)["flows"] if f["state"] == "ENABLED")["target_region"]
    wait_for(lambda: current_route(t, target=target) is not None, 40, 1, "a route")
    first = current_route(t, target=target)
    chaos("disconnect.sh", first)
    wait_for(lambda: route_state(t, target=target)["route"]["measurements"][first]["breaker"] == "OPEN", 30, 0.5, "breaker to open")
    assert current_route(t, target=target) != first and current_route(t, target=target) is not None  # moved to another approved path
    chaos("recover.sh", first)
    wait_for(lambda: route_state(t, target=target)["route"]["measurements"][first]["breaker"] == "CLOSED", 40, 0.5, "recovery probing to close the breaker")
    wait_for(
        lambda: first not in route_state(t, target=target)["route"]["excluded"], 10, 0.5, "the router to see the closed breaker"
    )  # decision is refreshed every second


# ===== all paths down: pause, local persistence, bounded backlog, backpressure, recovery =======================================
def test_all_paths_down_pauses_replication_bounds_the_backlog_and_applies_backpressure_then_recovers():
    t = uid("rt-down")  # REJECT_WRITES: the policy says to refuse writes at the hard backlog limit
    r = gw(
        "POST",
        "/topics",
        json={
            "name": t,
            "partitions": 2,
            "legal_zone": "GLOBAL",
            "classification": "PUBLIC",
            "allowed_storage_regions": ["region-a", "region-b"],
            "allowed_processing_regions": ["region-a", "region-b"],
            "allowed_transit_zones": ["EU"],
            "failover_priority": ["region-a", "region-b"],
            "regional_replica_count": 1,
            "durability_mode": "ASYNC_REGIONAL",
            "local_replication_factor": 1,
            "local_min_insync_replicas": 1,
            "lag_breach_action": "REJECT_WRITES",
        },
    )
    assert r.status_code == 201, r.text
    start_replication(t, 2)
    ids = produce(t, 30)
    wait_acked(t, "region-a", "region-b", 30)
    flow = flow_of(t, "region-a", "region-b")
    wait_for(lambda: (backlog(t) or {}).get("backlog_state") == "OK" and backlog(t)["backlog_records"] == 0, 20, 1, "an initial OK backlog report")

    chaos("disconnect.sh", "--pair", "a", "b")  # every eligible path is gone
    wait_for(lambda: current_route(t) is None, 40, 0.5, "the router to give up (no usable path)")
    wait_for(lambda: all(p["state"] == "PAUSED_NO_ROUTE" for p in route_state(t)["partitions"].values()), 15, 0.5, "pipelines to pause")
    stuck = acked_count(t, "region-a", "region-b")
    # local persistence continues: the gateway keeps accepting and storing events
    n = 30
    for _ in range(20):
        ids += produce(t, 1, start=n)
        n += 1
    time.sleep(4)
    b = backlog(t)
    assert acked_count(t, "region-a", "region-b") == stuck  # nothing was sent, nothing was checkpointed
    assert b["backlog_records"] == n - stuck == 20, b  # backlog measured exactly (from offsets)
    avg = b["backlog_bytes"] / b["backlog_records"]
    assert 100 < avg < 2000
    # set limits relative to what 20 events weigh: soft 1.5x, hard 3x  (=> about 60 events)
    unit = b["backlog_bytes"]
    r = api("PUT", f"/replication/flows/{flow['flow_id']}/limits", "admin", json={"soft_bytes": int(unit * 1.5), "hard_bytes": int(unit * 3)})
    assert r.status_code == 200
    wait_for(lambda: backlog(t)["backlog_state"] == "SOFT" or backlog(t)["backlog_state"] == "OK", 10, 1, "limits to apply")
    accepted_after, blocked = 0, None
    t0 = time.time()
    while time.time() - t0 < 90:
        ev = {"topic": t, "event_id": str(uuid.uuid4()), "partition_key": f"key-{n % 20}", "payload": {"seq": n}}
        r = gw("POST", "/events", json=ev)
        if r.status_code == 200:
            ids.append(ev["event_id"])
            n += 1
            accepted_after += 1
        else:
            blocked = r
            break
        time.sleep(0.1)
    assert blocked is not None and blocked.status_code == 503, "writes were never backpressured"
    err = blocked.json()["error"]
    assert err["code"] == "REPLICATION_BACKPRESSURE" and err["retryable"] and err["details"]["flows"]
    b = backlog(t)
    assert b["backlog_state"] == "HARD" and b["backlog_bytes"] >= unit * 3
    hard_records = int(3 * 20)
    assert 20 + accepted_after >= hard_records - 5  # backpressure came at the limit, not long before it
    assert 20 + accepted_after <= hard_records + 60, "overshoot beyond the reporting latency bound"
    print(f"[measured] hard limit ~{hard_records} records; backpressure after {20 + accepted_after}")
    st = route_state(t)
    assert st["backlog"]["max_pending_batches"] <= 8  # the in-memory queue stayed bounded while the log grew
    assert len(dump("region-a", t)) == len(ids)  # everything accepted is durably in the local cluster
    assert api("GET", f"/replication/topics/{t}/backpressure").json()["blocked"] is True
    # restore the network: replication resumes, the backlog drains, backpressure is released
    chaos("recover.sh", "--pair", "a", "b")
    wait_for(lambda: current_route(t) is not None, 60, 1, "a route to return")
    wait_acked(t, "region-a", "region-b", len(ids), timeout=120)
    d = wait_unique("region-b", t, len(ids))
    assert uniq(d) == set(ids)
    a = dump("region-a", t)
    assert first_occurrence_order(d) == first_occurrence_order(a)  # per-partition order preserved across the outage
    wait_for(lambda: backlog(t)["backlog_state"] == "OK" and backlog(t)["backlog_records"] == 0, 30, 1, "backlog to drain")
    wait_for(
        lambda: gw("POST", "/events", json={"topic": t, "event_id": str(uuid.uuid4()), "partition_key": "k", "payload": {"after": 1}}).status_code == 200,
        30,
        1,
        "backpressure to be released",
    )


def test_replication_order_is_preserved_across_route_changes():
    t = abc_topic("rt-order", partitions=3)
    target = next(f for f in topology(t)["flows"] if f["state"] == "ENABLED")["target_region"]
    ids, n = [], 0
    pair = f"a-{target[-1]}"  # the path ids of the flow's region pair
    plan = [
        (10, None),
        (30, lambda: chaos("latency.sh", f"{pair}-direct", "700")),
        (60, lambda: chaos("disconnect.sh", f"{pair}-peering")),
        (90, lambda: chaos("recover.sh", "--all")),
        (120, None),
    ]
    for upto, action in plan:
        if action:
            action()
        while n < upto:
            ids += produce(t, 5, start=n, keys=15)
            n += 5
            time.sleep(0.2)
    wait_acked(t, "region-a", target, n, timeout=150)
    d = wait_unique(target, t, n, timeout=120)
    a = dump("region-a", t)
    assert uniq(d) == set(ids)
    assert first_occurrence_order(d) == first_occurrence_order(a)  # first deliveries arrive in source order
    ck = ckpt(t)
    for p in range(3):
        assert ck[("kafka-a", f"kafka-{target[-1]}", p)][0] >= 0 and ck[("kafka-a", f"kafka-{target[-1]}", p)][2] == 1


@pytest.mark.skipif(os.environ.get("RUN_STRESS") != "1", reason="supplementary randomized-loss stress test (set RUN_STRESS=1)")
def test_stress_randomized_connection_resets_do_not_lose_or_misorder_events():
    t = two_region_topic("rt-stress")
    chaos("loss.sh", "a-b-direct", "0.3")
    chaos("loss.sh", "a-b-peering", "0.3")
    ids = produce(t, 300)
    wait_acked(t, "region-a", "region-b", 300, timeout=180)
    chaos_reset()
    d = wait_unique("region-b", t, 300)
    assert uniq(d) == set(ids) and first_occurrence_order(d) == first_occurrence_order(dump("region-a", t))
