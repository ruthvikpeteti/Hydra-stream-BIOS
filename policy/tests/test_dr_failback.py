"""Phase 6 scenario 2: split brain (a false-positive failure detector, with the old active region ALIVE and cut off from the
coordinator), an approved degraded-RPO promotion, and controlled failback (T14).

Needs a FRESH DR stack whose controllers allow degraded-RPO recovery with approval:
    scripts/down.sh --volumes && SS_DR_ALLOW_DEGRADED_RPO=1 scripts/up.sh --dr          (dr_run.sh does this)
Ordered scenario, destructive; do not run tests individually. Measured values go to dr-failback-results.json."""

import json
import os
import time
from pathlib import Path

import httpx
import pytest

from dr_itest import *  # noqa: F403
from itest import INFRA, produce, wait_acked, wait_unique
from test_dr import Writer, kafka_cmd, read_region
from test_scenarios import eu_restricted_topic, topic_exists

RESULTS = Path(__file__).with_name("dr-failback-results.json")
SESSION = Path(__file__).with_name(".dr-failback-session.json")
S: dict = {"results": {}}
if os.environ.get("SS_DR_RESUME") and SESSION.exists():
    S = json.loads(SESSION.read_text())
CHAOS = INFRA / "chaos"
ADMIN = {"X-Admin-Key": ENV["SS_POLICY_ADMIN_KEY"]}


def record(key, value):
    S["results"][key] = value
    RESULTS.write_text(json.dumps(S["results"], indent=2, sort_keys=True, default=str))


def fb(path, n=1, **body):
    return httpx.post(f"{DR_URL[n]}/failback/{path}", json=body, headers=ADMIN, timeout=60).json()


def leader():
    return 1 if dr_status(1)["leader"] else 2


@pytest.fixture(scope="module", autouse=True)
def fresh_stack():
    if os.environ.get("SS_DR_RESUME"):
        yield
        return
    st = dr_state()
    assert st and st[0]["phase"] == "HEALTHY_A" and st[0]["leadership_epoch"] == 1, "start from a fresh DR stack (see the module docstring)"
    assert dr_status(1)["allow_degraded_rpo"] and dr_status(2)["allow_degraded_rpo"], "controllers must run with SS_DR_ALLOW_DEGRADED_RPO=1"
    yield


@pytest.fixture(autouse=True)
def persist():
    yield
    SESSION.write_text(json.dumps(S, default=str))


def test_01_split_brain_false_positive_old_active_alive_but_cut_off_cannot_write_after_fence_and_degraded_rpo_needs_approval():
    t = gw_topic("dr-fb", 3, max_lag_ms=2000)  # a tight RPO: ~10 s of unreplicated writes must exceed it
    start_replication(t, 3)
    S["topic"] = t
    # a second topic that region C is FORBIDDEN to hold (EU-restricted, A and B only): the failback must never create anything for it in C
    eu = eu_restricted_topic("fb-eu")
    start_replication(eu, 3)
    produce(eu, 20)
    wait_unique("region-b", eu, 20)
    S["eu"] = eu
    base, _ = produce_at("region-a", t, 90)
    wait_acked(t, "region-a", "region-b", 90)
    wait_acked(t, "region-a", "region-c", 90)
    S["base"] = base
    # the replication paths out of A go down: from now on A acknowledges writes that nobody else holds
    sh(str(CHAOS / "disconnect.sh"), "--pair", "a", "b")
    sh(str(CHAOS / "disconnect.sh"), "--pair", "a", "c")
    w = Writer(t)
    w.guess = "region-a"
    w.start()
    time.sleep(6)
    # region A is ALIVE. Its gateway is cut off from the coordinator (it cannot hear a fence) and a false-positive detector declares it dead.
    sh("docker", "network", "disconnect", "ss-coord-net", "ss-a-gateway")
    for n in (1, 2):
        httpx.post(f"{DR_URL[n]}/debug/declare-down", json={"region": "region-a"}, headers=ADMIN, timeout=10)
    for n in (1, 2):
        wait_for(lambda: dr_status(n)["health"]["region-a"]["declared"] == "DOWN", 30, 0.5, "A to be declared down")
    wait_phase("NO_ELIGIBLE_TARGET", timeout=60)  # B and C are stale: nothing is promoted silently
    assert dr_state()[0]["leadership_epoch"] == 1
    # the operator approves a degraded-RPO recovery, naming themself; the approval is recorded
    refused = dr_failover(1, accept_degraded_rpo=True, approved_by="")
    assert refused["result"] == "REFUSED" and refused["reason"] == "APPROVER_REQUIRED"
    r = dr_failover(leader(), accept_degraded_rpo=True, approved_by="oncall@example.org", reason="A isolated; accept loss of unreplicated writes")
    assert r["result"] == "PROMOTED", r
    st = wait_phase("HEALTHY_B", timeout=60)
    lf = st["last_failover"]
    assert lf["degraded_rpo"] is True and lf["approved_by"] == "oncall@example.org" and st["leadership_epoch"] == 2
    time.sleep(3)
    w.stop()
    a_acks = [a for a in w.acked if a[2] == "region-a"]
    b_acks = [a for a in w.acked if a[2] == "region-b"]
    # ---- split-brain evidence: A (alive, isolated) stopped by lease expiry; B only after the lease-wait; never an overlap
    assert a_acks and b_acks
    assert max(a[1] for a in a_acks) * 1000 <= lf["fenced_ms"] + LEASE_TTL_S * 1000 + 500, "A kept acknowledging writes past its lease"
    assert min(a[1] for a in b_acks) * 1000 >= lf["activated_ms"] - 50, "B accepted a write before it was activated"
    assert max(a[1] for a in a_acks) * 1000 < lf["activated_ms"], "overlap: A acknowledged after B was activated"
    sh("docker", "network", "connect", "ss-coord-net", "ss-a-gateway")
    for n in (1, 2):
        httpx.post(f"{DR_URL[n]}/debug/declare-down", json={"region": "region-a", "down": False}, headers=ADMIN, timeout=10)
    sh(str(CHAOS / "recover.sh"), "--all")
    wait_for(lambda: gw_role("region-a") is not None and gw_role("region-a")["role"] == "passive", 60, 1, "A's gateway to hear it is fenced")
    assert write_status("region-a", t) == (421, "NOT_ACTIVE_REGION")
    acked_a = {a[0] for a in a_acks} | set(base)
    in_b = set(read_region("region-b", t))
    lost = sorted(acked_a - in_b)
    S["lost_from_live_history"] = lost
    S["a_acked_total"] = len(acked_a)
    record(
        "split_brain_and_degraded_rpo_A_to_B",
        {
            "A_alive_and_isolated": True,
            "A_last_ack_after_fence_s": round(max(a[1] for a in a_acks) - lf["fenced_ms"] / 1000, 2),
            "lease_ttl_s": LEASE_TTL_S,
            "B_first_ack_minus_activation_s": round(min(a[1] for a in b_acks) - lf["activated_ms"] / 1000, 2),
            "overlap_of_acknowledged_writes": False,
            "acknowledged_at_A": len(acked_a),
            "missing_from_B_RPO_gap_measured": len(lost),
            "expected_rpo_ms_at_decision": lf["expected_rpo_ms"].get(t),
            "approved_by": lf["approved_by"],
            "RTO_controller_s": round((lf["activated_ms"] - lf["failure_boundary_ms"]) / 1000, 2),
        },
    )
    assert len(lost) > 0, "the scenario must have produced acknowledged-but-unreplicated writes"


def test_02_failback_a_is_validated_resynced_quarantined_and_needs_explicit_approval():
    t = S["topic"]
    new_b, errs = produce_at("region-b", t, 60, start=1000)
    assert len(new_b) == 60, errs
    S["b_new"] = new_b
    # no failback without being asked; an unknown or healthy region is refused
    assert fb("start", region="region-b", requested_by="oncall")["result"] == "REFUSED"
    wait_for(lambda: dr_status(1)["health"]["region-a"]["declared"] == "UP", 90, 1, "A to be recovered")
    r = fb("start", region="region-a", requested_by="oncall@example.org")
    assert r["result"] == "STARTED", r
    assert fb("approve", approved_by="oncall@example.org")["reason"] == "NOT_READY"  # approval is only accepted at READY

    def stage():
        s = dr_state()[0]
        return (s.get("failback") or {}).get("stage")

    wait_for(lambda: stage() == "READY", 240, 2, "the failback to reach READY")
    st = dr_state()[0]
    assert st["phase"] == "FAILBACK_PENDING" and st["active_region"] == "region-b" and st["leadership_epoch"] == 2  # nothing switched yet
    q = st["failback"]["quarantine"][t]
    lost = set(S["lost_from_live_history"])
    assert q["records"] == len(lost), (q, len(lost))  # exactly the divergent suffix was set aside
    assert set(q["sample_event_ids"]) <= lost
    # the quarantined records really exist in A's quarantine topic and not in A's live (re-seeded) topic
    out = kafka_cmd(
        "ss-a-broker-2",
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server",
        "ss-a-broker-2:9092",
        "--topic",
        q["topic"],
        "--from-beginning",
        "--timeout-ms",
        "8000",
    ).stdout
    assert all(e in out for e in q["sample_event_ids"])
    a_live = set(read_region("region-a", t))
    assert not (a_live & lost), "the divergent suffix must not be merged into the live history"
    record(
        "failback_prepare",
        {
            "quarantined_records": q["records"],
            "quarantine_topic": q["topic"],
            "resync_s": round((st["failback"]["ready_ms"] - st["failback"]["started_ms"]) / 1000, 1),
        },
    )
    # still B that accepts writes, A cannot
    assert write_status("region-b", t)[0] == 200 and write_status("region-a", t)[0] == 421


def test_03_approved_failback_fences_b_verifies_lineage_and_activates_a():
    t = S["topic"]
    t_approve = time.time()
    r = fb("approve", n=leader(), approved_by="oncall@example.org")
    assert r["result"] == "APPROVED", r
    # B is fenced (nobody writes) before A is activated
    wait_for(lambda: (dr_state()[0].get("active_region") == "") or dr_state()[0]["phase"] == "HEALTHY_A", 60, 0.2, "the fence")
    st = wait_phase("HEALTHY_A", timeout=240)
    lb = st["last_failback"]
    assert st["active_region"] == "region-a" and st["leadership_epoch"] == 3 and "region-b" in st["fenced"]
    assert lb["fenced_ms"] < lb["activated_ms"] and lb["approved_by"] == "oncall@example.org"
    assert len([e for e in dr_audit() if e["event"] == "FAILBACK_FENCE"]) == 1 and len([e for e in dr_audit() if e["event"] == "FAILBACK_COMPLETED"]) == 1
    wait_for(lambda: active_by_gateways() == ["region-a"], 30, 0.5, "A to be the only active writer")
    assert write_status("region-b", t) == (421, "NOT_ACTIVE_REGION")
    # data: A holds everything acknowledged anywhere EXCEPT the divergent suffix that was quarantined
    a_ids = set(read_region("region-a", t))
    expect = (set(S["base"]) | set(S["b_new"])) - set(S["lost_from_live_history"])
    assert expect <= a_ids, sorted(expect - a_ids)[:5]
    assert not (a_ids & set(S["lost_from_live_history"]))
    for eid, offsets in lb["verified_end_offsets"].items():
        assert offsets, "end offsets of the fenced region were verified equal to A's"
    new_a, errs = produce_at("region-a", t, 25, start=5000)
    assert len(new_a) == 25, errs
    S["a_new"] = new_a
    record(
        "failback_switch",
        {
            "approve_to_activation_s": round(lb["activated_ms"] / 1000 - t_approve, 2),
            "fence_to_activation_s": round((lb["activated_ms"] - lb["fenced_ms"]) / 1000, 2),
            "epoch": st["leadership_epoch"],
            "approved_by": lb["approved_by"],
            "lineage_verified_topics": len(lb["verified_end_offsets"]),
            "stale_B_gateway_write": "421 NOT_ACTIVE_REGION",
        },
    )


def test_04_old_active_and_the_third_region_are_reprotected_as_replicas_of_a():
    t = S["topic"]
    wait_for(lambda: not dr_state()[0].get("reprotect"), 240, 2, "re-protection to finish")
    st = dr_state()[0]
    assert st["fenced"] == {} and st["active_region"] == "region-a"
    for tgt in ("region-b", "region-c"):
        assert next((f for f in topology(t)["flows"] if f["source_region"] == "region-a" and f["target_region"] == tgt and f["state"] == "ENABLED"), None)
    eu = S["eu"]
    assert not topic_exists("region-c", eu), "the failback must not create a copy of an EU-restricted topic in the US-only region"
    assert not any(f["target_region"] == "region-c" for f in topology(eu)["flows"]) and any(
        f["target_region"] == "region-b" and f["state"] == "ENABLED" for f in topology(eu)["flows"]
    )
    wait_unique("region-b", eu, 20)
    want = (set(S["base"]) | set(S["b_new"]) | set(S["a_new"])) - set(S["lost_from_live_history"])
    for r in ("region-b", "region-c"):
        wait_for(lambda: want <= set(read_region(r, t)), 240, 3, f"{r} to hold the full history again")
    more, _ = produce_at("region-a", t, 20, start=9000)
    for r in ("region-b", "region-c"):
        wait_for(lambda: set(more) <= set(read_region(r, t)), 120, 2, f"{r} to receive new writes from A")
    record("reprotect", {"replicas_rebuilt": ["region-b", "region-c"], "new_writes_replicated": len(more)})
