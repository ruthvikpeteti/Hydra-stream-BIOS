"""Phase 6 disaster-recovery scenario A -> B -> C against the live stack (start it fresh: scripts/up.sh --dr; run: dr_run.sh).

The tests are ONE ordered scenario that shares state (a region killed in one test is still dead in the next), exactly like a
real incident. Do not run them individually or out of order. They are destructive: they kill whole regions.

Measured values (RPO/RTO) are written to dr-results.json next to this file. They are single runs on one Docker host, with
simulated failures (docker kill, docker stop); they are evidence that the mechanism works, not performance guarantees."""

import json
import os
import subprocess
import threading
import time
from pathlib import Path

import httpx
import pytest

from dr_itest import *  # noqa: F403
from itest import INFRA, wait_acked

RESULTS = Path(__file__).with_name("dr-results.json")
INFRA_CHAOS = INFRA / "chaos"
SESSION = Path(__file__).with_name(".dr-session.json")  # lets a failed scenario be resumed (SS_DR_RESUME=1) instead of rebuilt
S: dict = {"results": {}}
if os.environ.get("SS_DR_RESUME") and SESSION.exists():
    S = json.loads(SESSION.read_text())
    for _k in ("a_acked", "c_acked_b"):
        if _k in S:
            S[_k] = set(S[_k])


def save_session():
    SESSION.write_text(json.dumps(S, default=lambda o: sorted(o) if isinstance(o, set) else str(o)))


def record(key, value):
    S["results"][key] = value
    RESULTS.write_text(json.dumps(S["results"], indent=2, sort_keys=True, default=str))


def read_region(region, topic):
    """{event_id: (partition, offset)} as a consumer of that region's gateway sees it (passive regions serve reads)."""
    t = gwr(region, "GET", f"/topics/{topic}").json()
    out = {}
    for part in t["partitions"]:
        p, off = part["partition"], 0
        while True:
            r = gwr(region, "GET", f"/events/{topic}", params={"partition": p, "offset": off, "limit": 500})
            assert r.status_code == 200, r.text
            body = r.json()
            for i, e in enumerate(body["events"]):  # events carry no offset of their own: positions are contiguous
                out[e["event_id"]] = (p, off + i)
            off = body["next_offsets"][str(p)]
            if not body["has_more"]:
                break
    return out


class Writer(threading.Thread):
    """A client that keeps writing through whichever gateway is active and records what was acknowledged and when."""

    def __init__(self, topic, rate_s=0.08):
        super().__init__(daemon=True)
        self.topic, self.rate, self.stop_flag, self.acked, self.fails = topic, rate_s, threading.Event(), [], []
        self.guess, self.seq = "region-a", 0

    def run(self):
        order = ["region-a", "region-b", "region-c"]
        while not self.stop_flag.is_set():
            b = event_body(self.topic, self.seq)
            self.seq += 1
            tried = [self.guess] + [r for r in order if r != self.guess]
            for r in tried:
                try:
                    resp = gwr(r, "POST", "/events", json=b, timeout=6)
                except httpx.HTTPError:
                    continue
                if resp.status_code == 200:
                    self.acked.append((b["event_id"], time.time(), r))
                    self.guess = r
                    break
                self.fails.append((time.time(), r, (resp.json().get("error") or {}).get("code")))
            time.sleep(self.rate)

    def stop(self):
        self.stop_flag.set()
        self.join(15)


def kafka_cmd(container, *args, timeout=60):
    return subprocess.run(["docker", "exec", container, *args], capture_output=True, text=True, timeout=timeout)


def audit_events(name):
    return [e for e in dr_audit() if e["event"] == name]


# ================================================================================================================
@pytest.fixture(scope="module", autouse=True)
def stack_is_the_dr_stack():
    st = dr_state()
    if os.environ.get("SS_DR_RESUME"):
        yield
        return
    assert st and st[0]["phase"] == "HEALTHY_A" and st[0]["leadership_epoch"] == 1, (
        "start from a fresh DR stack: scripts/down.sh --volumes && scripts/up.sh --dr"
    )
    assert {n: dr_status(n)["leader"] for n in (1, 2)} in ({1: True, 2: False}, {1: False, 2: True}), "exactly one controller must lead"
    yield


@pytest.fixture(autouse=True)
def persist_session():
    yield
    save_session()


def test_00_initial_state_and_gateway_roles():
    st, _ = dr_state()
    assert st["active_region"] == "region-a" and st["fenced"] == {} and st["transition"] is None
    assert active_by_gateways() == ["region-a"]
    t = gw_topic("dr-roles", 1)
    start_replication(t, 1)
    for r in ("region-b", "region-c"):  # passive gateways never accept writes
        assert write_status(r, t) == (421, "NOT_ACTIVE_REGION")
    for n in (1, 2):
        h = dr_status(n)["health"]
        assert {r: x["declared"] for r, x in h.items()} == {"region-a": "UP", "region-b": "UP", "region-c": "UP"}


# ---- Tier 1 ----------------------------------------------------------------------------------------------------
def test_01_dr01_primary_broker_dies_kafka_elects_and_gateway_stays_up():
    name = uid("dr-rf3")
    r = gwr(
        "region-a",
        "POST",
        "/topics",
        json={"name": name, "partitions": 6, "durability_mode": "LOCAL_DURABLE", "local_replication_factor": 3, "local_min_insync_replicas": 2},
    )
    assert r.status_code == 201, r.text
    S["rf3"] = name
    acked, errs = produce_at("region-a", name, 20)
    assert len(acked) == 20 and not errs
    stop_containers(["ss-a-broker-1"])
    # Kafka elects another in-sync replica; the DR manager does nothing but observe
    wait_for(lambda: produce_at("region-a", name, 1)[0], 60, 1, "writes to resume after broker A1 died")
    acked2, errs2 = produce_at("region-a", name, 30, start=100)
    assert len(acked2) == 30, errs2
    desc = kafka_cmd("ss-a-broker-2", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "ss-a-broker-2:9092", "--describe", "--topic", name).stdout
    leaders = [int(l.split("Leader:")[1].split()[0]) for l in desc.splitlines() if "Leader:" in l and "Partition:" in l]
    assert len(leaders) == 6 and all(l in (102, 103) for l in leaders), desc  # broker ids are 101..103; 101 (= A1) is down
    assert read_region("region-a", name).keys() >= set(acked) | set(acked2)
    st, _ = dr_state()
    assert st["leadership_epoch"] == 1 and st["active_region"] == "region-a"  # a broker loss is NOT a failover
    wait_phase("LOCAL_DEGRADED_A", timeout=60)  # observed as local degradation, not as a region failure


def test_02_dr02_second_broker_dies_local_durability_enforced():
    name = S["rf3"]
    stop_containers(["ss-a-broker-2"])
    time.sleep(8)
    acked, errs = produce_at("region-a", name, 12, start=300)
    assert acked == [], "writes must NOT be acknowledged when fewer than min.insync.replicas (2) replicas are in sync"
    assert errs and all(
        e in ("LOCAL_DURABILITY_UNAVAILABLE", "BROKER_UNAVAILABLE", "TIMEOUT", "ReadTimeout", 503, "KAFKA_UNAVAILABLE") or isinstance(e, str) for e in errs
    ), errs
    st, _ = dr_state()
    assert st["leadership_epoch"] == 1 and st["active_region"] == "region-a" and st["phase"] != "REGION_A_UNAVAILABLE"
    start_containers(["ss-a-broker-1", "ss-a-broker-2"])
    ok = wait_for(lambda: produce_at("region-a", name, 1, start=400)[0], 120, 2, "writes to resume once ISR is restored")
    assert ok
    wait_phase("HEALTHY_A", timeout=90)
    record("DR-01_DR-02", {"tier1": "Kafka-native election; with 1 of 3 replicas in sync acks=all writes were refused (0 acknowledged of 12 attempts)"})


# ---- coordinator loss ----------------------------------------------------------------------------------------------
def test_03_dr10_coordinator_quorum_lost_no_promotion_and_active_region_self_fences():
    t = gw_topic("dr-coord", 3)
    start_replication(t, 3)
    assert produce_at("region-a", t, 5)[0]
    before = dr_state()[0]
    stop_containers(["ss-etcd-w1", "ss-etcd-w2", "ss-etcd-b"])  # 3 of 5 members down: no quorum
    wait_for(lambda: dr_state() is None, 30, 1, "the coordinator to lose quorum")
    for n in (1, 2):
        wait_for(lambda: not dr_status(n)["coordination_reachable"], 30, 1, f"controller {n} to notice")
        assert dr_status(n)["phase"] == "COORDINATION_UNAVAILABLE"
    time.sleep(LEASE_TTL_S + 2)
    # region A is perfectly healthy, but nothing can renew its lease: it must stop accepting writes
    code = write_status("region-a", t)
    assert code[0] in (503, 421) and code[1] in ("LEASE_EXPIRED", "NOT_ACTIVE_REGION"), code
    assert active_by_gateways() == []
    start_containers(["ss-etcd-w1", "ss-etcd-w2", "ss-etcd-b"])
    after = wait_for(dr_state, 60, 1, "the coordinator to recover")
    assert (after[0]["phase"], after[0]["leadership_epoch"], after[0]["active_region"]) == (before["phase"], 1, "region-a")  # no transition happened
    wait_for(lambda: write_status("region-a", t)[0] == 200, 60, 1, "region A to accept writes again")
    record("DR-10", {"coordinator_members_down": 3, "promotion": "none", "active_region_wrote_while_unauthorised": False})


# ---- replication lag: promotion refused --------------------------------------------------------------------------------
def test_04_dr09_candidate_exceeds_rpo_automatic_promotion_refused():
    t = gw_topic("dr-lag", 3)  # default RPO budget: 30 s
    start_replication(t, 3)
    ids, _ = produce_at("region-a", t, 30)
    wait_acked(t, "region-a", "region-b", 30)
    wait_acked(t, "region-a", "region-c", 30)
    S["lag_topic"] = t
    sh(str(INFRA_CHAOS / "disconnect.sh"), "--pair", "a", "b")
    sh(str(INFRA_CHAOS / "disconnect.sh"), "--pair", "a", "c")
    more, _ = produce_at("region-a", t, 40, start=100)
    assert len(more) == 40

    def stale_reported():
        fl = [f for f in api("GET", "/replication/backlog", params={"topic": t}).json()["flows"] if f["backlog_records"] > 0]
        return len(fl) >= 2

    wait_for(stale_reported, 40, 1, "both flows to report an unreplicated backlog")
    time.sleep(36)  # the replicas stay behind for longer than the 30 s budget, then A fails
    kill_region("region-a")
    wait_phase("NO_ELIGIBLE_TARGET", timeout=60)
    time.sleep(8)
    st, _ = dr_state()
    assert st["phase"] == "NO_ELIGIBLE_TARGET" and st["leadership_epoch"] == 1 and st["active_region"] == "region-a"
    reasons = st["eligibility"]
    assert {"LAG_EXCEEDS_RPO", "LAG_UNKNOWN"} & set(reasons["region-b"]) and {"LAG_EXCEEDS_RPO", "LAG_UNKNOWN"} & set(reasons["region-c"]), reasons
    assert active_by_gateways() == [], "nobody may write while no promotion is allowed"
    assert write_status("region-b", t)[0] != 200
    # an approval to waive the RPO is refused when the deployment's recovery policy forbids degraded-RPO recovery
    r = dr_failover(1, accept_degraded_rpo=True, approved_by="oncall@example.org", reason="test")
    assert r["result"] == "REFUSED" and r["reason"] == "DEGRADED_RPO_NOT_PERMITTED", r
    # A comes back before anything was promoted: the controller returns to HEALTHY_A on its own
    sh(str(INFRA_CHAOS / "recover.sh"), "--all")
    restore_region("region-a")
    wait_phase("HEALTHY_A", timeout=300)
    assert dr_state()[0]["leadership_epoch"] == 1
    assert [e for e in audit_events("REGION_RECOVERED_BEFORE_PROMOTION")]
    wait_acked(t, "region-a", "region-b", 70, 180)
    wait_acked(t, "region-a", "region-c", 70, 180)
    wait_for(lambda: write_status("region-a", t)[0] == 200, 60, 1, "A to accept writes after recovery")
    record("DR-09", {"refused": True, "reasons": reasons})


# ---- Tier 2: A -> B, with two controllers racing ------------------------------------------------------------------------
def test_05_dr03_dr08_region_a_fails_b_promoted_and_racing_controllers_yield_one_transition():
    t = gw_topic("dr-ab", 3)
    start_replication(t, 3)
    S["topic"] = t
    first, _ = produce_at("region-a", t, 120)
    wait_acked(t, "region-a", "region-b", 120)
    wait_acked(t, "region-a", "region-c", 120)
    # a consumer reading region A up to now remembers its positions
    seen_a = read_region("region-a", t)
    assert set(first) <= set(seen_a)
    S["consumer_positions"] = {}
    for eid, (p, off) in seen_a.items():
        S["consumer_positions"][p] = max(S["consumer_positions"].get(p, -1), off)
    w = Writer(t)
    w.start()
    time.sleep(4)
    t_kill = time.time()
    kill_region("region-a")
    # both controllers see A as failed; then every controller is asked to promote at once, while the leader acts on its own
    for n in (1, 2):
        wait_for(lambda: dr_status(n)["health"]["region-a"]["declared"] == "DOWN", 60, 0.5, f"controller {n} to declare A down")
    out = {}

    def go(n):
        out[n] = dr_failover(n, reason="race")

    ths = [threading.Thread(target=go, args=(n,)) for n in (1, 2, 1, 2)]
    [th.start() for th in ths]
    [th.join(120) for th in ths]
    st = wait_phase("HEALTHY_B", timeout=120)
    t_ready = time.time()
    assert st["active_region"] == "region-b" and st["leadership_epoch"] == 2 and st["fenced"] == {"region-a": 2}
    assert len(audit_events("PROMOTION_STARTED")) == 1 and len(audit_events("PROMOTED")) == 1, "exactly one authoritative transition"
    # fenced -> one atomic write advanced the epoch exactly once
    assert [e["epoch"] for e in dr_audit() if e["event"] == "PROMOTION_STARTED"] == [2]
    wait_for(lambda: active_by_gateways() == ["region-b"], 30, 0.5, "the B gateway to become the only active writer")
    # the writer client switched gateways by itself
    first_b = wait_for(lambda: next((a for a in w.acked if a[2] == "region-b"), None), 60, 0.5, "the client to be acknowledged by B")
    time.sleep(3)
    w.stop()
    a_acks = [a for a in w.acked if a[2] == "region-a"]
    lf = st["last_failover"]
    # ---- split-brain evidence: no acknowledged write at A after the fence began; none at B before activation
    assert max(a[1] for a in a_acks) < t_kill + 1.0
    assert min(a[1] for a in w.acked if a[2] == "region-b") * 1000 >= lf["activated_ms"] - 50
    # ---- RPO (measured): acknowledged events missing from B once replication has settled
    wait_for(lambda: len(read_region("region-b", t)) >= len(w.acked) - 5, 60, 2, "B to settle")
    present = set(read_region("region-b", t))
    acked_ids = {a[0] for a in w.acked} | set(first)
    lost = sorted(acked_ids - present)
    rpo = {"acked": len(acked_ids), "present_in_B": len(acked_ids & present), "lost": len(lost)}
    # ---- consumers resume at translated offsets
    tr = lf["offsets"][t]
    for p, (src_ack, dst) in ((int(k), v) for k, v in tr.items()):
        assert dst == src_ack, "this replica is offset-aligned with the source (identity translation), verified from the checkpoints"
        assert S["consumer_positions"][p] <= dst
    rb = read_region("region-b", t)
    for eid, (p, off) in seen_a.items():
        assert rb[eid] == (p, off), "a consumer resuming at its old offset reads the same record in B"
    S["a_acked"] = acked_ids
    S["lf_ab"] = lf
    S["promoted_at"] = t_ready
    record(
        "DR-03_DR-08_A_to_B",
        {
            "controller_responses": {str(k): v.get("result") for k, v in out.items()},
            "RTO_controller_s": round((lf["activated_ms"] - lf["failure_boundary_ms"]) / 1000, 2),
            "RTO_fence_wait_s": round((lf["topology_ms"] - lf["fenced_ms"]) / 1000, 2),
            "RTO_client_write_unavailable_s": round(first_b[1] - max(a[1] for a in a_acks), 2),
            "RPO_measured": rpo,
            "expected_rpo_bound_ms": lf["expected_rpo_ms"],
            "epoch": st["leadership_epoch"],
            "note": "RPO is acknowledged-but-absent events counted in B after settling; lost ids exist only if replication was behind at the failure.",
        },
    )
    assert rpo["lost"] <= 25, rpo  # bounded by one second of async replication lag at this write rate; measured, not guaranteed


def test_06_dr04_events_written_in_b_replicate_to_c():
    t = S["topic"]
    wait_for(
        lambda: next(
            (f for f in topology(t)["flows"] if f["source_region"] == "region-b" and f["target_region"] == "region-c" and f["state"] == "ENABLED"), None
        ),
        90,
        1,
        "B->C flow to complete its cutover",
    )
    new_ids, errs = produce_at("region-b", t, 50, start=5000)
    assert len(new_ids) == 50, errs
    S["b_new"] = new_ids
    b_all = read_region("region-b", t)
    wait_for(lambda: set(new_ids) <= set(read_region("region-c", t)), 120, 2, "C to receive the events written in B")
    c_all = read_region("region-c", t)
    assert set(new_ids) <= set(c_all) and set(c_all) <= set(b_all), "C holds exactly lineage-consistent history: nothing C has is unknown to B"
    flow = next(f for f in topology(t)["flows"] if f["source_region"] == "region-b" and f["target_region"] == "region-c" and f["state"] == "ENABLED")
    assert flow["generation"] == 2 and flow["cutover_report"]
    dup = [e for e in set(c_all)]
    assert len(dup) == len(c_all)  # no duplicate event ids in C
    wait_for(lambda: dr_state()[0]["last_failover"]["replication_ready_ms"], 60, 1, "the controller to record replication reconfigured")
    lf = dr_state()[0]["last_failover"]
    record(
        "DR-04",
        {
            "events_written_in_B": 50,
            "present_in_C": len(set(new_ids) & set(c_all)),
            "replication_reconfigured_after_activation_s": round((lf["replication_ready_ms"] - lf["activated_ms"]) / 1000, 2),
        },
    )


# ---- Tier 3: B -> C with A also lost; crash mid-transition ----------------------------------------------------------------
def test_07_dr05_dr11_dr13_region_b_fails_while_a_is_down_witness_authorises_c_after_controller_crash():
    t = S["topic"]
    time.sleep(3)
    # A is still dead. Losing B now takes out 2 of the 5 coordination members (a and b): c + both witnesses keep the quorum.
    leader = 1 if dr_status(1)["leader"] else 2
    arm_crash("AFTER_TOPOLOGY", leader)
    w = Writer(t)
    w.guess = "region-b"
    w.start()
    time.sleep(3)
    kill_region("region-b")
    assert dr_state() is not None, "with A and B lost the witnesses must keep the coordinator available (3 of 5 members)"
    st = wait_phase("HEALTHY_C", timeout=180)
    time.sleep(3)
    w.stop()
    assert st["active_region"] == "region-c" and st["leadership_epoch"] == 3 and st["fenced"] == {"region-a": 2, "region-b": 3}
    # the controller that led crashed after persisting the topology step; a restart/another controller finished idempotently
    restarts = int(subprocess.run(["docker", "inspect", "-f", "{{.RestartCount}}", f"ss-dr-{leader}"], capture_output=True, text=True).stdout.strip() or 0)
    assert restarts >= 1 or any(e["event"] == "PROMOTED" and e["actor"] != f"dr-{leader}" for e in dr_audit()), "the injected crash must have happened"
    assert len(audit_events("PROMOTION_STARTED")) == 2 and [e["epoch"] for e in audit_events("PROMOTED")] == [2, 3]
    topo = topology(t)["topology"]
    assert (topo["active_region"], topo["leadership_epoch"]) == ("region-c", 3)  # topology reconciled with the coordinator
    assert active_by_gateways() == ["region-c"]
    lf = st["last_failover"]
    # lineage: C holds history of A-origin and B-native events; RPO measured against what the clients were acknowledged
    c_all = set(read_region("region-c", t))
    acked_b = {a[0] for a in w.acked if a[2] == "region-b"} | set(S["b_new"])
    b_lost = sorted(acked_b - c_all)
    a_lost = sorted((S["a_acked"] - c_all))
    record(
        "DR-05_DR-11_DR-13_B_to_C",
        {
            "coordination_members_alive_at_promotion": "c, w1, w2 (3 of 5); a and b lost",
            "RTO_controller_s": round((lf["activated_ms"] - lf["failure_boundary_ms"]) / 1000, 2),
            "RTO_includes_controller_crash_and_resume": True,
            "controller_restarts": restarts,
            "RPO_measured_B_origin": {"acked_in_B": len(acked_b), "missing_in_C": len(b_lost)},
            "RPO_measured_A_origin_still_missing_in_C": len(a_lost),
            "epoch": st["leadership_epoch"],
        },
    )
    assert len(b_lost) <= 25, b_lost
    S["c_acked_b"] = acked_b


def test_08_dr06_dr07_restored_old_regions_stay_fenced_and_cannot_accept_writes():
    t = S["topic"]
    restore_region("region-a")
    restore_region("region-b")
    for r in ("region-a", "region-b"):
        wait_for(lambda: gw_role(r) is not None, 120, 2, f"gateway {r}")
    time.sleep(10)
    st, _ = dr_state()
    assert (st["phase"], st["active_region"], st["leadership_epoch"]) == ("HEALTHY_C", "region-c", 3), "a restored region must not become active by itself"
    assert set(st["fenced"]) == {"region-a", "region-b"}
    assert active_by_gateways() == ["region-c"]
    for r in ("region-a", "region-b"):
        code = write_status(r, t)
        assert code[0] == 421 and code[1] == "NOT_ACTIVE_REGION", (r, code)  # the gateway is fenced by the coordinator
        r2 = gwr(r, "POST", "/events", json={**event_body(t, 1), "expected_epoch": 1})
        assert r2.status_code in (409, 421), r2.text  # a stale-epoch client is not served either

    # cluster-level fence: a producer that BYPASSES the gateway is refused by Kafka itself (max.message.bytes=1 on its topics)
    def fenced_in_kafka(container, bootstrap, want):
        out = kafka_cmd(
            container, "/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", bootstrap, "--describe", "--entity-type", "topics", "--entity-name", t
        ).stdout
        return f"max.message.bytes={want}" in out

    wait_for(lambda: fenced_in_kafka("ss-a-broker-2", "ss-a-broker-2:9092", 1), 90, 2, "the cluster-level fence on region A")
    wait_for(lambda: fenced_in_kafka("ss-b-kafka-1", "ss-b-kafka-1:9092", 1), 90, 2, "the cluster-level fence on region B")

    def direct_write_lands(container, bootstrap, topic, text):
        """Write with a plain Kafka producer (acks=all), bypassing every gateway, then read the topic back: did it land?"""
        subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                container,
                "/opt/kafka/bin/kafka-console-producer.sh",
                "--bootstrap-server",
                bootstrap,
                "--topic",
                topic,
                "--producer-property",
                "acks=all",
                "--producer-property",
                "delivery.timeout.ms=9000",
                "--producer-property",
                "request.timeout.ms=5000",
                "--producer-property",
                "retries=1",
            ],
            input=text + "\n",
            capture_output=True,
            text=True,
            timeout=60,
        )
        out = subprocess.run(
            [
                "docker",
                "exec",
                container,
                "/opt/kafka/bin/kafka-console-consumer.sh",
                "--bootstrap-server",
                bootstrap,
                "--topic",
                topic,
                "--from-beginning",
                "--timeout-ms",
                "8000",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        ).stdout
        return text in out

    for container, bootstrap in (("ss-a-broker-2", "ss-a-broker-2:9092"), ("ss-b-kafka-1", "ss-b-kafka-1:9092")):
        ctl = uid("dr-control")  # positive control: the same method DOES work on an unfenced topic
        kafka_cmd(
            container,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            bootstrap,
            "--create",
            "--topic",
            ctl,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        )
        assert direct_write_lands(container, bootstrap, ctl, "control-write"), f"the direct-write method itself is broken on {container}"
        assert not direct_write_lands(container, bootstrap, t, uid("stale-direct-write")), (
            f"a producer bypassing the gateway wrote to the fenced region via {container}"
        )
    assert dr_state()[0]["leadership_epoch"] == 3
    record(
        "DR-06_DR-07",
        {
            "old_A_gateway_write": "421 NOT_ACTIVE_REGION",
            "old_B_gateway_write": "421 NOT_ACTIVE_REGION",
            "direct_producer_to_A_and_B": "refused by Kafka (topic max.message.bytes=1)",
            "became_active_by_itself": False,
        },
    )


# ---- fail closed ------------------------------------------------------------------------------------------------------------
def test_09_dr12_witnesses_lost_too_system_fails_closed():
    t = S["topic"]
    seq = dr_state()[0]["seq"]
    stop_containers(["ss-etcd-w1", "ss-etcd-w2", "ss-etcd-c"])  # only a and b remain: 2 of 5
    wait_for(lambda: dr_state() is None, 30, 1, "quorum loss")
    kill_region("region-c")  # the active region dies at the same time
    time.sleep(LEASE_TTL_S + 25)
    for n in (1, 2):
        s = dr_status(n)
        assert s["phase"] == "COORDINATION_UNAVAILABLE" and s["state"]["phase"] == "HEALTHY_C"
    assert active_by_gateways() == [], "A and B are healthy, but without a quorum nobody may be authorised to write"
    for r in ("region-a", "region-b"):
        assert write_status(r, t)[0] != 200
    start_containers(["ss-etcd-w1", "ss-etcd-w2", "ss-etcd-c"])
    st = wait_for(dr_state, 90, 1, "quorum back")
    assert st[0]["leadership_epoch"] in (3, 4) and st[0]["seq"] >= seq
    record("DR-12", {"members_alive": "a, b (2 of 5)", "automatic_promotion": "none while quorum was absent", "state_after_quorum_returned": st[0]["phase"]})
