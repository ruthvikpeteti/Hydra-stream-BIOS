"""Phase 4 integration tests: the cross-region replication engine against the live stack (gateway, engine, PostgreSQL,
three regional receivers and three source agents). Tests run in file order; some restart components."""

import json
import time

import httpx
import pytest

from itest import *  # noqa: F401,F403

pytestmark = pytest.mark.skipif(not engine_up(), reason="stack is not running (infrastructure/scripts/up.sh)")
N = 1000


@pytest.fixture(scope="module")
def main():
    """One GLOBAL topic with replicas in B and C and 1,000 events produced through the gateway."""
    t = gw_topic("rep-main")
    start_replication(t, 3)
    ids = produce(t, N)
    return {"topic": t, "ids": ids, "a": dump("region-a", t)}


def per_partition(recs):
    out = {}
    for r in recs:
        out.setdefault(r["partition"], []).append(r)
    return out


# ===== 1-4: replication A->B and A->C, ids, ordering, independent checkpoints ===================================
def test_01_replicate_1000_events_a_to_b(main):
    wait_acked(main["topic"], "region-a", "region-b", N)
    b = wait_unique("region-b", main["topic"], N)
    assert len(b) == N and uniq(b) == set(main["ids"])  # nothing lost, nothing duplicated


def test_02_replicate_1000_events_a_to_c(main):
    wait_acked(main["topic"], "region-a", "region-c", N)
    c = wait_unique("region-c", main["topic"], N)
    assert len(c) == N and uniq(c) == set(main["ids"])


def test_03_event_ids_ordering_and_metadata_are_preserved(main):
    a = per_partition(main["a"])
    for region in ("region-b", "region-c"):
        got = per_partition(dump(region, main["topic"]))
        assert set(got) == set(a)  # same partitions, same partition numbers
        for p in a:
            assert [r["value"]["event_id"] for r in got[p]] == [r["value"]["event_id"] for r in a[p]], f"{region} partition {p} order differs"
            assert [r["offset"] for r in got[p]] == list(range(len(a[p])))  # destination offsets are contiguous (no gaps, no dups)
            for src, dst in zip(a[p], got[p]):
                h = dst["headers"]
                assert dst["value"] == src["value"] and dst["key"] == src["key"] and dst["timestamp"] == src["timestamp"]  # envelope untouched
                assert (h["ss-origin-cluster"], h["ss-origin-topic"], int(h["ss-origin-partition"]), int(h["ss-origin-offset"])) == (
                    "kafka-a",
                    main["topic"],
                    p,
                    src["offset"],
                )
                assert (h["ss-src-cluster"], int(h["ss-src-offset"]), h["ss-event-id"]) == ("kafka-a", src["offset"], src["value"]["event_id"])
                assert (h["ss-replication-generation"], h["ss-leadership-epoch"], h["ss-redelivery"]) == ("1", "1", "false")
                assert h["ss-hop-path"] == f"kafka-a>kafka-{region[-1]}"
                assert src["value"]["leadership_epoch"] == 1 and src["value"]["origin_cluster"] == "kafka-a"  # original epoch/cluster survive in the event
    assert any(len(v) > 1 for v in a.values())


def test_04_separate_checkpoints_for_each_target(main):
    ck, a = ckpt(main["topic"]), per_partition(main["a"])
    for target in ("kafka-b", "kafka-c"):
        for p, recs in a.items():
            ack, dst, gen = ck[("kafka-a", target, p)]
            assert ack == recs[-1]["offset"] and dst == len(recs) - 1 and gen == 1
    assert len({k[1] for k in ck}) == 2


# ===== 5: agent restart, checkpoint recovery =========================================================================
def test_05_agent_restart_recovers_from_checkpoints(main):
    t = main["topic"]
    before = ckpt(t)
    sh("docker", "stop", "ss-a-replicator", timeout=90)  # graceful: in-flight batches are drained and checkpointed
    ids = produce(t, 100, start=N)
    time.sleep(2)
    assert ckpt(t) == before  # nothing moved while the agent was down
    sh("docker", "start", "ss-a-replicator")
    main["ids"] += ids
    wait_acked(t, "region-a", "region-b", N + 100)
    wait_acked(t, "region-a", "region-c", N + 100)
    for region in ("region-b", "region-c"):
        d = wait_unique(region, t, N + 100)
        assert len(d) == N + 100 and uniq(d) == set(main["ids"])  # resumed exactly at the checkpoint: no gap, no duplicate
    # hard kill: no chance to drain; all data must still arrive (duplicates are allowed, and identifiable)
    more = produce(t, 100, start=N + 100)
    sh("docker", "kill", "ss-a-replicator")
    time.sleep(1)
    sh("docker", "start", "ss-a-replicator")
    main["ids"] += more
    for region in ("region-b", "region-c"):
        d = wait_unique(region, t, N + 200)
        assert uniq(d) == set(main["ids"])
        assert len(d) >= N + 200
        if len(d) > N + 200:
            assert any(r["headers"]["ss-redelivery"] == "true" for r in d)  # any duplicate is flagged


# ===== 6, 11: destination killed mid-transmission; B and C progress independently =====================================
def test_06_destination_killed_during_transmission_then_catch_up(main):
    t = gw_topic("rep-kill")
    start_replication(t, 3)
    ids = produce(t, 100)
    wait_acked(t, "region-a", "region-b", 100)
    sh("docker", "stop", "-t", "5", "ss-b-kafka-1")  # destination B dies
    try:
        ids += produce(t, 300, start=100)
        wait_acked(t, "region-a", "region-c", 400)  # C is unaffected and completes (independent progress)
        assert len(uniq(wait_unique("region-c", t, 400))) == 400
        assert acked_count(t, "region-a", "region-b") < 400  # B stalled, checkpoint did NOT run ahead of the destination
        stuck = acked_count(t, "region-a", "region-b")
        time.sleep(4)
        assert acked_count(t, "region-a", "region-b") == stuck  # still retrying, not advancing
        st = httpx.get(f"{REPLICATOR['region-a']}/status").json()
        pipes = [p for f in st["flows"] if f["topic"] == t and f["target"] == "region-b" for p in f["partitions"].values()]
        assert any(p["state"] in ("RETRYING", "BLOCKED") and p["retries"] > 0 for p in pipes)
    finally:
        sh("docker", "start", "ss-b-kafka-1")
    wait_acked(t, "region-a", "region-b", 400, timeout=240)  # restore -> catch-up
    b = wait_unique("region-b", t, 400, timeout=120)
    assert uniq(b) == set(ids)
    dups = len(b) - 400
    assert dups >= 0
    if dups:
        assert all(r["headers"]["ss-event-id"] == r["value"]["event_id"] for r in b)  # duplicates are identifiable by event_id
    assert len(uniq(dump("region-c", t))) == 400


def test_11_b_and_c_make_independent_progress(main):
    t = gw_topic("rep-indep")
    start_replication(t, 3)
    produce(t, 60)
    wait_acked(t, "region-a", "region-b", 60)
    wait_acked(t, "region-a", "region-c", 60)
    c_flow = flow_of(t, "region-a", "region-c")
    assert httpx.post(f"{ENGINE}/replication/flows/{c_flow['flow_id']}/pause", headers={"X-Service-Key": KEYS["admin"]}).status_code == 200
    time.sleep(3)
    c_before = {k: v for k, v in ckpt(t).items() if k[1] == "kafka-c"}
    produce(t, 100, start=60)
    wait_acked(t, "region-a", "region-b", 160)
    time.sleep(3)
    assert {k: v for k, v in ckpt(t).items() if k[1] == "kafka-c"} == c_before  # C untouched while paused
    assert len(uniq(dump("region-c", t))) == 60 and len(uniq(dump("region-b", t))) == 160
    assert httpx.post(f"{ENGINE}/replication/flows/{c_flow['flow_id']}/resume", headers={"X-Service-Key": KEYS["admin"]}).status_code == 200
    wait_acked(t, "region-a", "region-c", 160)
    assert len(uniq(wait_unique("region-c", t, 160))) == 160


# ===== 7, 8: response loss and checkpoint-after-ack ===========================================================================
def test_07_response_loss_creates_a_detectable_duplicate():
    t = gw_topic("rep-dup", partitions=1, regions=["region-a", "region-b"], replicas=1, transit=("EU",))
    start_replication(t, 1)
    fault("region-b", drop_response_after_write=1, topic=t)  # the write succeeds, the response is lost
    ids = produce(t, 50, amount=10)
    wait_acked(t, "region-a", "region-b", 50)
    b = dump("region-b", t)
    assert uniq(b) == set(ids)
    assert len(b) > 50, "the retry after a lost response should have produced a duplicate"
    by_id = {}
    for r in b:
        by_id.setdefault(r["value"]["event_id"], []).append(r)
    dup_ids = [k for k, v in by_id.items() if len(v) > 1]
    assert dup_ids
    for k in dup_ids:
        first, second = by_id[k][0], by_id[k][1]
        assert first["headers"]["ss-origin-offset"] == second["headers"]["ss-origin-offset"]  # same origin coordinates
        assert second["headers"]["ss-redelivery"] == "true" and int(second["headers"]["ss-delivery-attempt"]) >= 2
        assert first["headers"]["ss-redelivery"] == "false" and second["offset"] > first["offset"]  # the first written copy is not flagged
        assert int(second["headers"]["ss-delivery-attempt"]) > int(first["headers"]["ss-delivery-attempt"])
    # idempotent-consumer demonstration: processing is once per event even though the log holds duplicates
    demo = json.loads(sh("docker", "exec", "ss-b-agent", "python", "-m", "ss_replication.tools", "idempotent-demo", "--topic", t).stdout)
    assert demo["unique_events"] == 50 and demo["idempotent_total"] == 500
    assert demo["deliveries"] == len(b) and demo["naive_total"] == 10 * len(b) > 500
    assert demo["duplicate_deliveries"] == len(b) - 50 and demo["events_delivered_more_than_once"] == len(dup_ids)


def test_08_checkpoint_never_advances_before_destination_acknowledgement():
    t = gw_topic("rep-ack", partitions=1, regions=["region-a", "region-b"], replicas=1, transit=("EU",))
    start_replication(t, 1)
    produce(t, 3)
    wait_acked(t, "region-a", "region-b", 3)
    assert ckpt(t)[("kafka-a", "kafka-b", 0)][0] == 2
    w0 = written("region-b")
    fault("region-b", delay_response_ms=4000, delay_responses=1, topic=t)  # write, then hold the response for 4 s (< request timeout)
    produce(t, 5, start=3)
    deadline, seen = time.time() + 3.5, False
    while time.time() < deadline:  # fast polling: receiver counter vs durable checkpoint
        if written("region-b") - w0 >= 1 and ckpt(t)[("kafka-a", "kafka-b", 0)][0] == 2:  # written, acknowledgement still held back
            seen = True
            break
        time.sleep(0.1)
    assert seen, "expected the batch to be written to the destination while the checkpoint still pointed before it"
    assert len(dump("region-b", t)) >= 4  # and it really is in Kafka (3 earlier + at least 1)
    wait_acked(t, "region-a", "region-b", 8, timeout=20)  # advances once the acknowledgement arrives
    assert ckpt(t)[("kafka-a", "kafka-b", 0)][0] == 7 and len(uniq(dump("region-b", t))) == 8
    # a refusal before any write must leave both the destination and the checkpoint alone
    fault("region-b", reject_before_write=2, topic=t)
    produce(t, 2, start=8)
    time.sleep(0.8)
    assert ckpt(t)[("kafka-a", "kafka-b", 0)][0] == 7
    wait_acked(t, "region-a", "region-b", 10, timeout=30)
    assert len(uniq(dump("region-b", t))) == 10


# ===== 9: PostgreSQL restart ============================================================================================
def test_09_postgres_restart_keeps_checkpoints_and_replication_resumes():
    t = gw_topic("rep-pg", partitions=1, regions=["region-a", "region-b"], replicas=1, transit=("EU",))
    start_replication(t, 1)
    ids = produce(t, 20)
    wait_acked(t, "region-a", "region-b", 20)
    before = ckpt(t)
    assert before[("kafka-a", "kafka-b", 0)][0] == 19
    sh("docker", "stop", "-t", "5", "ss-postgres")
    try:
        ids += produce(t, 20, start=20)  # the gateway keeps accepting writes
        time.sleep(8)
        st = httpx.get(f"{REPLICATOR['region-a']}/status").json()
        pipe = next(p for f in st["flows"] if f["topic"] == t for p in f["partitions"].values())
        assert pipe["state"] in ("CHECKPOINT_STORE_UNAVAILABLE", "RUNNING", "RETRYING")
    finally:
        sh("docker", "start", "ss-postgres")
    wait_for(engine_up, 90, 2, "engine to recover")
    assert ckpt(t)[("kafka-a", "kafka-b", 0)][0] >= 19  # the checkpoint written before the restart survived it
    wait_acked(t, "region-a", "region-b", 40, timeout=120)
    b = wait_unique("region-b", t, 40)
    assert len(b) == 40 and uniq(b) == set(ids)  # the acknowledged batch was never re-sent: no duplicates


# ===== 12, 13: unauthorized topic and unauthorized generation ===========================================================
def test_12_unauthorized_topic_is_never_replicated():
    t = gw_topic("rep-eu", partitions=1, regions=["region-a", "region-b"], replicas=1, zone="EU", transit=("EU",))
    start_replication(t, 1)
    produce(t, 5)
    wait_acked(t, "region-a", "region-b", 5)
    r = api("POST", f"/replication/topics/{t}/flows", "admin", json={"source_region": "region-a", "target_region": "region-c"})
    assert r.status_code == 403 and {"LEGAL_ZONE_MISMATCH", "REGION_NOT_ALLOWED_STORAGE"} <= codes(r.json())
    r = send_batch("region-c", fake_batch(t, dest="region-c"))  # a rogue sender with a valid agent key
    assert r.status_code in (403, 409) and r.json()["error"]["code"] in ("POLICY_REJECTED", "FLOW_NOT_AUTHORIZED")
    assert t not in kafka_topics("ss-c-kafka-1") and len(dump("region-c", t)) == 0
    r = send_batch("region-c", fake_batch("topic-with-no-policy", dest="region-c"))
    assert r.status_code in (403, 503) and r.json()["error"]["code"] in ("POLICY_UNAVAILABLE", "POLICY_REJECTED")
    # the audit trail recorded the refusals with reasons and policy version
    rows = api("GET", "/audit", params={"topic": t, "source": "agent:region-c"}).json()["decisions"]
    assert rows and all(x["decision"] != "ALLOWED" and x["reasons"] and x["policy_version"] for x in rows)


def test_13_unauthorized_generation_epoch_identity_are_rejected(main):
    t = main["topic"]
    before = len(dump("region-b", t))
    cases = [
        (dict(generation=99), 403, "UNAUTHORIZED_GENERATION"),
        (dict(generation=0), 409, "STALE_GENERATION"),
        (dict(epoch=7), 409, "STALE_EPOCH"),
        (dict(version=0), 409, "POLICY_REJECTED"),
    ]
    for kw, status, code in cases:
        r = send_batch("region-b", fake_batch(t, first=100000, **kw))
        assert (r.status_code, r.json()["error"]["code"]) == (status, code), (kw, r.text)
    assert send_batch("region-b", fake_batch(t), key="wrong-key").status_code == 401
    assert send_batch("region-b", fake_batch(t), key=REPL_KEYS["region-c"]).status_code == 401  # c's key, claiming to be a
    r = send_batch("region-b", fake_batch(t, source="region-c", dest="region-b"), as_region="region-c")
    assert r.status_code == 403 and r.json()["error"]["code"] == "FLOW_NOT_AUTHORIZED"  # no flow C -> B
    r = send_batch("region-b", fake_batch(t, source="region-a", dest="region-c"))
    assert r.status_code == 403 and r.json()["error"]["code"] == "DESTINATION_MISMATCH"
    bad = fake_batch(t, first=100000)
    bad["records"][0]["value"] = "e30="  # '{}' has no event_id
    assert send_batch("region-b", bad).json()["error"]["code"] == "PATH_NOT_AUTHORIZED"  # a path must be declared first (Phase 5)
    assert send_batch("region-b", bad, path="a-b-direct").json()["error"]["code"] == "BATCH_INVALID"
    assert len(dump("region-b", t)) == before  # nothing was written


# ===== 14, 15: promotion, cutover, lineage, no loops ===================================================================
@pytest.fixture(scope="module")
def promoted():
    t = gw_topic("rep-promo")
    start_replication(t, 3)
    ids = produce(t, 400)
    wait_acked(t, "region-a", "region-b", 400)
    wait_acked(t, "region-a", "region-c", 400)
    c_flow = flow_of(t, "region-a", "region-c")
    assert httpx.post(f"{ENGINE}/replication/flows/{c_flow['flow_id']}/pause", headers={"X-Service-Key": KEYS["admin"]}).status_code == 200
    time.sleep(3)
    ids += produce(t, 600, start=400)  # B gets all 1000, C stays at 400
    wait_acked(t, "region-a", "region-b", 1000)
    time.sleep(2)
    assert acked_count(t, "region-a", "region-c") == 400
    c_before = dump("region-c", t)
    ck_before = ckpt(t)
    # B must be a usable replica before it can be promoted (replicator reports SYNCING / IN_SYNC)
    wait_for(
        lambda: (
            {a["replica_status"] for a in api("GET", f"/placement/{t}").json()["assignments"] if a["destination_region"] == "region-b"}
            <= {"SYNCING", "IN_SYNC"}
        ),
        30,
        1,
        "replica B to report SYNCING/IN_SYNC",
    )
    r = api("POST", f"/replication/topics/{t}/promote", "admin", json={"candidate_region": "region-b"})
    assert r.status_code == 200, r.text
    promo = r.json()
    cutover_flow = wait_for(lambda: flow_of(t, "region-b", "region-c", states=("ENABLED",)), 90, 2, "B->C cutover to be validated and activated")
    native_ids = []
    sh(
        "docker",
        "exec",
        "ss-b-agent",
        "python",
        "-m",
        "ss_replication.tools",
        "produce-native",
        "--topic",
        t,
        "--count",
        "30",
        "--epoch",
        "2",
        "--generation",
        "2",
        "--cluster",
        "kafka-b",
        "--region",
        "region-b",
    )
    b_all = dump("region-b", t)
    native_ids = [r["value"]["event_id"] for r in b_all if r["value"].get("origin_cluster") == "kafka-b"]
    assert len(native_ids) == 30
    return {"topic": t, "ids": ids, "native": native_ids, "promo": promo, "flow": cutover_flow, "c_before": c_before, "ck_before": ck_before}


def test_14_promote_b_cutover_position_lineage_and_continuity(promoted):
    t, p = promoted["topic"], promoted["promo"]
    assert (p["active_region"], p["previous_active_region"], p["leadership_epoch"], p["replication_generation"]) == ("region-b", "region-a", 2, 2)
    assert {(f["source_region"], f["target_region"]) for f in p["disabled_flows"]} == {("region-a", "region-b"), ("region-a", "region-c")}
    assert [(f["source_region"], f["target_region"]) for f in p["pending_flows"]] == [("region-b", "region-c")]
    topo = topology(t)
    assert topo["topology"]["active_region"] == "region-b" and topo["topology"]["leadership_epoch"] == 2
    live = [(f["source_region"], f["target_region"]) for f in topo["flows"] if f["state"] != "DISABLED"]
    assert live == [("region-b", "region-c")]
    # the cutover report: where B->C started and what each range of records is
    rep = {x["partition"]: x for x in promoted["flow"]["cutover_report"]["partitions"]}
    before = promoted["ck_before"]
    for part in range(3):
        ack_c, ack_b = before[("kafka-a", "kafka-c", part)][0], before[("kafka-a", "kafka-b", part)][0]
        x = rep[part]
        assert x["cutover_start_offset"] == ack_c + 1 and x["target_holds_old_source_through"] == ack_c
        assert x["new_source_holds_old_source_through"] == ack_b and x["native_start_offset"] == ack_b + 1
        assert x["missing_from_target"]["old_source_origin_records"] == ack_b - ack_c and not x["target_ahead"] and x["status"] == "OK"
        assert x["already_in_target"]["through_origin_offset"] == ack_c
    # B->C seeded checkpoints at the cutover position, in B's offset space, under the new generation
    cks = ckpt(t)
    for part in range(3):
        assert cks[("kafka-b", "kafka-c", part)][2] == 2
    # C converges: 1000 A-origin events + 30 B-native events, each exactly once
    c = wait_unique("region-c", t, 1030)
    assert len(c) == 1030 and uniq(c) == set(promoted["ids"]) | set(promoted["native"])
    # C's existing A-origin data was not discarded: the earlier records are still there, unchanged, at the same offsets
    now = {(r["partition"], r["offset"]): r for r in c}
    for r in promoted["c_before"]:
        assert now[(r["partition"], r["offset"])]["value"] == r["value"] and now[(r["partition"], r["offset"])]["headers"]["ss-src-cluster"] == "kafka-a"
    # lineage of every record in C is distinguishable from its headers
    a_origin = [r for r in c if r["headers"]["ss-origin-cluster"] == "kafka-a"]
    native = [r for r in c if r["headers"]["ss-origin-cluster"] == "kafka-b"]
    assert len(a_origin) == 1000 and len(native) == 30
    old_path = [r for r in a_origin if r["headers"]["ss-src-cluster"] == "kafka-a"]
    via_b = [r for r in a_origin if r["headers"]["ss-src-cluster"] == "kafka-b"]
    assert len(old_path) == 400 and len(via_b) == 600  # replicated from A before the cutover / from B after it
    assert all(r["headers"]["ss-hop-path"] == "kafka-a>kafka-c" and r["headers"]["ss-replication-generation"] == "1" for r in old_path)
    assert all(
        r["headers"]["ss-hop-path"] == "kafka-a>kafka-b>kafka-c"
        and r["headers"]["ss-replication-generation"] == "2"
        and r["headers"]["ss-leadership-epoch"] == "2"
        for r in via_b
    )
    assert all(r["headers"]["ss-hop-path"] == "kafka-b>kafka-c" and r["headers"]["ss-replication-generation"] == "2" for r in native)
    for part in range(3):  # A-origin offsets in C are strictly increasing and gap-free
        offs = [int(r["headers"]["ss-origin-offset"]) for r in sorted(a_origin, key=lambda r: r["offset"]) if r["partition"] == part]
        assert offs == list(range(len(offs)))
    assert all(r["value"]["leadership_epoch"] == 1 for r in a_origin)  # the event keeps the epoch it was created under


def test_15_no_replication_loop_and_obsolete_flows_are_dead(promoted):
    t = promoted["topic"]
    # engine refuses every flow that would feed the active region or give C a second source
    for src, dst, expect in (
        ("region-a", "region-b", "REPLICATION_INTO_ACTIVE"),
        ("region-c", "region-b", "REPLICATION_INTO_ACTIVE"),
        ("region-a", "region-c", "TARGET_ALREADY_HAS_SOURCE"),
    ):
        r = api("POST", f"/replication/topics/{t}/flows", "admin", json={"source_region": src, "target_region": dst})
        assert r.status_code == 403 and expect in codes(r.json()), (src, dst, r.text)
    # the obsolete flows are disabled and A's agent no longer runs them
    states = {(f["source_region"], f["target_region"]): f["state"] for f in topology(t)["flows"]}
    assert states[("region-a", "region-b")] == states[("region-a", "region-c")] == "DISABLED"
    st = httpx.get(f"{REPLICATOR['region-a']}/status").json()
    assert not [f for f in st["flows"] if f["topic"] == t]
    assert promoted["ck_before"] == {k: v for k, v in ckpt(t).items() if k in promoted["ck_before"]}  # old checkpoints preserved, not advanced
    # receivers refuse stale-flow and looping traffic
    r = send_batch("region-b", fake_batch(t, source="region-a", dest="region-b", generation=1, epoch=1))
    assert r.status_code == 403 and r.json()["error"]["code"] == "REPLICATION_LOOP"  # B is active: nothing may flow in
    r = send_batch("region-c", fake_batch(t, source="region-a", dest="region-c", generation=2, epoch=2))
    assert r.status_code == 403 and r.json()["error"]["code"] == "FLOW_NOT_AUTHORIZED"  # A -> C is gone
    r = send_batch("region-a", fake_batch(t, source="region-c", dest="region-a", generation=2, epoch=2), as_region="region-c")
    assert r.status_code == 403 and r.json()["error"]["code"] == "FLOW_NOT_AUTHORIZED"  # no C -> A
    r = send_batch("region-c", fake_batch(t, source="region-a", dest="region-c", generation=1, epoch=1))
    assert r.status_code == 403 and r.json()["error"]["code"] == "FLOW_NOT_AUTHORIZED"
    # the data itself shows no cycle: no cluster appears twice in any hop path, A received nothing after the promotion,
    # and B contains nothing that came back from C
    c, b, a = dump("region-c", t), dump("region-b", t), dump("region-a", t)
    for r in c + b:
        path = r["headers"].get("ss-hop-path", "").split(">")
        assert len(path) == len(set(path)), path
    assert all("kafka-c" not in r["headers"].get("ss-hop-path", "") for r in b)
    assert len(a) == 1000 and all(not r["headers"].get("ss-hop-path") for r in a)
    assert len(c) == 1030 and len(uniq(c)) == 1030
