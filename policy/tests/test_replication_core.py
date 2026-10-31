"""Pure tests for the replication core: cutover, topology rules, backoff, batch validation, idempotent consumer."""

import base64
import json
import uuid

import pytest

from ss_policy.topology import validate_new_flow
from ss_replication.batch import BatchIn, decode_record, validate_batch
from ss_replication.cutover import CutoverInvalid, Scanned, compute_partition_cutover
from ss_replication.flow_control import backoff_delay
from ss_replication.idempotent import IdempotentConsumer, find_duplicates


def codes(rs):
    return {r.code for r in rs}


# ---- topology: no loops, tree rooted at the active region -------------------------------------------------------
def test_initial_topology_a_to_b_and_a_to_c_is_valid():
    assert validate_new_flow("A", [], ("A", "B")) == []
    assert validate_new_flow("A", [("A", "B")], ("A", "C")) == []


def test_no_replication_into_the_active_region():
    assert "REPLICATION_INTO_ACTIVE" in codes(validate_new_flow("A", [("A", "B")], ("B", "A")))
    assert "REPLICATION_INTO_ACTIVE" in codes(validate_new_flow("B", [], ("C", "B")))


def test_cycle_a_b_c_a_is_impossible():
    edges = [("A", "B"), ("B", "C")]
    r = codes(validate_new_flow("A", edges, ("C", "A")))
    assert {"REPLICATION_INTO_ACTIVE", "REPLICATION_CYCLE"} & r
    # even with a different root the closing edge is refused
    assert "REPLICATION_CYCLE" in codes(validate_new_flow("X", [("A", "B"), ("B", "C"), ("X", "A")], ("C", "A")))


def test_a_target_has_exactly_one_source_and_sources_must_be_fed_from_the_root():
    assert "TARGET_ALREADY_HAS_SOURCE" in codes(validate_new_flow("A", [("A", "C")], ("B", "C")))
    assert "SOURCE_NOT_AUTHORIZED" in codes(validate_new_flow("A", [], ("B", "C")))
    assert validate_new_flow("A", [("A", "B")], ("B", "C")) == []
    assert "SAME_REGION" in codes(validate_new_flow("A", [], ("B", "B")))


def test_after_promotion_topology_b_to_c_is_valid_and_old_flows_are_not_reusable():
    assert validate_new_flow("B", [], ("B", "C")) == []
    assert "REPLICATION_INTO_ACTIVE" in codes(validate_new_flow("B", [("B", "C")], ("A", "B")))
    assert "SOURCE_NOT_AUTHORIZED" in codes(validate_new_flow("B", [("B", "C")], ("A", "C")))


# ---- cutover: where B->C starts, and the lineage ----------------------------------------------------------------
def a_rec(off, o):
    return Scanned(off, "kafka-a", o)


def native(off):
    return Scanned(off, None, None)


def test_cutover_starts_at_first_record_c_lacks_and_classifies_lineage():
    # B holds A-offsets 0..9 (B offsets 0..9) then 3 native records; C holds A-offsets through 5
    scanned = [a_rec(i, i) for i in range(10)] + [native(10), native(11), native(12)]
    r = compute_partition_cutover(0, scanned, "kafka-a", acked_by_target=5, checkpoint_acked_by_new_source=9, end_offset=13)
    assert r["cutover_start_offset"] == 6 and r["native_start_offset"] == 10 and r["status"] == "OK"
    assert r["already_in_target"]["through_origin_offset"] == 5
    assert r["missing_from_target"]["old_source_origin_records"] == 4 and r["missing_from_target"]["old_source_origin_range"] == [6, 9]
    assert r["missing_from_target"]["native_records"] == 3 and r["missing_from_target"]["native_range"] == [10, 12]
    assert not r["target_ahead"] and r["orphan_in_target"] is None


def test_cutover_when_target_already_has_everything_starts_at_native_records():
    scanned = [a_rec(i, i) for i in range(5)] + [native(5)]
    r = compute_partition_cutover(1, scanned, "kafka-a", acked_by_target=4, checkpoint_acked_by_new_source=4, end_offset=6)
    assert r["cutover_start_offset"] == 5 and r["missing_from_target"]["old_source_origin_records"] == 0


def test_cutover_with_nothing_in_target_replays_everything():
    scanned = [a_rec(i, i) for i in range(4)] + [native(4)]
    r = compute_partition_cutover(0, scanned, "kafka-a", -1, 3, 5)
    assert r["cutover_start_offset"] == 0 and r["missing_from_target"]["old_source_origin_records"] == 4


def test_cutover_with_target_ahead_keeps_its_data_and_flags_the_orphans():
    scanned = [a_rec(i, i) for i in range(5)] + [native(5), native(6)]  # new source only ever received 0..4
    r = compute_partition_cutover(0, scanned, "kafka-a", acked_by_target=8, checkpoint_acked_by_new_source=4, end_offset=7)
    assert r["status"] == "OK_WITH_TARGET_AHEAD" and r["target_ahead"]
    assert r["orphan_in_target"] == {"origin": "kafka-a", "origin_offset_range": [5, 8]}
    assert r["cutover_start_offset"] == 5  # only native records flow; nothing C holds is touched


def test_cutover_tolerates_duplicates_in_the_new_sources_log():
    scanned = [a_rec(0, 0), a_rec(1, 1), a_rec(2, 2), a_rec(3, 1), a_rec(4, 2), a_rec(5, 3), native(6)]  # 1,2 delivered twice
    r = compute_partition_cutover(0, scanned, "kafka-a", acked_by_target=1, checkpoint_acked_by_new_source=3, end_offset=7)
    assert r["cutover_start_offset"] == 2  # first record whose origin offset exceeds what C holds


def test_cutover_rejects_unfenced_old_flow_and_inconsistent_checkpoints():
    interleaved = [a_rec(0, 0), native(1), a_rec(2, 1)]
    with pytest.raises(CutoverInvalid, match="not fenced"):
        compute_partition_cutover(0, interleaved, "kafka-a", 0, 0, 3)
    with pytest.raises(CutoverInvalid, match="checkpoint claims"):
        compute_partition_cutover(0, [a_rec(0, 0), a_rec(1, 1)], "kafka-a", 0, 5, 2)
    with pytest.raises(CutoverInvalid, match="beyond the first native"):
        compute_partition_cutover(0, [a_rec(0, 0), native(1)], "kafka-a", 0, 0, 2, last_known_destination_offset=1)


# ---- flow control ---------------------------------------------------------------------------------------------------
def test_backoff_is_exponential_capped_and_jittered():
    lo = [backoff_delay(n, 0.25, 10, rng=lambda: 0.0) for n in range(8)]
    hi = [backoff_delay(n, 0.25, 10, rng=lambda: 1.0) for n in range(8)]
    assert hi[:5] == [0.25, 0.5, 1.0, 2.0, 4.0] and hi[-1] == 10 and lo[-1] == 5
    assert all(a <= b for a, b in zip(hi, hi[1:])) and all(l <= h for l, h in zip(lo, hi))
    import random

    r = random.Random(1)
    assert len({backoff_delay(3, 0.25, 10, rng=r.random) for _ in range(20)}) > 10  # jitter spreads retries


# ---- batch validation ---------------------------------------------------------------------------------------------
def env(eid=None, **kw):
    return base64.b64encode(json.dumps({"event_id": eid or str(uuid.uuid4()), **kw}).encode()).decode()


def batch(**kw):
    recs = kw.pop(
        "records",
        [
            {
                "offset": o,
                "timestamp": 1,
                "key": base64.b64encode(b"k").decode(),
                "value": env(),
                "origin": {"cluster": "kafka-a", "topic": "t", "partition": 0, "offset": o},
                "hop_path": "kafka-a",
            }
            for o in (10, 11, 12)
        ],
    )
    d = dict(
        batch_id="b1",
        attempt=1,
        stream_session="s",
        source_region="region-a",
        source_cluster="kafka-a",
        destination_region="region-b",
        topic="t",
        partition=0,
        policy_version=1,
        replication_generation=1,
        leadership_epoch=1,
        prev_offset=9,
        first_offset=10,
        last_offset=12,
        records=recs,
    )
    d.update(kw)
    return BatchIn(**d)


def test_valid_batch_passes_and_decodes():
    b = batch()
    assert validate_batch(b, 100, 10**6) == []
    k, v, eid = decode_record(b.records[0])
    assert k == b"k" and json.loads(v)["event_id"] == eid


@pytest.mark.parametrize("kw,needle", [(dict(first_offset=11), "do not match"), (dict(prev_offset=10), "prev_offset"), (dict(last_offset=99), "do not match")])
def test_batch_metadata_errors_are_reported(kw, needle):
    assert any(needle in e for e in validate_batch(batch(**kw), 100, 10**6))


def test_batch_size_limits_and_ordering():
    assert any("limit" in e for e in validate_batch(batch(), 2, 10**6))
    assert any("bytes" in e for e in validate_batch(batch(), 100, 10))
    recs = batch().records
    assert any(
        "increasing" in e
        for e in validate_batch(batch(records=[recs[1].model_dump(), recs[0].model_dump(), recs[2].model_dump()], first_offset=11, last_offset=12), 100, 10**6)
    )


def test_record_without_event_id_is_rejected():
    bad = batch().records[0].model_copy(update={"value": base64.b64encode(b'{"x":1}').decode()})
    with pytest.raises(ValueError):
        decode_record(bad)
    with pytest.raises(ValueError):
        decode_record(bad.model_copy(update={"value": "!!not base64!!"}))


# ---- idempotent consumer --------------------------------------------------------------------------------------------
def test_idempotent_consumer_processes_each_event_once_despite_duplicates():
    ids = [str(uuid.uuid4()) for _ in range(5)]
    deliveries = [{"value": {"event_id": i, "payload": {"amount": 10}}} for i in ids] + [
        {"value": {"event_id": ids[1], "payload": {"amount": 10}}},
        {"value": {"event_id": ids[3], "payload": {"amount": 10}}},
    ]
    naive = sum(d["value"]["payload"]["amount"] for d in deliveries)
    ledger = []
    c = IdempotentConsumer(lambda r: ledger.append(r["value"]["payload"]["amount"]))
    for d in deliveries:
        c.consume(d)
    assert naive == 70 and sum(ledger) == 50 and c.processed == 5 and c.skipped_duplicates == 2
    assert set(find_duplicates(deliveries)) == {ids[1], ids[3]}
