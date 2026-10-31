"""Phase 8 scenario 8: the tertiary replica C is deliberately made stale, then B (the promoted secondary) fails.
The configured RPO protection must prevent an unsafe automatic promotion of C. Needs a FRESH DR stack (dr_run.sh 3)."""

import json
import time
from pathlib import Path

import pytest

from dr_itest import *  # noqa: F403
from itest import INFRA, wait_acked
from test_dr import read_region

RESULTS = Path(__file__).with_name("dr-stale-c-results.json")
CHAOS = INFRA / "chaos"
S: dict = {}


@pytest.fixture(scope="module", autouse=True)
def fresh():
    st = dr_state()
    assert st and st[0]["phase"] == "HEALTHY_A" and st[0]["leadership_epoch"] == 1, "start from a fresh DR stack"
    yield


def test_scenario_8_stale_c_blocks_automatic_promotion_when_b_fails():
    t = gw_topic("s8", 3)  # the default RPO budget: 30 s
    start_replication(t, 3)
    base, _ = produce_at("region-a", t, 90)
    wait_acked(t, "region-a", "region-b", 90)
    wait_acked(t, "region-a", "region-c", 90)
    wait_for(  # steady state: the replicators have reported an empty backlog for both replicas
        lambda: all(
            f["backlog_records"] == 0 and f["report_age_s"] < 3
            for f in api("GET", "/replication/backlog", params={"topic": t}).json()["flows"]
            if f["flow_state"] == "ENABLED"
        ),
        60,
        1,
        "an empty backlog report",
    )
    time.sleep(4)
    # A -> B (a normal, eligible failover) so that B is the active region and C is B's replica
    kill_region("region-a")
    st = wait_phase("HEALTHY_B", timeout=180)
    assert st["leadership_epoch"] == 2
    wait_for(
        lambda: next(
            (f for f in topology(t)["flows"] if f["source_region"] == "region-b" and f["target_region"] == "region-c" and f["state"] == "ENABLED"), None
        ),
        120,
        1,
        "the B->C flow to finish its cutover",
    )
    # C is deliberately made stale: every B->C path is cut, and B keeps accepting writes
    sh(str(CHAOS / "disconnect.sh"), "--pair", "b", "c")
    new_b, errs = produce_at("region-b", t, 60, start=1000)
    assert len(new_b) == 60, errs

    def stale_reported():
        return next(
            (
                f
                for f in api("GET", "/replication/backlog", params={"topic": t}).json()["flows"]
                if f["source_region"] == "region-b" and f["target_region"] == "region-c" and f["backlog_records"] > 0
            ),
            None,
        )

    wait_for(stale_reported, 60, 1, "B->C to report an unreplicated backlog")
    time.sleep(36)  # C stays behind for longer than the 30 s budget
    in_c_before = set(read_region("region-c", t))
    assert not (set(new_b) & in_c_before), "C really is stale: it holds none of B's new writes"
    kill_region("region-b")  # now B fails: A is already dead, the coordinator has c, w1, w2
    wait_phase("NO_ELIGIBLE_TARGET", timeout=120)
    time.sleep(10)
    st, _ = dr_state()
    assert st["phase"] == "NO_ELIGIBLE_TARGET" and st["active_region"] == "region-b" and st["leadership_epoch"] == 2, "no unsafe automatic promotion"
    assert {"LAG_EXCEEDS_RPO", "LAG_UNKNOWN"} & set(st["eligibility"]["region-c"]), st["eligibility"]
    assert active_by_gateways() == [], "nobody is authorised to write while the only candidate is stale"
    assert write_status("region-c", t)[0] != 200
    r = dr_failover(1, accept_degraded_rpo=True, approved_by="oncall@example.org")
    assert r["result"] == "REFUSED" and r["reason"] == "DEGRADED_RPO_NOT_PERMITTED", r  # the deployment forbids degraded-RPO recovery
    # the actual recovery position: what an unsafe promotion of C would have lost
    in_c = set(read_region("region-c", t))
    missing = sorted(set(new_b) - in_c)
    RESULTS.write_text(
        json.dumps(
            {
                "acknowledged_in_B_since_C_was_cut_off": len(new_b),
                "present_in_C": len(set(new_b) & in_c),
                "would_have_been_lost_by_promoting_C": len(missing),
                "eligibility": st["eligibility"],
                "promotion": "refused",
                "epoch": st["leadership_epoch"],
            },
            indent=2,
        )
    )
    assert len(missing) == len(new_b)
    # B returns before anything was promoted: the controller goes back to HEALTHY_B and replication catches C up
    sh(str(CHAOS / "recover.sh"), "--all")
    restore_region("region-b")
    wait_phase("HEALTHY_B", timeout=300)
    wait_for(lambda: set(new_b) <= set(read_region("region-c", t)), 240, 3, "C to catch up once B and the paths are back")
    assert dr_state()[0]["leadership_epoch"] == 2
