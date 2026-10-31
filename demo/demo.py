"""A narrated end-to-end demonstration against a fresh DR stack (scripts/up.sh --dr). Every line it prints is real output of the running system;
the transcript is saved to demo/results/. Run:  policy/tests/.venv/bin/python demo/demo.py   (destructive: stops region A and fails back)"""

import json
import sys
import time
import uuid
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "policy"), str(ROOT / "policy" / "tests")]
from dr_itest import GW_KEY, GW_URL, active_by_gateways, dr_state, dr_status, kill_region, restore_region, wait_phase  # noqa: E402
from itest import ENV, api, sh, wait_for  # noqa: E402

OUT = Path(__file__).parent / "results"
lines: list[str] = []


def say(msg=""):
    print(msg, flush=True)
    lines.append(msg)


def step(n, title):
    say(f"\n=== {n}. {title}")


def gw(region, method, path, **kw):
    return httpx.request(method, f"{GW_URL[region]}{path}", headers={"X-API-Key": GW_KEY}, timeout=30, **kw)


def state_line():
    s = dr_state()[0]
    return f"phase={s['phase']} active={s['active_region'] or '(none: fencing)'} epoch={s['leadership_epoch']} fenced={s['fenced']}"


def count_events(region, topic):
    t = gw(region, "GET", f"/topics/{topic}").json()
    ids = []
    for p in t["partitions"]:
        off = 0
        while True:
            r = gw(region, "GET", f"/events/{topic}", params={"partition": p["partition"], "offset": off, "limit": 1000}).json()
            ids += [e["event_id"] for e in r["events"]]
            off = r["next_offsets"][str(p["partition"])]
            if not r["has_more"]:
                break
    return ids


def main():
    t_start = time.time()
    step(1, "Start state")
    say(state_line())
    say(f"gateways that are active and lease-valid: {active_by_gateways()}")
    step(2, "Create an EU-restricted topic from examples/policies/eu-restricted-topic.json and publish 100 events")
    body = json.loads((ROOT / "examples/policies/eu-restricted-topic.json").read_text())
    body.pop("_comment")
    body["name"] = f"payments-eu-{uuid.uuid4().hex[:6]}"
    topic = body["name"]
    r = gw("region-a", "POST", "/topics", json=body)
    say(f"create topic -> HTTP {r.status_code}; replica plan from the policy engine: {r.json().get('replica_plan')}")
    s = api("POST", f"/replication/topics/{topic}/start", "writer", json={"partitions": 3})
    say(f"start replication -> {s.status_code} flows: {[(f['source_region'], f['target_region'], f['state']) for f in s.json()['flows']]}")
    acked = []
    for i in range(100):
        eid = str(uuid.uuid4())
        rr = gw("region-a", "POST", "/events", json={"topic": topic, "event_id": eid, "partition_key": f"customer-{i % 10}", "payload": {"amount": i}})
        if rr.status_code == 200:
            acked.append(eid)
    say(f"acknowledged by gateway A: {len(acked)} of 100")
    wait_for(lambda: len(set(count_events("region-b", topic))) >= 100, 120, 2, "replication to B")
    say(f"events in B after replication: {len(set(count_events('region-b', topic)))} unique; all match: {set(count_events('region-b', topic)) == set(acked)}")
    step(3, "Residency: try to replicate the EU-restricted topic into region C (US only)")
    r = api("POST", f"/replication/topics/{topic}/flows", "admin", json={"source_region": "region-a", "target_region": "region-c"})
    say(f"engine answer -> HTTP {r.status_code} {r.json()['decision']}: {[x['code'] for x in r.json()['reasons']]}")
    say(
        f"topic exists in region C's Kafka: {topic in sh('docker', 'exec', 'ss-c-kafka-1', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'ss-c-kafka-1:9092', '--list').stdout.split()}"
    )
    step(4, "Let replication settle, then kill region A (all 11 containers)")
    wait_for(
        lambda: all(
            f["backlog_records"] == 0 and f["report_age_s"] < 3
            for f in api("GET", "/replication/backlog", params={"topic": topic}).json()["flows"]
            if f["flow_state"] == "ENABLED"
        ),
        60,
        1,
        "empty backlog",
    )
    time.sleep(4)
    t_kill = time.time()
    kill_region("region-a")
    say("region A stopped")
    seen = None
    while time.time() - t_kill < 120:
        s = dr_state()[0]
        line = f"{s['phase']} active={s['active_region'] or '-'} epoch={s['leadership_epoch']}"
        if line != seen:
            say(f"  +{time.time() - t_kill:5.1f}s  {line}")
            seen = line
        if s["phase"] == "HEALTHY_B":
            break
        time.sleep(0.5)
    lf = dr_state()[0]["last_failover"]
    say(
        f"promotion: failure boundary -> active = {(lf['activated_ms'] - lf['failure_boundary_ms']) / 1000:.1f} s (includes a {(lf['topology_ms'] - lf['fenced_ms']) / 1000:.1f} s lease wait); expected RPO bound {max(lf['expected_rpo_ms'].values()) / 1000:.1f} s"
    )
    step(5, "Region B is now the only writer; consumers and producers continue there")
    wait_for(lambda: active_by_gateways() == ["region-b"], 30, 0.5, "the B gateway to hold a valid lease")
    say(f"gateways that are active and lease-valid: {active_by_gateways()}")
    new_ids = []
    for i in range(30):
        eid = str(uuid.uuid4())
        if (
            gw(
                "region-b", "POST", "/events", json={"topic": topic, "event_id": eid, "partition_key": f"customer-{i % 10}", "payload": {"amount": 1000 + i}}
            ).status_code
            == 200
        ):
            new_ids.append(eid)
    say(
        f"acknowledged by gateway B: {len(new_ids)} of 30; B now holds {len(set(count_events('region-b', topic)))} unique events; missing from B among those acknowledged before the failure: {len(set(acked) - set(count_events('region-b', topic)))}"
    )
    step(6, "Restore region A: it must stay fenced")
    restore_region("region-a")
    wait_for(lambda: dr_status(1)["health"]["region-a"]["declared"] == "UP", 300, 2, "A UP")
    say(state_line())
    r = gw("region-a", "POST", "/events", json={"topic": topic, "event_id": str(uuid.uuid4()), "partition_key": "x", "payload": {"stale": True}})
    say(f"write to the restored A gateway -> HTTP {r.status_code} {r.json().get('error', {}).get('code')}")
    step(7, "Controlled failback to A (operator-approved)")
    admin = {"X-Admin-Key": ENV["SS_POLICY_ADMIN_KEY"]}
    r = httpx.post("http://localhost:8101/dr/v1/failback/start", json={"region": "region-a", "requested_by": "demo-operator"}, headers=admin, timeout=60)
    say(f"failback start -> {r.json()['result']}")
    wait_for(lambda: ((dr_state()[0]).get("failback") or {}).get("stage") == "READY", 300, 2, "READY")
    fb = dr_state()[0]["failback"]
    say(f"stage READY; divergent records quarantined from A: {sum(q['records'] for q in fb['quarantine'].values())}")
    r = httpx.post("http://localhost:8101/dr/v1/failback/approve", json={"approved_by": "demo-operator"}, headers=admin, timeout=60)
    say(f"approve -> {r.json()['result']}")
    wait_phase("HEALTHY_A", timeout=300)
    wait_for(lambda: not dr_state()[0].get("reprotect"), 600, 2, "re-protection")
    say(state_line())
    say(f"gateways that are active and lease-valid: {active_by_gateways()}")
    r = gw("region-b", "POST", "/events", json={"topic": topic, "event_id": str(uuid.uuid4()), "partition_key": "x", "payload": {"stale": True}})
    say(f"write to the old active B gateway -> HTTP {r.status_code} {r.json().get('error', {}).get('code')}")
    final = set(count_events("region-a", topic))
    say(f"A holds {len(final)} unique events; all acknowledged events present: {(set(acked) | set(new_ids)) <= final}")
    step(8, "Audit trail of the coordinator (state transitions)")
    for e in httpx.get("http://localhost:8101/dr/v1/audit", timeout=10).json()["entries"][-12:]:
        say(f"  seq {e['seq']:>3} {e['event']:<28} {e['from']} -> {e['to']} epoch {e['epoch']} by {e['actor']}")
    say(f"\nwhole demonstration took {time.time() - t_start:.0f} s on one machine; every failure above was simulated with docker stop.")
    OUT.mkdir(exist_ok=True)
    p = OUT / f"{time.strftime('%Y%m%d-%H%M%S')}-demo.txt"
    p.write_text("\n".join(lines) + "\n")
    print(f"transcript: {p}")


if __name__ == "__main__":
    main()
