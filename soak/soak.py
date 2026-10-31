"""Soak and recovery test (Phase 8, section D) against the DR stack (scripts/up.sh --dr).

A sustained, paced write workload runs while a fixed rotation of faults is injected: network latency, disconnected replication paths,
replication-worker restarts, regional gateway restarts, Kafka broker restarts, a killed regional cluster, and (optionally) a final
failure of the active region followed by a controlled failback. Invariants are sampled continuously; at the end the system is restored
and the data is verified.

  python soak.py --minutes 30 --rate 20 [--final-failover]

INVARIANTS checked (any violation is reported and fails the run):
  I1  never two regions that are both active and lease-valid (sampled every second; flagged when seen in two consecutive samples)
  I2  the coordinator's epoch and sequence never decrease
  I3  replication checkpoints never move backwards within a generation (sampled from PostgreSQL every 5 s)
  I4  the hash-chained audit log verifies at the end
  I5  at the end every acknowledged event is in the active region's log (loss is only allowed after the deliberate final failover and is
      then reported as the measured RPO gap) and every replica holds the active region's log in the same per-partition order
Duplicates in a replica are COUNTED, not failed: replication is at-least-once."""

import argparse
import json
import sys
import threading
import time
import uuid
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "policy"), str(ROOT / "policy" / "tests")]
from dr_itest import (
    GW_KEY,
    GW_URL,
    REGION_CONTAINERS,
    active_by_gateways,
    dr_state,
    dr_status,
    kill_region,
    restore_region,
    start_containers,
    wait_phase,
)  # noqa: E402
from itest import ENV, api, gw_topic, psql, sh, start_replication, wait_for  # noqa: E402

CHAOS = ROOT / "infrastructure" / "chaos"
ADMIN = {"X-Admin-Key": ENV["SS_POLICY_ADMIN_KEY"]}
OUT = Path(__file__).parent / "results"
log_lines: list[str] = []


def log(msg: str) -> None:
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    log_lines.append(line)
    print(line, flush=True)


class Workload:
    def __init__(self, topic: str, rate: float):
        self.topic, self.rate = topic, rate
        self.acked: list[tuple[str, float, str]] = []
        self.errors: dict[str, int] = {}
        self.stop = threading.Event()
        self.guess = "region-a"
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        seq = 0
        client = httpx.Client(headers={"X-API-Key": GW_KEY}, timeout=6)
        nxt = time.time()
        while not self.stop.is_set():
            eid = str(uuid.uuid4())
            body = {"topic": self.topic, "event_id": eid, "partition_key": f"k{seq % 24}", "payload": {"n": seq, "pad": "x" * 200}}
            seq += 1
            for r in [self.guess] + [x for x in GW_URL if x != self.guess]:
                try:
                    resp = client.post(f"{GW_URL[r]}/events", json=body)
                except httpx.HTTPError as exc:
                    self.errors[type(exc).__name__] = self.errors.get(type(exc).__name__, 0) + 1
                    continue
                if resp.status_code == 200:
                    self.acked.append((eid, time.time(), r))
                    self.guess = r
                    break
                code = (
                    (resp.json().get("error") or {}).get("code", str(resp.status_code))
                    if resp.headers.get("content-type", "").startswith("application/json")
                    else str(resp.status_code)
                )
                self.errors[code] = self.errors.get(code, 0) + 1
            nxt += 1.0 / self.rate
            time.sleep(max(0, nxt - time.time()))
            if nxt < time.time() - 2:
                nxt = time.time()


class Monitor(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.flag = threading.Event()
        self.violations: list[str] = []
        self.max_epoch, self.max_seq = 0, 0
        self.prev_multi = False
        self.ckpt: dict[tuple, tuple] = {}
        self.samples = 0
        self.max_backlog = 0
        self._last_ckpt = 0.0

    def check_checkpoints(self):
        out = psql(
            "select source_cluster,target_cluster,source_topic,source_partition,last_acknowledged_source_offset,replication_generation from replication_checkpoints"
        ).stdout
        for line in out.strip().splitlines():
            a, b, t, p, ack, gen = line.split("|")
            key, val = (a, b, t, int(p)), (int(gen), int(ack))
            old = self.ckpt.get(key)
            if old and (val[0] < old[0] or (val[0] == old[0] and val[1] < old[1])):
                self.violations.append(f"I3 checkpoint moved backwards for {key}: {old} -> {val}")
            self.ckpt[key] = max(val, old) if old else val

    def run(self):
        while not self.flag.is_set():
            self.samples += 1
            act = active_by_gateways()
            multi = len(act) > 1
            if multi and self.prev_multi:
                self.violations.append(f"I1 two active lease-valid regions at {time.strftime('%H:%M:%S')}: {act}")
            self.prev_multi = multi
            s = dr_state()
            if s:
                st = s[0]
                if st["leadership_epoch"] < self.max_epoch or st["seq"] < self.max_seq:
                    self.violations.append(f"I2 coordinator regressed: epoch {self.max_epoch}->{st['leadership_epoch']} seq {self.max_seq}->{st['seq']}")
                self.max_epoch, self.max_seq = max(self.max_epoch, st["leadership_epoch"]), max(self.max_seq, st["seq"])
            if time.time() - self._last_ckpt > 5:
                self._last_ckpt = time.time()
                try:
                    self.check_checkpoints()
                except Exception:  # noqa: BLE001
                    pass
            self.flag.wait(1.0)


def sh_q(*a):
    return sh(*a, check=False)


# ---- fault actions ------------------------------------------------------------------------------------------------------------------
def a_latency():
    sh_q(str(CHAOS / "latency.sh"), "a-b-direct", "400")
    sh_q(str(CHAOS / "jitter.sh"), "a-c-direct", "200", "100")
    time.sleep(30)
    sh_q(str(CHAOS / "recover.sh"), "--all")


def a_disconnect():
    sh_q(str(CHAOS / "disconnect.sh"), "--pair", "a", "b")
    sh_q(str(CHAOS / "disconnect.sh"), "--pair", "a", "c")
    time.sleep(25)
    sh_q(str(CHAOS / "recover.sh"), "--all")


def a_restart(*names):
    def go():
        for n in names:
            sh_q("docker", "restart", "-t", "2", n)

    return go


def a_kill_c():
    kill_region("region-c")
    time.sleep(45)
    restore_region("region-c", wait_gateway=False)


ACTIONS = [
    ("network latency/jitter on A->B and A->C", a_latency),
    ("disconnect all replication paths for 25 s", a_disconnect),
    ("restart replication workers (A replicator, B receiver)", a_restart("ss-a-replicator", "ss-b-agent")),
    ("restart the standby gateway (B)", a_restart("ss-b-gateway")),
    ("restart a Kafka broker in A (broker 2)", a_restart("ss-a-broker-2")),
    ("kill regional cluster C for 45 s", a_kill_c),
    ("restart the ACTIVE gateway (A)", a_restart("ss-a-gateway")),
    ("restart the single Kafka node of B", a_restart("ss-b-kafka-1")),
    ("restart replication workers (A replicator, C receiver)", a_restart("ss-a-replicator", "ss-c-agent")),
    ("restart the DR controllers one after the other", a_restart("ss-dr-1", "ss-dr-2")),
]


def read_all(region: str, topic: str) -> list[tuple[int, str]]:
    """[(partition, event_id)] in log order, read through that region's gateway."""
    c = httpx.Client(headers={"X-API-Key": GW_KEY}, timeout=30)
    t = c.get(f"{GW_URL[region]}/topics/{topic}").json()
    out = []
    for part in t["partitions"]:
        p, off = part["partition"], 0
        while True:
            r = c.get(f"{GW_URL[region]}/events/{topic}", params={"partition": p, "offset": off, "limit": 1000}).json()
            out += [(p, e["event_id"]) for e in r["events"]]
            off = r["next_offsets"][str(p)]
            if not r["has_more"]:
                break
    return out


def order_of(recs):
    d: dict[int, list[str]] = {}
    for p, e in recs:
        if e not in d.setdefault(p, []):
            d[p].append(e)
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=30)
    ap.add_argument("--rate", type=float, default=20)
    ap.add_argument("--interval", type=float, default=75, help="seconds between faults (after the previous one finished and recovered)")
    ap.add_argument("--final-failover", action="store_true")
    a = ap.parse_args()
    st0 = dr_state()[0]
    assert st0["phase"] == "HEALTHY_A", "start from a healthy DR stack"
    topic = gw_topic("soak", 3, max_lag_ms=60_000)
    start_replication(topic, 3)
    time.sleep(5)
    wl, mon = Workload(topic, a.rate), Monitor()
    started = time.time()
    wl.thread.start()
    mon.start()
    log(f"soak started: {a.minutes} min at {a.rate} events/s on {topic}, epoch {st0['leadership_epoch']}")
    deadline = started + a.minutes * 60
    done: list[dict] = []
    i = 0
    while time.time() < deadline:
        name, fn = ACTIONS[i % len(ACTIONS)]
        i += 1
        t0 = time.time()
        log(f"FAULT {i}: {name}")
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            log(f"  fault action raised {exc!r}")
        done.append({"n": i, "fault": name, "started_s": round(t0 - started, 1), "duration_s": round(time.time() - t0, 1)})
        # recovery gate: the system must come back to a healthy, writable, single-authority state before the next fault
        try:
            wait_for(
                lambda: dr_state() and dr_state()[0]["phase"] in ("HEALTHY_A", "LOCAL_DEGRADED_A") and active_by_gateways() == ["region-a"],
                180,
                2,
                "healthy single authority",
            )
        except TimeoutError as exc:
            mon.violations.append(f"RECOVERY after fault {i} ({name}): {exc}")
            log(f"  {exc}")
        time.sleep(max(5, a.interval - (time.time() - t0) % a.interval if a.interval > 0 else 5))
    log("faults finished; restoring everything")
    sh_q(str(CHAOS / "recover.sh"), "--all")
    for r in REGION_CONTAINERS:
        start_containers([c for c in REGION_CONTAINERS[r]])
    start_containers(["ss-dr-1", "ss-dr-2", "ss-etcd-w1", "ss-etcd-w2"])
    wait_phase("HEALTHY_A", timeout=300)
    failover = None
    if a.final_failover:
        log("FINAL FAULT: kill region A (failover), then restore A and fail back under the controller")
        tk = time.time()
        kill_region("region-a")
        stf = wait_phase("HEALTHY_B", timeout=300)
        log(f"promoted B at epoch {stf['leadership_epoch']}")
        time.sleep(15)
        restore_region("region-a")
        wait_for(lambda: dr_status(1)["health"]["region-a"]["declared"] == "UP", 300, 2, "A UP")
        httpx.post("http://localhost:8101/dr/v1/failback/start", json={"region": "region-a", "requested_by": "soak"}, headers=ADMIN, timeout=60)
        wait_for(lambda: ((dr_state()[0]).get("failback") or {}).get("stage") == "READY", 400, 2, "failback READY")
        httpx.post("http://localhost:8101/dr/v1/failback/approve", json={"approved_by": "soak"}, headers=ADMIN, timeout=60)
        wait_phase("HEALTHY_A", timeout=400)
        wait_for(lambda: not dr_state()[0].get("reprotect"), 900, 3, "re-protection")
        failover = {"total_s": round(time.time() - tk, 1), "epoch_after": dr_state()[0]["leadership_epoch"]}
    wl.stop.set()
    wl.thread.join(15)
    time.sleep(5)
    mon.flag.set()
    mon.join(5)
    log("verifying data")
    final = dr_state()[0]
    active = final["active_region"]
    wait_for(
        lambda: all(
            f["backlog_records"] == 0 for f in api("GET", "/replication/backlog", params={"topic": topic}).json()["flows"] if f["flow_state"] == "ENABLED"
        ),
        900,
        3,
        "replicas to catch up",
    )
    time.sleep(5)
    acked = {x[0] for x in wl.acked}
    src = read_all(active, topic)
    src_ids = {e for _, e in src}
    lost = sorted(acked - src_ids)
    rep = {}
    for r in GW_URL:
        if r == active:
            continue
        recs = read_all(r, topic)
        ids = [e for _, e in recs]
        rep[r] = {
            "events": len(ids),
            "unique": len(set(ids)),
            "duplicates": len(ids) - len(set(ids)),
            "same_order_as_active": order_of(recs) == order_of(src),
            "missing_vs_active": len(src_ids - set(ids)),
        }
        if not rep[r]["same_order_as_active"] or rep[r]["missing_vs_active"]:
            mon.violations.append(f"I5 replica {r} differs from the active region: {rep[r]}")
    if lost and not a.final_failover:
        mon.violations.append(f"I5 {len(lost)} acknowledged events missing from the active region without a failover")
    audit_ok = api("GET", "/audit/verify").json()
    if not audit_ok.get("ok"):
        mon.violations.append(f"I4 audit chain broken: {audit_ok}")
    result = {
        "duration_min": round((time.time() - started) / 60, 1),
        "workload": {
            "rate_events_per_s": a.rate,
            "acknowledged": len(acked),
            "errors_by_code": wl.errors,
            "acknowledged_by_region": {r: sum(1 for x in wl.acked if x[2] == r) for r in GW_URL},
        },
        "faults_injected": done,
        "final_failover": failover,
        "active_region_at_end": active,
        "epoch_at_end": final["leadership_epoch"],
        "phase_at_end": final["phase"],
        "monitor_samples": mon.samples,
        "checkpoint_keys_tracked": len(mon.ckpt),
        "verification": {
            "acknowledged_but_missing_from_active_region": len(lost),
            "rpo_gap_note": "non-zero only after the deliberate final failover (asynchronous replication)" if a.final_failover else "must be 0",
            "replicas": rep,
            "audit_chain": audit_ok,
        },
        "violations": mon.violations,
        "passed": not mon.violations,
        "log": log_lines,
    }
    OUT.mkdir(exist_ok=True)
    p = OUT / f"{time.strftime('%Y%m%d-%H%M%S')}-soak.json"
    p.write_text(json.dumps(result, indent=2, default=str))
    log(f"result written to {p}: {'PASSED' if result['passed'] else 'FAILED'} ({len(mon.violations)} violation(s))")
    sys.exit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
