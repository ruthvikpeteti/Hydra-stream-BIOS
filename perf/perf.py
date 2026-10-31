"""Repeatable performance evaluation of SovereignStream (Phase 8, section C).

Every number in PERFORMANCE.md is read from the JSON this script writes: nothing is typed by hand. The load generator runs on the SAME
machine as the whole stack (all three regions, PostgreSQL, etcd...), so results are bounded by that one machine and the generator competes
with the system for CPU. They are measurements of this setup, not capacity claims.

  python perf.py env                       environment report
  python perf.py throughput [--runs 3]     closed-loop throughput + ack latency + replication lag + catch-up (normal or DR stack)
  python perf.py network [--rounds 5]      route switching time under injected degradation; all paths down: backlog growth and catch-up
  python perf.py failover [--rounds 3]     (DR stack) A fails -> B promoted -> A restored -> controlled failback; repeated
Run from policy/tests' venv:  policy/tests/.venv/bin/python perf/perf.py ... (needs `PYTHONPATH=policy:policy/tests`)."""

import argparse
import asyncio
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "policy"), str(ROOT / "policy" / "tests")]
from dr_itest import GW_KEY, GW_URL, dr_state, dr_status, kill_region, produce_at, restore_region, wait_phase  # noqa: E402
from itest import ENV, api, gw_topic, sh, start_replication, wait_for  # noqa: E402

OUT = Path(__file__).parent / "results"
CHAOS = ROOT / "infrastructure" / "chaos"
ADMIN = {"X-Admin-Key": ENV["SS_POLICY_ADMIN_KEY"]}


def save(name: str, data: dict) -> Path:
    OUT.mkdir(exist_ok=True)
    p = OUT / f"{time.strftime('%Y%m%d-%H%M%S')}-{name}.json"
    data["_written_at"] = time.strftime("%Y-%m-%d %H:%M:%S %z")
    p.write_text(json.dumps(data, indent=2, sort_keys=True))
    print(f"wrote {p}")
    return p


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    v = sorted(values)
    k = (len(v) - 1) * q
    f = int(k)
    return v[f] if f + 1 >= len(v) else v[f] + (v[f + 1] - v[f]) * (k - f)


def sh_out(*a) -> str:
    try:
        return subprocess.run(a, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception:  # noqa: BLE001
        return ""


# ---- environment ------------------------------------------------------------------------------------------------------------------
def environment() -> dict:
    info = json.loads(sh_out("docker", "info", "--format", "{{json .}}") or "{}")
    images = sh_out("docker", "ps", "--format", "{{.Names}} {{.Image}}").splitlines()
    return {
        "host": {
            "os": platform.platform(),
            "machine": platform.machine(),
            "cpu": sh_out("sysctl", "-n", "machdep.cpu.brand_string") or platform.processor(),
            "physical_cores": sh_out("sysctl", "-n", "hw.physicalcpu"),
            "logical_cores": os.cpu_count(),
            "memory_gib": round(int(sh_out("sysctl", "-n", "hw.memsize") or 0) / 2**30, 1),
            "python_loadgen": platform.python_version(),
        },
        "docker": {
            "version": info.get("ServerVersion"),
            "os": info.get("OperatingSystem"),
            "cpus_available_to_containers": info.get("NCPU"),
            "memory_gib_available_to_containers": round(info.get("MemTotal", 0) / 2**30, 1),
            "containers_running": len(images),
        },
        "software": {"kafka": "apache/kafka 3.9.0 (KRaft)", "etcd": "v3.5.16", "postgres": "16-alpine", "toxiproxy": "2.9.0", "node": sh_out("node", "-v")},
        "topology": {
            "region_a": "3 controllers + 3 brokers, RF1 for replicated topics (RF3/min ISR 2 only for local topics)",
            "region_b": "1 combined node, RF1",
            "region_c": "1 combined node, RF1",
            "note": "all regions, the coordinator, PostgreSQL and the load generator share ONE machine and one Docker VM",
        },
        "gateway": {"producer": "aiokafka acks=all, idempotent", "workers": 1, "lease_ttl_ms": ENV.get("SS_LEASE_TTL_MS", "6000 (dr) / 10000")},
    }


# ---- gateway histogram -> percentiles ----------------------------------------------------------------------------------------------
def gw_hist(region="region-a") -> dict[float, int]:
    text = httpx.get(f"{GW_URL[region].replace('/api/v1', '')}/metrics", timeout=10).text
    out: dict[float, int] = {}
    for ln in text.splitlines():
        if ln.startswith("ss_producer_ack_latency_seconds_bucket"):
            le = ln.split('le="')[1].split('"')[0]
            out[float("inf") if le == "+Inf" else float(le)] = int(float(ln.rsplit(" ", 1)[1]))
    return out


def hist_quantile(before: dict, after: dict, q: float) -> float | None:
    """Estimated from histogram buckets (upper bound of the bucket holding the quantile): coarse by construction."""
    d = {le: after.get(le, 0) - before.get(le, 0) for le in after}
    total = d.get(float("inf"), 0)
    if total <= 0:
        return None
    for le in sorted(d):
        if d[le] >= q * total:
            return le
    return None


# ---- load ---------------------------------------------------------------------------------------------------------------------------
async def run_load(region: str, topic: str, concurrency: int, duration: float, warmup: float, payload_bytes: int, rate: float | None = None) -> dict:
    url = f"{GW_URL[region]}/events"
    payload = {"blob": "x" * payload_bytes}
    lat: list[float] = []
    errors: dict[str, int] = {}
    ok = 0
    start = time.perf_counter()
    stop_at, measure_from = start + warmup + duration, start + warmup
    per_worker_interval = (concurrency / rate) if rate else 0.0

    async def worker(i: int, client: httpx.AsyncClient):
        nonlocal ok
        n = 0
        nxt = time.perf_counter() + (i * per_worker_interval / max(1, concurrency))
        while time.perf_counter() < stop_at:
            if rate:
                wait = nxt - time.perf_counter()
                if wait > 0:
                    await asyncio.sleep(wait)
                nxt += per_worker_interval
            body = {"topic": topic, "event_id": str(uuid.uuid4()), "partition_key": f"k{(i * 7919 + n) % 64}", "payload": payload}
            n += 1
            t0 = time.perf_counter()
            try:
                r = await client.post(url, json=body)
                dt = time.perf_counter() - t0
                if r.status_code == 200:
                    if t0 >= measure_from:
                        lat.append(dt)
                        ok += 1
                else:
                    errors[str(r.status_code)] = errors.get(str(r.status_code), 0) + 1
            except httpx.HTTPError as exc:
                errors[type(exc).__name__] = errors.get(type(exc).__name__, 0) + 1

    h0 = gw_hist(region)
    async with httpx.AsyncClient(
        headers={"X-API-Key": GW_KEY}, timeout=30, limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    ) as client:
        await asyncio.gather(*(worker(i, client) for i in range(concurrency)))
    h1 = gw_hist(region)
    return {
        "concurrency": concurrency,
        "duration_s": duration,
        "warmup_s": warmup,
        "payload_bytes": payload_bytes,
        "target_rate": rate,
        "acknowledged": ok,
        "errors": errors,
        "throughput_events_per_s": round(ok / duration, 1),
        "throughput_payload_KiB_per_s": round(ok * payload_bytes / 1024 / duration, 1),
        "client_ack_latency_ms": {
            "p50": round(pct(lat, 0.5) * 1000, 2),
            "p95": round(pct(lat, 0.95) * 1000, 2),
            "p99": round(pct(lat, 0.99) * 1000, 2),
            "max": round(max(lat) * 1000, 2),
            "what": "HTTP request to the gateway until the 200 (includes the acks=all Kafka acknowledgement)",
        }
        if lat
        else None,
        "gateway_ack_latency_s_bucket_bound": {
            "p50": hist_quantile(h0, h1, 0.5),
            "p95": hist_quantile(h0, h1, 0.95),
            "p99": hist_quantile(h0, h1, 0.99),
            "what": "upper bound of the histogram bucket holding the quantile (includes warmup requests); gateway-side acks=all send only",
        },
    }


class LagSampler(threading.Thread):
    """Samples the engine's backlog reports of one topic once a second."""

    def __init__(self, topic: str):
        super().__init__(daemon=True)
        self.topic, self.samples, self.flag = topic, [], threading.Event()

    def run(self):
        while not self.flag.is_set():
            try:
                fl = api("GET", "/replication/backlog", params={"topic": self.topic}).json()["flows"]
                fl = [f for f in fl if f["flow_state"] == "ENABLED"]
                self.samples.append(
                    {
                        "t": time.time(),
                        "records": sum(f["backlog_records"] for f in fl),
                        "bytes": sum(f["backlog_bytes"] for f in fl),
                        "max_drain_s": max([f["drain_seconds"] or 0 for f in fl], default=0),
                        "hard": any(f["backlog_state"] == "HARD" for f in fl),
                    }
                )
            except Exception:  # noqa: BLE001
                pass
            self.flag.wait(1.0)

    def stop(self) -> list[dict]:
        self.flag.set()
        self.join(5)
        return self.samples


def backlog_now(topic: str) -> int:
    fl = [f for f in api("GET", "/replication/backlog", params={"topic": topic}).json()["flows"] if f["flow_state"] == "ENABLED"]
    return sum(f["backlog_records"] for f in fl)


def wait_drained(topic: str, timeout: float = 600) -> float:
    t0 = time.time()
    wait_for(lambda: backlog_now(topic) == 0, timeout, 0.5, "backlog to drain")
    return round(time.time() - t0, 2)


def summarize_lag(samples: list[dict]) -> dict:
    if not samples:
        return {}
    return {
        "samples": len(samples),
        "max_backlog_records": max(s["records"] for s in samples),
        "median_backlog_records": statistics.median(s["records"] for s in samples),
        "max_backlog_bytes": max(s["bytes"] for s in samples),
        "max_estimated_drain_s": round(max(s["max_drain_s"] for s in samples), 2),
    }


def new_topic(prefix: str, replicated: bool = True) -> str:
    t = (
        gw_topic(prefix, 3, replicas=2 if replicated else 0, regions=["region-a", "region-b", "region-c"] if replicated else ["region-a"])
        if replicated
        else None
    )
    if replicated:
        start_replication(t, 3)
        return t
    name = f"{prefix}-{uuid.uuid4().hex[:8]}"
    r = httpx.post(
        f"{GW_URL['region-a']}/topics",
        headers={"X-API-Key": GW_KEY},
        json={"name": name, "partitions": 3, "durability_mode": "LOCAL_DURABLE", "local_replication_factor": 1, "local_min_insync_replicas": 1},
    )
    assert r.status_code == 201, r.text
    return name


# ---- commands -----------------------------------------------------------------------------------------------------------------------
def cmd_env(_):
    save("env", environment())


def cmd_throughput(a):
    out = {
        "environment": environment(),
        "workload": {
            "payload_bytes": a.payload,
            "concurrency": a.concurrency,
            "duration_s": a.duration,
            "warmup_s": a.warmup,
            "runs": a.runs,
            "topic": "3 partitions, ASYNC_REGIONAL, replicas in B and C",
            "keys": 64,
        },
        "replicated": [],
        "local_only": [],
    }
    for kind, repl in (("replicated", True), ("local_only", False)):
        for i in range(a.runs):
            t = new_topic(f"perf-{kind[:4]}", repl)
            time.sleep(3)
            sampler = LagSampler(t) if repl else None
            if sampler:
                sampler.start()
            res = asyncio.run(run_load("region-a", t, a.concurrency, a.duration, a.warmup, a.payload))
            if repl:
                res["replication_lag_during_load"] = summarize_lag(sampler.stop())
                res["catch_up_after_load_s"] = wait_drained(t)
            print(kind, i + 1, res["throughput_events_per_s"], "ev/s", res["client_ack_latency_ms"])
            out[kind].append(res)
    for kind in ("replicated", "local_only"):
        rs = out[kind]
        out[f"{kind}_summary"] = {
            "median_throughput_events_per_s": statistics.median(r["throughput_events_per_s"] for r in rs),
            "min_throughput_events_per_s": min(r["throughput_events_per_s"] for r in rs),
            "max_throughput_events_per_s": max(r["throughput_events_per_s"] for r in rs),
            "median_p50_ms": statistics.median(r["client_ack_latency_ms"]["p50"] for r in rs),
            "median_p95_ms": statistics.median(r["client_ack_latency_ms"]["p95"] for r in rs),
            "median_p99_ms": statistics.median(r["client_ack_latency_ms"]["p99"] for r in rs),
        }
    save("throughput", out)


def replicator_route(target="region-b", source_port=8095) -> str | None:
    st = httpx.get(f"http://localhost:{source_port}/status", timeout=10).json()
    return next((f["route"]["current"] for f in st["flows"] if f["target"] == target), None)


def reset_chaos():
    sh(str(CHAOS / "recover.sh"), "--all", check=False)


def cmd_network(a):
    out = {"environment": environment(), "route_switching": [], "all_paths_down": {}}
    t = new_topic("perf-net")
    produce_at("region-a", t, 30)
    wait_for(lambda: replicator_route() == "a-b-direct", 120, 1, "baseline route a-b-direct")
    for rnd in range(a.rounds):
        reset_chaos()
        wait_for(lambda: replicator_route() == "a-b-direct", 180, 1, "route to return to a-b-direct")
        time.sleep(15)  # let the hold/cooldown timers of the previous switch expire
        threading.Thread(target=lambda: asyncio.run(run_load("region-a", t, 4, 60, 0, 1024, rate=100)), daemon=True).start()
        time.sleep(6)
        sampler = LagSampler(t)
        sampler.start()
        t0 = time.time()
        sh(str(CHAOS / "latency.sh"), "a-b-direct", "400")
        sh(str(CHAOS / "bandwidth.sh"), "a-b-direct", "20")
        switched_after = None
        seen = []
        end = t0 + 120
        while time.time() < end:
            r = replicator_route()
            if r != "a-b-direct":
                switched_after = round(time.time() - t0, 2)
                seen.append(r)
                break
            time.sleep(0.25)
        time.sleep(8)
        lag = summarize_lag(sampler.stop())
        out["route_switching"].append(
            {
                "round": rnd + 1,
                "injected": "400 ms latency + 20 KB/s bandwidth on a-b-direct",
                "switch_time_s": switched_after,
                "new_path": seen[0] if seen else None,
                "lag_after_injection": lag,
            }
        )
        print("route switch", rnd + 1, switched_after, seen)
        time.sleep(max(0, 60 - (time.time() - t0)) + 2)
    reset_chaos()
    times = [r["switch_time_s"] for r in out["route_switching"] if r["switch_time_s"] is not None]
    out["route_switching_summary"] = {
        "rounds": a.rounds,
        "switched": len(times),
        "median_s": statistics.median(times) if times else None,
        "min_s": min(times, default=None),
        "max_s": max(times, default=None),
    }
    # all eligible paths to B and C are down under steady load
    wait_for(lambda: replicator_route() is not None, 120, 1, "route")
    time.sleep(20)
    t1 = new_topic("perf-down")
    time.sleep(3)
    sampler = LagSampler(t1)
    sampler.start()
    sh(str(CHAOS / "disconnect.sh"), "--pair", "a", "b")
    sh(str(CHAOS / "disconnect.sh"), "--pair", "a", "c")
    res = asyncio.run(run_load("region-a", t1, 4, 40, 0, 1024, rate=100))
    time.sleep(3)
    down = summarize_lag(sampler.samples)
    reset_chaos()
    t_restore = time.time()
    drained = wait_drained(t1, 900)
    sampler.stop()
    out["all_paths_down"] = {
        "outage_s": 40,
        "offered_load_events_per_s": 100,
        "load_result": {k: res[k] for k in ("acknowledged", "errors", "throughput_events_per_s", "client_ack_latency_ms")},
        "backlog_during_outage": down,
        "catch_up_after_restore_s": drained,
        "restored_at": t_restore,
    }
    save("network", out)


def api_state():
    s = dr_state()
    return s[0] if s else None


def consumer_read_after(region: str, topic: str, event_id: str) -> bool:
    r = httpx.get(f"{GW_URL[region]}/events/{topic}", headers={"X-API-Key": GW_KEY}, params={"limit": 1000}, timeout=10)
    return r.status_code == 200 and any(e["event_id"] == event_id for e in r.json().get("events", []))


def cmd_failover(a):
    out = {"environment": environment(), "rounds": []}
    t = new_topic("perf-fo")
    produce_at("region-a", t, 60)
    time.sleep(8)
    for rnd in range(a.rounds):
        wait_phase("HEALTHY_A", timeout=300)
        st = api_state()
        epoch0 = st["leadership_epoch"]
        time.sleep(10)
        acked: list[tuple[str, float, str]] = []
        stop = threading.Event()
        guess = ["region-a"]

        def writer():
            seq = 0
            while not stop.is_set():
                for r in [guess[0]] + [x for x in GW_URL if x != guess[0]]:
                    eid = str(uuid.uuid4())
                    try:
                        resp = httpx.post(
                            f"{GW_URL[r]}/events",
                            headers={"X-API-Key": GW_KEY},
                            timeout=5,
                            json={"topic": t, "event_id": eid, "partition_key": f"k{seq % 16}", "payload": {"n": seq}},
                        )
                    except httpx.HTTPError:
                        continue
                    if resp.status_code == 200:
                        acked.append((eid, time.time(), r))
                        guess[0] = r
                        break
                seq += 1
                time.sleep(0.05)

        th = threading.Thread(target=writer, daemon=True)
        th.start()
        time.sleep(6)
        t_kill = time.time()
        kill_region("region-a")
        st = wait_phase("HEALTHY_B", timeout=240)
        t_active = time.time()
        lf = st["last_failover"]
        first_b = wait_for(lambda: next((x for x in acked if x[2] == "region-b"), None), 60, 0.2, "first acknowledged write in B")
        # consumer recovery: a consumer attached to the NEW active region reads an event written there after the promotion
        probe = str(uuid.uuid4())
        httpx.post(
            f"{GW_URL['region-b']}/events",
            headers={"X-API-Key": GW_KEY},
            timeout=10,
            json={"topic": t, "event_id": probe, "partition_key": "probe", "payload": {"probe": True}},
        )
        wait_for(lambda: consumer_read_after("region-b", t, probe), 30, 0.2, "the consumer to read a post-promotion event in B")
        consumer_recovery = time.time() - t_kill
        time.sleep(4)
        stop.set()
        th.join(10)
        wait_for(lambda: backlog_now(t) >= 0, 5, 1, "engine")
        time.sleep(10)
        present = set()
        r = httpx.get(f"{GW_URL['region-b']}/topics/{t}", headers={"X-API-Key": GW_KEY}).json()
        for part in r["partitions"]:
            off = 0
            while True:
                rr = httpx.get(
                    f"{GW_URL['region-b']}/events/{t}",
                    headers={"X-API-Key": GW_KEY},
                    params={"partition": part["partition"], "offset": off, "limit": 500},
                    timeout=20,
                ).json()
                present |= {e["event_id"] for e in rr["events"]}
                off = rr["next_offsets"][str(part["partition"])]
                if not rr["has_more"]:
                    break
        acked_ids = {x[0] for x in acked}
        lost = acked_ids - present
        last_a = max((x[1] for x in acked if x[2] == "region-a"), default=None)
        row = {
            "round": rnd + 1,
            "epoch_before": epoch0,
            "epoch_after": st["leadership_epoch"],
            "detection_s": round((lf["detected_ms"] / 1000) - t_kill, 2) if lf.get("detected_ms") else None,
            "failure_boundary_after_kill_s": round(lf["failure_boundary_ms"] / 1000 - t_kill, 2),
            "promotion_duration_s": round((lf["activated_ms"] - lf["fenced_ms"]) / 1000, 2),
            "lease_wait_included_s": round((lf["topology_ms"] - lf["fenced_ms"]) / 1000, 2),
            "kill_to_active_s": round(t_active - t_kill, 2),
            "controller_rto_s": round((lf["activated_ms"] - lf["failure_boundary_ms"]) / 1000, 2),
            "client_write_unavailable_s": round(first_b[1] - last_a, 2) if last_a else None,
            "consumer_recovery_s": round(consumer_recovery, 2),
            "acknowledged": len(acked_ids),
            "missing_from_new_active_rpo_gap_events": len(lost),
            "expected_rpo_bound_s": max((v for v in (lf.get("expected_rpo_ms") or {}).values()), default=0) / 1000,
        }
        print("failover", row)
        out["rounds"].append(row)
        # return to HEALTHY_A through the controlled failback (also timed)
        restore_region("region-a")
        wait_for(lambda: dr_status(1)["health"]["region-a"]["declared"] == "UP", 300, 2, "A to be UP")
        httpx.post("http://localhost:8101/dr/v1/failback/start", json={"region": "region-a", "requested_by": "perf"}, headers=ADMIN, timeout=60)
        t_fb0 = time.time()
        wait_for(lambda: ((api_state() or {}).get("failback") or {}).get("stage") == "READY", 300, 2, "failback READY")
        t_ready = time.time()
        httpx.post("http://localhost:8101/dr/v1/failback/approve", json={"approved_by": "perf"}, headers=ADMIN, timeout=60)
        wait_phase("HEALTHY_A", timeout=300)
        t_fb1 = time.time()
        wait_for(lambda: not (api_state() or {}).get("reprotect"), 600, 2, "re-protection")
        row["failback"] = {
            "resync_to_ready_s": round(t_ready - t_fb0, 2),
            "approve_to_active_s": round(t_fb1 - t_ready, 2),
            "reprotect_s": round(time.time() - t_fb1, 2),
        }
        time.sleep(15)
    rtos = [r["controller_rto_s"] for r in out["rounds"]]
    out["summary"] = {
        "rounds": a.rounds,
        "controller_rto_s": {"median": statistics.median(rtos), "min": min(rtos), "max": max(rtos)},
        "consumer_recovery_s": {"median": statistics.median(r["consumer_recovery_s"] for r in out["rounds"])},
        "rpo_gap_events_per_round": [r["missing_from_new_active_rpo_gap_events"] for r in out["rounds"]],
    }
    save("failover", out)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("env").set_defaults(fn=cmd_env)
    t = sub.add_parser("throughput")
    t.set_defaults(fn=cmd_throughput)
    t.add_argument("--runs", type=int, default=3)
    t.add_argument("--concurrency", type=int, default=16)
    t.add_argument("--duration", type=float, default=30)
    t.add_argument("--warmup", type=float, default=5)
    t.add_argument("--payload", type=int, default=1024)
    n = sub.add_parser("network")
    n.set_defaults(fn=cmd_network)
    n.add_argument("--rounds", type=int, default=5)
    f = sub.add_parser("failover")
    f.set_defaults(fn=cmd_failover)
    f.add_argument("--rounds", type=int, default=3)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
