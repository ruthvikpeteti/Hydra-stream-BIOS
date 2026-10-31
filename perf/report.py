"""Builds PERFORMANCE.md from the newest perf/results/*.json of each kind. Every figure printed is read from those files."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
R = ROOT / "perf" / "results"


def latest(kind):
    fs = sorted(R.glob(f"*-{kind}.json"))
    return (json.loads(fs[-1].read_text()), fs[-1].name) if fs else (None, None)


def f(v, nd=1):
    return "n/a" if v is None else (f"{v:.{nd}f}" if isinstance(v, float) else str(v))


out = [
    "# SovereignStream — Performance evaluation",
    "",
    "**Read this first.** Every number below was measured by `perf/perf.py` on ONE machine that also runs all three regions, the coordinator, PostgreSQL,",
    "the dashboard and the load generator itself, inside a Docker VM. These are measurements of that setup. They are **not** capacity claims, not production",
    "numbers, and not comparable to a real multi-host deployment. Failures are simulated (`docker stop`, Toxiproxy). The tables are generated from the result",
    "files named in each section (`python perf/report.py`).",
    "",
]
env, name = latest("env")
tp, tpn = latest("throughput")
env = (tp or {}).get("environment") or env
if env:
    h, d, s, t = env["host"], env["docker"], env["software"], env["topology"]
    out += [
        "## Environment",
        "",
        f"* Host: {h['os']} ({h['machine']}), {h['cpu']}, {h['physical_cores']} physical / {h['logical_cores']} logical cores, {h['memory_gib']} GiB RAM",
        f"* Docker {d['version']} ({d['os']}): {d['cpus_available_to_containers']} CPUs and {d['memory_gib_available_to_containers']} GiB available to containers; {d['containers_running']} containers running during the measurement",
        f"* Software: {s['kafka']}, etcd {s['etcd']}, PostgreSQL {s['postgres']}, Toxiproxy {s['toxiproxy']}, Node {s['node']}, load generator Python {h['python_loadgen']}",
        f"* Topology: region A {t['region_a']}; region B {t['region_b']}; region C {t['region_c']}",
        f"* {t['note']}",
        "",
    ]
if tp:
    w = tp["workload"]
    out += [
        "## Throughput and producer acknowledgement latency",
        "",
        f"Source: `{tpn}`. Closed-loop load through the gateway of region A: {w['concurrency']} concurrent clients, {w['payload_bytes']}-byte payloads,",
        f"{w['keys']} partition keys, {w['warmup_s']} s warm-up then {w['duration_s']} s measured, {w['runs']} runs each (a new topic per run). Topic: {w['topic']}.",
        "Latency is what the client observed: the HTTP request until the 200, which includes the gateway's `acks=all` Kafka acknowledgement. The gateway's own histogram is coarse (bucket upper bounds) and is in the JSON.",
        "",
    ]
    for kind, label in (
        ("replicated", "Replicated topic (ASYNC_REGIONAL, replicas in B and C)"),
        ("local_only", "Local-only topic (LOCAL_DURABLE, region A only)"),
    ):
        out += [
            f"### {label}",
            "",
            "| Run | events/s | payload KiB/s | p50 ms | p95 ms | p99 ms | max ms | errors | max backlog (records) | catch-up after load (s) |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
        for i, r in enumerate(tp[kind], 1):
            lag = r.get("replication_lag_during_load") or {}
            c = r["client_ack_latency_ms"]
            out.append(
                f"| {i} | {f(r['throughput_events_per_s'])} | {f(r['throughput_payload_KiB_per_s'])} | {f(c['p50'], 2)} | {f(c['p95'], 2)} | {f(c['p99'], 2)} | {f(c['max'], 1)} | {sum(r['errors'].values()) or 0} | {f(lag.get('max_backlog_records'))} | {f(r.get('catch_up_after_load_s'))} |"
            )
        sm = tp[f"{kind}_summary"]
        out += [
            "",
            f"Median of runs: **{f(sm['median_throughput_events_per_s'])} events/s** (min {f(sm['min_throughput_events_per_s'])}, max {f(sm['max_throughput_events_per_s'])}); p50 {f(sm['median_p50_ms'], 2)} ms, p95 {f(sm['median_p95_ms'], 2)} ms, p99 {f(sm['median_p99_ms'], 2)} ms.",
            "",
        ]
net, nn = latest("network")
if net:
    out += [
        "## Route switching and all-paths-down",
        "",
        f"Source: `{nn}`.",
        "",
        "### Route switching under injected degradation",
        "",
        "Steady paced load (100 events/s). At t=0, 400 ms latency and a 20 KB/s bandwidth limit are injected on the path in use (`a-b-direct`). Switch time = until the replicator's own route state names another path.",
        "",
        "| Round | switch time (s) | new path | max backlog after injection (records) |",
        "|---|---|---|---|",
    ]
    for r in net["route_switching"]:
        out.append(f"| {r['round']} | {f(r['switch_time_s'], 2)} | {r['new_path']} | {f((r.get('lag_after_injection') or {}).get('max_backlog_records'))} |")
    s = net["route_switching_summary"]
    out += [
        "",
        f"Switched in {s['switched']} of {s['rounds']} rounds; median {f(s['median_s'], 2)} s (min {f(s['min_s'], 2)}, max {f(s['max_s'], 2)}). This includes the router's configured observation window, hold and hysteresis (4 s / 4 s / 25 %), so it is a property of those settings as much as of detection.",
        "",
    ]
    a = net["all_paths_down"]
    d = a["backlog_during_outage"]
    out += [
        "### All paths down",
        "",
        f"All paths A→B and A→C disconnected for {a['outage_s']} s under {a['offered_load_events_per_s']} events/s. Writes continued locally: {a['load_result']['acknowledged']} acknowledged, errors {a['load_result']['errors'] or 'none'}.",
        f"Backlog at its largest: {f(d.get('max_backlog_records'))} records (~{f(d.get('max_backlog_bytes'))} bytes, estimated). **Catch-up after the network was restored: {f(a['catch_up_after_restore_s'])} s.**",
        "",
    ]
fo, fn = latest("failover")
if fo:
    out += [
        "## Failover, promotion and recovery",
        "",
        f"Source: `{fn}`. Region A is stopped (`docker stop -t 1` of its 11 containers) under a continuous ~20 events/s workload; the controller detects, fences and promotes B; A is restored and a controlled failback returns authority to A. Repeated; each round starts from `HEALTHY_A`.",
        "",
        "| Round | detection → boundary (s after kill) | controller RTO (s) | promotion (fence → active) (s) | of which lease wait (s) | kill → active (s) | client write gap (s) | consumer recovery (s) | acknowledged | missing in B (RPO gap) | expected RPO bound (s) |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in fo["rounds"]:
        out.append(
            f"| {r['round']} | {f(r['failure_boundary_after_kill_s'], 2)} | {f(r['controller_rto_s'], 2)} | {f(r['promotion_duration_s'], 2)} | {f(r['lease_wait_included_s'], 2)} | {f(r['kill_to_active_s'], 2)} | {f(r['client_write_unavailable_s'], 2)} | {f(r['consumer_recovery_s'], 2)} | {r['acknowledged']} | {r['missing_from_new_active_rpo_gap_events']} | {f(r['expected_rpo_bound_s'], 2)} |"
        )
    out += ["", "| Round | failback: resync → READY (s) | approve → A active (s) | re-protection (s) |", "|---|---|---|---|"]
    for r in fo["rounds"]:
        b = r.get("failback") or {}
        out.append(f"| {r['round']} | {f(b.get('resync_to_ready_s'))} | {f(b.get('approve_to_active_s'))} | {f(b.get('reprotect_s'))} |")
    s = fo["summary"]
    out += [
        "",
        "Definitions: *controller RTO* = first failed probe round → B activated; *promotion* = the fencing write → activation (it contains the deliberate lease wait of TTL + 1 s = 7 s, which exists so a gateway that cannot hear the fence has expired its own lease); *client write gap* = last write acknowledged by A → first write acknowledged by B; *consumer recovery* = kill → a consumer attached to B reads an event written there after the promotion; *RPO gap* = events a client was acknowledged for that are absent from B once replication settled (asynchronous replication: this can be non-zero, and this table is where it would show).",
        "",
    ]
out += [
    "## What these numbers do not tell you",
    "",
    "* Nothing about multi-host, WAN, real disks, or production load; nothing about tail behaviour over hours (see SOAK.md for what was run).",
    "* The load generator shares the machine with the system under test.",
    "* Latency percentiles are over the measured window of single runs; with 3 runs the spread between runs is the honest uncertainty (shown per run).",
    "* Region B and C have one broker with RF 1; their durability and availability are not claimed.",
    "",
]
(ROOT / "PERFORMANCE.md").write_text("\n".join(out) + "\n")
print("wrote PERFORMANCE.md")
