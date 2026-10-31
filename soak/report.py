"""Builds SOAK.md from the newest soak/results/*-soak.json."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
import sys

fs = [Path(sys.argv[1])] if len(sys.argv) > 1 else sorted((ROOT / "soak" / "results").glob("*-soak.json"))
if not fs:
    raise SystemExit("no soak result")
r = json.loads(fs[-1].read_text())
w, v = r["workload"], r["verification"]
out = [
    "# SovereignStream — Soak and recovery test",
    "",
    f"Source: `{fs[-1].name}` (`soak/soak.py`). **Result: {'PASSED' if r['passed'] else 'FAILED'}.** Duration {r['duration_min']} min on one machine (see PERFORMANCE.md for it); all faults are simulated on that machine.",
    "",
    f"Workload: {w['rate_events_per_s']} events/s (≈200-byte payloads, 24 keys, 3 partitions, asynchronous replicas in B and C), {w['acknowledged']} events acknowledged. By region: {w['acknowledged_by_region']}.",
    f"Client-visible errors during the faults (each such write was NOT acknowledged; the workload simply moved on to the next event): {w['errors_by_code'] or 'none'}.",
    "",
    "## Faults injected (in this order, repeated)",
    "",
    "| # | fault | started (s) | took (s) |",
    "|---|---|---|---|",
]
for d in r["faults_injected"]:
    out.append(f"| {d['n']} | {d['fault']} | {d['started_s']} | {d['duration_s']} |")
out += [
    "",
    f"After every fault the run waited for a healthy, writable, single-authority state before continuing. The final fault: {r['final_failover'] or 'none (no failover was part of this run)'}.",
    "",
    "## Invariants",
    "",
    "| Invariant | Result |",
    "|---|---|",
    f"| I1 never two lease-valid active regions (sampled every second, {r['monitor_samples']} samples) | {'held' if not any(x.startswith('I1') for x in r['violations']) else 'VIOLATED'} |",
    f"| I2 coordinator epoch and sequence never decrease (epoch at end {r['epoch_at_end']}) | {'held' if not any(x.startswith('I2') for x in r['violations']) else 'VIOLATED'} |",
    f"| I3 replication checkpoints never moved backwards ({r['checkpoint_keys_tracked']} checkpoint keys tracked, every 5 s) | {'held' if not any(x.startswith('I3') for x in r['violations']) else 'VIOLATED'} |",
    f"| I4 audit hash chain verifies ({v['audit_chain'].get('checked')} entries) | {'held' if not any(x.startswith('I4') for x in r['violations']) else 'VIOLATED'} |",
    f"| I5 acknowledged events missing from the active region ({r['active_region_at_end']}) | {v['acknowledged_but_missing_from_active_region']} ({v['rpo_gap_note']}) |",
    "",
]
out += [
    "## Replicas after catch-up",
    "",
    "| Region | events | unique | duplicates (at-least-once) | missing vs active | same per-partition order |",
    "|---|---|---|---|---|---|",
]
for k, x in v["replicas"].items():
    out.append(f"| {k} | {x['events']} | {x['unique']} | {x['duplicates']} | {x['missing_vs_active']} | {x['same_order_as_active']} |")
persisted = next((x["unique"] for x in v["replicas"].values() if x["missing_vs_active"] == 0), None)
if persisted is not None:
    out += [
        "",
        f"The active region's log holds {persisted} unique events against {w['acknowledged']} acknowledged: the difference ({persisted - w['acknowledged']}) are writes whose outcome the client never learned (a timeout or reset while a gateway or broker was restarting) but which were committed. That is the ordinary ambiguity of a retried write, not a loss.",
    ]
out += [
    "",
    f"Violations: {r['violations'] or 'none'}",
    "",
    "## Limits of this evidence",
    "",
    f"* {r['duration_min']} minutes is a short soak: it shows the recovery paths work repeatedly, not that nothing degrades over days.",
    "* Faults run one at a time with recovery in between; overlapping faults (for example a broker restart during a failover) were not part of this run.",
    "* Single machine: no clock skew, no real network partitions, no disk faults.",
    "",
]
(ROOT / "SOAK.md").write_text("\n".join(out) + "\n")
print("wrote SOAK.md")
