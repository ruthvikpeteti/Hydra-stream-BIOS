# SovereignStream — Soak and recovery test

Source: `20261101-005146-soak.json` (`soak/soak.py`). **Result: PASSED.** Duration 31.7 min on one machine (see PERFORMANCE.md for it); all faults are simulated on that machine.

Workload: 20.0 events/s (≈200-byte payloads, 24 keys, 3 partitions, asynchronous replicas in B and C), 36550 events acknowledged. By region: {'region-a': 35616, 'region-b': 934, 'region-c': 0}.
Client-visible errors during the faults (each such write was NOT acknowledged; the workload simply moved on to the next event): {'ReadTimeout': 3, 'NOT_ACTIVE_REGION': 1159, 'ReadError': 45, 'ConnectError': 223}.

## Faults injected (in this order, repeated)

| # | fault | started (s) | took (s) |
|---|---|---|---|
| 1 | network latency/jitter on A->B and A->C | 0.0 | 30.7 |
| 2 | disconnect all replication paths for 25 s | 75.0 | 25.6 |
| 3 | restart replication workers (A replicator, B receiver) | 150.0 | 1.3 |
| 4 | restart the standby gateway (B) | 225.0 | 0.6 |
| 5 | restart a Kafka broker in A (broker 2) | 300.0 | 2.2 |
| 6 | kill regional cluster C for 45 s | 375.0 | 49.4 |
| 7 | restart the ACTIVE gateway (A) | 450.0 | 0.8 |
| 8 | restart the single Kafka node of B | 525.0 | 2.1 |
| 9 | restart replication workers (A replicator, C receiver) | 600.0 | 1.3 |
| 10 | restart the DR controllers one after the other | 675.0 | 1.6 |
| 11 | network latency/jitter on A->B and A->C | 750.0 | 30.8 |
| 12 | disconnect all replication paths for 25 s | 825.0 | 25.7 |
| 13 | restart replication workers (A replicator, B receiver) | 900.0 | 1.6 |
| 14 | restart the standby gateway (B) | 975.0 | 0.7 |
| 15 | restart a Kafka broker in A (broker 2) | 1050.0 | 2.3 |
| 16 | kill regional cluster C for 45 s | 1125.0 | 49.6 |
| 17 | restart the ACTIVE gateway (A) | 1200.0 | 0.8 |
| 18 | restart the single Kafka node of B | 1275.0 | 0.8 |
| 19 | restart replication workers (A replicator, C receiver) | 1350.1 | 1.5 |
| 20 | restart the DR controllers one after the other | 1425.1 | 1.5 |
| 21 | network latency/jitter on A->B and A->C | 1500.1 | 30.7 |
| 22 | disconnect all replication paths for 25 s | 1575.1 | 25.8 |
| 23 | restart replication workers (A replicator, B receiver) | 1650.1 | 1.2 |
| 24 | restart the standby gateway (B) | 1725.1 | 0.7 |

After every fault the run waited for a healthy, writable, single-authority state before continuing. The final fault: {'total_s': 76.1, 'epoch_after': 3}.

## Invariants

| Invariant | Result |
|---|---|
| I1 never two lease-valid active regions (sampled every second, 1738 samples) | held |
| I2 coordinator epoch and sequence never decrease (epoch at end 3) | held |
| I3 replication checkpoints never moved backwards (12 checkpoint keys tracked, every 5 s) | held |
| I4 audit hash chain verifies (65091 entries) | held |
| I5 acknowledged events missing from the active region (region-a) | 0 (non-zero only after the deliberate final failover (asynchronous replication)) |

## Replicas after catch-up

| Region | events | unique | duplicates (at-least-once) | missing vs active | same per-partition order |
|---|---|---|---|---|---|
| region-b | 36552 | 36552 | 0 | 0 | True |
| region-c | 36552 | 36552 | 0 | 0 | True |

The active region's log holds 36552 unique events against 36550 acknowledged: the difference (2) are writes whose outcome the client never learned (a timeout or reset while a gateway or broker was restarting) but which were committed. That is the ordinary ambiguity of a retried write, not a loss.

Violations: none

## Limits of this evidence

* 31.7 minutes is a short soak: it shows the recovery paths work repeatedly, not that nothing degrades over days.
* Faults run one at a time with recovery in between; overlapping faults (for example a broker restart during a failover) were not part of this run.
* Single machine: no clock skew, no real network partitions, no disk faults.

