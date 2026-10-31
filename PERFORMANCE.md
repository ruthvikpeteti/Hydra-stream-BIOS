# SovereignStream — Performance evaluation

**Read this first.** Every number below was measured by `perf/perf.py` on ONE machine that also runs all three regions, the coordinator, PostgreSQL,
the dashboard and the load generator itself, inside a Docker VM. These are measurements of that setup. They are **not** capacity claims, not production
numbers, and not comparable to a real multi-host deployment. Failures are simulated (`docker stop`, Toxiproxy). The tables are generated from the result
files named in each section (`python perf/report.py`).

## Environment

* Host: macOS-27.0.1-arm64-arm-64bit-Mach-O (arm64), Apple M4, 10 physical / 10 logical cores, 16.0 GiB RAM
* Docker 29.9.2 (Docker Desktop): 10 CPUs and 7.7 GiB available to containers; 31 containers running during the measurement
* Software: apache/kafka 3.9.0 (KRaft), etcd v3.5.16, PostgreSQL 16-alpine, Toxiproxy 2.9.0, Node v24.13.0, load generator Python 3.14.2
* Topology: region A 3 controllers + 3 brokers, RF1 for replicated topics (RF3/min ISR 2 only for local topics); region B 1 combined node, RF1; region C 1 combined node, RF1
* all regions, the coordinator, PostgreSQL and the load generator share ONE machine and one Docker VM

## Throughput and producer acknowledgement latency

Source: `20261031-235124-throughput.json`. Closed-loop load through the gateway of region A: 16 concurrent clients, 1024-byte payloads,
64 partition keys, 5.0 s warm-up then 30.0 s measured, 3 runs each (a new topic per run). Topic: 3 partitions, ASYNC_REGIONAL, replicas in B and C.
Latency is what the client observed: the HTTP request until the 200, which includes the gateway's `acks=all` Kafka acknowledgement. The gateway's own histogram is coarse (bucket upper bounds) and is in the JSON.

### Replicated topic (ASYNC_REGIONAL, replicas in B and C)

| Run | events/s | payload KiB/s | p50 ms | p95 ms | p99 ms | max ms | errors | max backlog (records) | catch-up after load (s) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 369.2 | 369.2 | 33.50 | 99.80 | 169.35 | 469.2 | 0 | 247 | 2.6 |
| 2 | 403.1 | 403.1 | 30.20 | 94.61 | 161.73 | 525.1 | 0 | 376 | 2.1 |
| 3 | 412.0 | 412.0 | 29.13 | 93.33 | 156.99 | 433.5 | 0 | 27 | 1.1 |

Median of runs: **403.1 events/s** (min 369.2, max 412.0); p50 30.20 ms, p95 94.61 ms, p99 161.73 ms.

### Local-only topic (LOCAL_DURABLE, region A only)

| Run | events/s | payload KiB/s | p50 ms | p95 ms | p99 ms | max ms | errors | max backlog (records) | catch-up after load (s) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 696.3 | 696.3 | 18.76 | 45.61 | 86.08 | 325.9 | 0 | n/a | n/a |
| 2 | 669.9 | 669.9 | 19.29 | 48.35 | 98.98 | 326.3 | 0 | n/a | n/a |
| 3 | 587.4 | 587.4 | 21.09 | 59.04 | 126.11 | 310.9 | 0 | n/a | n/a |

Median of runs: **669.9 events/s** (min 587.4, max 696.3); p50 19.29 ms, p95 48.35 ms, p99 98.98 ms.

## Route switching and all-paths-down

Source: `20261101-000029-network.json`.

### Route switching under injected degradation

Steady paced load (100 events/s). At t=0, 400 ms latency and a 20 KB/s bandwidth limit are injected on the path in use (`a-b-direct`). Switch time = until the replicator's own route state names another path.

| Round | switch time (s) | new path | max backlog after injection (records) |
|---|---|---|---|
| 1 | 19.34 | a-b-relay | 1490 |
| 2 | 19.61 | a-b-relay | 1630 |
| 3 | 18.86 | a-b-relay | 1582 |
| 4 | 19.07 | a-b-relay | 1630 |
| 5 | 19.32 | a-b-relay | 1542 |

Switched in 5 of 5 rounds; median 19.32 s (min 18.86, max 19.61). This includes the router's configured observation window, hold and hysteresis (4 s / 4 s / 25 %), so it is a property of those settings as much as of detection.

### All paths down

All paths A→B and A→C disconnected for 40 s under 100 events/s. Writes continued locally: 4000 acknowledged, errors none.
Backlog at its largest: 8000 records (~10604789 bytes, estimated). **Catch-up after the network was restored: 12.2 s.**

## Failover, promotion and recovery

Source: `20261101-000741-failover.json`. Region A is stopped (`docker stop -t 1` of its 11 containers) under a continuous ~20 events/s workload; the controller detects, fences and promotes B; A is restored and a controlled failback returns authority to A. Repeated; each round starts from `HEALTHY_A`.

| Round | detection → boundary (s after kill) | controller RTO (s) | promotion (fence → active) (s) | of which lease wait (s) | kill → active (s) | client write gap (s) | consumer recovery (s) | acknowledged | missing in B (RPO gap) | expected RPO bound (s) |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 4.67 | 10.98 | 7.14 | 7.13 | 16.18 | 16.40 | 16.66 | 120 | 0 | 3.80 |
| 2 | 6.58 | 10.48 | 7.05 | 7.05 | 17.24 | 17.40 | 17.71 | 114 | 0 | 11.97 |
| 3 | 6.63 | 10.52 | 7.04 | 7.04 | 17.31 | 18.65 | 18.83 | 113 | 0 | 11.85 |

| Round | failback: resync → READY (s) | approve → A active (s) | re-protection (s) |
|---|---|---|---|
| 1 | 18.3 | 27.2 | 8.1 |
| 2 | 16.3 | 27.0 | 8.1 |
| 3 | 18.3 | 27.0 | 10.3 |

Definitions: *controller RTO* = first failed probe round → B activated; *promotion* = the fencing write → activation (it contains the deliberate lease wait of TTL + 1 s = 7 s, which exists so a gateway that cannot hear the fence has expired its own lease); *client write gap* = last write acknowledged by A → first write acknowledged by B; *consumer recovery* = kill → a consumer attached to B reads an event written there after the promotion; *RPO gap* = events a client was acknowledged for that are absent from B once replication settled (asynchronous replication: this can be non-zero, and this table is where it would show).

## What these numbers do not tell you

* Nothing about multi-host, WAN, real disks, or production load; nothing about tail behaviour over hours (see SOAK.md for what was run).
* The load generator shares the machine with the system under test.
* Latency percentiles are over the measured window of single runs; with 3 runs the spread between runs is the honest uncertainty (shown per run).
* Region B and C have one broker with RF 1; their durability and availability are not claimed.

