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

Source: `20261101-015009-throughput.json`. Closed-loop load through the gateway of region A: 16 concurrent clients, 1024-byte payloads,
64 partition keys, 5.0 s warm-up then 30.0 s measured, 3 runs each (a new topic per run). Topic: 3 partitions, ASYNC_REGIONAL, replicas in B and C.
Latency is what the client observed: the HTTP request until the 200, which includes the gateway's `acks=all` Kafka acknowledgement. The gateway's own histogram is coarse (bucket upper bounds) and is in the JSON.

### Replicated topic (ASYNC_REGIONAL, replicas in B and C)

| Run | events/s | payload KiB/s | p50 ms | p95 ms | p99 ms | max ms | errors | max backlog (records) | catch-up after load (s) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 358.4 | 358.4 | 34.33 | 107.98 | 179.94 | 374.3 | 0 | 27 | 3.2 |
| 2 | 382.8 | 382.8 | 31.09 | 102.57 | 178.74 | 562.2 | 0 | 476 | 2.1 |
| 3 | 375.4 | 375.4 | 31.67 | 105.00 | 184.82 | 616.6 | 0 | 36 | 3.2 |

Median of runs: **375.4 events/s** (min 358.4, max 382.8); p50 31.67 ms, p95 105.00 ms, p99 179.94 ms.

### Local-only topic (LOCAL_DURABLE, region A only)

| Run | events/s | payload KiB/s | p50 ms | p95 ms | p99 ms | max ms | errors | max backlog (records) | catch-up after load (s) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 678.3 | 678.3 | 19.26 | 47.84 | 87.14 | 256.1 | 0 | n/a | n/a |
| 2 | 618.2 | 618.2 | 20.08 | 57.24 | 115.63 | 308.9 | 0 | n/a | n/a |
| 3 | 549.6 | 549.6 | 21.96 | 66.92 | 142.43 | 427.7 | 0 | n/a | n/a |

Median of runs: **618.2 events/s** (min 549.6, max 678.3); p50 20.08 ms, p95 57.24 ms, p99 115.63 ms.

## Route switching and all-paths-down

Source: `20261101-015913-network.json`.

### Route switching under injected degradation

Steady paced load (100 events/s). At t=0, 400 ms latency and a 20 KB/s bandwidth limit are injected on the path in use (`a-b-direct`). Switch time = until the replicator's own route state names another path.

| Round | switch time (s) | new path | max backlog after injection (records) |
|---|---|---|---|
| 1 | 19.34 | a-b-relay | 1452 |
| 2 | 19.60 | a-b-relay | 1661 |
| 3 | 18.71 | a-b-relay | 1597 |
| 4 | 19.02 | a-b-relay | 1476 |
| 5 | 19.03 | a-b-relay | 1510 |

Switched in 5 of 5 rounds; median 19.03 s (min 18.71, max 19.60). This includes the router's configured observation window, hold and hysteresis (4 s / 4 s / 25 %), so it is a property of those settings as much as of detection.

### All paths down

All paths A→B and A→C disconnected for 40 s under 100 events/s. Writes continued locally: 4001 acknowledged, errors none.
Backlog at its largest: 8002 records (~10606467 bytes, estimated). **Catch-up after the network was restored: 12.7 s.**

## Failover, promotion and recovery

Source: `20261101-020619-failover.json`. Region A is stopped (`docker stop -t 1` of its 11 containers) under a continuous ~20 events/s workload; the controller detects, fences and promotes B; A is restored and a controlled failback returns authority to A. Repeated; each round starts from `HEALTHY_A`.

| Round | detection → boundary (s after kill) | controller RTO (s) | promotion (fence → active) (s) | of which lease wait (s) | kill → active (s) | client write gap (s) | consumer recovery (s) | acknowledged | missing in B (RPO gap) | expected RPO bound (s) |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 5.30 | 11.55 | 8.14 | 8.14 | 16.92 | 17.52 | 17.81 | 114 | 0 | 4.81 |
| 2 | 6.17 | 10.48 | 7.06 | 7.05 | 16.91 | 17.82 | 17.98 | 115 | 0 | 11.13 |
| 3 | 6.41 | 10.49 | 7.05 | 7.04 | 17.19 | 17.87 | 18.07 | 116 | 0 | 12.09 |

| Round | failback: resync → READY (s) | approve → A active (s) | re-protection (s) |
|---|---|---|---|
| 1 | 16.3 | 27.1 | 8.1 |
| 2 | 14.2 | 28.0 | 8.1 |
| 3 | 16.3 | 27.6 | 8.1 |

Definitions: *controller RTO* = first failed probe round → B activated; *promotion* = the fencing write → activation (it contains the deliberate lease wait of TTL + 1 s = 7 s, which exists so a gateway that cannot hear the fence has expired its own lease); *client write gap* = last write acknowledged by A → first write acknowledged by B; *consumer recovery* = kill → a consumer attached to B reads an event written there after the promotion; *RPO gap* = events a client was acknowledged for that are absent from B once replication settled (asynchronous replication: this can be non-zero, and this table is where it would show).

## What these numbers do not tell you

* Nothing about multi-host, WAN, real disks, or production load; nothing about tail behaviour over hours (see SOAK.md for what was run).
* The load generator shares the machine with the system under test.
* Latency percentiles are over the measured window of single runs; with 3 runs the spread between runs is the honest uncertainty (shown per run).
* Region B and C have one broker with RF 1; their durability and availability are not claimed.

