# SovereignStream

A policy-governed, multi-region Kafka platform, built and tested as a **single-machine simulation** of three regions (A, B, C). Clients talk to an
HTTP gateway (not the Kafka wire protocol). Residency policy decides where data may live; asynchronous replication copies it; a fenced, coordinator-backed
failover moves the active region when one fails; a dashboard shows the authoritative state.

> **Read [RELEASE.md](RELEASE.md) before relying on any statement here.** It separates what was tested, what is only simulated, and what is future
> production work, and lists the claims this project does *not* make (no zero data loss, no exactly-once, no production availability claim).

## What it is

| Layer | What | Where |
|---|---|---|
| Three Kafka clusters | A: 3 controllers + 3 brokers (RF 3 / min ISR 2 available for local topics); B and C: **one combined node each, RF 1 — not highly available** | `infrastructure/docker-compose.yml` |
| Gateway | HTTP API, idempotency, write lease + epoch fencing, policy checks, backpressure | `gateway/` ([GATEWAY.md](GATEWAY.md)) |
| Policy engine | deny-by-default residency decisions, signed bundles, hash-chained audit, PostgreSQL | `policy/ss_policy`, `policy/engine_service` ([POLICY.md](POLICY.md)) |
| Replication | per-partition, checkpointed, at-least-once A→B/A→C, cutover after promotion | `policy/ss_replication`, `policy/agent_service` ([REPLICATION.md](REPLICATION.md)) |
| Adaptive routing | measured paths, eligibility before score, hysteresis, circuit breaker, Toxiproxy fault injection | [ROUTING.md](ROUTING.md), `infrastructure/chaos/` |
| Disaster recovery | 5-member etcd (3 regions + 2 witnesses), fencing, eligibility, Tier 2/3 promotion, failback | `policy/ss_dr` ([DR.md](DR.md)) |
| Dashboard and metrics | React/Vite UI, Prometheus metrics, protected admin operations | `dashboard/`, `policy/ss_obs` ([DASHBOARD.md](DASHBOARD.md)) |

Architecture and design decisions: [ARCHITECTURE.md](ARCHITECTURE.md), [docs/adr/](docs/adr/), [DATA_MODEL.md](DATA_MODEL.md), [API_SPEC.md](API_SPEC.md),
[SECURITY.md](SECURITY.md). Failures and expected behaviour: [FAILURE_MATRIX.md](FAILURE_MATRIX.md). Operations: [OPERATIONS.md](OPERATIONS.md).
Evidence: [SCENARIOS.md](SCENARIOS.md), [TEST_RESULTS.md](TEST_RESULTS.md), [PERFORMANCE.md](PERFORMANCE.md), [SOAK.md](SOAK.md).

## Quick start (tested with Docker Desktop given 10 CPUs and 7.7 GiB; the full `--dr` stack is 31 containers)

```
infrastructure/scripts/up.sh --dr          # three regions + etcd coordinator + gateways + policy engine + DR controllers + dashboard
open http://localhost:8200                 # dashboard; tokens: SS_DASH_VIEWER_TOKEN / SS_DASH_OPERATOR_TOKEN in infrastructure/.env
infrastructure/scripts/up.sh --dr --monitoring   # adds Prometheus (http://localhost:9090)
infrastructure/scripts/status.sh
infrastructure/scripts/down.sh [--volumes]       # --volumes deletes ALL data
```
`up.sh` without `--dr` starts the Phase 1–5 stack (one gateway, dev control file, no coordinator, no dashboard).
Example topic policies: [examples/policies/](examples/policies/). Environment template: `infrastructure/config/env.example`.
Database schema and migrations: `policy/engine_service/migrations/`. Chaos scripts: `infrastructure/chaos/`.

## Tests

```
ci/run_stage.sh lint | typecheck | unit | db | kafka | replication | policy | failover | chaos | frontend | scenarios | soak
policy/tests/dr_run.sh            # the destructive DR scenarios, each on a fresh stack
dashboard/run_tests.sh unit|live|e2e-destructive
perf/perf.py throughput|network|failover     soak/soak.py --minutes 30 [--final-failover]
```
Most stages start a fresh stack and **delete all data of this Compose project**. CI definition: `.github/workflows/ci.yml` (not executed on GitHub by the
authors; the stages were run locally, see [TEST_RESULTS.md](TEST_RESULTS.md)). Dependencies and base images are pinned (`*/requirements.lock`, `ci/images.lock`,
`dashboard/package-lock.json`, image digests in the Dockerfiles and Compose files).

## Layout

```
gateway/  policy/{ss_policy,ss_replication,ss_dr,ss_obs,engine_service,agent_service,tests}  dashboard/  infrastructure/{docker-compose*.yml,scripts,chaos,tests,config}
perf/  soak/  ci/  examples/  docs/adr/  *.md
```
