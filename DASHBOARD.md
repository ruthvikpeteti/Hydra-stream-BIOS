# SovereignStream — Observability and Dashboard (Phase 7)

Related: [DR.md](DR.md) · [ROUTING.md](ROUTING.md) · [REPLICATION.md](REPLICATION.md) · [POLICY.md](POLICY.md) · [SECURITY.md](SECURITY.md) · [DEPLOYMENT.md](DEPLOYMENT.md)

The dashboard is a **read-only view of authoritative backend state**, plus a few explicitly protected administrative operations (failover request, failback start/approve/abort) that are *forwarded* to the DR controller. It never decides, infers or caches a conclusion: it never says a promotion succeeded, it shows what the coordinator reports.

## 1. Architecture

```
browser (React/Vite, static files)
   │  Bearer token (viewer | operator)
   ▼
ss-dashboard  (FastAPI, ss_obs)  ── reads ──► etcd coordinator (linearizable)      active region, epoch, fence, DR audit
   │  GET /api/v1/snapshot  (every 2 s)  ──►   DR controller /dr/v1/status          health, eligibility, failover state
   │  GET /metrics (Prometheus)           ──►   policy engine (service key)          policies, flows, backlog, paths, audit
   │  POST /api/v1/admin/*  ─forward─►          replicators /status                    routes, RTT, loss, bandwidth, breakers
   ▼                                           gateways /health, /metrics             health, producer ack latency
DR controller (holds the admin key; the browser never sees it)
```

Run it with the DR stack: `infrastructure/scripts/up.sh --dr` (service `ss-dashboard`, http://localhost:8200). Tokens are generated into `infrastructure/.env` as `SS_DASH_VIEWER_TOKEN` and `SS_DASH_OPERATOR_TOKEN`. Prometheus (`--monitoring` profile) scrapes `ss-dashboard:8200/metrics`.

## 2. Sections and where each value comes from

| Section | Shows | Source |
|---|---|---|
| Global topology | regions A/B/C, active/standby/fenced, broker count, controller quorum, gateway/agent/replicator health, leadership epochs, replication connections and their active route | coordinator (role, epoch), DR controller (health samples), replicators, engine |
| Residency policies | classification, legal zone, allowed and prohibited regions, replica placement and status, policy version, recent blocked operations | engine `/policies`, `/audit/recent` |
| Adaptive network routing | per region pair: every path, the selected one, RTT, loss, bandwidth, score, circuit-breaker state, last route change | replicator `/status` (the same numbers the router uses) |
| Replication | source/target cluster, status, last source and destination offset, lag in records and estimated drain time, backlog bytes, throughput, last acknowledgement | engine checkpoints and backlog reports, replicator pipeline state |
| Disaster recovery | active and standby regions, failover state, epoch, last failover (measured RTO, lease wait, expected RPO bound, approval), candidate eligibility with reasons, fence status, failback progress, actual RPO | coordinator and DR controller |
| Audit logs | policy decisions, replication changes, route switches, failover and failback transitions, administrative actions, filterable by category | engine audit (hash-chained) plus the coordinator's transition log |

"Actual RPO" is shown only when it has been measured (after a failback, from the quarantined records). Before that the screen says it is not known and why; the expected bound at decision time is shown separately and labelled as a bound.

## 3. Real-time updates

The API collects every 2 s and the browser polls `GET /api/v1/snapshot` every 2 s. Polling was chosen over streaming because the data is a small snapshot, every poll is independently checkable, and a stalled stream is harder to distinguish from a quiet system than a missed poll. Each response carries per-source age, `ok`, `error` and `stale`.

States the UI shows: **loading** (before the first snapshot), **error** (backend not ready or unreachable, with retry), **empty** (every table has an explanation instead of a blank), **stale** (see below), and for permissions a **read-only** note.

**Stale-data indicators.** The whole screen is marked stale (banner, dimmed sections with `data-stale="true"`, "STALE ·" in each section header) when the last *successful* fetch is older than 10 s. A single source that the backend itself reports as stale (no update for more than max(10 s, 4 × interval)) marks only the sections that depend on it. After a failed fetch the previous snapshot stays on screen, marked stale, never silently presented as current. If the coordinator is unreachable the DR section says so.

## 4. Administrative operations and permissions

| Role | Can |
|---|---|
| none / unknown token | nothing (401) |
| `viewer` | read everything (no admin controls rendered, API answers 403 to any admin call) |
| `operator` | read, and request failover, start/approve/abort failback |

* Every admin request needs an explicit **confirmation** in the UI, is validated by the API, forwarded to the DR controller with the controller's admin key (held only by the API), and recorded in the policy engine's audit with the requester (`DASH_*` operations).
* The browser shows **the DR controller's own answer** verbatim (for example `REFUSED (ACTIVE_REGION_NOT_FAILED)`) and nothing else. The state sections change only when the coordinator reports a new state. A "PROMOTED" answer does not change the screen by itself.
* "Request failover" is accepted by the controller only after it has declared the active region failed. **A planned switchover of a healthy region is not implemented.** Degraded-RPO approval is possible only where the deployment allows it (`SS_DR_ALLOW_DEGRADED_RPO=1`); the UI disables the option otherwise.

## 5. Sensitive data

* **Whitelist first:** every view is built field by field from the sources; unknown fields are not copied.
* **Scrub second:** the API removes forbidden keys anywhere in a response (payload, value, records, partition key, api key, token, password, secret, authorization, envelope…) and redacts any string containing a known credential (service key, admin key, dashboard tokens).
* **Never fetched:** the dashboard has no access to Kafka topics, so payloads are not even in its memory. Audit entries carry codes and region names only.
* **UI third:** components render named fields only; an unexpected `payload` field from the backend is not displayed (tested).
* The controller's admin key never reaches the browser; the browser holds only its own bearer token (in `sessionStorage`).

## 6. Prometheus metrics

Exported by the dashboard API at `/metrics` (derived from the authoritative sources), plus the gateway's own ack-latency histogram which the API passes through.

| Metric | Labels |
|---|---|
| `ss_producer_ack_latency_seconds` (histogram), `ss_gateway_writes_total`, `ss_gateway_lease_valid` | region (gateway-side) |
| `ss_replication_lag_seconds` (estimated drain time, max over topics), `ss_replication_backlog_records`, `ss_replication_backlog_bytes`, `ss_replication_throughput_bytes_per_second` | source, target (aggregated over topics) |
| `ss_active_region`, `ss_leadership_epoch`, `ss_dr_state` (one-hot) | region / state |
| `ss_policy_rejections_total`, `ss_policy_decisions_total` | point, operation, decision |
| `ss_replica_placements` (REVOKED = blocked by policy) | status |
| `ss_path_health`, `ss_path_rtt_seconds`, `ss_route_switches_total` | path / source, target |
| `ss_failover_duration_seconds`, `ss_rpo_expected_bound_seconds`, `ss_rpo_gap_records` | from, to |
| `ss_region_health` (2 UP, 1 DEGRADED, 0 DOWN), `ss_region_brokers_up`, `ss_worker_up`, `ss_dashboard_source_age_seconds` | region, worker |

**Label policy (enforced in code, tested):** only the labels above; values must match `[A-Za-z0-9_.:+-]{1,48}` and must not look like a UUID or a long hex id. A metric that violates it raises instead of being exported. No topic names, event ids, partition keys or customer identifiers are ever labels; replication metrics are aggregated per region pair for that reason. Per-path metrics are bounded (18 paths in this deployment).

## 7. Tests

| Level | Where | Count / result |
|---|---|---|
| UI component tests (vitest, React Testing Library) | `dashboard/src/__tests__` | loading, error, empty, stale (fetch age and backend-flagged), accuracy of brokers/active region/epoch/lag/backlog/routes, a route change appearing only after the next backend poll, viewer vs operator controls, confirmation, refusal never shown as success, 403 handling, sensitive fields never rendered, sign-in |
| Backend unit tests | `policy/tests/test_obs.py` | view accuracy, staleness, scrub, metric label policy, role enforcement, verbatim controller answer |
| Live integration tests | `policy/tests/test_obs_integration.py` | broker loss and recovery shown, active region/epoch equal the coordinator, route change shown only after the replicator confirms it, lag and backlog equal the engine and the metrics, stale source flagged then cleared, unauthorised failover refused with the state unchanged, payload marker and every credential absent, label policy on live metrics |
| Browser automation (Playwright, Chromium) | `dashboard/e2e/dashboard.spec.ts` | sign-in rejection, all six sections against real state, viewer read-only and 403 on a direct API attempt, operator confirm-and-refusal flow with audit entry, stale banner when the network is cut and cleared on resume, no payload or credential in the page |
| Browser workflow with a real failover | `dashboard/e2e/failover.spec.ts` (`run_tests.sh e2e-destructive`) | region A is stopped, the operator requests failover in the browser, the screen reaches `HEALTHY_B`, epoch 2, only after the coordinator reports it; the restored A is shown fenced and not active |

`dashboard/run_tests.sh unit | live | e2e-destructive`.

## 8. Limits (what this is not)

* **Authentication is minimal:** static bearer tokens from the environment. No OIDC, MFA, per-user accounts, token rotation or session expiry. The token sits in `sessionStorage`, so an XSS bug would expose it. No TLS in this dev stack.
* **Permission checks are real but coarse** (two roles). There is no approval workflow (four-eyes) for admin operations beyond the controller's own rules.
* **Single instance, not part of DR:** if the dashboard is down you lose the view, not the system. If the coordinator is down the dashboard shows the last state, flagged.
* **Metrics are derived** by the dashboard API from other components' reports, not exported natively by each component (except the gateway histogram). Counters that come from a replicator (`ss_route_switches_total`) restart from zero when that replicator restarts. The replication "lag" is the replicator's estimate of the time to drain its backlog, not an end-to-end timestamp lag.
* Producer acknowledgement latency is measured at the gateway around the `acks=all` send; it excludes the client-to-gateway hop.
* Limits of volume: up to 200 policies and 100 topics' checkpoints per snapshot; the audit view shows the latest 150 entries.
* Tested only in headless Chromium; no accessibility audit; not load-tested.
* The Prometheus scrape was checked (target up, `ss_active_region` queryable); no alert rules or Grafana dashboards are provided.
