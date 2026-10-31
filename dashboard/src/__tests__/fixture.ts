import type { Snapshot } from "../types";

export function snapshot(over: Partial<Snapshot> = {}): Snapshot {
  const base: Snapshot = {
    generated_at: "2026-01-01T00:00:00+00:00",
    generated_epoch: 1,
    interval_s: 2,
    sources: Object.fromEntries(
      ["coordinator", "dr", "engine", "replicators", "gateways"].map((k) => [k, { ok: true, age_s: 0.5, error: null, stale: false }]),
    ),
    topology: {
      active_region: "region-a",
      phase: "HEALTHY_A",
      leadership_epoch: 4,
      regions: [
        {
          region: "region-a",
          role: "active",
          declared_health: "DEGRADED",
          brokers_up: 2,
          brokers_expected: 3,
          controller_quorum: true,
          kafka_reachable: true,
          gateway_healthy: true,
          agent_healthy: true,
          replicator_up: true,
          leadership_epoch: 4,
          fenced_at_epoch: null,
        },
        {
          region: "region-b",
          role: "standby",
          declared_health: "UP",
          brokers_up: 1,
          brokers_expected: 1,
          controller_quorum: true,
          kafka_reachable: true,
          gateway_healthy: true,
          agent_healthy: true,
          replicator_up: true,
          leadership_epoch: 4,
          fenced_at_epoch: null,
        },
        {
          region: "region-c",
          role: "fenced",
          declared_health: "DOWN",
          brokers_up: 0,
          brokers_expected: 1,
          controller_quorum: false,
          kafka_reachable: false,
          gateway_healthy: false,
          agent_healthy: false,
          replicator_up: false,
          leadership_epoch: 4,
          fenced_at_epoch: 3,
        },
      ],
      connections: [{ source_region: "region-a", target_region: "region-b", flows: 1, states: { ENABLED: 1 }, route: "a-b-direct" }],
    },
    policies: {
      policies: [
        {
          topic: "orders",
          classification: "RESTRICTED",
          legal_zone: "EU",
          home_region: "region-a",
          allowed_storage_regions: ["region-a", "region-b"],
          allowed_processing_regions: ["region-a", "region-b"],
          prohibited_regions: ["region-c"],
          policy_version: 3,
          active_region: "region-a",
          replicas: [{ region: "region-b", status: "IN_SYNC" }],
        },
      ],
      recent_blocked: [
        {
          id: 5,
          ts: "2026-01-01T00:00:03+00:00",
          source: "policy",
          category: "policy",
          operation: "PLACEMENT",
          decision: "REJECTED",
          topic: "pii",
          region: "region-c",
          requester: "svc",
          reasons: ["LEGAL_STORAGE"],
        },
      ],
    },
    routing: [
      {
        source_region: "region-a",
        target_region: "region-b",
        selected_path: "a-b-direct",
        route_changes: 3,
        last_route_change: "2026-01-01T00:00:00+00:00",
        reason: "SUSTAINED_IMPROVEMENT",
        paths: [
          {
            path_id: "a-b-direct",
            kind: "DIRECT",
            selected: true,
            rtt_ms: 7.4,
            loss: 0,
            bandwidth_bps: 4200000,
            score: 0.04,
            breaker: "CLOSED",
            excluded: null,
            last_sample_age_s: 0.4,
          },
          {
            path_id: "a-b-peering",
            kind: "PEERING",
            selected: false,
            rtt_ms: 120,
            loss: 0.4,
            bandwidth_bps: 100000,
            score: 0.31,
            breaker: "OPEN",
            excluded: null,
            last_sample_age_s: 0.4,
          },
        ],
      },
    ],
    replication: [
      {
        topic: "orders",
        flow_id: 1,
        source_region: "region-a",
        target_region: "region-b",
        source_cluster: "kafka-a",
        target_cluster: "kafka-b",
        status: "ENABLED",
        pipeline_states: ["RUNNING"],
        generation: 4,
        last_source_offset: 120,
        last_destination_offset: 120,
        last_ack_at: "2026-01-01T00:00:02+00:00",
        lag_records: 40,
        lag_seconds: 2.5,
        backlog_bytes: 8192,
        throughput_bps: 3200,
        backlog_state: "OK",
      },
    ],
    dr: {
      phase: "HEALTHY_A",
      coordination_reachable: true,
      active_region: "region-a",
      standby_regions: ["region-b"],
      leadership_epoch: 4,
      fenced: { "region-c": 3 },
      transition: null,
      last_failover: {
        from: "region-a",
        to: "region-b",
        epoch: 2,
        activated_at: "2026-01-01T00:00:00+00:00",
        degraded_rpo: false,
        approved_by: null,
        measured_rto_s: 10,
        fence_wait_s: 7.1,
        replication_reconfigured_s: 5,
        expected_rpo_bound_s: 3.4,
      },
      candidate_eligibility: { "region-b": { eligible: false, reasons: ["LAG_EXCEEDS_RPO"], topics_checked: 2, degraded_rpo: false, max_lag_ms: 9000 } },
      actual_rpo: null,
      failback: null,
      last_failback: null,
      controller: { instance: "dr-1", is_leader: true, auto: true, allow_degraded_rpo: false },
    },
    audit: [
      {
        id: "c8",
        ts: "2026-01-01T00:00:04+00:00",
        source: "coordinator",
        category: "failover",
        operation: "PROMOTED",
        decision: "ALLOWED",
        topic: null,
        region: "region-b",
        requester: "dr-1",
        reasons: [],
        detail: "PROMOTING_B -> HEALTHY_B (epoch 2)",
      },
      {
        id: 4,
        ts: "2026-01-01T00:00:02+00:00",
        source: "policy",
        category: "route",
        operation: "ROUTE_SWITCH",
        decision: "ALLOWED",
        topic: "orders",
        region: null,
        requester: "agent",
        reasons: [],
      },
    ],
  };
  return { ...base, ...over };
}

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

/** A fake backend: `snap()` is re-evaluated on every poll, so a test changes what the "backend" says by changing what snap returns. */
export function backend(opts: {
  role?: "viewer" | "operator";
  snap: () => Snapshot | Response;
  admin?: (path: string, body: unknown) => Response;
  calls?: string[];
}) {
  const calls = opts.calls ?? [];
  const f = (async (url: RequestInfo | URL, init?: RequestInit) => {
    const u = String(url);
    calls.push(`${init?.method ?? "GET"} ${u}`);
    if (u.endsWith("/api/v1/me")) return jsonResponse({ user: "tester", role: opts.role ?? "viewer", can_administer: (opts.role ?? "viewer") === "operator" });
    if (u.endsWith("/api/v1/snapshot")) {
      const s = opts.snap();
      return s instanceof Response ? s : jsonResponse(s);
    }
    if (u.includes("/api/v1/admin/")) return opts.admin ? opts.admin(u, init?.body) : jsonResponse({ detail: "no" }, 403);
    return jsonResponse({ detail: "not found" }, 404);
  }) as typeof fetch;
  return { f, calls };
}
