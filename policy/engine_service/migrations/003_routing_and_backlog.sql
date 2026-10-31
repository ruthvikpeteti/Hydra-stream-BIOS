-- ===== Phase 5: network paths, backlog and backpressure =====================================================
CREATE TABLE IF NOT EXISTS network_paths (
    path_id        text PRIMARY KEY,
    source_region  text NOT NULL REFERENCES regions(region_id),
    target_region  text NOT NULL REFERENCES regions(region_id),
    kind           text NOT NULL CHECK (kind IN ('DIRECT', 'PEERING', 'RELAY')),
    via_regions    text[] NOT NULL DEFAULT '{}',
    transit_zones  text[] NOT NULL DEFAULT '{}',
    endpoint_url   text NOT NULL,
    encrypted      boolean NOT NULL DEFAULT false,
    cost           double precision NOT NULL DEFAULT 1 CHECK (cost >= 0),
    fallback       boolean NOT NULL DEFAULT false,
    enabled        boolean NOT NULL DEFAULT true,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CHECK (source_region <> target_region)
);
ALTER TABLE replication_flows ADD COLUMN IF NOT EXISTS backlog_soft_bytes bigint;
ALTER TABLE replication_flows ADD COLUMN IF NOT EXISTS backlog_hard_bytes bigint;
CREATE TABLE IF NOT EXISTS flow_backlog (
    flow_id         bigint PRIMARY KEY REFERENCES replication_flows(flow_id),
    topic           text NOT NULL,
    backlog_records bigint NOT NULL,
    backlog_bytes   bigint NOT NULL,
    drain_seconds   double precision,
    throughput_bps  double precision NOT NULL DEFAULT 0,
    state           text NOT NULL CHECK (state IN ('OK', 'SOFT', 'HARD')),
    route           text,
    updated_at      timestamptz NOT NULL DEFAULT now()
);
