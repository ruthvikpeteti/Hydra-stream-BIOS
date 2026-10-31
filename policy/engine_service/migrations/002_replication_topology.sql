-- ===== Phase 4: replication topology =======================================================================
ALTER TABLE regions ADD COLUMN IF NOT EXISTS cluster_id text;
UPDATE regions SET cluster_id = 'kafka-' || right(region_id, 1)
 WHERE cluster_id IS NULL AND region_id IN ('region-a', 'region-b', 'region-c');

-- Which region is active for a topic, and under which failover epoch / replication generation.
CREATE TABLE IF NOT EXISTS topic_topology (
    topic                  text PRIMARY KEY REFERENCES topic_policies(topic),
    active_region          text NOT NULL,
    leadership_epoch       int  NOT NULL CHECK (leadership_epoch >= 1),
    replication_generation int  NOT NULL CHECK (replication_generation >= 1),
    previous_active_region text,
    updated_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS replication_flows (
    flow_id              bigserial PRIMARY KEY,
    topic                text NOT NULL REFERENCES topic_policies(topic),
    source_region        text NOT NULL,
    target_region        text NOT NULL,
    generation           int  NOT NULL,
    state                text NOT NULL CHECK (state IN ('ENABLED', 'PENDING_CUTOVER', 'DISABLED')),
    paused               boolean NOT NULL DEFAULT false,
    fence_bundle_version bigint,
    created_at           timestamptz NOT NULL DEFAULT now(),
    disabled_at          timestamptz,
    disabled_reason      text,
    cutover_report       jsonb,
    CHECK (source_region <> target_region)
);
-- Exactly one live source per (topic, target): no fan-in, which together with the guard below rules out loops.
CREATE UNIQUE INDEX IF NOT EXISTS replication_flows_one_source_per_target
    ON replication_flows (topic, target_region) WHERE state IN ('ENABLED', 'PENDING_CUTOVER');

CREATE OR REPLACE FUNCTION ss_guard_flow() RETURNS trigger AS $$
DECLARE act text;
BEGIN
    IF NEW.state = 'DISABLED' THEN RETURN NEW; END IF;
    SELECT active_region INTO act FROM topic_topology WHERE topic = NEW.topic;
    IF NOT FOUND THEN RAISE EXCEPTION 'ss_replication: topic % has no topology', NEW.topic USING ERRCODE = 'check_violation'; END IF;
    IF NEW.target_region = act THEN
        RAISE EXCEPTION 'ss_replication: a flow may not replicate into the active region (%)', act USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS ss_guard_flow_t ON replication_flows;
CREATE TRIGGER ss_guard_flow_t BEFORE INSERT OR UPDATE ON replication_flows
    FOR EACH ROW EXECUTE FUNCTION ss_guard_flow();

