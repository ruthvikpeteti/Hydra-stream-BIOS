-- SovereignStream policy engine schema. Idempotent: safe to run at every start.

CREATE TABLE IF NOT EXISTS regions (
    region_id               text PRIMARY KEY,
    legal_zone              text NOT NULL,
    availability_status     text NOT NULL CHECK (availability_status IN ('AVAILABLE','DEGRADED','UNAVAILABLE','DRAINING')),
    storage_total_bytes     bigint NOT NULL CHECK (storage_total_bytes >= 0),
    storage_used_bytes      bigint NOT NULL DEFAULT 0 CHECK (storage_used_bytes >= 0),
    processing_capabilities text[] NOT NULL DEFAULT '{}',
    supported_transit_zones text[] NOT NULL DEFAULT '{}',
    failure_domain          text NOT NULL,
    compute_headroom        double precision NOT NULL DEFAULT 0.5 CHECK (compute_headroom BETWEEN 0 AND 1),
    max_replication_factor  int NOT NULL DEFAULT 3 CHECK (max_replication_factor >= 1),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS links (
    from_region   text NOT NULL REFERENCES regions(region_id),
    to_region     text NOT NULL REFERENCES regions(region_id),
    transit_zones text[] NOT NULL DEFAULT '{}',
    latency_ms    int NOT NULL CHECK (latency_ms >= 0),
    status        text NOT NULL DEFAULT 'UP' CHECK (status IN ('UP','DOWN')),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (from_region, to_region),
    CHECK (from_region <> to_region)
);

CREATE TABLE IF NOT EXISTS topic_policies (
    topic                      text PRIMARY KEY,
    classification             text NOT NULL CHECK (classification IN ('PUBLIC','INTERNAL','CONFIDENTIAL','RESTRICTED')),
    legal_zone                 text NOT NULL,
    home_region                text NOT NULL,
    allowed_storage_regions    text[] NOT NULL,
    allowed_processing_regions text[] NOT NULL,
    allowed_transit_zones      text[] NOT NULL DEFAULT '{}',
    replication_factor         int NOT NULL CHECK (replication_factor >= 1),
    regional_replica_count     int NOT NULL CHECK (regional_replica_count >= 0),
    preferred_regions          text[] NOT NULL DEFAULT '{}',
    excluded_regions           text[] NOT NULL DEFAULT '{}',
    maximum_replication_lag_ms bigint CHECK (maximum_replication_lag_ms IS NULL OR maximum_replication_lag_ms >= 0),
    durability_mode            text NOT NULL CHECK (durability_mode IN ('LOCAL_DURABLE','ASYNC_REGIONAL','SYNC_REGIONAL')),
    policy_version             int NOT NULL CHECK (policy_version >= 1),
    created_at                 timestamptz NOT NULL DEFAULT now(),
    updated_at                 timestamptz NOT NULL DEFAULT now(),
    updated_by                 text NOT NULL
);

-- Every version ever in force, so a decision can always be traced to the exact policy text.
CREATE TABLE IF NOT EXISTS topic_policy_versions (
    topic          text NOT NULL,
    policy_version int  NOT NULL,
    body           jsonb NOT NULL,
    actor          text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, policy_version)
);

CREATE TABLE IF NOT EXISTS replica_assignments (
    topic              text NOT NULL REFERENCES topic_policies(topic),
    partition          int  NOT NULL CHECK (partition >= 0),
    source_region      text NOT NULL,
    destination_region text NOT NULL,
    replica_status     text NOT NULL CHECK (replica_status IN ('PENDING','PROVISIONED','SYNCING','IN_SYNC','FAILED','REVOKED')),
    policy_version     int  NOT NULL,
    assigned_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition, destination_region),
    CHECK (source_region <> destination_region)
);

-- Last line of defence: even a buggy or bypassed application cannot record a live assignment to a region
-- that the topic's CURRENT policy does not allow. REVOKED rows are exempt (revoking must always work).
CREATE OR REPLACE FUNCTION ss_guard_assignment() RETURNS trigger AS $$
DECLARE pol topic_policies%ROWTYPE; reg regions%ROWTYPE;
BEGIN
    IF NEW.replica_status = 'REVOKED' THEN RETURN NEW; END IF;
    SELECT * INTO pol FROM topic_policies WHERE topic = NEW.topic;
    IF NOT FOUND THEN RAISE EXCEPTION 'ss_policy: no policy for topic %', NEW.topic USING ERRCODE = 'check_violation'; END IF;
    SELECT * INTO reg FROM regions WHERE region_id = NEW.destination_region;
    IF NOT FOUND THEN RAISE EXCEPTION 'ss_policy: unknown region %', NEW.destination_region USING ERRCODE = 'check_violation'; END IF;
    IF NOT (NEW.destination_region = ANY (pol.allowed_storage_regions))
       OR NEW.destination_region = ANY (pol.excluded_regions)
       OR (pol.legal_zone <> 'GLOBAL' AND reg.legal_zone <> pol.legal_zone) THEN
        RAISE EXCEPTION 'ss_policy: region % may not store topic % under policy v%', NEW.destination_region, NEW.topic, pol.policy_version
            USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.policy_version <> pol.policy_version THEN
        RAISE EXCEPTION 'ss_policy: assignment carries policy v% but topic % is at v%', NEW.policy_version, NEW.topic, pol.policy_version
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS ss_guard_assignment_t ON replica_assignments;
CREATE TRIGGER ss_guard_assignment_t BEFORE INSERT OR UPDATE ON replica_assignments
    FOR EACH ROW EXECUTE FUNCTION ss_guard_assignment();

-- Monotonic version of the whole registry+policy state; bumped on every change; agents never accept a lower one.
CREATE TABLE IF NOT EXISTS registry_meta (id int PRIMARY KEY CHECK (id = 1), bundle_version bigint NOT NULL);
INSERT INTO registry_meta (id, bundle_version) VALUES (1, 1) ON CONFLICT DO NOTHING;

-- Audit trail of policy decisions. Metadata only: no message payloads, ever. Append-only and hash-chained.
CREATE TABLE IF NOT EXISTS policy_decisions (
    id                bigserial PRIMARY KEY,
    ts                timestamptz NOT NULL DEFAULT now(),
    source            text NOT NULL,              -- 'engine' or 'agent:<region>'
    operation         text NOT NULL,
    enforcement_point text NOT NULL,
    topic             text,
    policy_version    int,
    subject           jsonb NOT NULL DEFAULT '{}',
    decision          text NOT NULL CHECK (decision IN ('ALLOWED','REJECTED','INSUFFICIENT_ELIGIBLE_REGIONS','POLICY_UNAVAILABLE')),
    reasons           jsonb NOT NULL DEFAULT '[]',
    requester         text,
    request_id        text,
    prev_hash         text NOT NULL,
    hash              text NOT NULL
);
CREATE INDEX IF NOT EXISTS policy_decisions_topic_idx ON policy_decisions (topic, id);

CREATE OR REPLACE FUNCTION ss_audit_immutable() RETURNS trigger AS $$
BEGIN RAISE EXCEPTION 'ss_policy: policy_decisions is append-only'; END $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS ss_audit_no_update ON policy_decisions;
CREATE TRIGGER ss_audit_no_update BEFORE UPDATE OR DELETE ON policy_decisions
    FOR EACH ROW EXECUTE FUNCTION ss_audit_immutable();
DROP TRIGGER IF EXISTS ss_audit_no_truncate ON policy_decisions;
CREATE TRIGGER ss_audit_no_truncate BEFORE TRUNCATE ON policy_decisions
    FOR EACH STATEMENT EXECUTE FUNCTION ss_audit_immutable();

