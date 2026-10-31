-- Replication checkpoints (applied by every replication agent at start-up; identical text)
CREATE TABLE IF NOT EXISTS replication_checkpoints (
    source_cluster                  text   NOT NULL,
    target_cluster                  text   NOT NULL,
    source_topic                    text   NOT NULL,
    source_partition                int    NOT NULL CHECK (source_partition >= 0),
    source_region                   text   NOT NULL,
    target_region                   text   NOT NULL,
    last_acknowledged_source_offset bigint NOT NULL,   -- -1: nothing acknowledged yet; the next offset to read is this + 1
    last_known_destination_offset   bigint NOT NULL,   -- destination offset of the last acknowledged record (-1: unknown)
    replication_generation          int    NOT NULL,
    policy_version                  int    NOT NULL,
    last_success_timestamp          timestamptz,
    updated_at                      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_cluster, target_cluster, source_topic, source_partition)
);
CREATE OR REPLACE FUNCTION ss_guard_checkpoint() RETURNS trigger AS $$
BEGIN
    IF NEW.replication_generation < OLD.replication_generation
       OR (NEW.replication_generation = OLD.replication_generation
           AND NEW.last_acknowledged_source_offset < OLD.last_acknowledged_source_offset) THEN
        RAISE EXCEPTION 'ss_replication: checkpoint may not move backwards (% -> %, generation % -> %)',
            OLD.last_acknowledged_source_offset, NEW.last_acknowledged_source_offset,
            OLD.replication_generation, NEW.replication_generation USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS ss_guard_checkpoint_t ON replication_checkpoints;
CREATE TRIGGER ss_guard_checkpoint_t BEFORE UPDATE ON replication_checkpoints
    FOR EACH ROW EXECUTE FUNCTION ss_guard_checkpoint();
