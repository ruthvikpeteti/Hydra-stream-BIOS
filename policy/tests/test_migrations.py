"""The numbered migrations must add up to the schema the engine applies, and the checkpoint migration to the one the agents apply."""

from pathlib import Path

import re

D = Path(__file__).resolve().parents[1] / "engine_service"


def test_migrations_add_up_to_schema_sql():
    parts = "".join((D / "migrations" / n).read_text() for n in ("001_policy_core.sql", "002_replication_topology.sql", "003_routing_and_backlog.sql"))
    assert parts == (D / "schema.sql").read_text()


def test_checkpoint_migration_is_the_agents_schema():
    src = (D.parent / "ss_replication" / "checkpoints.py").read_text()
    schema = re.search(r'SCHEMA_SQL = """(.*?)"""', src, re.S).group(1).strip()
    assert schema in (D / "migrations" / "004_replication_checkpoints.sql").read_text()


def test_migrations_are_idempotent_by_construction():
    for f in sorted((D / "migrations").glob("0*.sql")):
        for stmt in re.findall(r"(?im)^\s*(CREATE\s+(?:TABLE|INDEX|UNIQUE INDEX)\b[^;]*?)(?:\(|ON )", f.read_text()):
            assert "IF NOT EXISTS" in stmt.upper(), (f.name, stmt)
