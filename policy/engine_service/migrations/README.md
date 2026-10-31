# Database migrations

The policy engine applies `../schema.sql` at every start. It is idempotent (`CREATE ... IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `CREATE OR REPLACE`), so
there is no destructive migration. The files here are the same statements split by the phase that introduced them, so that a database can be
brought up (or audited) step by step without starting the engine:

| File | Introduced | Content |
|---|---|---|
| 001_policy_core.sql | Phase 3 | regions, links, topic policies and their versions, replica assignments (+ guard trigger), registry version, hash-chained audit log (+ immutability triggers) |
| 002_replication_topology.sql | Phase 4 | topic topology, replication flows (+ topology guard) |
| 003_routing_and_backlog.sql | Phase 5 | network paths, backlog limits, flow backlog |
| 004_replication_checkpoints.sql | Phase 4 | checkpoints (+ no-regression trigger); applied by the replication agents |

`infrastructure/scripts/db-migrate.sh` applies them in order inside the PostgreSQL container and records them in `schema_migrations`.
`policy/tests/test_migrations.py` fails if the files ever stop adding up to `schema.sql`, so the two cannot drift.
There are no down-migrations: the audit log is append-only by design, and data is never rewritten by a migration.
