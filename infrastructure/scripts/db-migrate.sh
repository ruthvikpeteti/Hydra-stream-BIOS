#!/usr/bin/env bash
# Applies policy/engine_service/migrations/*.sql in order to the running PostgreSQL container and records them in schema_migrations.
set -euo pipefail
DIR="$(cd "$(dirname "$0")/../../policy/engine_service/migrations" && pwd)"
psqlc() { docker exec -i ss-postgres psql -v ON_ERROR_STOP=1 -U sspolicy -d sspolicy "$@"; }
psqlc -q -c "CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
for f in "$DIR"/[0-9]*.sql; do
  n=$(basename "$f")
  if [[ $(psqlc -tA -c "SELECT 1 FROM schema_migrations WHERE name='$n'") == 1 ]]; then echo "skip   $n (recorded)"; continue; fi
  psqlc -q < "$f" && psqlc -q -c "INSERT INTO schema_migrations(name) VALUES ('$n')" && echo "applied $n"
done
