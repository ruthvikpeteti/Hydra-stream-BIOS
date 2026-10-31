#!/usr/bin/env bash
# Executable end-to-end replication test (Phase 4). Requires the stack (scripts/up.sh). Runs, in order, against live
# services: A->B and A->C of 1,000 events, ordering and ids, agent restart, destination kill and catch-up, response loss
# and duplicate detection, checkpoint-after-ack, PostgreSQL restart, independent progress, unauthorized topic and
# generation, promotion of B with cutover validation, and loop checks. About 8 minutes.
set -euo pipefail
exec "$(dirname "$0")/../../policy/tests/run.sh" test_replication.py "$@"
