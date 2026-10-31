#!/usr/bin/env bash
# Phase 6 disaster-recovery scenarios, each on a FRESH stack (they kill whole regions and leave the system in a post-incident state).
# DESTRUCTIVE: deletes all Kafka/PostgreSQL/etcd data of this compose project. Takes roughly 20-30 minutes.
#   dr_run.sh   all scenarios     dr_run.sh 1  A->B->C chain     dr_run.sh 2  split brain + degraded RPO + failback     dr_run.sh 3  stale tertiary (scenario 8)
set -euo pipefail
cd "$(dirname "$0")"
export SS_DR_TESTS=1
INFRA=../../infrastructure/scripts
which=${1:-all}
./run.sh test_dr_core.py -q
if [[ $which == all || $which == 1 ]]; then
  $INFRA/down.sh --volumes >/dev/null; $INFRA/up.sh --dr >/dev/null
  rm -f .dr-session.json dr-results.json
  ./run.sh test_dr.py -v --tb=short
fi
if [[ $which == all || $which == 2 ]]; then
  $INFRA/down.sh --volumes >/dev/null; SS_DR_ALLOW_DEGRADED_RPO=1 $INFRA/up.sh --dr >/dev/null
  rm -f .dr-failback-session.json dr-failback-results.json
  ./run.sh test_dr_failback.py -v --tb=short
fi
if [[ $which == all || $which == 3 ]]; then
  $INFRA/down.sh --volumes >/dev/null; $INFRA/up.sh --dr >/dev/null
  rm -f dr-stale-c-results.json
  ./run.sh test_dr_stale_c.py -v --tb=short
fi
