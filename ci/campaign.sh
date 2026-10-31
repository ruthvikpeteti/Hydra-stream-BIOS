#!/usr/bin/env bash
# Runs the whole verification campaign in a fixed order and records the outcome of every stage in ci/results/campaign.tsv
# (stage, exit code, start, seconds). Long (about 3 hours) and DESTRUCTIVE (deletes the project's data between stacks).
#   ci/campaign.sh [stage ...]        default: everything
set -uo pipefail
cd "$(dirname "$0")/.."
RES=ci/results; mkdir -p $RES
export SOAK_MINUTES=${SOAK_MINUTES:-30}
P=policy/tests/.venv/bin/python
run() { # run <name> <command...>
  local name=$1; shift; local t0=$(date +%s); echo "=== $name $(date '+%F %T')"
  grep -v "^$name	" $RES/campaign.tsv > $RES/campaign.tsv.tmp 2>/dev/null; mv $RES/campaign.tsv.tmp $RES/campaign.tsv
  "$@" > "$RES/$name.log" 2>&1; local rc=$?
  printf '%s\t%s\t%s\t%s\n' "$name" "$rc" "$(date -r $t0 '+%F %T')" "$(( $(date +%s) - t0 ))" >> $RES/campaign.tsv
  echo "    exit $rc ($(( $(date +%s) - t0 )) s)"
}
sel=("$@")
has() { [[ ${#sel[@]} -eq 0 ]] && return 0; for s in "${sel[@]}"; do [[ $s == "$1" ]] && return 0; done; return 1; }
[[ ${#sel[@]} -eq 0 ]] && : > $RES/campaign.tsv
has lint        && run lint        ci/run_stage.sh lint
has typecheck   && run typecheck   ci/run_stage.sh typecheck
has unit        && run unit        ci/run_stage.sh unit
has db          && run db          ci/run_stage.sh db
has kafka       && run kafka       ci/run_stage.sh kafka --no-stack
has replication && run replication ci/run_stage.sh replication
has policy      && run policy      ci/run_stage.sh policy --no-stack
if has perf; then
  infrastructure/scripts/down.sh --volumes >/dev/null 2>&1; infrastructure/scripts/up.sh --dr >/dev/null 2>&1 || { sleep 20; infrastructure/scripts/up.sh --dr >/dev/null 2>&1; }
  run perf-env        $P perf/perf.py env
  run perf-throughput $P perf/perf.py throughput --runs 3 --duration 30 --warmup 5
  run perf-network    $P perf/perf.py network --rounds 5
  run perf-failover   $P perf/perf.py failover --rounds 3
fi
has chaos       && run chaos       ci/run_stage.sh chaos
has soak        && run soak        ci/run_stage.sh soak
has scenarios   && run scenarios   ci/run_scenarios.sh
has frontend    && run frontend    ci/run_stage.sh frontend
echo "campaign finished $(date '+%F %T')"
