#!/usr/bin/env bash
# The eleven end-to-end scenarios of Phase 8 on fresh stacks. Each scenario maps to the automated tests that check it (see SCENARIOS.md).
# DESTRUCTIVE (deletes all project data between groups). Results: ci/results/scenarios-*.xml
set -euo pipefail
cd "$(dirname "$0")/.."
RES=$PWD/ci/results; mkdir -p "$RES"
export SS_DR_TESTS=1
up() {
  infrastructure/scripts/down.sh --volumes >/dev/null 2>&1 || true
  local flag=""; [[ $1 == dr ]] && flag="--dr"
  export SS_DR_ALLOW_DEGRADED_RPO=${2:-0}
  infrastructure/scripts/up.sh $flag >/dev/null || { echo "stack start failed once; retrying"; sleep 20; infrastructure/scripts/up.sh $flag >/dev/null; }
}
pol() { (cd policy/tests && ./run.sh "$@"); }
# normal stack: scenarios 1, 2 (new), 3, 4 (routing: network degradation, all paths down)
up normal
pol test_scenarios.py -q --junitxml="$RES/scenarios-1-2.xml"
pol test_routing_integration.py -q -k "path_b or all_paths" --junitxml="$RES/scenarios-3-4.xml"
# DR stack: scenarios 5, 10 (broker failure, coordinator failure) and 6, 7 (primary and secondary regional failure), then 9 and 11, then 8
up dr
pol test_dr.py -q --junitxml="$RES/scenarios-5-6-7-10.xml"
up dr 1
pol test_dr_failback.py -q --junitxml="$RES/scenarios-9-11.xml"
up dr
pol test_dr_stale_c.py -q --junitxml="$RES/scenarios-8.xml"
