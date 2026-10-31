#!/usr/bin/env bash
# One entry point per CI stage, used identically by a developer and by the GitHub Actions workflow.
#   ci/run_stage.sh <stage> [--no-stack]        stages: lint typecheck unit db kafka replication policy failover chaos frontend scenarios soak
# Stages that need infrastructure start a FRESH stack first (deleting all project data) unless --no-stack is given.
# JUnit XML of every pytest run goes to ci/results/<stage>.xml.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD; RES=$ROOT/ci/results; mkdir -p "$RES"
PY="$ROOT/ci/.venv/bin"
stage=${1:?stage}; nostack=${2:-}
need_tools() { [[ -x "$PY/ruff" ]] || { python3 -m venv ci/.venv && ci/.venv/bin/pip install -q -r ci/requirements-dev.txt; }; }
fresh() {  # fresh normal|dr
  [[ "$nostack" == --no-stack ]] && return 0
  infrastructure/scripts/down.sh --volumes >/dev/null 2>&1 || true
  local flag=""; [[ $1 == dr ]] && flag="--dr"
  infrastructure/scripts/up.sh $flag >/dev/null || { echo "stack start failed once; retrying (up.sh is idempotent)"; sleep 20; infrastructure/scripts/up.sh $flag >/dev/null; }
}
pol() { (cd policy/tests && ./run.sh "$@"); }
gw()  { (cd gateway/tests && ./run.sh "$@"); }
case $stage in
  lint)       need_tools; "$PY/ruff" check .; "$PY/ruff" format --check .; (cd dashboard && npm ci --no-audit --no-fund >/dev/null && npx prettier --check src e2e) ;;
  typecheck)  need_tools; (cd policy && "$PY/mypy" ss_policy ss_replication ss_dr ss_obs engine_service agent_service); (cd gateway && "$PY/mypy" app); (cd dashboard && npm ci --no-audit --no-fund >/dev/null && npx tsc --noEmit) ;;
  unit)       pol test_policy_engine.py test_agent_core.py test_properties.py test_replication_core.py test_routing.py test_dr_core.py test_obs.py test_migrations.py -q --junitxml="$RES/unit-policy.xml"
              gw test_unit.py test_examples.py -q --junitxml="$RES/unit-gateway.xml"; (cd dashboard && npm ci --no-audit --no-fund >/dev/null && npx vitest run --reporter=junit --outputFile="$RES/unit-frontend.xml") ;;
  db)         fresh normal
              pol test_integration.py -q -k "crud_and_versioning or db_guard or audit_log_is_append_only or every_decision_is_audited or store_outage or agents_reject_extra_fields" --junitxml="$RES/db-policy.xml"
              pol test_replication.py -q -k "test_08 or test_09" --junitxml="$RES/db-checkpoints.xml" ;;
  kafka)      fresh normal; (cd infrastructure/tests && bash run_all.sh 2>&1 | grep -E "PASS|FAIL" | tee "$RES/kafka-infrastructure.txt"; ! grep -q FAIL "$RES/kafka-infrastructure.txt")
              gw test_gateway.py -q --junitxml="$RES/kafka-gateway.xml" ;;
  replication) fresh normal; pol test_replication.py -q --junitxml="$RES/replication.xml" ;;
  policy)     fresh normal; pol test_integration.py test_scenarios.py -q --junitxml="$RES/policy-enforcement.xml" ;;
  failover)   SS_DR_TESTS=1 policy/tests/dr_run.sh ;;
  chaos)      fresh dr; pol test_routing_integration.py -q --junitxml="$RES/chaos-routing.xml"; "$ROOT/policy/tests/.venv/bin/python" soak/soak.py --minutes "${CHAOS_SOAK_MINUTES:-8}" --rate 15 --interval 60 ;;
  scenarios)  SS_DR_TESTS=1 ci/run_scenarios.sh ;;
  frontend)   fresh dr; dashboard/run_tests.sh unit; dashboard/run_tests.sh live; dashboard/run_tests.sh e2e-destructive ;;
  soak)       fresh dr; "$ROOT/policy/tests/.venv/bin/python" soak/soak.py --minutes "${SOAK_MINUTES:-60}" --rate 20 --final-failover ;;
  *) echo "unknown stage $stage"; exit 2 ;;
esac
