#!/usr/bin/env bash
# Phase 7 tests. Levels:
#   run_tests.sh unit    UI component tests (vitest) + type check + production build + backend unit tests. Needs nothing running.
#   run_tests.sh live    integration (pytest) and browser (Playwright) tests against a running DR stack: scripts/up.sh --dr
#   run_tests.sh e2e-destructive   the browser workflow that kills region A (restores it afterwards). Needs a healthy, fresh DR stack.
set -euo pipefail
cd "$(dirname "$0")"
case "${1:-unit}" in
  unit)
    npx tsc --noEmit && npx vitest run && npm run build >/dev/null
    (cd ../policy/tests && ./run.sh test_obs.py -q) ;;
  live)
    (cd ../policy/tests && ./run.sh test_obs_integration.py -q)
    npx playwright test e2e/dashboard.spec.ts ;;
  e2e-destructive)
    SS_E2E_DESTRUCTIVE=1 npx playwright test e2e/failover.spec.ts ;;
  *) echo "usage: run_tests.sh unit|live|e2e-destructive"; exit 2 ;;
esac
