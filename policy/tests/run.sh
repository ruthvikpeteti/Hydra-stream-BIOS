#!/usr/bin/env bash
# Policy engine tests. Pure + property tests need nothing running; test_integration.py needs the stack up
# (infrastructure/scripts/up.sh). Usage: policy/tests/run.sh [pytest args]   e.g.  run.sh -k "not integration"
set -euo pipefail
cd "$(dirname "$0")"
[[ -d .venv ]] || { python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }
exec .venv/bin/python -m pytest -v -p no:cacheprovider "$@"
