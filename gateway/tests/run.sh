#!/usr/bin/env bash
# Integration + unit tests for the gateway. Requires: infrastructure/scripts/up.sh already running.
# Usage: tests/run.sh [pytest args]     (creates tests/.venv on first run)
set -euo pipefail
cd "$(dirname "$0")"
[[ -d .venv ]] || { python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }
exec .venv/bin/python -m pytest -v -p no:cacheprovider "$@"
