#!/usr/bin/env bash
# jitter.sh <proxy> [base_ms=50] [jitter_ms=100]   latency that varies by +-jitter around the base
set -euo pipefail; source "$(dirname "$0")/lib.sh"
need "${1:-}" "jitter.sh <proxy> [base_ms=50] [jitter_ms=100]"
exec "$(dirname "$0")/latency.sh" "$1" "${2:-50}" "${3:-100}"
