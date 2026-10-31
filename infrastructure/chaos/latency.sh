#!/usr/bin/env bash
# latency.sh <proxy> [ms=400] [jitter_ms=0]   add latency (and optional jitter) to the response direction of a path
set -euo pipefail; source "$(dirname "$0")/lib.sh"
need "${1:-}" "latency.sh <proxy> [ms=400] [jitter_ms=0]"
add_toxic "$1" latency latency downstream 1.0 "{\"latency\":${2:-400},\"jitter\":${3:-0}}"
echo "$1: +${2:-400} ms latency (jitter ${3:-0} ms)"
