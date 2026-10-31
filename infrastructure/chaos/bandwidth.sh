#!/usr/bin/env bash
# bandwidth.sh <proxy> [KB_per_s=50]   limit the throughput of a path (both directions)
set -euo pipefail; source "$(dirname "$0")/lib.sh"
need "${1:-}" "bandwidth.sh <proxy> [KB_per_s=50]"
add_toxic "$1" bw_down bandwidth downstream 1.0 "{\"rate\":${2:-50}}"
add_toxic "$1" bw_up bandwidth upstream 1.0 "{\"rate\":${2:-50}}"
echo "$1: limited to ${2:-50} KB/s"
