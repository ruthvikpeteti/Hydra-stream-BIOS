#!/usr/bin/env bash
# reset.sh <proxy>   TCP reset: every connection through the path is reset immediately
set -euo pipefail; source "$(dirname "$0")/lib.sh"
need "${1:-}" "reset.sh <proxy>"
add_toxic "$1" tcp_reset reset_peer downstream 1.0 '{"timeout":0}'
echo "$1: all connections reset"
