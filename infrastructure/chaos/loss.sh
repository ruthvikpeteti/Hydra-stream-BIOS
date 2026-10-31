#!/usr/bin/env bash
# loss.sh <proxy> [fraction=0.3]   EMULATED loss: this share of new connections is reset (not per-packet loss)
set -euo pipefail; source "$(dirname "$0")/lib.sh"
need "${1:-}" "loss.sh <proxy> [fraction=0.3]"
add_toxic "$1" loss reset_peer downstream "${2:-0.3}" '{"timeout":0}'
echo "$1: ${2:-0.3} of connections reset"
