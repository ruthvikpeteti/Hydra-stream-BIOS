#!/usr/bin/env bash
# disconnect.sh <proxy>... | disconnect.sh --pair <src> <dst>   complete disconnection (existing connections dropped, new ones refused)
set -euo pipefail; source "$(dirname "$0")/lib.sh"
need "${1:-}" "disconnect.sh <proxy>...  |  disconnect.sh --pair <a|b|c> <a|b|c>"
if [[ "$1" == "--pair" ]]; then set -- "$2-$3-direct" "$2-$3-peering" "$2-$3-relay"; fi
for p in "$@"; do toxi POST "/proxies/$p" '{"enabled":false}' >/dev/null; echo "$p: disconnected"; done
