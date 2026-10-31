#!/usr/bin/env bash
# recover.sh <proxy>... | recover.sh --pair <src> <dst> | recover.sh --all   remove every toxic and re-enable the path(s)
set -euo pipefail; source "$(dirname "$0")/lib.sh"
need "${1:-}" "recover.sh <proxy>...  |  --pair <src> <dst>  |  --all"
if [[ "$1" == "--all" ]]; then set -- $(curl -fsS "$TOXI/proxies" | python3 -c 'import sys,json; print(" ".join(sorted(json.load(sys.stdin))))');
elif [[ "$1" == "--pair" ]]; then set -- "$2-$3-direct" "$2-$3-peering" "$2-$3-relay"; fi
for p in "$@"; do
  for t in $(curl -fsS "$TOXI/proxies/$p/toxics" | python3 -c 'import sys,json; print(" ".join(x["name"] for x in json.load(sys.stdin)))'); do toxi DELETE "/proxies/$p/toxics/$t" >/dev/null; done
  toxi POST "/proxies/$p" '{"enabled":true}' >/dev/null; echo "$p: recovered"
done
