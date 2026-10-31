#!/usr/bin/env bash
# list.sh   show every path proxy with its state and active toxics
set -euo pipefail; source "$(dirname "$0")/lib.sh"
curl -fsS "$TOXI/proxies" | python3 -c "$(cat <<'PY'
import sys, json
for name, p in sorted(json.load(sys.stdin).items()):
    state = "enabled" if p["enabled"] else "DISABLED"
    toxics = [t["name"] for t in p["toxics"]] or "-"
    print(f"{name:18s} {state:9s} {p['listen']:>15s} -> {p['upstream']:16s} toxics: {toxics}")
PY
)"
