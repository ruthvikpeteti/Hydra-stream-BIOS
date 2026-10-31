#!/usr/bin/env bash
# docker compose with every overlay (DR + exposed Kafka NOT included): scripts/dc.sh up -d --build ss-dr-1
set -euo pipefail
export SS_DR=1
source "$(dirname "$0")/lib.sh"
set -a; source "$INFRA_DIR/.env"; set +a
exec "${COMPOSE[@]}" "$@"
