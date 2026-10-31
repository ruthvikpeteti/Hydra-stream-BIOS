#!/usr/bin/env bash
# Stop everything. Data is KEPT unless --volumes is given (then ALL Kafka data in all regions is deleted).
set -euo pipefail
export SS_EXPOSE_KAFKA=1 SS_DR=1   # so every override file is included and nothing is left behind
source "$(dirname "$0")/lib.sh"
if [[ "${1:-}" == "--volumes" ]]; then
  "${COMPOSE[@]}" --profile monitoring down --volumes --remove-orphans
  echo "Volumes removed. Run scripts/init-cluster-ids.sh --force (or delete .env) for fresh cluster IDs."
else
  "${COMPOSE[@]}" --profile monitoring down --remove-orphans
fi
