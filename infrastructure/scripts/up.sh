#!/usr/bin/env bash
# Start all clusters + the gateway (and wait for healthy).
# --monitoring: also Prometheus.  --expose-kafka: DEBUG, publish Kafka listeners to the host (breaks gateway-only model).
set -euo pipefail
source "$(dirname "$0")/lib.sh"
"$INFRA_DIR/scripts/init-cluster-ids.sh" >/dev/null
args=()
for a in "$@"; do
  case "$a" in
    --monitoring) args+=(--profile monitoring) ;;
    --dr) export SS_DR=1; source "$(dirname "$0")/lib.sh" ;;
    --expose-kafka) export SS_EXPOSE_KAFKA=1; source "$(dirname "$0")/lib.sh" ;;   # re-source to add the override file
    *) echo "unknown option $a"; exit 2 ;;
  esac
done
"${COMPOSE[@]}" ${args[@]+"${args[@]}"} up -d --build --wait --wait-timeout 300
"$INFRA_DIR/scripts/status.sh"
