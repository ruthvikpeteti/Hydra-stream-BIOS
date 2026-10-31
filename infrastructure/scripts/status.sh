#!/usr/bin/env bash
# Per-region summary: container health, cluster id, metadata quorum leader.
set -uo pipefail
source "$(dirname "$0")/lib.sh"
for r in a b c; do
  echo "== region-$r =="
  extra=""; [[ $r == a ]] && extra="ss-a-controller-1 ss-a-controller-2 ss-a-controller-3"
  for c in $extra $(region_containers "$r"); do
    printf '  %-16s %s\n' "$c" "$(docker inspect -f '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{else}}n-a{{end}}' "$c" 2>/dev/null || echo missing)"
  done
  if region_exec_container "$r" >/dev/null 2>&1; then
    echo "  cluster-id: $(cluster_id "$r")"
    kcli_bs "$r" kafka-metadata-quorum.sh describe --status 2>/dev/null | grep -E '^(LeaderId|CurrentVoters)' | sed 's/^/  /'
  fi
done
echo "== monitoring =="
for p in 9308 9309 9310; do
  printf '  http://localhost:%s/metrics -> %s\n' "$p" "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://localhost:$p/metrics" || true)"
done
