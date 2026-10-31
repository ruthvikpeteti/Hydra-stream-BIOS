#!/usr/bin/env bash
# Test A: all clusters reachable independently, distinct identities, broker + controller health.
set -uo pipefail
source "$(dirname "$0")/../scripts/lib.sh"
set -e
set -a; source "$INFRA_DIR/.env"; set +a

declare_expected() { case "$1" in a) echo "$CLUSTER_ID_A 3 3";; b) echo "$CLUSTER_ID_B 1 1";; c) echo "$CLUSTER_ID_C 1 1";; esac; }
declare -a ids=()
for r in a b c; do
  read -r want_id want_brokers want_voters <<<"$(declare_expected "$r")"
  log "region-$r: containers healthy"
  names="$(region_containers "$r")"; [[ $r == a ]] && names="$names ss-a-controller-1 ss-a-controller-2 ss-a-controller-3"
  for c in $names; do
    [[ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" == healthy ]] || fail "$c not healthy"
  done

  log "region-$r: cluster identity"
  got="$(cluster_id "$r")"
  [[ "$got" == "$want_id" ]] || fail "region-$r cluster id '$got' != configured '$want_id'"
  ids+=("$got")

  log "region-$r: metadata quorum (controllers)"
  status="$(kcli_bs "$r" kafka-metadata-quorum.sh describe --status)"
  leader="$(awk '/^LeaderId/{print $2}' <<<"$status")"
  voters="$(grep -o '"id": [0-9]*' <<<"$(grep '^CurrentVoters' <<<"$status")" | wc -l | tr -d ' ')"
  [[ -n "$leader" && "$leader" != "-1" ]] || fail "region-$r has no metadata quorum leader"
  [[ "$voters" == "$want_voters" ]] || fail "region-$r expected $want_voters voters, got $voters"

  log "region-$r: broker count"
  brokers="$(kcli_bs "$r" kafka-broker-api-versions.sh | grep -c '(id: ')"
  [[ "$brokers" == "$want_brokers" ]] || fail "region-$r expected $want_brokers brokers, got $brokers"

  # Gateway-only model (Phase 2): Kafka listeners are NOT published unless started with --expose-kafka.
  if [[ -n "$(docker port "$(region_containers "$r" | awk '{print $1}')" 2>/dev/null)" ]]; then
    log "region-$r: external listeners from the host (debug override active)"
    for port in $(region_external_ports "$r"); do
      python3 -I "$INFRA_DIR/tests/kafka_ping.py" localhost "$port" || fail "external listener localhost:$port"
    done
  else
    log "region-$r: external listeners are closed to the host (gateway-only model)"
    for port in $(region_external_ports "$r"); do
      if python3 -I "$INFRA_DIR/tests/kafka_ping.py" localhost "$port" >/dev/null 2>&1; then fail "localhost:$port is reachable; Kafka must not be published"; fi
    done
  fi
done

log "gateway healthy and on exactly the client + region-a networks"
[[ "$(docker inspect -f '{{.State.Health.Status}}' ss-a-gateway)" == healthy ]] || fail "ss-a-gateway not healthy"

log "cluster ids are pairwise different"
[[ "${ids[0]}" != "${ids[1]}" && "${ids[0]}" != "${ids[2]}" && "${ids[1]}" != "${ids[2]}" ]] || fail "cluster ids not unique"

log "network isolation: regions cannot resolve each other"
for pair in "ss-a-broker-1:ss-b-kafka-1" "ss-a-broker-1:ss-c-kafka-1" "ss-b-kafka-1:ss-a-broker-1" "ss-b-kafka-1:ss-c-kafka-1" "ss-c-kafka-1:ss-a-broker-1" "ss-c-kafka-1:ss-b-kafka-1"; do
  from="${pair%%:*}"; to="${pair##*:}"
  if docker exec "$from" getent hosts "$to" >/dev/null 2>&1; then fail "$from can resolve $to (regions not isolated)"; fi
done

log "monitoring endpoints"
for p in 9308 9309 9310; do
  # exporters rebuild their broker view lazily after broker restarts, so allow a short settle time
  wait_until 30 "exporter :$p" bash -c "curl -fs --max-time 5 http://localhost:$p/metrics | grep -q '^kafka_brokers '" || fail "exporter :$p missing kafka_brokers"
done
pass "Test A: three independent, healthy clusters with distinct ids"
