#!/usr/bin/env bash
# Test D: with only one in-sync replica left, min.insync.replicas=2 must reject acks=all writes.
set -uo pipefail
source "$(dirname "$0")/../scripts/lib.sh"
set -e
STATE="$INFRA_DIR/tests/.state"
[[ -f "$STATE/failover-topic" ]] || fail "run test_c first (needs the failover topic)"
T="$(cat "$STATE/failover-topic")"

log "leave exactly one region-a broker running (whichever is up; test C may have stopped any of them)"
running=(); for c in $(region_containers a); do is_running "$c" && running+=("$c"); done
[[ ${#running[@]} -ge 1 ]] || fail "no region-a broker is running"
survivor="${running[$((${#running[@]}-1))]}"; sid=$((100 + ${survivor##*-}))
for c in "${running[@]}"; do [[ "$c" == "$survivor" ]] || docker stop -t 10 "$c" >/dev/null; done
log "survivor: $survivor (broker id $sid)"

log "wait until the survivor is leader with ISR=1 for every partition"
wait_until 120 "ISR shrinks to 1" bash -c "
  source '$INFRA_DIR/scripts/lib.sh'
  topic_table a '$T' | awk -v id=$sid '\$2!=id || \$3!=id {bad=1} END{exit bad||NR==0}'" \
  || { topic_table a "$T"; fail "ISR did not shrink to the single remaining broker"; }
topic_table a "$T" | sed 's/^/  partition leader isr = /'

log "produce with acks=all: must be rejected (no retries so the error is surfaced)"
out="$(PRODUCER_EXTRA="enable.idempotence=false retries=0 request.timeout.ms=5000 delivery.timeout.ms=8000 max.block.ms=10000" \
  SEQ_START=900 "$INFRA_DIR/scripts/produce-events.sh" a "$T" 10 10 2>&1 || true)"
echo "$out" | grep -E 'NotEnoughReplicas|NOT_ENOUGH_REPLICAS' | head -2 | sed 's/^/  /'
echo "$out" | grep -qE 'NotEnoughReplicas|NOT_ENOUGH_REPLICAS' || { echo "$out" | tail -15; fail "expected NotEnoughReplicas from acks=all produce"; }
pass "Test D: acks=all writes are refused when ISR (1) < min.insync.replicas (2); nothing was acknowledged"
