#!/usr/bin/env bash
# Test E: restart stopped brokers; replicas catch up, ISR returns to 3, writes resume, no data lost
# or duplicated, and the writes rejected in test D did not appear.
set -uo pipefail
source "$(dirname "$0")/../scripts/lib.sh"
set -e
STATE="$INFRA_DIR/tests/.state"
[[ -f "$STATE/failover-topic" ]] || fail "run tests c and d first"
T="$(cat "$STATE/failover-topic")"; ACKED="$(cat "$STATE/failover-acked")"

log "restart every stopped region-a container"
for c in $(region_containers a); do is_running "$c" || { log "starting $c"; docker start "$c" >/dev/null; }; done
wait_until 240 "brokers healthy" bash -c 'for c in ss-a-broker-1 ss-a-broker-2 ss-a-broker-3; do [[ "$(docker inspect -f "{{.State.Health.Status}}" $c)" == healthy ]] || exit 1; done' \
  || fail "brokers did not become healthy"

log "wait for ISR = 3 on every partition"
wait_until 180 "ISR 3" isr_ok a "$T" 3 || { topic_table a "$T"; fail "ISR did not return to 3"; }
topic_table a "$T" | sed 's/^/  partition leader isr = /'

log "produce 100 more events with acks=all"
SEQ_START=500 "$INFRA_DIR/scripts/produce-events.sh" a "$T" 100 10 2>&1 | tee /dev/stderr | grep -qiE 'error|exception' && fail "producer errors after recovery"
TOTAL=$((ACKED+100)); echo "$TOTAL" > "$STATE/failover-acked"

log "consume and verify $TOTAL events (D's rejected writes must be absent)"
out="$(mktemp)"; trap 'rm -f "$out"' EXIT
"$INFRA_DIR/scripts/consume-events.sh" a "$T" "$TOTAL" 60000 > "$out"
verify_events "$out" "$TOTAL" || fail "verification failed"
pass "Test E: brokers recovered, ISR back to 3, no loss or duplication"
