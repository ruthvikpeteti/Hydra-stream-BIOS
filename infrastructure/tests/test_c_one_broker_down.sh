#!/usr/bin/env bash
# Test C: stop one broker in A; a replica from the ISR takes over; acks=all writes still succeed.
set -uo pipefail
source "$(dirname "$0")/../scripts/lib.sh"
set -e
STATE="$INFRA_DIR/tests/.state"; mkdir -p "$STATE"
T="ss.test-failover-$(date +%s)"

log "precondition: all region-a brokers running"
for c in $(region_containers a); do is_running "$c" || docker start "$c" >/dev/null; done
wait_until 180 "brokers healthy" bash -c 'for c in ss-a-broker-1 ss-a-broker-2 ss-a-broker-3; do [[ "$(docker inspect -f "{{.State.Health.Status}}" $c)" == healthy ]] || exit 1; done'

"$INFRA_DIR/scripts/create-topic.sh" a "$T" 3 3
wait_until 60 "ISR 3" isr_ok a "$T" 3 || fail "ISR never reached 3"
"$INFRA_DIR/scripts/produce-events.sh" a "$T" 300 10 2>&1 | tail -2
echo "$T" > "$STATE/failover-topic"; echo 300 > "$STATE/failover-acked"

before="$(topic_table a "$T")"
echo "$before" | sed 's/^/  before: partition leader isr = /'
victim_id="$(awk '$1==0{print $2}' <<<"$before")"
victim="ss-a-broker-$((victim_id-100))"
log "stopping $victim (leader of partition 0)"
docker stop -t 10 "$victim" >/dev/null
echo "$victim" > "$STATE/stopped-1"

log "wait for re-election: every partition has a live leader and |ISR| >= 2"
wait_until 90 "leaders re-elected" isr_ok a "$T" 2 || fail "leaders/ISR did not settle"
after="$(topic_table a "$T")"
echo "$after" | sed 's/^/  after:  partition leader isr = /'

while read -r part leader isr; do
  [[ "$leader" != "$victim_id" ]] || fail "partition $part still led by stopped broker"
  grep -qw "$leader" <<<"$(tr ',' ' ' <<<"$(awk -v p="$part" '$1==p{print $3}' <<<"$before")")" \
    || fail "partition $part leader $leader was not in the previous ISR (unclean election?)"
  grep -qw "$victim_id" <<<"$(tr ',' ' ' <<<"$isr")" && fail "stopped broker still in ISR of partition $part"
done <<<"$after"

log "produce 200 more events with acks=all while ISR=2"
SEQ_START=300 "$INFRA_DIR/scripts/produce-events.sh" a "$T" 200 10 2>&1 | tee /dev/stderr | grep -qiE 'error|exception' && fail "producer reported errors with ISR=2"
echo 500 > "$STATE/failover-acked"

log "consume everything and verify"
out="$(mktemp)"; trap 'rm -f "$out"' EXIT
"$INFRA_DIR/scripts/consume-events.sh" a "$T" 500 60000 > "$out"
verify_events "$out" 500 || fail "verification failed"
pass "Test C: leader failover succeeded and acks=all writes continued with 2 in-sync replicas"
