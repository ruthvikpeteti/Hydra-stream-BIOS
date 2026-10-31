#!/usr/bin/env bash
# Test B: topic in A (3 partitions, RF 3), 1000 ordered events acks=all, verified consumption.
set -uo pipefail
source "$(dirname "$0")/../scripts/lib.sh"
set -e
T="ss.test-ordered-$(date +%s)"
N=1000

log "create $T in region-a (3 partitions, RF 3)"
"$INFRA_DIR/scripts/create-topic.sh" a "$T" 3 3
desc="$("$INFRA_DIR/scripts/describe-topic.sh" a "$T")"
echo "$desc" | grep -q 'PartitionCount: 3'        || fail "partition count != 3"
echo "$desc" | grep -q 'ReplicationFactor: 3'     || fail "replication factor != 3"
echo "$desc" | grep -q 'min.insync.replicas=2'    || fail "min.insync.replicas != 2"
wait_until 60 "ISR 3" isr_ok a "$T" 3 || fail "ISR never reached 3"

log "produce $N events (acks=all, 10 keys)"
"$INFRA_DIR/scripts/produce-events.sh" a "$T" "$N" 10 2>&1 | tail -3

log "consume and verify"
out="$(mktemp)"; trap 'rm -f "$out"' EXIT
"$INFRA_DIR/scripts/consume-events.sh" a "$T" "$N" 60000 > "$out"
verify_events "$out" "$N" || fail "consumed data failed verification"
parts="$(cut -f1 "$out" | sort -u | wc -l | tr -d ' ')"
[[ "$parts" == 3 ]] || log "note: events landed on $parts partition(s)"
pass "Test B: $N events produced with acks=all and consumed in per-key order"
