#!/usr/bin/env bash
# produce-events.sh <region> <topic> [count=100] [keys=10]
# Sends "key-<k>:<seq>" with key "key-<k>" using config/producer.properties (acks=all).
# <seq> is the global sequence number, so per-key (hence per-partition) order is checkable.
# SEQ_START=<n> starts the sequence at n (default 0).
# PRODUCER_EXTRA="request.timeout.ms=5000 retries=0" appends extra --producer-property pairs.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
r="${1:?region}"; t="${2:?topic}"; n="${3:-100}"; keys="${4:-10}"
extra=(); for kv in ${PRODUCER_EXTRA:-}; do extra+=(--producer-property "$kv"); done
awk -v n="$n" -v k="$keys" -v s="${SEQ_START:-0}" 'BEGIN{for(i=s;i<s+n;i++) printf "key-%d:%d\n", i%k, i}' \
 | kcli_bs "$r" kafka-console-producer.sh --topic "$t" \
     --producer.config /opt/ss-config/producer.properties \
     --property parse.key=true --property key.separator=: ${extra[@]+"${extra[@]}"}
