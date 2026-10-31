#!/usr/bin/env bash
# Shared helpers. Source this file; do not execute it.
# Kafka CLIs run INSIDE the broker containers (internal listener), so results do not depend on
# host networking or advertised EXTERNAL listeners. The EXTERNAL listeners are checked separately
# by tests/kafka_ping.py.

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$INFRA_DIR/docker-compose.yml" --project-directory "$INFRA_DIR")
# Debug override that publishes Kafka listeners to the host (breaks the gateway-only model).
[[ "${SS_EXPOSE_KAFKA:-0}" == 1 ]] && COMPOSE+=(-f "$INFRA_DIR/docker-compose.expose-kafka.yml")
# Phase 6 overlay: etcd coordinator + witnesses, gateways in B and C, failover controllers (scripts/up.sh --dr).
[[ "${SS_DR:-0}" == 1 ]] && COMPOSE+=(-f "$INFRA_DIR/docker-compose.dr.yml")
KBIN=/opt/kafka/bin

# region -> broker containers / bootstrap / external ports / defaults
region_containers() {
  case "$1" in
    a) echo "ss-a-broker-1 ss-a-broker-2 ss-a-broker-3" ;;
    b) echo "ss-b-kafka-1" ;;
    c) echo "ss-c-kafka-1" ;;
    *) echo "unknown region '$1' (use a|b|c)" >&2; return 1 ;;
  esac
}
region_bootstrap() {  # internal bootstrap list (valid inside the region network)
  local out=() c; for c in $(region_containers "$1"); do out+=("$c:9092"); done
  (IFS=,; echo "${out[*]}")
}
region_external_ports() {
  case "$1" in a) echo "29092 29093 29094";; b) echo 39092;; c) echo 49092;; esac
}
region_rf()  { case "$1" in a) echo 3;; *) echo 1;; esac; }
region_min_isr() { case "$1" in a) echo 2;; *) echo 1;; esac; }

is_running() { [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]; }

# First running broker container of a region (exec target).
region_exec_container() {
  local c; for c in $(region_containers "$1"); do is_running "$c" && { echo "$c"; return 0; }; done
  echo "no running broker in region $1" >&2; return 1
}
# Bootstrap restricted to running brokers (avoids long DNS/connect waits on stopped nodes).
region_live_bootstrap() {
  local out=() c; for c in $(region_containers "$1"); do is_running "$c" && out+=("$c:9092"); done
  (IFS=,; echo "${out[*]}")
}

# kcli <region> <script.sh> [args...]  -- run a Kafka CLI in a live broker of the region.
kcli() {
  local r="$1" tool="$2"; shift 2
  docker exec -i "$(region_exec_container "$r")" "$KBIN/$tool" "$@"
}
# Same, but injects --bootstrap-server with the live brokers.
kcli_bs() {
  local r="$1" tool="$2"; shift 2
  kcli "$r" "$tool" --bootstrap-server "$(region_live_bootstrap "$r")" "$@"
}

cluster_id() {  # cluster id as reported by the cluster itself
  kcli_bs "$1" kafka-metadata-quorum.sh describe --status 2>/dev/null | awk -F: '/^ClusterId/ {gsub(/ /,"",$2); print $2}'
}

# Partition table for a topic: lines "partition leader isr(comma list)".
topic_table() {
  kcli_bs "$1" kafka-topics.sh --describe --topic "$2" 2>/dev/null \
    | awk '/Partition:/ { for(i=1;i<=NF;i++){ if($i=="Partition:")p=$(i+1); if($i=="Leader:")l=$(i+1); if($i=="Isr:")s=$(i+1) } print p, l, s }'
}

log()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { printf '\033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }
pass() { printf '\033[32mPASS\033[0m %s\n' "$*"; }

# wait_until <timeout_s> <description> <command...>
wait_until() {
  local t="$1" d="$2"; shift 2; local end=$((SECONDS+t))
  until "$@" >/dev/null 2>&1; do
    (( SECONDS >= end )) && { echo "timeout waiting for: $d" >&2; return 1; }
    sleep 2
  done
}

# verify_events <file-from-consume-events.sh> <expected-count>
# Checks: exact count, no duplicate values, every key stays on one partition, and per-key order is
# strictly increasing in consumption order (Kafka guarantees order per partition, not globally).
verify_events() {
  awk -F'\t' -v want="$2" '
    { n++; p=$1; k=$2; v=$3+0
      if (v in seen) { dup++ } seen[v]=1
      if (k in part && part[k]!=p) { moved++ } part[k]=p
      if (k in last && v<=last[k]) { disorder++ } last[k]=v }
    END { printf "events=%d expected=%d duplicates=%d key_moved_partition=%d out_of_order=%d\n", n, want, dup+0, moved+0, disorder+0
          exit !(n==want && dup==0 && moved==0 && disorder==0) }' "$1"
}

# wait_isr <region> <topic> <min-isr-size> -> all partitions have a leader and |ISR| >= size
isr_ok() {
  local r="$1" t="$2" min="$3" rows
  rows="$(topic_table "$r" "$t")"; [[ -n "$rows" ]] || return 1
  echo "$rows" | awk -v m="$min" '{ n=split($3,a,","); if ($2=="none" || $2=="-1" || n<m) bad=1 } END{exit bad}'
}
