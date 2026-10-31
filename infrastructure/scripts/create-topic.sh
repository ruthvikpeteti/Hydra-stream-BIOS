#!/usr/bin/env bash
# create-topic.sh <region a|b|c> <topic> [partitions=3] [replication-factor=region default]
# min.insync.replicas is set explicitly per topic (A=2, B/C=1) so it never silently depends on defaults.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
r="${1:?region}"; t="${2:?topic}"; p="${3:-3}"; rf="${4:-$(region_rf "$r")}"
kcli_bs "$r" kafka-topics.sh --create --if-not-exists --topic "$t" \
  --partitions "$p" --replication-factor "$rf" \
  --config "min.insync.replicas=$(region_min_isr "$r")"
