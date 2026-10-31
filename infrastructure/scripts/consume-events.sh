#!/usr/bin/env bash
# consume-events.sh <region> <topic> [max-messages=all] [timeout-ms=15000]
# Prints "partition<TAB>key<TAB>value" lines (read-only; no consumer group is committed).
set -euo pipefail
source "$(dirname "$0")/lib.sh"
r="${1:?region}"; t="${2:?topic}"; max="${3:-}"; to="${4:-15000}"
args=(--topic "$t" --from-beginning --timeout-ms "$to" --consumer.config /opt/ss-config/consumer.properties
      --property print.partition=true --property print.key=true --property key.separator=$'\t')
[[ -n "$max" ]] && args+=(--max-messages "$max")
kcli_bs "$r" kafka-console-consumer.sh "${args[@]}" 2>/dev/null | sed -E 's/^Partition:([0-9]+)\t/\1\t/'
