#!/usr/bin/env bash
# describe-topic.sh <region> <topic>
set -euo pipefail
source "$(dirname "$0")/lib.sh"
kcli_bs "${1:?region}" kafka-topics.sh --describe --topic "${2:?topic}"
