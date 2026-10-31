#!/usr/bin/env bash
# Creates the smoke-test topic in each region with that region's local durability settings.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
for r in a b c; do "$INFRA_DIR/scripts/create-topic.sh" "$r" ss.smoke 3; done
