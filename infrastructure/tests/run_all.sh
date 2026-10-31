#!/usr/bin/env bash
# Runs tests A-E in order. Requires the stack to be up (scripts/up.sh). C-E are stateful and must run in order.
set -uo pipefail
cd "$(dirname "$0")"
rc=0
for t in test_a_independent_clusters test_b_ordered_events test_c_one_broker_down test_d_two_brokers_down test_e_recovery test_f_replication_e2e; do
  echo "=== $t ==="
  bash "./$t.sh" || { echo "!!! $t failed"; rc=1; break; }
done
exit $rc
