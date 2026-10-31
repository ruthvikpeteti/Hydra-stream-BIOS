#!/usr/bin/env bash
# Bring every container and fault back (test aid). Does not change coordinator state.
cd "$(dirname "$0")/../../infrastructure"
for c in $(docker ps -a --filter "name=ss-" --filter "status=exited" --format '{{.Names}}'); do docker start "$c" >/dev/null; done
./chaos/recover.sh --all >/dev/null
