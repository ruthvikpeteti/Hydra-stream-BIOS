#!/usr/bin/env bash
# Prints the current digest of every image used (pull first). Review the output and update ci/images.lock and the FROM/image lines together.
set -euo pipefail
while read -r ref; do
  [[ -z "$ref" || "$ref" == \#* ]] && continue
  tag=${ref%@*}; docker pull -q "$tag" >/dev/null; echo "$tag@$(docker image inspect --format '{{index .RepoDigests 0}}' "$tag" | sed 's/.*@//')"
done < "$(dirname "$0")/images.lock"
