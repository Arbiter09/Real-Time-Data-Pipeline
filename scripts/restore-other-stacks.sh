#!/usr/bin/env bash
# Restarts the containers that were running before this project took the host.
# Generated during Phase 0; see .runtime/containers-to-restore.txt
set -euo pipefail
LIST="$(dirname "$0")/../.runtime/containers-to-restore.txt"
[ -f "$LIST" ] || { echo "no restore list at $LIST"; exit 1; }
while read -r name; do
  [ -z "$name" ] && continue
  echo "starting $name"
  docker start "$name" >/dev/null 2>&1 || echo "  (could not start $name)"
done < "$LIST"
echo "done"
