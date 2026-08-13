#!/usr/bin/env bash
# Restore the Docker containers this project displaced while measuring.
#
# Benchmarking needs the host to itself. Other stacks were not just stopped but
# had their restart policies cleared first, because `restart: always` containers
# come straight back the moment Docker Desktop restarts - which is exactly what
# happened when memory was reallocated mid-session, and a runaway container
# burning 190% CPU silently invalidated a throughput run.
#
# This restores both the policy and the running state.
set -uo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"
POLICIES="$DIR/.runtime/restart-policies.txt"
RUNNING="$DIR/.runtime/containers-to-restore.txt"

if [ -f "$POLICIES" ]; then
  echo "restoring restart policies..."
  while read -r name policy; do
    [ -z "${name:-}" ] && continue
    kind="${policy%%:*}"
    count="${policy##*:}"
    case "$kind" in
      on-failure) flag="--restart=on-failure:${count:-0}" ;;
      always|unless-stopped) flag="--restart=${kind}" ;;
      *) flag="--restart=no" ;;
    esac
    docker update "$flag" "$name" >/dev/null 2>&1 \
      && echo "  $name -> $kind" \
      || echo "  $name (not found)"
  done < "$POLICIES"
else
  echo "no policy file at $POLICIES - skipping policy restore"
fi

if [ -f "$RUNNING" ]; then
  echo "starting containers that were running before..."
  while read -r name; do
    [ -z "${name:-}" ] && continue
    docker start "$name" >/dev/null 2>&1 \
      && echo "  started $name" \
      || echo "  could not start $name"
  done < "$RUNNING"
else
  echo "no container list at $RUNNING"
fi

echo "done"
