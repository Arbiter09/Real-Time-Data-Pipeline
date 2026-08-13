#!/usr/bin/env bash
# Creates the pipeline's topics with the exact topology the README claims.
# Idempotent: safe to re-run, exits 0 if topics already exist with right shape.
set -euo pipefail

KB=/opt/kafka/bin
BOOTSTRAP="${BOOTSTRAP:?}"

echo "[topics] waiting for cluster metadata on ${BOOTSTRAP}"
for i in $(seq 1 30); do
  if "${KB}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

create() {
  local topic="$1" parts="$2" rf="$3" minisr="$4"
  if "${KB}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${topic}" >/dev/null 2>&1; then
    echo "[topics] ${topic} already exists"
  else
    echo "[topics] creating ${topic} partitions=${parts} rf=${rf} min.insync.replicas=${minisr}"
    "${KB}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --create \
      --topic "${topic}" --partitions "${parts}" --replication-factor "${rf}" \
      --config min.insync.replicas="${minisr}" \
      --config retention.ms=86400000 \
      --config unclean.leader.election.enable=false
  fi
}

# unclean.leader.election.enable=false is the setting that makes "zero silent
# loss" a real claim instead of a hopeful one: a replica that was never in the
# ISR can never be elected leader, so an out-of-sync broker cannot truncate
# acknowledged writes after a failover.
create "${TOPIC_TRIPS}" "${PARTS}" "${RF}" "${MIN_ISR}"

# DLQ is deliberately lower-throughput and longer-retention: 7 days, because a
# quarantined event is worthless if it expires before anyone drains it.
if "${KB}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC_DLQ}" >/dev/null 2>&1; then
  echo "[topics] ${TOPIC_DLQ} already exists"
else
  "${KB}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --create \
    --topic "${TOPIC_DLQ}" --partitions 3 --replication-factor "${RF}" \
    --config min.insync.replicas="${MIN_ISR}" \
    --config retention.ms=604800000 \
    --config unclean.leader.election.enable=false
fi

echo "[topics] final state:"
"${KB}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC_TRIPS}"
"${KB}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC_DLQ}"
echo "[topics] done"
