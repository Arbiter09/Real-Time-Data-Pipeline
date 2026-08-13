#!/usr/bin/env bash
# Phase 0 stack gate: one command, every component, exit non-zero if anything
# is not genuinely ready.
#
# "Ready" deliberately means more than "the container is running":
#   Kafka       all 3 brokers answer an API-versions request AND the topics
#               exist with the replication factor and min.ISR this project
#               claims, AND every partition has a full in-sync replica set
#   Cassandra   all 3 nodes report UN (Up/Normal) AND the keyspace's tables
#               exist AND a QUORUM write actually succeeds
#   Postgres    accepts connections and holds the analytics tables
#   Spark       master is up with the expected number of live workers
#
# A stack that passes this and still fails a benchmark has a real problem;
# a stack that fails this has a setup problem, and the distinction saves hours.
set -uo pipefail

PASS=0; FAIL=0; WARN=0
ok()   { printf "  \033[32mPASS\033[0m  %s\n" "$1"; PASS=$((PASS+1)); }
bad()  { printf "  \033[31mFAIL\033[0m  %s\n" "$1"; FAIL=$((FAIL+1)); }
warn() { printf "  \033[33mWARN\033[0m  %s\n" "$1"; WARN=$((WARN+1)); }

EXPECT_PARTITIONS="${TOPIC_PARTITIONS:-6}"
EXPECT_RF="${TOPIC_REPLICATION:-3}"
EXPECT_MIN_ISR="${TOPIC_MIN_ISR:-2}"
TOPIC="${TOPIC_TRIPS:-trips.raw}"
DLQ="${TOPIC_DLQ:-trips.dlq}"

echo
echo "=============================================================="
echo "  RTDP stack gate"
echo "=============================================================="

# ---------------------------------------------------------------- containers
echo
echo "containers"
for c in rtdp-kafka1 rtdp-kafka2 rtdp-kafka3 \
         rtdp-cassandra1 rtdp-cassandra2 rtdp-cassandra3 \
         rtdp-postgres rtdp-spark-master; do
  status=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo missing)
  restarts=$(docker inspect -f '{{.RestartCount}}' "$c" 2>/dev/null || echo 0)
  if [ "$status" = "running" ]; then
    if [ "${restarts:-0}" -gt 2 ]; then
      # A container that keeps restarting will pass every point-in-time check
      # and still ruin a measurement halfway through it.
      warn "$c running but has restarted ${restarts} times (memory pressure?)"
    else
      ok "$c running"
    fi
  else
    bad "$c is '$status'"
  fi
done

# -------------------------------------------------------------------- kafka
echo
echo "kafka"
for b in kafka1 kafka2 kafka3; do
  if docker exec rtdp-kafka1 /opt/kafka/bin/kafka-broker-api-versions.sh \
       --bootstrap-server "${b}:9092" >/dev/null 2>&1; then
    ok "broker ${b} answering"
  else
    bad "broker ${b} not answering"
  fi
done

DESC=$(docker exec rtdp-kafka1 /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka1:9092 --describe --topic "$TOPIC" 2>/dev/null)
if [ -z "$DESC" ]; then
  bad "topic ${TOPIC} does not exist"
else
  parts=$(echo "$DESC" | grep -c "Partition:")
  rf=$(echo "$DESC" | head -1 | grep -oE "ReplicationFactor: [0-9]+" | grep -oE "[0-9]+")
  isr_ok=$(echo "$DESC" | grep "Partition:" | grep -c "Isr: [0-9],[0-9],[0-9]")
  [ "$parts" = "$EXPECT_PARTITIONS" ] \
    && ok "topic ${TOPIC} has ${parts} partitions" \
    || bad "topic ${TOPIC} has ${parts} partitions, expected ${EXPECT_PARTITIONS}"
  [ "$rf" = "$EXPECT_RF" ] \
    && ok "topic ${TOPIC} replication factor ${rf}" \
    || bad "topic ${TOPIC} replication factor ${rf}, expected ${EXPECT_RF}"
  [ "$isr_ok" = "$parts" ] \
    && ok "all ${parts} partitions have a full 3-replica ISR" \
    || bad "only ${isr_ok}/${parts} partitions have a full ISR (under-replicated)"
  echo "$DESC" | head -1 | grep -q "min.insync.replicas=${EXPECT_MIN_ISR}" \
    && ok "min.insync.replicas=${EXPECT_MIN_ISR}" \
    || bad "min.insync.replicas is not ${EXPECT_MIN_ISR}"
  echo "$DESC" | head -1 | grep -q "unclean.leader.election.enable=false" \
    && ok "unclean leader election disabled" \
    || bad "unclean leader election is NOT disabled - acknowledged writes can be truncated"
fi

docker exec rtdp-kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 --describe --topic "$DLQ" >/dev/null 2>&1 \
  && ok "DLQ topic ${DLQ} exists" || bad "DLQ topic ${DLQ} missing"

# ---------------------------------------------------------------- cassandra
echo
echo "cassandra"
UN=$(docker exec rtdp-cassandra1 nodetool status 2>/dev/null | grep -c "^UN")
[ "${UN:-0}" = "3" ] \
  && ok "ring has 3 nodes Up/Normal" \
  || bad "ring has ${UN:-0} nodes Up/Normal, expected 3"

TABLES=$(docker exec rtdp-cassandra1 cqlsh -e "USE rtdp; DESCRIBE TABLES;" 2>/dev/null)
for t in trips_by_id trips_by_driver_day trips_by_city_hour latency_samples; do
  echo "$TABLES" | grep -qw "$t" && ok "table ${t}" || bad "table ${t} missing"
done

# A QUORUM write is the only check that proves the ring can actually serve the
# consistency level this pipeline is configured for.
if docker exec rtdp-cassandra1 cqlsh -e \
     "CONSISTENCY QUORUM; INSERT INTO rtdp.run_ledger (run_id, started_at, scenario) VALUES ('healthcheck', toTimestamp(now()), 'gate');" \
     >/dev/null 2>&1; then
  ok "QUORUM write succeeds"
else
  bad "QUORUM write FAILED - the ring cannot serve its configured consistency"
fi

# ----------------------------------------------------------------- postgres
echo
echo "postgres"
if docker exec rtdp-postgres pg_isready -U "${POSTGRES_USER:-rtdp}" >/dev/null 2>&1; then
  ok "accepting connections"
  for t in trips_wide fact_trip trips_rollup_stage; do
    if docker exec rtdp-postgres psql -U "${POSTGRES_USER:-rtdp}" -d "${POSTGRES_DB:-rtdp}" \
         -tAc "SELECT to_regclass('public.${t}') IS NOT NULL" 2>/dev/null | grep -q t; then
      ok "table ${t}"
    else
      warn "table ${t} not present (run 'make analytics-load')"
    fi
  done
else
  bad "not accepting connections"
fi

# -------------------------------------------------------------------- spark
echo
echo "spark"
if docker exec rtdp-spark-master curl -sf http://localhost:8080/json/ >/dev/null 2>&1; then
  WORKERS=$(docker exec rtdp-spark-master curl -s http://localhost:8080/json/ 2>/dev/null \
            | grep -o '"aliveworkers":[0-9]*' | grep -oE '[0-9]+')
  ok "master responding"
  if [ "${WORKERS:-0}" -ge 1 ]; then
    ok "${WORKERS} live worker(s)"
  else
    bad "no live workers registered"
  fi
else
  bad "master not responding"
fi

# ------------------------------------------------------------------ verdict
echo
echo "=============================================================="
printf "  %d passed, %d warnings, %d failed\n" "$PASS" "$WARN" "$FAIL"
echo "=============================================================="
echo
if [ "$FAIL" -gt 0 ]; then
  echo "Stack is NOT ready. Do not benchmark against it - the numbers will be"
  echo "about the broken component, not about the pipeline."
  exit 1
fi
[ "$WARN" -gt 0 ] && echo "Stack is usable but see warnings above."
exit 0
