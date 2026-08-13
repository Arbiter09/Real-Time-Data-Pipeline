#!/usr/bin/env bash
# Submit the ingest stream from inside the driver container.
#
# client deploy mode is deliberate: the driver stays in its own container so
# `docker kill` on a WORKER removes an executor without taking the driver with
# it. That separation is what makes the Section 7.3 executor-kill test a test
# of checkpoint recovery rather than a test of restarting the whole job.
set -euo pipefail

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --deploy-mode client \
  --conf spark.driver.host="${SPARK_DRIVER_HOST:-spark-driver}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-640m}" \
  --conf spark.ui.port=4040 \
  --conf spark.sql.streaming.metricsEnabled=true \
  --conf spark.executorEnv.PYTHONPATH=/opt/app \
  --conf spark.executorEnv.RETRY_BACKOFF_SCHEDULE_MS="${RETRY_BACKOFF_SCHEDULE_MS:-1000,2000,4000}" \
  --conf spark.executorEnv.RETRY_MAX_ATTEMPTS="${RETRY_MAX_ATTEMPTS:-4}" \
  --conf spark.executorEnv.RETRY_JITTER_RATIO="${RETRY_JITTER_RATIO:-0.1}" \
  /opt/app/streaming/stream_job.py "$@"
