"""Prometheus metric definitions, shared so names cannot drift between the
producer and the streaming job.

Section 7.4's point is that *lag* is the health signal, not throughput. Kafka
consumer-group lag is exported by kafka-exporter (it reads the group offsets
directly and does not need cooperation from this code); what this module adds
is the pipeline-internal view: batch duration, write latency, retry counts and
DLQ volume.
"""
from __future__ import annotations

import logging
from typing import Optional

from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = logging.getLogger(__name__)

# --- Producer ---------------------------------------------------------------
PRODUCER_EVENTS = Counter(
    "rtdp_producer_events_total", "Events successfully acknowledged by Kafka",
    ["topic"])
PRODUCER_DELIVERY_FAILURES = Counter(
    "rtdp_producer_delivery_failures_total",
    "Terminal delivery failures reported to the delivery callback", ["reason"])
PRODUCER_DLQ = Counter(
    "rtdp_producer_dlq_total", "Events quarantined to the DLQ by the producer")
PRODUCER_TARGET_RATE = Gauge(
    "rtdp_producer_target_rate", "Configured target events/sec")
PRODUCER_ACTUAL_RATE = Gauge(
    "rtdp_producer_actual_rate", "Observed events/sec over the last interval")
# Pulled straight out of librdkafka's statistics callback - these are the
# client's own retry counters, not something this code estimates.
PRODUCER_TXRETRIES = Gauge(
    "rtdp_producer_broker_txretries", "librdkafka per-broker request retries",
    ["broker"])
PRODUCER_REQ_TIMEOUTS = Gauge(
    "rtdp_producer_broker_req_timeouts", "librdkafka per-broker request timeouts",
    ["broker"])
PRODUCER_QUEUE_DEPTH = Gauge(
    "rtdp_producer_queue_depth", "Messages awaiting delivery in librdkafka queue")

# --- Streaming job ----------------------------------------------------------
STREAM_BATCH_DURATION = Histogram(
    "rtdp_stream_batch_duration_seconds", "Spark micro-batch wall duration",
    buckets=(0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 13, 21, 34, 60))
STREAM_ROWS_WRITTEN = Counter(
    "rtdp_stream_rows_written_total", "Rows written to Cassandra", ["table"])
STREAM_CASSANDRA_RETRIES = Counter(
    "rtdp_stream_cassandra_retries_total", "Cassandra write retries consumed")
STREAM_CASSANDRA_EXHAUSTED = Counter(
    "rtdp_stream_cassandra_exhausted_total",
    "Cassandra writes that exhausted the retry budget and went to the DLQ")
STREAM_DLQ = Counter(
    "rtdp_stream_dlq_total", "Records quarantined by the streaming job", ["stage"])
STREAM_LATE_EVENTS = Counter(
    "rtdp_stream_late_events_total", "Events arriving beyond the watermark")
STREAM_E2E_LATENCY = Histogram(
    "rtdp_stream_e2e_latency_ms",
    "produced_at -> Cassandra write, milliseconds",
    buckets=(10, 25, 50, 100, 200, 350, 500, 750, 1000, 1500, 2500, 5000, 10000, 30000))
STREAM_INPUT_ROWS = Gauge(
    "rtdp_stream_input_rows_per_batch", "Rows in the most recent micro-batch")
STREAM_PROCESSED_RATE = Gauge(
    "rtdp_stream_processed_rows_per_second", "Spark-reported processing rate")

# --- Replay -----------------------------------------------------------------
REPLAY_DRAINED = Counter(
    "rtdp_replay_drained_total", "DLQ records re-injected into the pipeline")
REPLAY_PERMANENT = Counter(
    "rtdp_replay_permanent_total",
    "DLQ records judged permanently un-replayable and parked")

_server_started = False


def serve(port: int) -> None:
    """Idempotently start the metrics endpoint."""
    global _server_started
    if _server_started:
        return
    try:
        start_http_server(port)
        _server_started = True
        log.info("prometheus metrics on :%d/metrics", port)
    except OSError as exc:
        # A benchmark harness re-running in the same container should not die
        # because the port is already bound by a previous run.
        log.warning("metrics server not started on :%d (%s)", port, exc)


def observe_librdkafka_stats(stats: dict, queue_depth: Optional[int] = None) -> None:
    """Translate a librdkafka statistics blob into gauges."""
    for name, b in (stats.get("brokers") or {}).items():
        if b.get("nodeid", -1) < 0:
            continue  # bootstrap/logical entries carry no useful counters
        PRODUCER_TXRETRIES.labels(broker=name).set(b.get("txretries", 0))
        PRODUCER_REQ_TIMEOUTS.labels(broker=name).set(b.get("req_timeouts", 0))
    if queue_depth is None:
        queue_depth = stats.get("msg_cnt", 0)
    PRODUCER_QUEUE_DEPTH.set(queue_depth)


def librdkafka_retry_totals(stats: dict) -> tuple[int, int]:
    """Sum txretries and req_timeouts across real brokers."""
    retries = timeouts = 0
    for b in (stats.get("brokers") or {}).values():
        if b.get("nodeid", -1) < 0:
            continue
        retries += int(b.get("txretries", 0))
        timeouts += int(b.get("req_timeouts", 0))
    return retries, timeouts

# --- Consumer lag -----------------------------------------------------------
# Sourced from Spark's own Kafka source metrics, NOT from a consumer-group
# exporter: Structured Streaming keeps offsets in its checkpoint and never
# registers a consumer group, so group-lag exporters report nothing for it.
STREAM_KAFKA_LAG_MAX = Gauge(
    "rtdp_stream_kafka_max_offsets_behind_latest",
    "Largest per-partition offset lag reported by the Spark Kafka source")
STREAM_KAFKA_LAG_AVG = Gauge(
    "rtdp_stream_kafka_avg_offsets_behind_latest",
    "Mean per-partition offset lag reported by the Spark Kafka source")
