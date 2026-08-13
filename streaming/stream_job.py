"""Spark Structured Streaming: trips.raw -> Cassandra, with DLQ and watermarking.

Delivery semantics implemented here, stated precisely:

    Kafka -> Spark            at-least-once (offsets committed to the
                              checkpoint AFTER the batch's writes complete, so
                              a crash mid-batch re-reads that batch)
    Spark -> Cassandra        idempotent upsert on a primary key derived
                              entirely from event fields
    ------------------------------------------------------------------
    net effect                EFFECTIVELY-ONCE. Not exactly-once: no Kafka
                              transaction spans the Cassandra write, and none
                              could - Cassandra is not a transactional
                              participant. Re-delivery happens and is absorbed
                              by the key design, which is a different and
                              weaker claim than exactly-once.

WATERMARKING - why it is enforced by hand.

`withWatermark` is declared so the allowed-lateness contract lives in one
place, but late records are classified and routed inside foreachBatch rather
than by a stateful operator. Spark's stateful operators DROP beyond-watermark
rows silently, and silent drops are exactly what this project claims not to
have. Classifying lateness in the batch means every late event is counted and,
under the default policy, quarantined to the DLQ where it stays replayable.

The optional --with-rollup query is where the watermark drives a real stateful
operator (an event-time windowed aggregation). It is off during throughput
benchmarks so its state store does not distort the ingest numbers.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType)

from common import metrics
from common.config import CassandraConfig, KafkaConfig, RetryConfig, StreamConfig
from streaming import cassandra_sink

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-5s [stream] %(message)s",
)
log = logging.getLogger("stream_job")

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("event_time", StringType()),
    StructField("produced_at_ms", LongType()),
    StructField("city_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("rider_id", StringType()),
    StructField("pickup_zone_id", IntegerType()),
    StructField("dropoff_zone_id", IntegerType()),
    StructField("vehicle_class", StringType()),
    StructField("payment_type", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("duration_sec", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("surge_multiplier", DoubleType()),
    StructField("status", StringType()),
    StructField("producer_id", StringType()),
    StructField("schema_version", IntegerType()),
])

OUTCOME_SCHEMA = StructType([
    StructField("kind", StringType()),
    StructField("key", StringType()),
    StructField("value", StringType()),
])


def parse_watermark_seconds(spec: str) -> int:
    """'2 minutes' -> 120. Kept tiny and explicit; no dependency needed."""
    parts = spec.strip().split()
    if len(parts) != 2:
        raise ValueError(f"cannot parse watermark spec {spec!r}")
    n, unit = int(parts[0]), parts[1].rstrip("s")
    factor = {"second": 1, "minute": 60, "hour": 3600, "millisecond": 0.001}[unit]
    return int(n * factor)


class TripStream:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.kcfg = KafkaConfig()
        self.ccfg = CassandraConfig()
        self.scfg = StreamConfig()
        self.rcfg = RetryConfig()

        self.allowed_lateness_sec = parse_watermark_seconds(self.scfg.watermark_delay)
        self.late_policy = self.scfg.late_event_policy

        # Driver-side watermark tracking. Mirrors Spark's own semantics: the
        # watermark applied to batch N is derived from the max event time seen
        # up to batch N-1, so a batch can never invalidate its own rows.
        self._max_event_ms: Optional[int] = None
        self._batch_reports: List[Dict[str, Any]] = []
        self._lag_samples: List[Dict[str, Any]] = []
        self._stop = threading.Event()

        self.spark = self._build_session()
        self.sink_conf = {
            "hosts": self.ccfg.hosts,
            "keyspace": self.ccfg.keyspace,
            "write_consistency": self.ccfg.write_consistency,
            "tables": (cassandra_sink.ALL_TABLES if args.tables == "all"
                       else cassandra_sink.CORE_TABLES),
            "latency_sample_rate": args.latency_sample_rate,
            "run_id": args.run_id,
            "source_topic": self.kcfg.topic_trips,
            "concurrency": args.write_concurrency,
            "chunk_size": args.chunk_size,
            "request_timeout": args.cassandra_timeout,
        }
        log.info("tables=%s consistency=%s %s late_policy=%s allowed_lateness=%ds",
                 self.sink_conf["tables"], self.ccfg.write_consistency,
                 self.rcfg.describe(), self.late_policy, self.allowed_lateness_sec)

    def _build_session(self) -> SparkSession:
        b = (SparkSession.builder
             .appName(f"rtdp-trips-{self.args.run_id}")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", str(self.args.shuffle_partitions))
             .config("spark.streaming.stopGracefullyOnShutdown", "true")
             # Executors are pinned so the topology reported alongside the
             # throughput number is the topology that actually ran.
             .config("spark.cores.max", str(self.args.total_cores))
             .config("spark.executor.cores", str(self.args.executor_cores))
             .config("spark.executor.memory", self.args.executor_memory)
             .config("spark.python.worker.reuse", "true")
             .config("spark.task.maxFailures", str(self.args.task_max_failures)))
        return b.getOrCreate()

    # -- pipeline ------------------------------------------------------------
    def _source(self) -> DataFrame:
        raw = (self.spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", self.kcfg.bootstrap)
               .option("subscribe", self.kcfg.topic_trips)
               .option("startingOffsets", self.args.starting_offsets)
               .option("maxOffsetsPerTrigger", str(self.args.max_offsets_per_trigger))
               .option("kafka.group.id", self.kcfg.consumer_group)
               # failOnDataLoss stays TRUE. If Kafka loses data under us, the
               # job must fail loudly - a pipeline that claims zero silent loss
               # cannot be configured to silently skip missing offsets.
               .option("failOnDataLoss", "true")
               .load())

        parsed = (raw
                  .select(
                      F.col("value").cast("string").alias("_raw"),
                      F.col("topic").alias("_topic"),
                      F.col("partition").alias("_partition"),
                      F.col("offset").alias("_offset"),
                      F.col("timestamp").alias("_kafka_ts"))
                  .withColumn("_parsed", F.from_json(F.col("_raw"), EVENT_SCHEMA)))

        # A record that will not parse has no event_id and no event_time. It is
        # separated here rather than being allowed to NPE downstream.
        return (parsed
                .withColumn("_unparseable", F.col("_parsed").isNull()
                            | F.col("_parsed.event_id").isNull())
                .select("_raw", "_topic", "_partition", "_offset", "_kafka_ts",
                        "_unparseable", F.col("_parsed.*"))
                .withColumn("event_time", F.to_timestamp(F.col("event_time")))
                .withWatermark("event_time", self.scfg.watermark_delay))

    # -- per-batch -----------------------------------------------------------
    def _process_batch(self, batch: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        spark = batch.sparkSession
        batch = batch.persist()

        try:
            total = batch.count()
            if total == 0:
                self._record_batch(batch_id, {}, 0, 0, 0, 0, time.time() - t0)
                return

            # ---- unparseable -> DLQ ---------------------------------------
            bad = batch.filter(F.col("_unparseable"))
            n_bad = bad.count()
            if n_bad:
                self._write_dlq(bad.select(
                    F.coalesce(F.col("event_id"), F.lit("unparseable")).alias("key"),
                    F.to_json(F.struct(
                        F.col("_raw").alias("original_payload"),
                        F.lit("JsonParseError: value did not match event schema").alias("failure_reason"),
                        F.lit("parse").alias("failure_stage"),
                        F.lit(0).alias("retry_count"),
                        (F.unix_timestamp() * 1000).cast("long").alias("first_failed_at_ms"),
                        (F.unix_timestamp() * 1000).cast("long").alias("quarantined_at_ms"),
                        F.col("_topic").alias("source_topic"),
                        F.col("_partition").alias("source_partition"),
                        F.col("_offset").alias("source_offset"),
                        F.lit(0).alias("replay_count"),
                        F.lit(1).alias("dlq_schema_version"),
                    )).alias("value")))

            good = batch.filter(~F.col("_unparseable"))

            # ---- watermark enforcement ------------------------------------
            # Applied against the PREVIOUS high-water mark, exactly as Spark's
            # own watermark lags one batch behind the data.
            n_late = 0
            if self._max_event_ms is not None and self.late_policy != "process":
                cutoff_ms = self._max_event_ms - self.allowed_lateness_sec * 1000
                cutoff = datetime.fromtimestamp(cutoff_ms / 1000.0, tz=timezone.utc)
                late = good.filter(F.col("event_time") < F.lit(cutoff))
                n_late = late.count()
                if n_late:
                    metrics.STREAM_LATE_EVENTS.inc(n_late)
                    if self.late_policy == "dlq":
                        # Quarantined, not dropped. A late event is still a
                        # real trip; the DLQ keeps it replayable into the
                        # batch layer rather than deleting it.
                        self._write_dlq(late.select(
                            F.col("event_id").alias("key"),
                            F.to_json(F.struct(
                                F.col("_raw").alias("original_payload"),
                                F.concat(
                                    F.lit("LateEvent: event_time older than watermark "),
                                    F.lit(cutoff.isoformat())).alias("failure_reason"),
                                F.lit("late_event").alias("failure_stage"),
                                F.lit(0).alias("retry_count"),
                                (F.unix_timestamp() * 1000).cast("long").alias("first_failed_at_ms"),
                                (F.unix_timestamp() * 1000).cast("long").alias("quarantined_at_ms"),
                                F.col("event_id").alias("event_id"),
                                F.col("_topic").alias("source_topic"),
                                F.col("_partition").alias("source_partition"),
                                F.col("_offset").alias("source_offset"),
                                F.lit(0).alias("replay_count"),
                                F.lit(1).alias("dlq_schema_version"),
                            )).alias("value")))
                    good = good.filter(F.col("event_time") >= F.lit(cutoff))

            # ---- Cassandra write ------------------------------------------
            conf = dict(self.sink_conf, batch_id=batch_id)
            outcomes = (good.rdd
                        .mapPartitions(lambda it: cassandra_sink.write_partition(it, conf))
                        .toDF(OUTCOME_SCHEMA)
                        .persist())

            # One action materializes the writes; both the DLQ rows and the
            # per-partition stats fall out of the same pass. Recomputation on
            # task retry is safe precisely because the writes are upserts.
            dlq_rows = outcomes.filter(F.col("kind") == "dlq")
            stats_rows = outcomes.filter(F.col("kind") == "stats").collect()

            n_dlq = dlq_rows.count()
            if n_dlq:
                self._write_dlq(dlq_rows.select("key", "value"))

            agg = self._merge_stats(stats_rows)
            outcomes.unpersist()

            # ---- advance the high-water mark ------------------------------
            hw = good.agg(F.max("event_time").alias("m")).collect()[0]["m"]
            if hw is not None:
                hw_ms = int(hw.replace(tzinfo=timezone.utc).timestamp() * 1000)
                self._max_event_ms = max(self._max_event_ms or 0, hw_ms)

            self._record_batch(batch_id, agg, total, n_bad, n_late, n_dlq,
                               time.time() - t0)
        finally:
            batch.unpersist()

    def _write_dlq(self, df: DataFrame) -> None:
        (df.write.format("kafka")
           .option("kafka.bootstrap.servers", self.kcfg.bootstrap)
           .option("topic", self.kcfg.topic_dlq)
           .option("kafka.enable.idempotence", "true")
           .option("kafka.acks", "all")
           .save())

    @staticmethod
    def _merge_stats(rows) -> Dict[str, Any]:
        agg: Dict[str, Any] = {
            "rows_written": 0, "dlq_emitted": 0, "validation_rejects": 0,
            "retried_writes": 0, "retried_writes_retries": 0,
            "latency_sum_ms": 0, "latency_count": 0, "latency_max_ms": 0,
            "attempts": 0, "successes": 0, "failures_exhausted": 0,
            "retries_before_success": 0, "total_backoff_ms": 0.0,
            "failure_reasons": {},
        }
        for r in rows:
            p = json.loads(r["value"])
            for k in ("rows_written", "dlq_emitted", "validation_rejects",
                      "retried_writes", "retried_writes_retries",
                      "latency_sum_ms", "latency_count"):
                agg[k] += p.get(k, 0)
            agg["latency_max_ms"] = max(agg["latency_max_ms"], p.get("latency_max_ms", 0))
            rs = p.get("retry_stats", {})
            for k in ("attempts", "successes", "failures_exhausted",
                      "retries_before_success", "total_backoff_ms"):
                agg[k] += rs.get(k, 0)
            for reason, n in (rs.get("failure_reasons") or {}).items():
                agg["failure_reasons"][reason] = agg["failure_reasons"].get(reason, 0) + n
        return agg

    # Every key the summary reader expects, so an empty batch produces a
    # report with the same shape as a full one rather than a sparse dict the
    # aggregator then has to defend against.
    _EMPTY_AGG: Dict[str, Any] = {
        "rows_written": 0, "dlq_emitted": 0, "validation_rejects": 0,
        "retried_writes": 0, "retried_writes_retries": 0,
        "latency_sum_ms": 0, "latency_count": 0, "latency_max_ms": 0,
        "attempts": 0, "successes": 0, "failures_exhausted": 0,
        "retries_before_success": 0, "total_backoff_ms": 0.0,
        "failure_reasons": {},
    }

    def _record_batch(self, batch_id: int, agg: Dict[str, Any], total: int,
                      n_bad: int, n_late: int, n_dlq: int, seconds: float) -> None:
        agg = {**self._EMPTY_AGG, **(agg or {})}
        rows_written = agg.get("rows_written", 0)
        metrics.STREAM_BATCH_DURATION.observe(seconds)
        metrics.STREAM_INPUT_ROWS.set(total)
        if rows_written:
            metrics.STREAM_ROWS_WRITTEN.labels(table="all").inc(rows_written)
        if agg.get("retries_before_success"):
            metrics.STREAM_CASSANDRA_RETRIES.inc(agg["retries_before_success"])
        if agg.get("failures_exhausted"):
            metrics.STREAM_CASSANDRA_EXHAUSTED.inc(agg["failures_exhausted"])
        if n_dlq:
            metrics.STREAM_DLQ.labels(stage="sink").inc(n_dlq)
        if n_bad:
            metrics.STREAM_DLQ.labels(stage="parse").inc(n_bad)
        if agg.get("latency_count"):
            mean = agg["latency_sum_ms"] / agg["latency_count"]
            metrics.STREAM_E2E_LATENCY.observe(mean)
        if seconds > 0:
            metrics.STREAM_PROCESSED_RATE.set(total / seconds)

        report = {
            "batch_id": batch_id,
            "wall_ts": time.time(),
            "input_rows": total,
            "unparseable": n_bad,
            "late": n_late,
            "dlq": n_dlq,
            "batch_seconds": round(seconds, 4),
            "rows_per_sec": round(total / seconds, 1) if seconds > 0 else 0,
            **{k: v for k, v in agg.items() if k != "failure_reasons"},
            "failure_reasons": agg.get("failure_reasons", {}),
        }
        self._batch_reports.append(report)
        if self.args.progress_file:
            with open(self.args.progress_file, "a") as fh:
                fh.write(json.dumps(report, separators=(",", ":")) + "\n")

        if batch_id % self.args.log_every == 0 or n_dlq or n_late:
            log.info("batch=%d in=%d written=%d late=%d dlq=%d %.2fs (%.0f rows/s)",
                     batch_id, total, rows_written, n_late, n_dlq, seconds,
                     report["rows_per_sec"])

    # -- optional stateful rollup -------------------------------------------
    def _start_rollup(self, source: DataFrame):
        """Event-time windowed aggregation - the watermark's stateful user.

        Deliberately a separate query: its state store and shuffle would
        otherwise be charged to the ingest path's throughput number.
        """
        rollup = (source
                  .filter(~F.col("_unparseable") & (F.col("status") == "completed"))
                  .groupBy(F.window(F.col("event_time"), self.args.rollup_window),
                           F.col("city_id"))
                  .agg(F.count("*").alias("trips"),
                       F.sum("fare_amount").alias("revenue"),
                       F.avg("duration_sec").alias("avg_duration_sec"),
                       F.approx_count_distinct("driver_id").alias("active_drivers")))

        def write_rollup(df: DataFrame, batch_id: int) -> None:
            rows = df.collect()
            if not rows:
                return
            from cassandra.cluster import Cluster
            cluster = Cluster(self.ccfg.hosts)
            try:
                sess = cluster.connect(self.ccfg.keyspace)
                stmt = sess.prepare(
                    "INSERT INTO city_minute_rollup (city_id, window_start, "
                    "window_end, trips, revenue, avg_duration_sec, "
                    "active_drivers, computed_at) VALUES (?,?,?,?,?,?,?,?)")
                now = datetime.now(timezone.utc)
                for r in rows:
                    sess.execute(stmt, (
                        r["city_id"], r["window"]["start"], r["window"]["end"],
                        r["trips"], float(r["revenue"] or 0.0),
                        float(r["avg_duration_sec"] or 0.0),
                        r["active_drivers"], now))
            finally:
                cluster.shutdown()

        return (rollup.writeStream
                .outputMode("update")
                .foreachBatch(write_rollup)
                .option("checkpointLocation", self.scfg.checkpoint_dir + "-rollup")
                .trigger(processingTime=self.args.rollup_trigger)
                .start())

    # -- lag tracking --------------------------------------------------------
    def _lag_monitor(self, query) -> None:
        """Poll the query's Kafka source metrics and export lag.

        WHY NOT kafka-exporter FOR THIS NUMBER:
        Spark Structured Streaming does not commit offsets to a Kafka consumer
        group - offsets live in the checkpoint directory, which is the whole
        point of checkpointed recovery. So `kafka-consumer-groups --describe`
        and any consumer-group lag exporter report nothing for this consumer.

        The lag published here is `offsetsBehindLatest` from Spark's own Kafka
        source, computed against the authoritative offset store. It is the same
        quantity a consumer-group lag metric would report, taken from the place
        that actually knows. kafka-exporter is still scraped, but for
        topic-level partition offsets, not for this consumer's lag.
        """
        while not self._stop.is_set() and query.isActive:
            try:
                prog = query.lastProgress
                if prog and prog.get("sources"):
                    src = prog["sources"][0]
                    m = src.get("metrics") or {}
                    sample = {
                        "wall_ts": time.time(),
                        "batch_id": prog.get("batchId"),
                        "input_rows_per_second": prog.get("inputRowsPerSecond"),
                        "processed_rows_per_second": prog.get("processedRowsPerSecond"),
                        "batch_duration_ms": prog.get("batchDuration"),
                        "max_offsets_behind": int(m.get("maxOffsetsBehindLatest", 0) or 0),
                        "min_offsets_behind": int(m.get("minOffsetsBehindLatest", 0) or 0),
                        "avg_offsets_behind": float(m.get("avgOffsetsBehindLatest", 0) or 0),
                    }
                    metrics.STREAM_KAFKA_LAG_MAX.set(sample["max_offsets_behind"])
                    metrics.STREAM_KAFKA_LAG_AVG.set(sample["avg_offsets_behind"])
                    if prog.get("processedRowsPerSecond") is not None:
                        metrics.STREAM_PROCESSED_RATE.set(prog["processedRowsPerSecond"])
                    self._lag_samples.append(sample)
                    if self.args.lag_file:
                        with open(self.args.lag_file, "a") as fh:
                            fh.write(json.dumps(sample, separators=(",", ":")) + "\n")
            except Exception as exc:      # never let monitoring kill the job
                log.debug("lag monitor: %s", exc)
            self._stop.wait(self.args.lag_interval)

    # -- run -----------------------------------------------------------------
    def run(self) -> int:
        source = self._source()

        writer = (source.writeStream
                  .outputMode("append")
                  .foreachBatch(self._process_batch)
                  .option("checkpointLocation", self.args.checkpoint_dir)
                  .queryName(f"trips-ingest-{self.args.run_id}"))
        if self.args.trigger_interval:
            writer = writer.trigger(processingTime=self.args.trigger_interval)
        query = writer.start()

        rollup_query = self._start_rollup(source) if self.args.with_rollup else None

        log.info("query id=%s checkpoint=%s", query.id, self.args.checkpoint_dir)

        lag_thread = threading.Thread(target=self._lag_monitor, args=(query,),
                                      daemon=True, name="lag-monitor")
        lag_thread.start()

        # A readiness marker the scenario runner polls for. Producing before
        # the stream is listening would silently shift the measurement window,
        # since the source starts from `latest` offsets.
        if self.args.ready_file:
            with open(self.args.ready_file, "w") as fh:
                json.dump({"query_id": str(query.id), "started_at": time.time()}, fh)

        def handle_stop(*_):
            log.info("shutdown signal - stopping query gracefully")
            self._stop.set()
        signal.signal(signal.SIGTERM, handle_stop)
        signal.signal(signal.SIGINT, handle_stop)

        deadline = time.time() + self.args.duration if self.args.duration > 0 else None
        try:
            while query.isActive:
                # A stop FILE, not a signal. `docker exec` does not forward
                # signals to the process it started, and pkill inside the
                # container matches both the JVM and the Python driver - killing
                # the JVM first loses the summary this whole run exists to
                # produce. A file check is unambiguous and needs no signal path.
                if self.args.stop_file and os.path.exists(self.args.stop_file):
                    log.info("stop file observed - shutting down")
                    break
                if self._stop.is_set() or (deadline and time.time() > deadline):
                    break
                query.awaitTermination(2)
        finally:
            for q in (rollup_query, query):
                if q is not None and q.isActive:
                    q.stop()
            self._write_summary(query)
            self.spark.stop()
        return 0

    def _write_summary(self, query) -> None:
        reports = self._batch_reports
        non_empty = [r for r in reports if r["input_rows"] > 0]
        total_in = sum(r["input_rows"] for r in reports)
        total_written = sum(r["rows_written"] for r in reports)
        total_dlq = sum(r["dlq"] for r in reports)
        total_late = sum(r["late"] for r in reports)
        total_bad = sum(r["unparseable"] for r in reports)
        successes = sum(r.get("successes", 0) for r in reports)
        retries = sum(r.get("retries_before_success", 0) for r in reports)
        retried_writes = sum(r.get("retried_writes", 0) for r in reports)
        retried_retries = sum(r.get("retried_writes_retries", 0) for r in reports)
        lat_sum = sum(r.get("latency_sum_ms", 0) for r in reports)
        lat_cnt = sum(r.get("latency_count", 0) for r in reports)
        reasons: Dict[str, int] = {}
        for r in reports:
            for k, v in (r.get("failure_reasons") or {}).items():
                reasons[k] = reasons.get(k, 0) + v

        durations = sorted(r["batch_seconds"] for r in non_empty)

        def pct(p: float) -> float:
            if not durations:
                return 0.0
            idx = min(len(durations) - 1, int(round(p / 100.0 * (len(durations) - 1))))
            return round(durations[idx], 4)

        summary = {
            "run_id": self.args.run_id,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "batches": len(reports),
            "non_empty_batches": len(non_empty),
            "input_rows": total_in,
            "rows_written_cassandra": total_written,
            "write_amplification": (round(total_written / total_in, 3)
                                    if total_in else None),
            "tables": self.sink_conf["tables"],
            "unparseable": total_bad,
            "late_events": total_late,
            "dlq_total": total_dlq,
            "cassandra_write_successes": successes,
            "cassandra_write_retries": retries,
            "avg_retries_to_success_all_writes": (
                round(retries / successes, 5) if successes else 0.0),
            "writes_needing_retry": retried_writes,
            "avg_retries_among_retried_writes": (
                round(retried_retries / retried_writes, 3) if retried_writes else 0.0),
            "mean_e2e_latency_ms_sampled": (
                round(lat_sum / lat_cnt, 2) if lat_cnt else None),
            "latency_samples": lat_cnt,
            "batch_seconds_p50": pct(50),
            "batch_seconds_p95": pct(95),
            "batch_seconds_p99": pct(99),
            "failure_reasons": dict(sorted(reasons.items())),
            "config": {
                "backoff": self.rcfg.describe(),
                "consistency": self.ccfg.write_consistency,
                "allowed_lateness_sec": self.allowed_lateness_sec,
                "late_policy": self.late_policy,
                "max_offsets_per_trigger": self.args.max_offsets_per_trigger,
                "trigger_interval": self.args.trigger_interval,
                "executor_cores": self.args.executor_cores,
                "total_cores": self.args.total_cores,
                "partitions": self.kcfg.partitions,
                "latency_sample_rate": self.args.latency_sample_rate,
            },
        }
        print(json.dumps(summary, indent=2))
        if self.args.summary_file:
            os.makedirs(os.path.dirname(self.args.summary_file), exist_ok=True)
            with open(self.args.summary_file, "w") as fh:
                json.dump(summary, fh, indent=2)
            log.info("summary -> %s", self.args.summary_file)


def main(argv: Optional[List[str]] = None) -> int:
    scfg = StreamConfig()
    p = argparse.ArgumentParser(description="Trips ingest stream")
    p.add_argument("--run-id", default=os.environ.get("RUN_ID", "adhoc"))
    p.add_argument("--duration", type=float, default=0, help="0 = run until stopped")
    p.add_argument("--checkpoint-dir", default=scfg.checkpoint_dir)
    p.add_argument("--starting-offsets", default="latest")
    p.add_argument("--max-offsets-per-trigger", type=int, default=scfg.max_offsets_per_trigger)
    p.add_argument("--trigger-interval", default="", help="e.g. '1 second'; empty = as fast as possible")
    p.add_argument("--tables", choices=["all", "core"], default=os.environ.get("CASSANDRA_TABLES", "all"))
    p.add_argument("--latency-sample-rate", type=float,
                   default=float(os.environ.get("LATENCY_SAMPLE_RATE", 0.05)))
    p.add_argument("--write-concurrency", type=int, default=48)
    p.add_argument("--chunk-size", type=int, default=400)
    p.add_argument("--cassandra-timeout", type=float, default=10.0)
    p.add_argument("--executor-cores", type=int, default=int(os.environ.get("SPARK_EXECUTOR_CORES", 3)))
    p.add_argument("--executor-memory", default=os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))
    p.add_argument("--total-cores", type=int, default=6)
    p.add_argument("--shuffle-partitions", type=int, default=6)
    p.add_argument("--task-max-failures", type=int, default=4)
    p.add_argument("--with-rollup", action="store_true")
    p.add_argument("--rollup-window", default="1 minute")
    p.add_argument("--rollup-trigger", default="30 seconds")
    p.add_argument("--metrics-port", type=int, default=9109)
    p.add_argument("--log-every", type=int, default=10)
    p.add_argument("--progress-file", default=None)
    p.add_argument("--lag-file", default=None)
    p.add_argument("--lag-interval", type=float, default=1.0)
    p.add_argument("--ready-file", default=None)
    p.add_argument("--stop-file", default=None)
    p.add_argument("--summary-file", default=None)
    args = p.parse_args(argv)

    for path in (args.progress_file, args.lag_file):
        if path:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            open(path, "w").close()
    for path in (args.ready_file, args.stop_file):
        if path and os.path.exists(path):
            os.remove(path)

    metrics.serve(args.metrics_port)
    return TripStream(args).run()


if __name__ == "__main__":
    sys.exit(main())
