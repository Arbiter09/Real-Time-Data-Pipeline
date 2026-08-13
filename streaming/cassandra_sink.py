"""Executor-side Cassandra writer with measured retry behaviour.

This runs inside `mapPartitions`, so everything here executes on Spark
executors. Two consequences shape the design:

  * The Cassandra session is a module-level singleton. Spark reuses Python
    worker processes across micro-batches, so a session built on the first
    batch is reused by every later batch on that executor. Building one per
    batch would add a full connection+prepare cycle to every trigger and would
    dominate the latency being measured.

  * Nothing here talks to Kafka. Failed records are RETURNED as data and the
    driver writes them to the DLQ through Spark's Kafka sink. That keeps one
    Kafka client in the streaming path, and it means a DLQ write inherits the
    same delivery guarantees as any other Spark Kafka write.

Retry accounting is deliberately explicit because "average retries to success"
is a number this project has to defend:

    fast path   -> execute_concurrent, one attempt per statement
    retry path  -> call_with_retry over the failures only, with backoff
    exhausted   -> DLQ record carrying payload, reason and retry count
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

from cassandra import ConsistencyLevel, InvalidRequest, OperationTimedOut, Unavailable, WriteTimeout
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, NoHostAvailable, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.concurrent import execute_concurrent
from cassandra.query import PreparedStatement

from common import dlq as dlqmod
from common.config import RetryConfig
from common.retry import BackoffPolicy, RetriesExhausted, RetryStats, batched, call_with_retry

# Errors worth retrying: the coordinator was busy, a replica was slow, a node
# was bouncing. All of these can succeed on a later attempt.
RETRYABLE = (WriteTimeout, Unavailable, OperationTimedOut, NoHostAvailable, ConnectionError)
# Errors that will fail identically forever. Retrying these just delays the DLQ.
PERMANENT = (InvalidRequest, TypeError, ValueError)

_cluster: Optional[Cluster] = None
_session: Optional[Session] = None
_prepared: Dict[str, PreparedStatement] = {}


class PermanentDefect(Exception):
    """Raised by validation for records that can never be written as-is."""


# ---------------------------------------------------------------------------
# CQL
# ---------------------------------------------------------------------------
CQL = {
    "trips_by_id": """
        INSERT INTO {ks}.trips_by_id (
            trip_id, event_id, event_time, produced_at_ms, ingested_at_ms,
            city_id, driver_id, rider_id, pickup_zone_id, dropoff_zone_id,
            vehicle_class, payment_type, distance_km, duration_sec,
            fare_amount, surge_multiplier, status, producer_id, schema_version)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    "trips_by_driver_day": """
        INSERT INTO {ks}.trips_by_driver_day (
            driver_id, event_date, event_time, trip_id, event_id, city_id,
            rider_id, vehicle_class, payment_type, distance_km, duration_sec,
            fare_amount, surge_multiplier, status, ingested_at_ms)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    "trips_by_city_hour": """
        INSERT INTO {ks}.trips_by_city_hour (
            city_id, event_hour, trip_id, event_id, event_time, driver_id,
            rider_id, pickup_zone_id, dropoff_zone_id, vehicle_class,
            payment_type, distance_km, duration_sec, fare_amount,
            surge_multiplier, status, ingested_at_ms)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    "latency_samples": """
        INSERT INTO {ks}.latency_samples (
            run_id, minute_bucket, event_id, produced_at_ms, written_at_ms,
            latency_ms, batch_id)
        VALUES (?,?,?,?,?,?,?)""",
}

CORE_TABLES = ["trips_by_id"]
ALL_TABLES = ["trips_by_id", "trips_by_driver_day", "trips_by_city_hour"]


def get_session(conf: Dict[str, Any]) -> Tuple[Session, Dict[str, PreparedStatement]]:
    """Build (or reuse) this executor's Cassandra session and statements."""
    global _cluster, _session, _prepared
    if _session is not None and not _session.is_shutdown:
        return _session, _prepared

    consistency = getattr(ConsistencyLevel, conf["write_consistency"])
    profile = ExecutionProfile(
        # Token-aware routing sends each write straight to a replica, so the
        # coordinator is also a replica and one network hop disappears. On a
        # QUORUM write that hop is a meaningful share of the latency budget.
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=conf.get("local_dc", "dc1"))),
        consistency_level=consistency,
        request_timeout=conf.get("request_timeout", 10.0),
    )
    _cluster = Cluster(
        contact_points=conf["hosts"],
        port=conf.get("port", 9042),
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        protocol_version=conf.get("protocol_version", 5),
        connect_timeout=15,
        idle_heartbeat_interval=15,
    )
    _session = _cluster.connect(conf["keyspace"])
    _prepared = {}
    for name, cql in CQL.items():
        stmt = _session.prepare(cql.format(ks=conf["keyspace"]))
        stmt.consistency_level = consistency
        _prepared[name] = stmt
    # Measurement rows are not business data: a lost latency sample costs a
    # data point, so they are written at ONE rather than burning quorum budget.
    _prepared["latency_samples"].consistency_level = ConsistencyLevel.ONE
    return _session, _prepared


# ---------------------------------------------------------------------------
# Row -> CQL parameters
# ---------------------------------------------------------------------------
def _validate(row: Dict[str, Any]) -> None:
    """Reject records that can never be written. These skip the retry budget.

    Retrying a null fare four times with 1s/2s/4s backoff spends seven seconds
    to reach the same DLQ it could have reached immediately.
    """
    for field in ("event_id", "trip_id", "event_time", "city_id", "driver_id"):
        if row.get(field) in (None, ""):
            raise PermanentDefect(f"missing required field {field!r}")
    if row.get("fare_amount") is None:
        raise PermanentDefect("fare_amount is null")
    if row.get("produced_at_ms") is None:
        raise PermanentDefect("produced_at_ms is null")


def build_statements(
    row: Dict[str, Any],
    tables: List[str],
    ingested_at_ms: int,
) -> List[Tuple[str, tuple]]:
    """Return [(prepared_key, params), ...] for one event."""
    _validate(row)

    trip_uuid = uuid.UUID(row["trip_id"])
    event_uuid = uuid.UUID(row["event_id"])
    et: datetime = row["event_time"]          # Spark hands us a datetime
    if et.tzinfo is None:
        et = et.replace(tzinfo=timezone.utc)
    event_date = et.date()
    event_hour = et.replace(minute=0, second=0, microsecond=0)

    out: List[Tuple[str, tuple]] = []

    if "trips_by_id" in tables:
        out.append(("trips_by_id", (
            trip_uuid, event_uuid, et, row["produced_at_ms"], ingested_at_ms,
            row["city_id"], row["driver_id"], row["rider_id"],
            row["pickup_zone_id"], row["dropoff_zone_id"], row["vehicle_class"],
            row["payment_type"], row["distance_km"], row["duration_sec"],
            row["fare_amount"], row["surge_multiplier"], row["status"],
            row["producer_id"], row["schema_version"])))

    if "trips_by_driver_day" in tables:
        out.append(("trips_by_driver_day", (
            row["driver_id"], event_date, et, trip_uuid, event_uuid,
            row["city_id"], row["rider_id"], row["vehicle_class"],
            row["payment_type"], row["distance_km"], row["duration_sec"],
            row["fare_amount"], row["surge_multiplier"], row["status"],
            ingested_at_ms)))

    if "trips_by_city_hour" in tables:
        out.append(("trips_by_city_hour", (
            row["city_id"], event_hour, trip_uuid, event_uuid, et,
            row["driver_id"], row["rider_id"], row["pickup_zone_id"],
            row["dropoff_zone_id"], row["vehicle_class"], row["payment_type"],
            row["distance_km"], row["duration_sec"], row["fare_amount"],
            row["surge_multiplier"], row["status"], ingested_at_ms)))

    return out


def _row_json(row: Dict[str, Any]) -> str:
    """Re-serialize a row for the DLQ payload.

    The DLQ must carry something replayable. `_raw` is the original Kafka
    value, kept on the row precisely so a quarantined record round-trips
    byte-for-byte rather than as this pipeline's reinterpretation of it.
    """
    raw = row.get("_raw")
    if raw:
        return raw
    return json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v)
                       for k, v in row.items() if not k.startswith("_")},
                      separators=(",", ":"), default=str)


# ---------------------------------------------------------------------------
# The partition writer
# ---------------------------------------------------------------------------
def write_partition(rows: Iterator, conf: Dict[str, Any]) -> Iterator[tuple]:
    """Write a partition's rows to Cassandra; yield ('dlq'|'stats', key, value).

    Yielding rather than side-effecting is what lets the driver route DLQ
    records through Spark's Kafka sink and collect exact per-partition stats
    without relying on accumulators (which Spark may double-count on task
    retry).
    """
    session, prepared = get_session(conf)
    retry_cfg = RetryConfig()
    tables = conf["tables"]
    batch_id = conf.get("batch_id", -1)
    run_id = conf.get("run_id", "adhoc")
    sample_rate = conf.get("latency_sample_rate", 0.0)
    chunk_size = conf.get("chunk_size", 400)
    concurrency = conf.get("concurrency", 48)

    # The retry path gets one fewer attempt than the configured budget because
    # the concurrent fast path already spent attempt #1.
    retry_policy = BackoffPolicy(RetryConfig(
        backoff_schedule_ms=retry_cfg.backoff_schedule_ms,
        max_attempts=max(1, retry_cfg.max_attempts - 1),
        jitter_ratio=retry_cfg.jitter_ratio,
    ))

    stats = RetryStats()
    rows_written = 0
    dlq_emitted = 0
    latency_sum = 0
    latency_count = 0
    latency_max = 0
    validation_rejects = 0
    retried_writes = 0            # writes that needed >= 1 retry
    retried_writes_retries = 0    # total retries spent on those
    t_start = time.time()

    import random as _random
    rnd = _random.Random(f"{run_id}:{batch_id}")

    for chunk in batched(rows, chunk_size):
        work: List[Tuple[Dict[str, Any], str, tuple]] = []
        latency_work: List[tuple] = []
        ingested_at_ms = int(time.time() * 1000)

        for spark_row in chunk:
            row = spark_row.asDict() if hasattr(spark_row, "asDict") else dict(spark_row)
            try:
                for key, params in build_statements(row, tables, ingested_at_ms):
                    work.append((row, key, params))
            except PermanentDefect as exc:
                validation_rejects += 1
                dlq_emitted += 1
                rec = dlqmod.build(_row_json(row), f"PermanentDefect: {exc}",
                                   dlqmod.STAGE_PARSE, retry_count=0,
                                   source_topic=conf.get("source_topic"))
                yield ("dlq", rec.event_id or "unknown", rec.to_json())
                continue
            except Exception as exc:
                dlq_emitted += 1
                rec = dlqmod.build(_row_json(row), f"{type(exc).__name__}: {exc}",
                                   dlqmod.STAGE_PARSE, retry_count=0,
                                   source_topic=conf.get("source_topic"))
                yield ("dlq", rec.event_id or "unknown", rec.to_json())
                continue

            # Latency sampling. produced_at_ms is producer wall clock; the
            # write timestamp is taken here, immediately before the write is
            # issued, so the sample brackets the pipeline and not this loop.
            if sample_rate > 0 and rnd.random() < sample_rate:
                lat = ingested_at_ms - int(row["produced_at_ms"])
                if lat >= 0:
                    latency_sum += lat
                    latency_count += 1
                    latency_max = max(latency_max, lat)
                    latency_work.append((
                        run_id,
                        int(row["produced_at_ms"] // 60000),
                        uuid.UUID(row["event_id"]),
                        int(row["produced_at_ms"]),
                        ingested_at_ms,
                        int(lat),
                        int(batch_id),
                    ))

        if not work:
            continue

        # ---- fast path: one attempt per statement, fully concurrent --------
        requests = [(prepared[key], params) for _, key, params in work]
        results = execute_concurrent(session, requests, concurrency=concurrency,
                                     raise_on_first_error=False)

        failures: List[Tuple[Dict[str, Any], str, tuple, BaseException]] = []
        for (row, key, params), (ok, outcome) in zip(work, results):
            stats.attempts += 1
            if ok:
                stats.successes += 1
                rows_written += 1
            else:
                stats.record_reason(outcome if isinstance(outcome, BaseException)
                                    else Exception(str(outcome)))
                failures.append((row, key, params, outcome))

        # ---- retry path: only the failures, with the documented backoff ----
        for row, key, params, first_exc in failures:
            if isinstance(first_exc, PERMANENT):
                dlq_emitted += 1
                rec = dlqmod.build(_row_json(row),
                                   f"{type(first_exc).__name__}: {first_exc}",
                                   dlqmod.STAGE_CASSANDRA_SINK, retry_count=0,
                                   source_topic=conf.get("source_topic"))
                yield ("dlq", rec.event_id or "unknown", rec.to_json())
                continue

            attempt_stats = RetryStats()
            succeeded = False
            try:
                call_with_retry(
                    lambda k=key, p=params: session.execute(prepared[k], p),
                    policy=retry_policy,
                    stats=attempt_stats,
                    retry_on=RETRYABLE,
                    give_up_on=PERMANENT,
                )
                succeeded = True
            except RetriesExhausted as exc:
                dlq_emitted += 1
                rec = dlqmod.build(
                    _row_json(row),
                    f"{type(exc.last_error).__name__}: {exc.last_error}",
                    dlqmod.STAGE_CASSANDRA_SINK,
                    retry_count=exc.attempts + 1,   # +1: the fast-path attempt
                    source_topic=conf.get("source_topic"))
                yield ("dlq", rec.event_id or "unknown", rec.to_json())

            # Fold this write's attempts into the partition totals. The fast
            # path already spent one attempt and recorded its own failure
            # reason, so only the retry-loop attempts are merged here.
            stats.merge(attempt_stats)

            if succeeded:
                rows_written += 1
                retried_writes += 1
                # Retries spent on this write = the fast-path failure (1) plus
                # any further retries inside the loop. Counting only the loop's
                # retries would understate every write by exactly one.
                spent = attempt_stats.retries_before_success + 1
                retried_writes_retries += spent
                stats.retries_before_success += 1

        # ---- latency samples: fire and forget, never retried ---------------
        if latency_work:
            try:
                execute_concurrent(
                    session,
                    [(prepared["latency_samples"], p) for p in latency_work],
                    concurrency=concurrency, raise_on_first_error=False)
            except Exception:
                pass   # measurement loss is acceptable; business loss is not
            latency_work = []

    payload = {
        "rows_written": rows_written,
        "dlq_emitted": dlq_emitted,
        "validation_rejects": validation_rejects,
        "retried_writes": retried_writes,
        "retried_writes_retries": retried_writes_retries,
        "latency_sum_ms": latency_sum,
        "latency_count": latency_count,
        "latency_max_ms": latency_max,
        "partition_seconds": round(time.time() - t_start, 4),
        "retry_stats": stats.as_dict(),
    }
    yield ("stats", "stats", json.dumps(payload, separators=(",", ":")))


def shutdown() -> None:
    global _cluster, _session
    if _session is not None:
        _session.shutdown()
    if _cluster is not None:
        _cluster.shutdown()
    _session = _cluster = None
