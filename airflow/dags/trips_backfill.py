"""Backfill: reprocess an arbitrary time window from Cassandra into Postgres.

Triggered manually with a window, not scheduled:

    airflow dags trigger trips_backfill \\
      --conf '{"start":"2026-08-13T00:00:00Z","end":"2026-08-14T00:00:00Z"}'

Why this is safe to run at any time, on any window, as many times as you like:
the load is an upsert on trip_id. Reprocessing a window that is already loaded
converges to the same rows. That property is inherited from the same decision
that makes the streaming path replay-safe - keys derived from the data - and it
is what separates a backfill you can run during an incident from one you have
to schedule at 3am with a truncate in front of it.

The window is walked hour by hour rather than issued as one enormous range
query, because `trips_by_city_hour` is partitioned by (city, hour). Hour-sized
reads are bounded partition reads; a multi-day range would be a scan.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param

from rtdp_common import (CITIES, DEFAULT_ARGS, cassandra_session, emit_alert,
                         pg_conn, sla_miss_callback)

log = logging.getLogger(__name__)

MAX_HOURS = 24 * 14      # a fortnight; beyond that, run it in chunks on purpose


def _parse(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


with DAG(
    dag_id="trips_backfill",
    description="Reprocess a time window from Cassandra into Postgres (manual)",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
    params={
        "start": Param("2026-08-13T00:00:00Z", type="string",
                       description="window start, ISO-8601 UTC"),
        "end": Param("2026-08-14T00:00:00Z", type="string",
                     description="window end (exclusive), ISO-8601 UTC"),
    },
    tags=["rtdp", "batch", "backfill", "manual"],
) as dag:

    @task(task_id="plan_window")
    def plan_window(**context) -> Dict[str, Any]:
        conf = context["params"]
        start, end = _parse(conf["start"]), _parse(conf["end"])
        if end <= start:
            raise ValueError(f"end {end} must be after start {start}")
        hours = int((end - start).total_seconds() // 3600)
        if hours > MAX_HOURS:
            raise ValueError(
                f"window spans {hours}h, above the {MAX_HOURS}h ceiling. "
                f"Run it in chunks - one enormous backfill competing with live "
                f"ingest for the same ring is how a backfill causes an outage.")
        log.info("backfill plan: %s .. %s (%d hours)", start, end, hours)

        conn = pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO backfill_audit (window_start, window_end, "
                    "dag_run_id, notes) VALUES (%s,%s,%s,%s) RETURNING id",
                    (start, end, context["run_id"], f"{hours} hours planned"))
                audit_id = cur.fetchone()[0]
            conn.commit()
        finally:
            conn.close()

        return {"start": start.isoformat(), "end": end.isoformat(),
                "hours": hours, "audit_id": audit_id}

    @task(task_id="reprocess_window", sla=timedelta(hours=2))
    def reprocess_window(plan: Dict[str, Any]) -> Dict[str, Any]:
        from psycopg2.extras import execute_values
        start, end = _parse(plan["start"]), _parse(plan["end"])
        cluster, session = cassandra_session()
        conn = pg_conn()
        total_read = total_loaded = 0
        empty_hours = 0

        try:
            hour = start
            while hour < end:
                payload: List[tuple] = []
                for city in CITIES:
                    res = session.execute(
                        "SELECT trip_id, event_id, event_time, driver_id, "
                        "rider_id, pickup_zone_id, dropoff_zone_id, "
                        "vehicle_class, payment_type, distance_km, duration_sec, "
                        "fare_amount, surge_multiplier, status "
                        "FROM trips_by_city_hour WHERE city_id=%s AND event_hour=%s",
                        (city, hour))
                    for r in res:
                        payload.append((
                            str(r.trip_id), str(r.event_id), r.event_time, city,
                            r.driver_id, r.rider_id, r.pickup_zone_id,
                            r.dropoff_zone_id, r.vehicle_class, r.payment_type,
                            float(r.distance_km or 0), int(r.duration_sec or 0),
                            float(r.fare_amount or 0),
                            float(r.surge_multiplier or 1.0), r.status))

                if payload:
                    with conn.cursor() as cur:
                        execute_values(cur, """
                            INSERT INTO trips_rollup_stage (
                                trip_id, event_id, event_time, city_id, driver_id,
                                rider_id, pickup_zone_id, dropoff_zone_id,
                                vehicle_class, payment_type, distance_km,
                                duration_sec, fare_amount, surge_multiplier, status)
                            VALUES %s
                            ON CONFLICT (trip_id) DO UPDATE SET
                                event_time = EXCLUDED.event_time,
                                fare_amount = EXCLUDED.fare_amount,
                                status = EXCLUDED.status,
                                loaded_at = now()
                        """, payload, page_size=1000)
                    conn.commit()
                    total_loaded += len(payload)
                else:
                    empty_hours += 1
                total_read += len(payload)
                log.info("  %s: %d rows", hour.isoformat(), len(payload))
                hour += timedelta(hours=1)
        finally:
            cluster.shutdown()
            conn.close()

        return {**plan, "rows_read": total_read, "rows_loaded": total_loaded,
                "empty_hours": empty_hours}

    @task(task_id="close_audit")
    def close_audit(result: Dict[str, Any]) -> Dict[str, Any]:
        conn = pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE backfill_audit SET finished_at = now(), "
                    "rows_loaded = %s, notes = %s WHERE id = %s",
                    (result["rows_loaded"],
                     f"{result['hours']}h window, {result['empty_hours']} empty hours",
                     result["audit_id"]))
            conn.commit()
        finally:
            conn.close()

        if result["empty_hours"] == result["hours"]:
            # Not a failure - the window may genuinely predate any data - but
            # a backfill that loaded nothing at all is almost always a wrong
            # window, and saying so beats a green tick that means nothing.
            emit_alert("backfill_empty", result)
        log.info("backfill complete: %d rows over %d hours",
                 result["rows_loaded"], result["hours"])
        return result

    close_audit(reprocess_window(plan_window()))
