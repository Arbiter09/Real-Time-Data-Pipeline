"""Hourly Cassandra -> PostgreSQL rollup: the pipeline's batch layer.

This is the DAG that has to earn Airflow's place in the architecture. What it
does that a cron line could not:

  * A SENSOR gates the extract. The rollup for hour H does not start because
    the clock says H+1; it starts when data for hour H has actually landed in
    Cassandra. Under lag, a timer-triggered job would happily roll up a
    half-written hour and write a wrong number that nobody notices.
  * An SLA on the critical-path task, with alerting on the MISS rather than
    only on failure. A rollup that finishes 40 minutes late has not failed.
  * Retries with exponential backoff at the task level.
  * Idempotent loads keyed on trip_id, so a retry or a manual re-run replaces
    the hour rather than double-counting it.

The extract reads `trips_by_city_hour`, whose partition key is (city_id,
event_hour). One hour is therefore a bounded set of 8 partition reads, not a
full-table scan. Scanning Cassandra by time without a time-bucketed partition
key is the standard way to take a ring down.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.sensors.python import PythonSensor

from rtdp_common import (CITIES, DEFAULT_ARGS, cassandra_session, emit_alert,
                         hour_bounds, pg_conn, sla_miss_callback)

log = logging.getLogger(__name__)

MIN_ROWS_TO_PROCEED = 1     # an hour with genuinely no trips is still valid


def _hour_for(context) -> datetime:
    """The hour that just CLOSED, as a plain stdlib datetime.

    Rolling up the current hour would produce a partial number that a later
    run would silently contradict.

    The conversion to a native `datetime` is load-bearing, not tidiness.
    Airflow hands over a `pendulum.DateTime`, and the Cassandra driver has no
    encoder registered for that type - so instead of binding it as a timestamp
    it falls back to `str()` and splices the value into the CQL unquoted,
    producing `event_hour=2026-08-14 06:00:00+00:00` and a syntax error. The
    driver gives no hint that a parameter type is unsupported; it just emits
    bad CQL.
    """
    logical = context["logical_date"]
    hour = logical.replace(minute=0, second=0, microsecond=0)
    return datetime(hour.year, hour.month, hour.day, hour.hour,
                    tzinfo=timezone.utc)


with DAG(
    dag_id="trips_batch_rollup",
    description="Hourly Cassandra -> Postgres star-schema rollup",
    default_args=DEFAULT_ARGS,
    schedule="10 * * * *",          # 10 past, giving the closed hour time to settle
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
    tags=["rtdp", "batch", "serving"],
) as dag:

    def _data_has_landed(**context) -> bool:
        """Sensor: has hour H actually got data in Cassandra yet?

        Returns False to keep poking rather than raising, so a slow upstream
        shows as a waiting sensor instead of a burnt retry budget.
        """
        hour = _hour_for(context)
        cluster, session = cassandra_session()
        try:
            total = 0
            for city in CITIES:
                row = session.execute(
                    "SELECT COUNT(*) AS c FROM trips_by_city_hour "
                    "WHERE city_id=%s AND event_hour=%s", (city, hour)).one()
                total += int(row.c or 0)
            log.info("sensor: hour=%s rows_in_cassandra=%d", hour.isoformat(), total)
            context["ti"].xcom_push(key="sensed_rows", value=total)
            return total >= MIN_ROWS_TO_PROCEED
        finally:
            cluster.shutdown()

    wait_for_data = PythonSensor(
        task_id="wait_for_hour_data",
        python_callable=_data_has_landed,
        poke_interval=60,
        timeout=60 * 45,
        mode="reschedule",      # frees the worker slot between pokes
        soft_fail=False,
    )

    @task(
        task_id="extract_and_load_hour",
        # SLA on the critical path. Missing it fires sla_miss_callback, which
        # alerts even though the task itself may go on to succeed.
        sla=timedelta(minutes=20),
        retries=3,
    )
    def extract_and_load_hour(**context) -> Dict[str, Any]:
        hour = _hour_for(context)
        start, end = hour_bounds(hour)
        cluster, session = cassandra_session()
        rows: List[tuple] = []
        try:
            for city in CITIES:
                res = session.execute(
                    "SELECT trip_id, event_id, event_time, driver_id, rider_id, "
                    "pickup_zone_id, dropoff_zone_id, vehicle_class, payment_type, "
                    "distance_km, duration_sec, fare_amount, surge_multiplier, status "
                    "FROM trips_by_city_hour WHERE city_id=%s AND event_hour=%s",
                    (city, hour))
                for r in res:
                    rows.append((city, r))
        finally:
            cluster.shutdown()

        log.info("extracted %d rows for %s", len(rows), hour.isoformat())
        if not rows:
            return {"hour": hour.isoformat(), "extracted": 0, "loaded": 0}

        from psycopg2.extras import execute_values
        conn = pg_conn()
        try:
            with conn.cursor() as cur:
                # The load is idempotent on trip_id. A retried task, a manual
                # re-run, or a backfill over the same hour all converge to the
                # same table state instead of doubling the revenue figure.
                payload = []
                for city, r in rows:
                    payload.append((
                        str(r.trip_id), str(r.event_id), r.event_time, city,
                        r.driver_id, r.rider_id, r.pickup_zone_id,
                        r.dropoff_zone_id, r.vehicle_class, r.payment_type,
                        float(r.distance_km or 0), int(r.duration_sec or 0),
                        float(r.fare_amount or 0), float(r.surge_multiplier or 1.0),
                        r.status))
                execute_values(cur, """
                    INSERT INTO trips_rollup_stage (
                        trip_id, event_id, event_time, city_id, driver_id,
                        rider_id, pickup_zone_id, dropoff_zone_id, vehicle_class,
                        payment_type, distance_km, duration_sec, fare_amount,
                        surge_multiplier, status)
                    VALUES %s
                    ON CONFLICT (trip_id) DO UPDATE SET
                        event_time = EXCLUDED.event_time,
                        fare_amount = EXCLUDED.fare_amount,
                        status = EXCLUDED.status,
                        loaded_at = now()
                """, payload, page_size=1000)
                loaded = cur.rowcount
            conn.commit()
        finally:
            conn.close()

        return {"hour": hour.isoformat(), "extracted": len(rows), "loaded": loaded}

    @task(task_id="verify_hour")
    def verify_hour(result: Dict[str, Any], **context) -> Dict[str, Any]:
        """Reconcile what Cassandra held against what Postgres now holds.

        A load that silently drops rows is exactly the failure this whole
        project is about, so the batch layer checks itself rather than assuming
        the INSERT worked.
        """
        hour = datetime.fromisoformat(result["hour"])
        start, end = hour_bounds(hour)
        conn = pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM trips_rollup_stage "
                    "WHERE event_time >= %s AND event_time < %s", (start, end))
                in_pg = cur.fetchone()[0]
        finally:
            conn.close()

        extracted = result["extracted"]
        drift = extracted - in_pg
        out = {**result, "in_postgres": in_pg, "drift": drift}
        if extracted and drift != 0:
            emit_alert("rollup_drift", out)
            raise ValueError(
                f"rollup drift for {result['hour']}: extracted {extracted} "
                f"but Postgres holds {in_pg}")
        log.info("verified %s: %d rows", result["hour"], in_pg)
        return out

    loaded = extract_and_load_hour()
    wait_for_data >> loaded
    verify_hour(loaded)
