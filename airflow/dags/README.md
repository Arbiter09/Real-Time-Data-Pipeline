# Airflow DAGs — WRITTEN BUT NOT EXECUTED

**Status, stated plainly: the three DAGs in this directory have not been run.**

Every other component in this repository has been executed and measured, and
the numbers in [RESULTS.md](../../RESULTS.md) come from harnesses that actually
ran. These DAGs are the exception. They are written and reviewed, but the
Airflow profile was never started, so they have not been parsed by a scheduler,
their imports have not been resolved at runtime, and no task in them has ever
executed.

Treat them as **design, not evidence**. A reviewer should assume they contain
the kind of errors that only a first run surfaces.

To verify them yourself:

```bash
make airflow                       # starts scheduler + webserver on :18088
docker exec rtdp-airflow-scheduler airflow dags list
docker exec rtdp-airflow-scheduler airflow dags list-import-errors
docker exec rtdp-airflow-scheduler airflow tasks test trips_batch_rollup extract_and_load_hour 2026-08-13
```

## What each DAG is for

| DAG | Schedule | Purpose |
|---|---|---|
| `trips_batch_rollup` | hourly, 10 past | Cassandra → Postgres rollup, gated by a sensor on data actually landing, with an SLA on the critical path |
| `dlq_drain` | every 15 min | Drains the DLQ back into the pipeline; refuses to run above a circuit-breaker ceiling |
| `trips_backfill` | manual | Reprocesses an arbitrary window; safe to re-run because the load upserts on `trip_id` |

## The design arguments they encode

These are the reasons Airflow is in the architecture at all rather than a cron
line, and they stand on their own merits even though the code is unverified:

- **The rollup is gated on data, not on the clock.** A timer-triggered job runs
  at H+1 whether or not hour H finished landing. Under consumer lag that rolls
  up a partial hour and writes a wrong number nobody notices. The sensor waits
  for rows to exist in `trips_by_city_hour` for that hour.

- **The SLA alerts on the miss, not only on failure.** A rollup that succeeds
  40 minutes late has not failed, and no failure callback will ever fire for
  it — but everything reading that table is looking at stale data.

- **The drain has a circuit breaker.** Draining a 50k-record DLQ caused by a
  dead Cassandra node just replays 50k failures into a still-dead node. Above
  the ceiling it refuses and alerts instead.

- **The backfill is safe to run during an incident.** The load upserts on
  `trip_id`, so reprocessing an already-loaded window converges instead of
  double-counting. That property is inherited from the same decision that makes
  the streaming path replay-safe: keys derived from the data.

- **The extract reads a time-bucketed table.** `trips_by_city_hour` is
  partitioned by `(city_id, event_hour)`, so one hour is 8 bounded partition
  reads rather than a full-ring scan.
