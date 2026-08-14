# Airflow DAGs — verified against live data

All three DAGs have been **executed end to end** against a running stack: real
rows produced through Kafka → Spark → Cassandra, then rolled up into Postgres.

| DAG | Schedule | Verified |
|---|---|---|
| `trips_batch_rollup` | hourly, 10 past | full DagRun **success** — sensor → extract → verify |
| `dlq_drain` | every 15 min | `inspect_dlq` **success** against a live (empty) DLQ |
| `trips_backfill` | manual | full DagRun **success** over a one-hour window |

## What the run actually proved

```
wait_for_hour_data   sensor: hour=2026-08-14T06:00:00Z rows_in_cassandra=22
                     Success criteria met.
extract_and_load_hour  extracted 22 rows → {'extracted': 22, 'loaded': 22}
verify_hour            verified: 22 rows in Postgres, drift 0
DagRun Finished        state=success
```

**Idempotency was verified by re-running, not asserted.** Executing
`extract_and_load_hour` a second time over the same hour left
`trips_rollup_stage` at exactly 22 rows. That is the `ON CONFLICT (trip_id) DO
UPDATE` doing its job, and it is why the backfill is safe to run during an
incident rather than something you schedule for 3am behind a truncate.

`airflow dags list-import-errors` → **no errors** for all three DAGs.

## The bug that only a first run could find

The rollup DAG was wrong before it was ever executed, in a way no amount of
re-reading would have caught:

```
cassandra.protocol.SyntaxException: line 1:84 no viable alternative at input '-08'
(... city_id='nyc' AND event_hour=2026[-08]-14 06:00:00+00:00)
```

Airflow hands `logical_date` over as a **`pendulum.DateTime`**. The Cassandra
driver has no encoder registered for that type, so rather than binding it as a
timestamp parameter it falls back to `str()` and splices the value into the CQL
**unquoted**. The driver gives no warning that a parameter type is unsupported
— it just emits malformed CQL and the server rejects it.

The fix is to convert to a native `datetime` before it reaches the driver
(`_hour_for` in `trips_batch_rollup.py`). Every parameterised Cassandra call in
these DAGs now receives stdlib types only.

Two driver warnings surfaced by the same run were also fixed in
`rtdp_common.cassandra_session`: an `ExecutionProfile` with contact points but
no load-balancing policy (deprecated, will raise in a future driver major), and
an unset `protocol_version` causing a 66 → 65 → 5 renegotiation on every
connection.

## Reproduce it

```bash
make up && make airflow
docker exec rtdp-airflow-scheduler airflow dags list-import-errors
docker exec rtdp-airflow-scheduler airflow dags test trips_batch_rollup 2026-08-14T06:00:00+00:00
```

## The design arguments they encode

These are the reasons Airflow is in the architecture at all rather than a cron
line:

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

- **The backfill is safe to run during an incident**, verified above by
  re-running it.

- **The extract reads a time-bucketed table.** `trips_by_city_hour` is
  partitioned by `(city_id, event_hour)`, so one hour is 8 bounded partition
  reads rather than a full-ring scan.

## Still not exercised

Honesty about what these runs did *not* cover:

- **The SLA-miss callback has never fired.** Triggering it needs a task that
  genuinely overruns 20 minutes.
- **The DLQ drain's re-inject and park paths** ran against an empty queue, so
  routing was exercised only as a no-op. `make dlq-dry-run` after a
  quorum-break run would cover them.
- **The circuit breaker has never tripped** — it needs a DLQ above 20k records.
- These were single runs on one host, not a soak test.
