-- ===========================================================================
-- Cassandra serving schema.
--
-- IDEMPOTENCY IS A SCHEMA DECISION HERE, NOT APPLICATION LOGIC.
-- Every primary key below is derived entirely from fields carried on the
-- event. Cassandra upserts on primary key, so replaying an event - from the
-- DLQ drain, from a Spark restart that re-reads uncommitted offsets, or from a
-- producer retry - overwrites the same row with the same values. Row counts do
-- not move. tests/test_idempotency.py asserts exactly that.
--
-- What this does NOT give you: it does not dedupe events that are semantically
-- the same trip but carry different trip_ids. Nothing in this design claims to.
-- ===========================================================================

CREATE KEYSPACE IF NOT EXISTS rtdp
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}
  AND durable_writes = true;

USE rtdp;

-- --------------------------------------------------------------------------
-- 1. Point lookup by trip. The idempotency anchor.
--    PK is trip_id alone: one row per trip, replay overwrites in place.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trips_by_id (
    trip_id          uuid PRIMARY KEY,
    event_id         uuid,
    event_time       timestamp,
    produced_at_ms   bigint,
    ingested_at_ms   bigint,
    city_id          text,
    driver_id        text,
    rider_id         text,
    pickup_zone_id   int,
    dropoff_zone_id  int,
    vehicle_class    text,
    payment_type     text,
    distance_km      double,
    duration_sec     int,
    fare_amount      double,
    surge_multiplier double,
    status           text,
    producer_id      text,
    schema_version   int
) WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND comment = 'Point-lookup serving table. PK derived from event => replay-safe.';

-- --------------------------------------------------------------------------
-- 2. Keyed serving read: one driver's trips for one day, newest first.
--    Partition is (driver_id, event_date) so a partition stays small and
--    bounded - a driver does maybe 30 trips a day, not 30 million.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trips_by_driver_day (
    driver_id        text,
    event_date       date,
    event_time       timestamp,
    trip_id          uuid,
    event_id         uuid,
    city_id          text,
    rider_id         text,
    vehicle_class    text,
    payment_type     text,
    distance_km      double,
    duration_sec     int,
    fare_amount      double,
    surge_multiplier double,
    status           text,
    ingested_at_ms   bigint,
    PRIMARY KEY ((driver_id, event_date), event_time, trip_id)
) WITH CLUSTERING ORDER BY (event_time DESC, trip_id ASC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'HOURS',
                    'compaction_window_size': 6}
  AND comment = 'Driver day-view. Clustering key includes trip_id so two trips at the same millisecond do not collide.';

-- --------------------------------------------------------------------------
-- 3. Time-bucketed extraction table.
--    Exists so the Airflow batch rollup can pull a bounded time window into
--    Postgres WITHOUT a full-table scan. Scanning Cassandra by time with no
--    time-bucketed partition key is the classic way to melt a ring; this is
--    the standard fix. It doubles as a city-hour ops serving read.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trips_by_city_hour (
    city_id          text,
    event_hour       timestamp,
    trip_id          uuid,
    event_id         uuid,
    event_time       timestamp,
    driver_id        text,
    rider_id         text,
    pickup_zone_id   int,
    dropoff_zone_id  int,
    vehicle_class    text,
    payment_type     text,
    distance_km      double,
    duration_sec     int,
    fare_amount      double,
    surge_multiplier double,
    status           text,
    ingested_at_ms   bigint,
    PRIMARY KEY ((city_id, event_hour), trip_id)
) WITH compaction = {'class': 'TimeWindowCompactionStrategy',
                     'compaction_window_unit': 'HOURS',
                     'compaction_window_size': 1}
  AND comment = 'Batch extraction + city ops view. Partition = one city-hour.';

-- --------------------------------------------------------------------------
-- 4. Latency samples for Section 7.2.
--    Written for a configurable fraction of events (LATENCY_SAMPLE_RATE).
--    Partitioned by (run_id, minute bucket) so the reporter reads a bounded
--    set of partitions instead of scanning.
--    Sampling exists because writing a latency row for every event would add
--    a fourth write per event and depress the very throughput being measured.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS latency_samples (
    run_id          text,
    minute_bucket   int,
    event_id        uuid,
    produced_at_ms  bigint,
    written_at_ms   bigint,
    latency_ms      int,
    batch_id        bigint,
    PRIMARY KEY ((run_id, minute_bucket), event_id)
) WITH default_time_to_live = 86400
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND comment = 'Measurement-only. 24h TTL - these are not business data.';

-- --------------------------------------------------------------------------
-- 5. Run ledger. One row per harness run so a report can be tied back to the
--    exact configuration that produced it.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS run_ledger (
    run_id        text PRIMARY KEY,
    started_at    timestamp,
    scenario      text,
    config_json   text,
    notes         text
);

-- --------------------------------------------------------------------------
-- 6. Event-time windowed rollup, written by the optional --with-rollup query.
--    This is the table that makes the watermark load-bearing: the aggregation
--    feeding it is a genuine stateful operator keyed on event time, not
--    processing time.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS city_minute_rollup (
    city_id          text,
    window_start     timestamp,
    window_end       timestamp,
    trips            bigint,
    revenue          double,
    avg_duration_sec double,
    active_drivers   bigint,
    computed_at      timestamp,
    PRIMARY KEY ((city_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND comment = 'Streaming rollup. PK from (city, window) => recomputation upserts.';
