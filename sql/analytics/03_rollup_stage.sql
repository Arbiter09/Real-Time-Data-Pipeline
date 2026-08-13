-- ===========================================================================
-- Landing table for the Airflow batch rollup (Cassandra -> Postgres).
--
-- Separate from `trips_wide` on purpose. trips_wide is the CONTROL ARM of the
-- Section 8 benchmark and must hold exactly the corpus the star schema holds;
-- letting a live hourly rollup append into it would change the benchmark's
-- data underneath it between runs and make the two arms diverge.
--
-- PRIMARY KEY (trip_id) is what makes the rollup idempotent: a retried task, a
-- manual re-run, or a backfill over an already-loaded window all converge to
-- the same rows via ON CONFLICT DO UPDATE.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS trips_rollup_stage (
    trip_id          uuid PRIMARY KEY,
    event_id         uuid         NOT NULL,
    event_time       timestamptz  NOT NULL,
    city_id          text         NOT NULL,
    driver_id        text         NOT NULL,
    rider_id         text         NOT NULL,
    pickup_zone_id   integer      NOT NULL,
    dropoff_zone_id  integer      NOT NULL,
    vehicle_class    text         NOT NULL,
    payment_type     text         NOT NULL,
    distance_km      real         NOT NULL,
    duration_sec     integer      NOT NULL,
    fare_amount      numeric(10,2) NOT NULL,
    surge_multiplier real         NOT NULL,
    status           text         NOT NULL,
    loaded_at        timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rollup_event_time ON trips_rollup_stage (event_time);
CREATE INDEX IF NOT EXISTS idx_rollup_city_time  ON trips_rollup_stage (city_id, event_time);
CREATE INDEX IF NOT EXISTS idx_rollup_loaded_at  ON trips_rollup_stage (loaded_at);

-- Audit trail for backfills: which window was reprocessed, when, and by whom.
CREATE TABLE IF NOT EXISTS backfill_audit (
    id           bigserial PRIMARY KEY,
    window_start timestamptz NOT NULL,
    window_end   timestamptz NOT NULL,
    dag_run_id   text,
    rows_loaded  bigint,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    notes        text
);
