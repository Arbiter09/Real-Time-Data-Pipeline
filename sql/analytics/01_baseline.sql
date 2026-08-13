-- ===========================================================================
-- BASELINE: the single wide table.
--
-- This is the control arm of the Section 8 experiment, and it is deliberately
-- NOT a strawman. It is what a competent engineer produces when they land
-- events in Postgres without dimensional modelling:
--
--   * every attribute denormalized inline, including the city_name/region
--     that a lookup would otherwise supply
--   * native types, not everything-as-text
--   * indexes on the columns these queries actually filter and group on
--
-- If the baseline had no indexes the comparison would be rigged, and any
-- speedup measured against it would be a speedup over incompetence rather
-- than over normalization. The only things the star schema gets that this
-- does not are integer surrogate keys, a narrower fact row, and precomputed
-- calendar attributes.
-- ===========================================================================

DROP TABLE IF EXISTS trips_wide;

CREATE TABLE trips_wide (
    trip_id          uuid PRIMARY KEY,
    event_id         uuid         NOT NULL,
    event_time       timestamptz  NOT NULL,
    city_id          text         NOT NULL,
    city_name        text         NOT NULL,
    region           text         NOT NULL,
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
    status           text         NOT NULL
);

-- The indexes a reasonable person would create for this query set.
CREATE INDEX idx_wide_event_time  ON trips_wide (event_time);
CREATE INDEX idx_wide_city_time   ON trips_wide (city_id, event_time);
CREATE INDEX idx_wide_driver      ON trips_wide (driver_id);
CREATE INDEX idx_wide_vclass_time ON trips_wide (vehicle_class, event_time);
CREATE INDEX idx_wide_status      ON trips_wide (status) WHERE status = 'completed';
