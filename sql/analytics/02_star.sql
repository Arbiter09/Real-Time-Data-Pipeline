-- ===========================================================================
-- TREATMENT: fact + dimension star schema.
--
-- What this buys over the wide table, mechanically:
--
--   1. A narrower fact row. Text city/vehicle/payment become 2-byte smallints
--      and the calendar attributes leave the row entirely. Fewer bytes per row
--      means fewer heap pages to read for the same aggregation, and these
--      queries are I/O bound on the fact scan.
--   2. Precomputed calendar attributes. The baseline computes EXTRACT(dow ...)
--      per row at query time; the star joins a 400-row dimension already
--      carrying is_weekend, iso_week and month.
--   3. Integer grouping keys. Hashing and sorting int2/int4 is cheaper than
--      hashing text, and the hash tables stay in work_mem longer.
--
-- What it costs: joins. For queries that touch few columns and no dimension
-- attributes, the wide table can win outright - it has no joins to pay for.
-- That is a real possible outcome of this experiment and the harness reports
-- it per query rather than blending it away.
-- ===========================================================================

DROP TABLE IF EXISTS fact_trip;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_city;
DROP TABLE IF EXISTS dim_driver;
DROP TABLE IF EXISTS dim_vehicle;
DROP TABLE IF EXISTS dim_payment;

-- --------------------------------------------------------------------------
-- Dimensions
-- --------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key     integer PRIMARY KEY,        -- YYYYMMDD
    full_date    date    NOT NULL UNIQUE,
    year         smallint NOT NULL,
    quarter      smallint NOT NULL,
    month        smallint NOT NULL,
    month_name   text     NOT NULL,
    day_of_month smallint NOT NULL,
    day_of_week  smallint NOT NULL,          -- 0 = Sunday
    day_name     text     NOT NULL,
    iso_week     smallint NOT NULL,
    iso_year     smallint NOT NULL,   -- ISO year, which diverges from `year` in the
                                      -- days either side of 1 January
    is_weekend   boolean  NOT NULL
);

CREATE TABLE dim_city (
    city_key  smallint PRIMARY KEY,
    city_id   text     NOT NULL UNIQUE,
    city_name text     NOT NULL,
    region    text     NOT NULL,
    country   text     NOT NULL
);

CREATE TABLE dim_driver (
    driver_key  integer PRIMARY KEY,
    driver_id   text    NOT NULL UNIQUE,
    tenure_band text    NOT NULL
);

CREATE TABLE dim_vehicle (
    vehicle_key   smallint PRIMARY KEY,
    vehicle_class text     NOT NULL UNIQUE,
    tier          text     NOT NULL
);

CREATE TABLE dim_payment (
    payment_key  smallint PRIMARY KEY,
    payment_type text     NOT NULL UNIQUE,
    is_cashless  boolean  NOT NULL
);

-- --------------------------------------------------------------------------
-- Fact. Column order groups the narrow keys together so Postgres' per-row
-- alignment padding does not eat the size advantage the surrogate keys buy.
-- --------------------------------------------------------------------------
CREATE TABLE fact_trip (
    trip_sk          bigserial PRIMARY KEY,
    trip_id          uuid      NOT NULL,
    date_key         integer   NOT NULL REFERENCES dim_date(date_key),
    driver_key       integer   NOT NULL REFERENCES dim_driver(driver_key),
    duration_sec     integer   NOT NULL,
    pickup_zone_id   integer   NOT NULL,
    fare_amount      numeric(10,2) NOT NULL,
    distance_km      real      NOT NULL,
    surge_multiplier real      NOT NULL,
    city_key         smallint  NOT NULL REFERENCES dim_city(city_key),
    vehicle_key      smallint  NOT NULL REFERENCES dim_vehicle(vehicle_key),
    payment_key      smallint  NOT NULL REFERENCES dim_payment(payment_key),
    hour_of_day      smallint  NOT NULL,
    is_completed     boolean   NOT NULL,
    is_surged        boolean   NOT NULL
);

-- Indexes mirroring the baseline's, expressed over surrogate keys.
CREATE INDEX idx_fact_date        ON fact_trip (date_key);
CREATE INDEX idx_fact_city_date   ON fact_trip (city_key, date_key);
CREATE INDEX idx_fact_driver      ON fact_trip (driver_key);
CREATE INDEX idx_fact_vehicle_date ON fact_trip (vehicle_key, date_key);
CREATE INDEX idx_fact_completed   ON fact_trip (date_key) WHERE is_completed;
