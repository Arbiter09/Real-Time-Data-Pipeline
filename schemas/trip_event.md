# Event schema — `trips.raw`

Domain: **ride-hailing trip completions**. One event = one terminal trip record.

Serialization: JSON, UTF-8, one event per Kafka message.
Kafka message key: `trip_id` (see *Partition key* below).
`schema_version` is carried on every event so a reader can reject what it does
not understand rather than silently mis-parse it.

## Fields

| Field | Type | Notes |
|---|---|---|
| `event_id` | UUIDv4 string | Unique per emission. Idempotency + duplicate detection. |
| `trip_id` | UUIDv4 string | Entity key. Cassandra partition key of `trips_by_id`. |
| `event_time` | ISO-8601 UTC, ms precision | **Business event time, stamped at the producer.** Deliberately back-dated for a configurable fraction of events to exercise watermarking. |
| `produced_at_ms` | int64 epoch ms | **Wall-clock at send, stamped at the producer.** Latency is measured against this, not `event_time` — see below. |
| `city_id` | string enum | 8 cities, Zipf-distributed. Source of realistic partition skew. |
| `driver_id` | string `d-NNNNNN` | Dimension. |
| `rider_id` | string `r-NNNNNN` | Dimension. |
| `pickup_zone_id` | int | Dimension (zone within city). |
| `dropoff_zone_id` | int | Dimension. |
| `vehicle_class` | string enum | `economy` \| `comfort` \| `xl` \| `black`. |
| `payment_type` | string enum | `card` \| `cash` \| `wallet`. |
| `distance_km` | float | Measure. |
| `duration_sec` | int | Measure. |
| `fare_amount` | float | Measure. Primary aggregation target. |
| `surge_multiplier` | float | 1.0–3.0. |
| `status` | string enum | `completed` \| `cancelled`. |
| `producer_id` | string | Which producer instance emitted it. |
| `schema_version` | int | Currently `1`. |

## Why two timestamps

`event_time` is the business timestamp and is what the watermark operates on.
Because the harness deliberately back-dates some events to test allowed
lateness, `event_time` is **not** a valid basis for latency measurement — a
back-dated event would report a fake multi-minute latency.

`produced_at_ms` is wall clock at the moment of `producer.produce()`. Section
7.2 end-to-end latency is `cassandra_write_ms − produced_at_ms`.

**Caveat stated up front:** producer and consumer run in containers on one
host, sharing a clock. This measurement therefore contains no clock skew, which
also means it does not prove anything about cross-host clock discipline. A
distributed deployment would need to account for skew; this number does not.

## Partition key

Kafka message key is `trip_id`.

| Candidate key | Ordering guarantee | Balance | Verdict |
|---|---|---|---|
| `trip_id` | per trip | even (UUID hash) | **chosen** |
| `city_id` | per city | severely skewed — measured in `bench/partition_skew.py` | rejected |
| `driver_id` | per driver | mildly skewed | viable alternative |

Each trip emits exactly one terminal event today, so per-entity ordering is
currently vacuous — but keying on `trip_id` is the choice that stays correct if
the model grows to `requested → accepted → completed` per trip, and it costs
nothing now.

`city_id` was rejected on measured evidence rather than intuition: the city
distribution is Zipf, so keying on it collapses most traffic onto two of the six
partitions. `bench/partition_skew.py` reports the actual imbalance for both
strategies; the numbers are in the README.

The tradeoff being taken: **even partition balance in exchange for giving up
per-city ordering.** Nothing downstream requires per-city ordering — the
aggregations are commutative and Cassandra writes are keyed upserts.
