# Architecture and design decisions

Every decision here has a reason, and where the reason is a measurement, the
measurement is linked. Decisions defended only by "that's the standard
pattern" are marked as such honestly.

## Component roles

| Component | Role | What it is NOT |
|---|---|---|
| **Kafka** | Durable, partitioned, replayable log. Buffers producer bursts from consumer speed. | Not a database. Retention is 24h on `trips.raw`, 7 days on `trips.dlq`. |
| **Spark Structured Streaming** | Stateful stream processing with checkpointed offsets. | Not DStreams. DStreams is effectively deprecated and would be the first thing a reviewer flagged. |
| **Cassandra** | Write-heavy serving store, queried by known key. | **Not an analytical database.** Every read pattern here hits a known partition key. There is no OLAP claim anywhere in this repo. |
| **PostgreSQL** | Transactional store and the home of the star schema for analytical queries. | Not in the hot ingest path. |
| **Airflow** | Orchestration, retries, SLAs, backfill, DLQ drain. | Not a timer that runs a script — see [why the DAGs earn their place](#airflow). |

## Delivery semantics

```
Kafka  -> Spark       at-least-once
                      Offsets commit to the checkpoint AFTER the batch's
                      writes complete. A crash mid-batch re-reads that batch.

Spark  -> Cassandra   idempotent upsert
                      Every primary key is derived entirely from event fields.

net                   EFFECTIVELY-ONCE
```

**This is not exactly-once, and the repo never says it is.** No Kafka
transaction spans the Cassandra write, and none could — Cassandra is not a
transactional participant. Re-delivery genuinely happens; it is absorbed by the
key design. That is a weaker and different claim than exactly-once, and it is
the claim a platform reviewer will test.

## Idempotency is a schema decision

```sql
trips_by_id           PRIMARY KEY (trip_id)
trips_by_driver_day   PRIMARY KEY ((driver_id, event_date), event_time, trip_id)
trips_by_city_hour    PRIMARY KEY ((city_id, event_hour), trip_id)
```

Nothing in that list contains an ingestion timestamp, a batch id, or a
generated key. Replaying an event — from a DLQ drain, a Spark restart, or a
producer retry — writes the same primary key with the same values. Row counts
do not move.

Asserted by `tests/test_idempotency.py`, which includes a control case that
fails if the writes were silently no-ops.

## Watermarking, and why it is enforced by hand

`withWatermark` declares the allowed-lateness contract, but late records are
classified and routed **inside `foreachBatch`**, not by a stateful operator.

Spark's stateful operators drop beyond-watermark rows **silently**. Silent
drops are precisely what this project claims not to have. So the job computes
lateness against the previous batch's high-water mark — mirroring Spark's own
one-batch watermark lag — and routes late events to the DLQ, where they remain
counted and replayable.

The `--with-rollup` query is where the watermark drives a genuine stateful
operator (an event-time windowed aggregation). It is off during throughput
benchmarks so its state store does not distort the ingest numbers.

### Replaying a late event is a trap

A naive drain re-injects a late event into the same stream, where it is late
again and quarantined again — forever. The drain therefore routes by stage:

| Stage | Meaning | Action |
|---|---|---|
| `cassandra_sink` | Transient; the ring was unhealthy | Re-inject to `trips.raw` |
| `producer` | Never reached Kafka | Re-inject to `trips.raw` |
| `late_event` | Valid, but beyond allowed lateness | **Write directly to Cassandra**, bypassing the watermark |
| `parse` | Permanent; the payload is malformed | **Park** to a file for a human |

A `replay_count` ceiling stops anything cycling forever.

## Consumer lag: where the number comes from

**Spark Structured Streaming does not commit offsets to a Kafka consumer
group.** Offsets live in the checkpoint — that is the entire point of
checkpointed recovery. So `kafka-consumer-groups --describe` and any
consumer-group lag exporter report *nothing* for this consumer.

The lag series is therefore exported from Spark's own Kafka source metrics
(`maxOffsetsBehindLatest`), which is the same quantity computed from the
authoritative offset store. `kafka-exporter` is still scraped, but for what it
genuinely knows: topic-level partition offsets and ISR health.

This distinction is easy to get wrong and produces a dashboard that reads
"0 lag" forever while the pipeline drowns.

## Partition key choice

Kafka message key is `trip_id`. Measured with `bench/partition_skew.py`, which
reimplements Kafka's murmur2 partitioner exactly:

| Key | Imbalance ratio | Hottest partition | Empty partitions |
|---|---|---|---|
| **`trip_id`** (chosen) | **1.002** | 16.7% | 0 |
| `driver_id` | 1.046 | 17.4% | 0 |
| `rider_id` | 1.023 | 17.0% | 0 |
| `city_id` (rejected) | **2.038** | 34.0% | **1 of 6** |

`city_id` has only 8 distinct values across 6 partitions, so assignment is
decided by 8 hash values and cannot balance at any volume. One partition
receives nothing at all.

**The tradeoff taken:** even partition balance in exchange for giving up
per-city ordering. Nothing downstream needs it — the aggregations are
commutative and the Cassandra writes are keyed upserts.

## Cluster shape, stated honestly

Three Kafka broker containers and three Cassandra containers on **one physical
host**. This is a multi-broker cluster and a multi-node ring on a single host.
**It is not a multi-node deployment.**

Killing a container here proves failover logic. It does not prove anything
about datacentre failover, network partitions between racks, or cross-host
clock discipline. A reviewer who runs Kafka for a living can tell the
difference, and stating it correctly costs nothing.

## Airflow

What the DAGs do that a cron line could not:

- **A sensor gates the rollup on data actually landing**, not on the clock
  reaching H+1. Under lag, a timer-triggered job rolls up a half-written hour
  and writes a wrong number nobody notices.
- **An SLA on the critical path, alerting on the miss** rather than only on
  failure. A rollup that succeeds 40 minutes late has not failed, and no
  failure callback ever fires for it — but its consumers are reading stale
  data.
- **A circuit breaker on the DLQ drain.** Draining a 50k-record DLQ caused by a
  dead Cassandra node just replays 50k failures into a still-dead node.
- **A backfill that is safe to run during an incident**, because the load
  upserts on `trip_id` rather than requiring a truncate first.

## Known limitations

1. **Single host.** See above. The biggest one.
2. **Shared clock.** Producer and consumer share a host clock, so the latency
   figure contains no clock skew — clean as a pipeline measurement, but not
   evidence about distributed clock discipline.
3. **The analytics corpus is generated, not streamed.** The query set spans
   weeks; producing that through the streaming path would take weeks of wall
   clock. The Cassandra→Postgres path is real and exercised by the Airflow
   rollup on live data; the benchmark corpus is bulk-loaded.
4. **Latency is sampled**, at `LATENCY_SAMPLE_RATE`. Writing a latency row per
   event would add a fourth write per event and depress the throughput being
   measured.
5. **Memory-constrained host.** Documented per-run in the results, because a
   Cassandra node restarting mid-run invalidates the run.
