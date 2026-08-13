# Real-Time Data Pipeline

A streaming pipeline built to be **broken on purpose and measured honestly**.

Kafka → Spark Structured Streaming → Cassandra, with a dead letter queue, a
replay path, and a measurement harness that reconciles every event acked by
Kafka against what actually landed. PostgreSQL holds a star schema for
analytical queries; Airflow orchestrates the batch layer, the DLQ drain and
backfills.

The point of this project is **streaming reliability** — behaviour under
failure and load, not that data moved on the happy path.

> **Every number in this README was produced by a harness in this repo, and
> the harness is named next to the number.** Where a measurement has not been
> taken yet, the table says so rather than carrying an estimate. Raw harness
> output is committed under `results/`.

---

## Architecture

```
   synthetic producer ──► Kafka  (trips.raw, 6 partitions, RF=3, min.ISR=2)
   idempotent, rate-controlled        │
                                      ▼
                    Spark Structured Streaming
                    (checkpointed, event-time aware)
                          │                    │
              idempotent writes          retries → DLQ topic
              (keys derived from                 │  (trips.dlq, RF=3, 7d)
               the event itself)                 │
                          │                      ▼
                          ▼                 replay path
                    Cassandra                (drain DAG:
                 (3-node ring, RF=3,          re-inject │ direct-write │ park)
                  CL=QUORUM, keyed
                  serving reads)
                          │
                          │  Airflow hourly rollup (sensor-gated)
                          ▼
                    PostgreSQL
              (star schema, analytical queries)

   Airflow: producer runs · batch rollups · DLQ drain · backfill
   Prometheus + Grafana: consumer lag, batch duration, DLQ rate, latency
   Everything runs under one Docker Compose stack.
```

**Component roles** — and what each is *not* — are in
[`docs/architecture.md`](docs/architecture.md). Two worth repeating here:

- **Cassandra is a write-heavy serving store queried by known key. It is not an
  analytical database**, and nothing in this repo describes it as OLAP.
- **Spark Structured Streaming**, not DStreams.

### Cluster shape, stated honestly

Three Kafka broker containers and three Cassandra containers on **one physical
host**. That is a multi-broker cluster and a multi-node ring *on a single
host*. **It is not a multi-node deployment.** Killing a container here proves
failover logic; it proves nothing about datacentre failover or cross-host
network partitions.

---

## Delivery semantics

```
Kafka  -> Spark       at-least-once   (offsets commit to the checkpoint AFTER
                                       the batch's writes complete)
Spark  -> Cassandra   idempotent upsert on keys derived entirely from the event
------------------------------------------------------------------------------
net effect            EFFECTIVELY-ONCE
```

**Not exactly-once.** No Kafka transaction spans the Cassandra write, and none
could — Cassandra is not a transactional participant. Re-delivery genuinely
happens and is absorbed by the key design. That is a weaker, different claim,
and it is the one a platform reviewer will test.

Loss policy: **zero silent loss.** Every event acked by Kafka is either written
to Cassandra or quarantined in the DLQ where it stays replayable. The harness
reconciles the two and reports any remainder as silent loss by name, rather
than folding it into a percentage.

---

## Domain and event schema

Ride-hailing trip completions. Full field list and rationale:
[`schemas/trip_event.md`](schemas/trip_event.md).

Two timestamps, deliberately:

| Field | Meaning | Used for |
|---|---|---|
| `event_time` | Business event time, stamped at the producer. Back-dated for a configurable fraction of events. | Watermarking / lateness |
| `produced_at_ms` | Wall clock at `producer.produce()` | **Latency measurement** |

Measuring latency against `event_time` would report a fabricated multi-minute
latency for every deliberately-late event.

---

## Topology

| Setting | Value | Why |
|---|---|---|
| Kafka brokers | 3 (KRaft, no ZooKeeper) | Makes the broker-kill test possible |
| `trips.raw` partitions | **6** | Matches 6 Spark executor cores; consumer parallelism is capped by partition count |
| Replication factor | 3 | |
| `min.insync.replicas` | 2 | One broker down still leaves a writable ISR |
| `unclean.leader.election.enable` | **false** | An out-of-sync replica can never be elected leader, so a failover cannot truncate acknowledged writes. This is what makes "zero silent loss" a real claim rather than a hopeful one. |
| Kafka message key | `trip_id` | Measured — see below |
| Producer | `enable.idempotence=true`, `acks=all` | |
| Cassandra nodes | 3, RF=3 | |
| Write consistency | `QUORUM` | Survives one node down |
| Watermark / allowed lateness | 2 minutes | |
| Late-data policy | `dlq` (quarantine, not drop) | |
| Retry backoff | 1s → 2s → 4s, ±10% jitter, 4 attempts | Configurable; empty schedule = A/B control arm |

---

## Measured results

### Partition balance by key choice — `bench/partition_skew.py`

Reimplements Kafka's murmur2 partitioner exactly, so the answer is
reproducible offline. 100,000 events, 6 partitions:

| Key | Imbalance ratio | Hottest partition | Empty partitions |
|---|---|---|---|
| **`trip_id`** (chosen) | **1.002** | 16.7% | 0 |
| `rider_id` | 1.023 | 17.0% | 0 |
| `driver_id` | 1.046 | 17.4% | 0 |
| `city_id` (rejected) | **2.038** | 34.0% | **1 of 6** |

`city_id` has only 8 distinct values across 6 partitions, so assignment is
decided by 8 hash values and cannot balance at any volume — **one partition
receives nothing at all.** The tradeoff taken is even balance in exchange for
per-city ordering, which nothing downstream needs.

Raw: [`results/raw/partition_skew.json`](results/raw/partition_skew.json)

### Analytical layer: star schema vs wide table — `bench/analytics_bench.py`

Protocol: result-set equivalence asserted before any timing, both arms warmed,
timed runs **interleaved** so machine drift hits both equally, median of 5+
runs, **per-query ratios with no blended headline**.

The baseline is deliberately fair — native types and the indexes a competent
engineer would create. A strawman baseline would make any speedup a speedup
over incompetence.

*Preliminary, 200k-row corpus (full 3M-row run pending — see Status):*

| Query | Baseline | Star | Result |
|---|---|---|---|
| Surge share by city by ISO week | 103.4 ms | 30.7 ms | **star 3.37×** |
| Hourly demand by city | 57.0 ms | 29.0 ms | **star 1.97×** |
| Revenue by region by day | 42.1 ms | 23.3 ms | **star 1.81×** |
| Weekend split by vehicle class | 21.5 ms | 21.8 ms | baseline 1.02× |
| Top 25 drivers by revenue | 21.5 ms | 30.1 ms | **baseline 1.40×** |

Geometric mean **1.53×**; best 3.37×, worst 0.71×.

The spread is the interesting part. The star wins where a dimension supplies a
**precomputed** attribute (`iso_week`, `is_surged`, `is_weekend`) and where the
narrower fact row means fewer heap pages. It **loses** on top-drivers, where
the query needs only a key the wide table already carries inline and the join
to `dim_driver` buys nothing. Fact heap is **60%** of the wide table's.

### Tests

15 passing — `make test`.

- `tests/test_idempotency.py` — Section 5's required assertion: ingest the same
  event twice, row counts unchanged. Includes a **control case** that fails if
  the writes were silently no-ops, without which the idempotency tests would
  prove nothing.
- `tests/test_retry.py` — 12 tests pinning the backoff schedule (exactly
  1s/2s/4s), jitter bounds, permanent-error short-circuiting, and the
  retries-vs-attempts accounting that decides whether the reported "average
  retries" figure is inflated by exactly 1.0.

---

## Claims table

Filled from harness output. **Blank rows are blank because the measurement has
not been taken, not because it was inconvenient.**

| Claim | Previously written as | Measured |
|---|---|---|
| Sustained ingest rate | 1,000+ events/sec | *pending — see Status* |
| End-to-end latency | sub-second | *pending* |
| Partitioned topics, parallel consumers | asserted | **6 partitions, RF=3, 6 executor cores.** Balance measured: 1.002 imbalance on `trip_id` |
| Cluster shape | "multi-node cluster" | **3 brokers + 3 Cassandra nodes as containers on ONE host.** Multi-broker, not multi-node. Wording corrected. |
| Permanent message loss reduction | 95% | *pending.* Note the reframing below — the honest metric is quarantine reduction, because permanent loss is **zero in both arms**. |
| Backoff schedule | 1s → 2s → 4s | **Confirmed**, ±10% jitter, 4 attempts max. Pinned by `tests/test_retry.py`. |
| Average retries to recovery | 1.3 | *pending* |
| Nature of retried failures | "broker and write-timeout" | *pending* — the harness records actual exception types per run rather than asserting them |
| Analytical query improvement | 2× | **1.53× geometric mean** at 200k rows; range 0.71×–3.37×. Full-corpus run pending. |

### The loss claim needs reframing regardless of the number

"95% less permanent loss" invites the question of what happened to the other
5%. This pipeline's answer is that **permanent loss is zero** — everything that
exhausts its retry budget lands in the DLQ, replayable. So the backoff A/B
measures the reduction in events **forced into quarantine**, not a reduction in
loss. Quoting it as "reduced data loss by N%" would be false in both arms.

The honest sentence shape is:
**"Zero silent loss; N events quarantined and replayable; backoff cut
quarantine volume by X%."**

---

## Status

**Measurement is blocked on host memory.** The full stack (3 Kafka + 3
Cassandra + Spark + Postgres) needs ~12 GiB; Docker Desktop is currently capped
at 7.65 GiB. Under that cap the Cassandra nodes restart-loop under write load —
`rtdp-cassandra1` restarted 64 times during a capacity probe — and a ring that
bounces mid-run produces numbers about the memory ceiling, not the pipeline.

The stack gate catches this rather than letting it through:

```bash
make health
```

It verifies ISR completeness, `min.insync.replicas`, unclean leader election,
ring state, an actual QUORUM write, and **flags containers that keep
restarting** — which is how the ceiling was found.

Everything else is built and tested. Once memory is available, the remaining
figures come from:

```bash
make throughput   # 7.1  sustained rate
make latency      # 7.2  p50/p95/p99
make chaos        # 7.3  kill scenarios + backoff A/B
make analytics    # 8    full-corpus star vs wide
```

---

## Running it

Requires Docker with **≥12 GiB** allocated.

```bash
make build && make up && make health
```

Then a measurement run:

```bash
make run RUN_ID=demo RATE=1000 DURATION=120
```

Optional profiles:

```bash
make obs        # Prometheus :19090, Grafana :13000
make airflow    # Airflow :18088 (admin/admin)
```

| Target | Section | What it measures |
|---|---|---|
| `make health` | 0 | Stack gate |
| `make skew` | 4 | Partition balance per key strategy |
| `make test` | 5 | Idempotency + retry accounting |
| `make throughput` | 7.1 | Sustained rate (lag-flat, not peak) |
| `make latency RUN_ID=x` | 7.2 | p50/p95/p99 |
| `make chaos` | 7.3 | Kill scenarios + backoff A/B |
| `make analytics` | 8 | Star schema vs wide table |
| `make dlq` / `make dlq-drain` | 6 | Inspect / drain the DLQ |

### What "sustained rate" means here

The highest target rate at which **all** of these hold:

1. The producer actually attained its target (≥95%) — a step where the
   producer under-delivered says nothing about the consumer.
2. Lag did not grow monotonically beyond the tolerated run — the same
   condition the Grafana alert fires on.
3. The backlog fully drained.
4. Reconciliation showed zero unaccounted events.

It is **not** the highest rate the producer can emit, and not the best single
run. Reported alongside partition count, executor count and write
amplification, because throughput without topology is meaningless.

---

## Observability

The dashboard leads with **consumer lag**, not throughput. Throughput tells you
what the system did; lag tells you whether it is healthy — the same instinct as
scaling on queue depth rather than CPU.

The alert that matters:

```yaml
- alert: ConsumerLagGrowingMonotonically
  expr: deriv(rtdp_stream_kafka_max_offsets_behind_latest[2m]) > 0
  for: 5m
```

A single high lag reading is a burst and resolves itself. Lag that rises across
*consecutive* intervals means the consumer is losing to the producer and will
never catch up unaided.

**Where that series comes from matters.** Spark Structured Streaming keeps
offsets in its checkpoint and never registers a Kafka consumer group, so
`kafka-consumer-groups --describe` and every consumer-group lag exporter report
nothing for this consumer. The lag is exported from Spark's own Kafka source
metrics — the authoritative offset store. `kafka-exporter` is still scraped,
for topic-level offsets and ISR health.

---

## Repository layout

```
common/       config, retry/backoff policy, DLQ envelope, metrics
producer/     event generator + rate-controlled idempotent producer
streaming/    Structured Streaming job + Cassandra sink with retry/DLQ
replay/       DLQ inspection and the replay drain
bench/        every measurement harness
airflow/dags/ rollup (sensor + SLA), DLQ drain, backfill
monitoring/   Prometheus scrape + alert rules, Grafana dashboard
sql/          Cassandra schema, Postgres baseline + star schema
tests/        idempotency and retry-accounting tests
results/      committed harness output
docs/         architecture and design decisions
```

## Scope

Deliberately no Flink, Trino, Ray, Kubernetes or cloud migration. The value
here is streaming reliability; extra technology names would dilute it.
