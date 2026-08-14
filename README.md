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

Full detail with protocol notes: **[RESULTS.md](RESULTS.md)**. Raw harness
output is committed under `results/raw/`.

### Sustained ingest — 1,200 events/sec

| Target | Achieved | Silent loss | Drain ratio | Effective | Verdict |
|---|---|---|---|---|---|
| 800/s | 799.9 | **0** | 0.07 | 750/s | sustained |
| 1,000/s | 999.9 | **0** | 0.09 | 918/s | sustained |
| **1,200/s** | **1,199.8** | **0** | **0.12** | **1,069/s** | **sustained** |
| 1,400/s | 1,399.7 | 0 | 0.33 | 1,050/s | **failed** — consumer behind |

6 partitions · 6 executor cores · RF=3 · CL=QUORUM · **3.0× write
amplification**, so 1,200 events/sec is **3,600 Cassandra writes/sec**.

A flat lag gauge was not sufficient to call a rate sustained: with a large
`maxOffsetsPerTrigger`, Spark claims every offset each trigger and lag reads ≈0
while batches take 50s. The sweep now also requires the consumer to keep pace.

### Behaviour under induced failure

Every fault is a `SIGKILL` under sustained 800 events/sec.

| Fault | Recovery | Silent loss | Outcome |
|---|---|---|---|
| Kafka broker | 2.48s / 1.61s | **0** | all recovered |
| Spark executor | 9.09s / 0.02s | **0** | all recovered |
| Cassandra node | 3.37s / 1.52s | **0** | all recovered |

**Zero silent loss in all six runs.** Executor recovery is bimodal — 9.09s when
the killed worker held active tasks, 0.02s when it did not — so both are shown
rather than a meaningless median.

### End-to-end latency — sub-second does NOT hold

| Operating point | p50 | p95 | % under 1s |
|---|---|---|---|
| Overloaded (1,400/s) | 18,733 ms | 29,035 ms | 0% |
| Latency-optimized (500/s, 1s trigger) | 1,791 ms | 5,580 ms | 18.5% |
| Latency floor (300/s, 500ms trigger, 1 table) | 1,238 ms | 5,451 ms | — |

Smaller batches improve p50 and leave the tail alone. The ~5.4s p95 floor is
structural on this hardware.

### Partition balance by key

| Key | Imbalance | Empty partitions |
|---|---|---|
| **`trip_id`** (chosen) | **1.002** | 0 |
| `city_id` (rejected) | **2.038** | **1 of 6** |

`city_id` has 8 distinct values across 6 partitions — it cannot balance at any
volume, and one partition receives nothing.

### Star schema vs wide table

Geometric mean **1.53×** (200k-row corpus), range 0.71×–3.37×. Best: surge-by-
week 3.37×, where the dimension supplies precomputed `iso_week`/`is_surged`.
Worst: top-drivers 0.71×, where the join to `dim_driver` buys nothing the wide
table did not already carry inline.

---

## Claims table

| Claim | Previously written as | Measured |
|---|---|---|
| Sustained ingest rate | 1,000+ events/sec | **1,200 events/sec** (= 3,600 Cassandra writes/sec at 3× amplification). 1,400/s fails. |
| End-to-end latency | sub-second | **FALSE.** p50 1.24s, p95 5.45s at the latency floor; only 18.5% of events under 1s even when tuned for latency. |
| Partitioned topics, parallel consumers | asserted | **6 partitions, RF=3, 6 executor cores.** Balance measured: 1.002 imbalance on `trip_id` vs 2.038 on `city_id`. |
| Cluster shape | "multi-node cluster" | **3 brokers + 3 Cassandra nodes as containers on ONE host.** Multi-broker, not multi-node. |
| Permanent message loss reduction | 95% | **Zero permanent loss in every run measured** — see reframing below. |
| Backoff schedule | 1s → 2s → 4s | **Confirmed**, ±10% jitter, 4 attempts. Pinned by `tests/test_retry.py`. |
| Average retries to recovery | 1.3 | **0.0 under normal operation** — no Cassandra write needed a retry across the entire sweep. Retries only occur when quorum is broken. |
| Nature of retried failures | "broker and write-timeout" | Recorded per run as actual exception types rather than asserted. |
| Analytical query improvement | 2× | **1.53× geometric mean**, range 0.71×–3.37× (200k corpus). |

> **Airflow DAGs are written but never executed.** Every other component here
> was run and measured; the DAGs were not. See
> [`airflow/dags/README.md`](airflow/dags/README.md). They are design, not
> evidence.

### The strongest line is not on the original list

> **A Kafka broker, a Spark executor and a Cassandra node were each SIGKILLed
> mid-stream under sustained load. Every event was accounted for: 2.0s median
> broker failover, zero silent loss across all six runs.**

That is worth more than the retry statistic it replaces, and it is reproducible
with `make chaos`.

### Why the loss claim needs reframing

"95% less permanent loss" invites the question of what happened to the other
5%. The measured answer here is that **permanent loss is zero** — everything
that exhausts its retry budget lands in the DLQ, replayable. So the backoff A/B
measures reduction in events **forced into quarantine**, not reduction in loss.

The honest sentence shape:
**"Zero silent loss; N events quarantined and replayable; backoff cut
quarantine volume by X%."**

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
