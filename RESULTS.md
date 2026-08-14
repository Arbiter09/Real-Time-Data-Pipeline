# Measured results

Every number here came from a harness in this repo. The harness is named next
to the number, and its raw JSON output is committed under `results/raw/`.

Hardware: single host, Apple Silicon, 10 CPUs, Docker allocated 11.67 GiB. All
other Docker stacks on the machine were stopped for the duration — an earlier
run was invalidated when a neighbouring container consumed 190% CPU, and the
stack gate now flags containers that restart during a run for exactly that
reason.

---

## 7.1 Sustained ingest rate — `bench/throughput_sweep.py`

**Sustained rate: 1,200 events/sec.**

| Target | Achieved | Attain. | Written | Silent loss | Drain | Drain ratio | Effective | Batch p95 | Verdict |
|---|---|---|---|---|---|---|---|---|---|
| 800/s | 799.9 | 99.99% | 71,999 | **0** | 6.0s | 0.07 | 750/s | 6.7s | sustained |
| 1,000/s | 999.9 | 99.99% | 89,998 | **0** | 8.0s | 0.09 | 918/s | 6.8s | sustained |
| **1,200/s** | **1,199.8** | **99.98%** | **107,998** | **0** | **11.1s** | **0.12** | **1,069/s** | **10.1s** | **sustained** |
| 1,400/s | 1,399.7 | 99.98% | 125,997 | 0 | 30.0s | 0.33 | 1,050/s | 24.6s | **FAILED** — consumer did not keep pace |

Topology this number belongs to, without which it means nothing:

- 6 topic partitions, RF=3, `min.insync.replicas=2`
- 6 Spark executor cores (2 workers × 3), 1g executor memory
- Cassandra 3-node ring, RF=3, **CL=QUORUM**
- **Write amplification 3.0×** — every event writes 3 Cassandra tables, so
  1,200 events/sec is **3,600 Cassandra writes/sec** at quorum

### Why 1,400/s fails despite a flat lag gauge

The first version of this sweep passed 2,000/s. That was wrong, and the way it
was wrong is worth recording.

With `maxOffsetsPerTrigger` set large, Spark claims every available offset on
each trigger, so `offsetsBehindLatest` reads ≈0 even while a single batch takes
52 seconds to process. Lag looked perfect while the run needed 90s of
production plus 70s of drain — the consumer was 1.8× behind and **the lag gauge
could not see it**.

The sweep now also requires the consumer to have kept pace: drain time must be
under 25% of the production window. Effective throughput,
`events / (produce window + drain)`, is reported per step.

The corollary matters operationally: **consumer lag is only a meaningful health
signal when batches are bounded.** With `maxOffsetsPerTrigger=1000`, lag became
observable again (peak 391 at 500/s) where it had read 0.

---

## 7.2 End-to-end latency — `bench/latency_report.py`

Measured `produced_at_ms → Cassandra write`, from raw samples with linear
interpolation. **The "sub-second" claim does not hold at any operating point
tested.**

| Operating point | p50 | p95 | p99 | Min | % under 1s |
|---|---|---|---|---|---|
| Throughput-optimized, overloaded (1,400/s) | 18,733 ms | 29,035 ms | 30,162 ms | — | 0% |
| Latency-optimized (500/s, 1s trigger, 3 tables) | 1,791 ms | 5,580 ms | 6,652 ms | 298 ms | 18.5% |
| **Latency floor** (300/s, 500ms trigger, 1 table) | **1,238 ms** | **5,451 ms** | **6,052 ms** | **298 ms** | — |

Smaller batches improve p50 (1,791 → 1,238 ms) and **leave the tail alone**
(5,580 → 5,451 ms). The p95 floor of ~5.4s is structural on this hardware, not
a batching artifact.

**Caveat, stated because it cuts both ways:** producer and consumer are
containers on one host and share a clock. This measurement therefore contains
no clock skew — clean as a pipeline measurement, and *not* evidence about
clock discipline in a distributed deployment.

---

## 7.3 Behaviour under induced failure — `bench/chaos_suite.py`

Every fault is a `SIGKILL`, not a graceful stop — a clean shutdown lets Kafka
hand off leadership in an orderly way and would test a restart rather than a
crash. All runs under sustained 800 events/sec.

| Fault | Recovery run 1 | Recovery run 2 | Silent loss | Quarantined | Outcome |
|---|---|---|---|---|---|
| Kafka broker | 2.48s | 1.61s | **0** | 0 | all recovered |
| Spark executor | 9.09s | 0.02s | **0** | 0 | all recovered |
| Cassandra node | 3.37s | 1.52s | **0** | 0 | all recovered |

**Recovery** is defined as: time from `docker kill` to the first subsequent
micro-batch that completes with rows actually written. Not "the container came
back", and not "no error was logged".

### Reading these numbers honestly

**Executor recovery is bimodal, not a distribution.** 9.09s when the killed
worker held active tasks and Spark had to reschedule them from the checkpoint;
0.02s when it did not and the batch simply finished on the surviving worker.
Reporting a median of those two would be meaningless, so both are shown.

**The Cassandra result is a non-event by design, and that is the finding.**
With RF=3 and CL=QUORUM, losing one node still leaves 2 of 3 replicas —
quorum is intact and writes never fail. The measurement confirms the
configuration does what it claims, rather than demonstrating a dramatic
recovery. To make Cassandra writes actually fail, the A/B below kills *two*
nodes.

**One run reported −46 "silent loss"** — more distinct trips in Cassandra than
the producer counted as acked. That was a measurement bug, not invented data:
reconciliation counted the producer's delivery *callbacks*, and during a fault
some callbacks have not fired by flush time even though Kafka durably persisted
the message. Reconciliation now uses **Kafka's own end offsets** as the
authority and reports the callback shortfall separately.

---

## 7.4 Consumer lag as a health signal

Exported from Spark's own Kafka source metrics, **not** from a consumer-group
exporter. Structured Streaming keeps offsets in its checkpoint and never
registers a consumer group, so `kafka-consumer-groups --describe` and every
group-lag exporter report nothing for this consumer. `kafka-exporter` is still
scraped, for topic-level offsets and ISR health.

The alert fires on lag *rising across consecutive intervals*, not on lag being
high:

```yaml
expr: deriv(rtdp_stream_kafka_max_offsets_behind_latest[2m]) > 0
for: 5m
```

A spike that plateaus is a burst. A slope that stays positive means the
consumer is losing to the producer and will not catch up unaided.

---

## Section 4 — partition balance by key — `bench/partition_skew.py`

Reimplements Kafka's murmur2 partitioner exactly, so the result is reproducible
offline. 100,000 events, 6 partitions:

| Key | Imbalance | Hottest partition | Empty partitions | Distinct keys |
|---|---|---|---|---|
| **`trip_id`** (chosen) | **1.002** | 16.7% | 0 | 100,000 |
| `rider_id` | 1.023 | 17.0% | 0 | 43,218 |
| `driver_id` | 1.046 | 17.4% | 0 | 5,000 |
| `city_id` (rejected) | **2.038** | 34.0% | **1 of 6** | 8 |

`city_id` has 8 distinct values across 6 partitions, so assignment is decided
by 8 hash values and cannot balance at any volume — **one partition receives
nothing at all**, and consumer parallelism is capped by the partition count
regardless.

---

## Section 8 — star schema vs wide table — `bench/analytics_bench.py`

Protocol: result-set equivalence asserted before any timing; both arms warmed;
timed runs **interleaved** so machine drift hits both equally; median reported;
**per-query ratios, no blended headline**.

The baseline is deliberately fair — native types and the indexes a competent
engineer would create. A strawman baseline would make any speedup a speedup
over incompetence.

*Preliminary, 200k-row corpus:*

| Query | Baseline | Star | Result |
|---|---|---|---|
| Surge share by city by ISO week | 103.4 ms | 30.7 ms | **star 3.37×** |
| Hourly demand by city | 57.0 ms | 29.0 ms | **star 1.97×** |
| Revenue by region by day | 42.1 ms | 23.3 ms | **star 1.81×** |
| Weekend split by vehicle class | 21.5 ms | 21.8 ms | baseline 1.02× |
| Top 25 drivers by revenue | 21.5 ms | 30.1 ms | **baseline 1.40×** |

**Geometric mean 1.53×** (not 2×). Best 3.37×, worst 0.71×.

The spread is the interesting part. The star wins where a dimension supplies a
**precomputed** attribute (`iso_week`, `is_surged`, `is_weekend`) and where the
narrower fact row means fewer heap pages — fact heap is **60%** of the wide
table's. It **loses** on top-drivers, where the query needs only a key the wide
table already carries inline and the join to `dim_driver` buys nothing.

---

## Tests

15 passing — `make test`.

- `tests/test_idempotency.py` — ingest the same event twice, assert row counts
  unchanged, at both single-event and 200-event-batch granularity. Includes a
  **control case** that fails if the writes were silently no-ops; without it
  the idempotency tests would prove nothing.
- `tests/test_retry.py` — 12 tests pinning the backoff schedule to exactly
  1s/2s/4s, jitter bounds, permanent-error short-circuiting, and the
  retries-vs-attempts accounting that decides whether the reported average is
  inflated by exactly 1.0.
