"""Rate-controlled synthetic producer for `trips.raw`.

Two things make this a measurement instrument rather than a demo script:

  * The rate is a *target*, held by a drift-corrected pacing loop, and the
    achieved rate is reported alongside it. Section 7.1 sweeps this target; a
    producer that silently under-delivers would make the sweep meaningless, so
    the report always states target vs achieved and the gap between them.

  * Retry counts come from librdkafka's own statistics callback, not from an
    estimate. `txretries` is the client's counter for request retries; when a
    broker is killed mid-run, that number is the evidence.

Idempotence is on (`enable.idempotence=true`), which pins acks=all,
max.in.flight<=5 and retries=INT_MAX, and makes broker-side deduplication of
producer retries automatic within a producer session.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from collections import Counter as CollCounter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from confluent_kafka import KafkaError, KafkaException, Producer

from common import dlq as dlqmod
from common import metrics
from common.config import KafkaConfig, RetryConfig
from common.retry import BackoffPolicy, RetriesExhausted, RetryStats, call_with_retry
from producer.events import TripGenerator, duplicate_of

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-5s [producer] %(message)s",
)
log = logging.getLogger("produce")


class ProducerRunner:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.kcfg = KafkaConfig()
        self.retry_cfg = RetryConfig()
        self.policy = BackoffPolicy(self.retry_cfg)
        self.retry_stats = RetryStats()

        self.topic = args.topic or self.kcfg.topic_trips
        self.dlq_topic = self.kcfg.topic_dlq

        self.sent = 0
        self.acked = 0
        self.failed = 0
        self.dlq_count = 0
        self.partition_counts: CollCounter = CollCounter()
        self._failed_payloads: List[tuple] = []
        self._failed_lock = threading.Lock()
        self._latest_stats: Dict[str, Any] = {}
        self._stats_lock = threading.Lock()
        self._running = True

        self.gen = TripGenerator(
            seed=args.seed,
            producer_id=args.producer_id,
            late_event_ratio=args.late_ratio,
            malformed_ratio=args.malformed_ratio,
        )
        self.producer = Producer(self._client_config())

    # -- configuration -------------------------------------------------------
    def _client_config(self) -> Dict[str, Any]:
        sched = self.retry_cfg.backoff_schedule_ms
        # Map the documented schedule onto librdkafka's exponential retry
        # backoff. With backoff disabled the client still retries (idempotence
        # forces retries=INT_MAX) but does so with a 1ms floor - which is
        # exactly the "no backoff" control arm: retry storms, no spacing.
        backoff_min = sched[0] if sched else 1
        backoff_max = sched[-1] if sched else 1

        cfg: Dict[str, Any] = {
            "bootstrap.servers": self.kcfg.bootstrap,
            "client.id": f"rtdp-producer-{self.args.producer_id}",
            # --- durability ---
            "enable.idempotence": True,      # implies acks=all, ordered retries
            "acks": "all",
            "compression.type": "lz4",
            # --- retry / backoff (Section 6) ---
            "retry.backoff.ms": backoff_min,
            "retry.backoff.max.ms": backoff_max,
            "message.timeout.ms": self.args.message_timeout_ms,
            "request.timeout.ms": 15000,
            # --- throughput shaping ---
            "linger.ms": self.args.linger_ms,
            "batch.size": 262144,
            "queue.buffering.max.messages": 400000,
            "queue.buffering.max.kbytes": 262144,
            # --- observability ---
            "statistics.interval.ms": 2000,
            "stats_cb": self._on_stats,
            "error_cb": self._on_error,
        }
        return cfg

    # -- callbacks -----------------------------------------------------------
    def _on_stats(self, stats_json: str) -> None:
        try:
            stats = json.loads(stats_json)
        except Exception:
            return
        with self._stats_lock:
            self._latest_stats = stats
        metrics.observe_librdkafka_stats(stats)

    def _on_error(self, err: KafkaError) -> None:
        # Broker-down notices arrive here during the Section 7.3 kill test.
        # They are informational: librdkafka recovers on its own, and treating
        # them as fatal would hide the very recovery being measured.
        level = logging.WARNING if err.fatal() else logging.DEBUG
        log.log(level, "client error: %s", err)

    def _on_delivery(self, err: Optional[KafkaError], msg) -> None:
        if err is not None:
            self.failed += 1
            metrics.PRODUCER_DELIVERY_FAILURES.labels(reason=err.name()).inc()
            with self._failed_lock:
                self._failed_payloads.append((msg.value(), str(err)))
            return
        self.acked += 1
        self.partition_counts[msg.partition()] += 1
        metrics.PRODUCER_EVENTS.labels(topic=self.topic).inc()

    # -- send paths ----------------------------------------------------------
    def _produce_once(self, key: str, payload: bytes, topic: Optional[str] = None) -> None:
        self.producer.produce(
            topic or self.topic, key=key, value=payload, on_delivery=self._on_delivery)

    def _send(self, key: str, payload: bytes) -> None:
        """Enqueue with application-level retry around local queue pressure.

        BufferError means librdkafka's local queue is full - the client is
        applying backpressure because the brokers are not keeping up. Retrying
        that with backoff is correct; spinning on it is how a producer turns a
        transient broker slowdown into an OOM.
        """
        def attempt():
            try:
                self._produce_once(key, payload)
            except BufferError as exc:
                self.producer.poll(0)
                raise exc

        try:
            call_with_retry(
                attempt,
                policy=self.policy,
                stats=self.retry_stats,
                retry_on=(BufferError, KafkaException),
            )
            self.sent += 1
        except RetriesExhausted as exc:
            self._to_dlq(payload, f"{type(exc.last_error).__name__}: {exc.last_error}",
                         dlqmod.STAGE_PRODUCER, exc.attempts)

    def _to_dlq(self, payload: bytes, reason: str, stage: str, retries: int) -> None:
        rec = dlqmod.build(
            payload.decode("utf-8", errors="replace"), reason, stage, retries,
            source_topic=self.topic)
        try:
            self.producer.produce(self.dlq_topic, key=rec.event_id or "unknown",
                                  value=rec.to_json().encode())
            self.dlq_count += 1
            metrics.PRODUCER_DLQ.inc()
        except Exception as exc:
            # If the DLQ itself is unwritable the loss is real and must be
            # loud. Silently dropping here is precisely the failure mode the
            # "zero silent loss" claim is about.
            log.error("DLQ WRITE FAILED - EVENT LOST: %s", exc)

    def _drain_failures(self) -> None:
        with self._failed_lock:
            pending, self._failed_payloads = self._failed_payloads, []
        for payload, reason in pending:
            self._to_dlq(payload, reason, dlqmod.STAGE_PRODUCER,
                         self.retry_cfg.max_attempts)

    # -- main loop -----------------------------------------------------------
    def run(self) -> Dict[str, Any]:
        args = self.args
        rate = args.rate
        metrics.PRODUCER_TARGET_RATE.set(rate if rate > 0 else -1)

        log.info("topic=%s target_rate=%s duration=%ss %s",
                 self.topic, rate if rate > 0 else "UNTHROTTLED",
                 args.duration, self.retry_cfg.describe())

        generated = 0
        dup_emitted = 0
        t0 = time.perf_counter()
        wall_start = time.time()
        deadline = t0 + args.duration
        last_report = t0
        last_count = 0

        while self._running and time.perf_counter() < deadline:
            now = time.perf_counter()

            if rate > 0:
                budget = int((now - t0) * rate) - generated
                if budget <= 0:
                    self.producer.poll(0)
                    time.sleep(0.0005)
                    continue
                budget = min(budget, 2000)   # cap a burst after a GC pause
            else:
                budget = 2000

            for _ in range(budget):
                ev = self.gen.next_event()
                payload = json.dumps(ev, separators=(",", ":")).encode()
                self._send(ev["trip_id"], payload)
                generated += 1

                # Deliberate duplicate emission. Cassandra's primary keys are
                # derived from event fields, so these must land as upserts and
                # leave row counts unchanged - asserted by tests/test_idempotency.py.
                if args.duplicate_ratio > 0 and self.gen.rng.random() < args.duplicate_ratio:
                    dup = duplicate_of(ev)
                    self._send(dup["trip_id"],
                               json.dumps(dup, separators=(",", ":")).encode())
                    dup_emitted += 1

            self.producer.poll(0)
            self._drain_failures()

            if now - last_report >= 5.0:
                observed = (generated - last_count) / (now - last_report)
                metrics.PRODUCER_ACTUAL_RATE.set(observed)
                log.info("generated=%d acked=%d failed=%d dlq=%d rate=%.0f/s queue=%d",
                         generated, self.acked, self.failed, self.dlq_count,
                         observed, len(self.producer))
                last_report, last_count = now, generated

        log.info("flushing (up to %ss)...", args.flush_timeout)
        remaining = self.producer.flush(args.flush_timeout)
        if remaining:
            log.warning("%d messages still undelivered after flush", remaining)
        self._drain_failures()
        self.producer.flush(10)

        elapsed = time.perf_counter() - t0
        return self._report(generated, dup_emitted, elapsed, wall_start)

    def _report(self, generated: int, dup_emitted: int, elapsed: float,
                wall_start: float) -> Dict[str, Any]:
        with self._stats_lock:
            stats = dict(self._latest_stats)
        txretries, req_timeouts = metrics.librdkafka_retry_totals(stats)

        total = sum(self.partition_counts.values()) or 1
        dist = {str(p): {"count": c, "pct": round(100 * c / total, 2)}
                for p, c in sorted(self.partition_counts.items())}
        counts = list(self.partition_counts.values())
        imbalance = (max(counts) / (sum(counts) / len(counts))) if counts else 0.0

        return {
            "run_id": self.args.run_id,
            "started_at": datetime.fromtimestamp(wall_start, tz=timezone.utc).isoformat(),
            "topic": self.topic,
            "target_rate": self.args.rate,
            "duration_sec": round(elapsed, 3),
            "generated": generated,
            "duplicates_emitted": dup_emitted,
            "acked": self.acked,
            "delivery_failures": self.failed,
            "dlq": self.dlq_count,
            "achieved_rate": round(generated / elapsed, 2) if elapsed else 0,
            "acked_rate": round(self.acked / elapsed, 2) if elapsed else 0,
            "rate_attainment_pct": (
                round(100 * (generated / elapsed) / self.args.rate, 2)
                if self.args.rate > 0 and elapsed else None),
            "late_events_generated": self.gen.late_count,
            "malformed_generated": self.gen.malformed_count,
            "librdkafka_txretries": txretries,
            "librdkafka_req_timeouts": req_timeouts,
            "app_retry_stats": self.retry_stats.as_dict(),
            "backoff": self.retry_cfg.describe(),
            "partition_distribution": dist,
            "partition_imbalance_ratio": round(imbalance, 3),
            "config": {
                "bootstrap": self.kcfg.bootstrap,
                "enable_idempotence": True,
                "acks": "all",
                "linger_ms": self.args.linger_ms,
                "message_timeout_ms": self.args.message_timeout_ms,
            },
        }

    def stop(self, *_: Any) -> None:
        log.info("stop requested")
        self._running = False


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Synthetic ride-hailing trip producer")
    p.add_argument("--rate", type=float, default=float(os.environ.get("PRODUCER_TARGET_RATE", 1000)),
                   help="target events/sec; <=0 means unthrottled")
    p.add_argument("--duration", type=float, default=float(os.environ.get("PRODUCER_DURATION_SEC", 60)))
    p.add_argument("--topic", default=None)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--producer-id", default="p-1")
    p.add_argument("--late-ratio", type=float, default=0.0,
                   help="fraction of events back-dated beyond the watermark")
    p.add_argument("--malformed-ratio", type=float, default=0.0,
                   help="fraction of events emitted with a permanent defect")
    p.add_argument("--duplicate-ratio", type=float, default=0.0,
                   help="fraction of events re-emitted byte-identically")
    p.add_argument("--linger-ms", type=int, default=20)
    p.add_argument("--message-timeout-ms", type=int, default=120000)
    p.add_argument("--flush-timeout", type=float, default=60.0)
    p.add_argument("--metrics-port", type=int, default=9108)
    p.add_argument("--run-id", default=os.environ.get("RUN_ID", "adhoc"))
    p.add_argument("--report", default=None, help="write the JSON report here")
    args = p.parse_args(argv)

    metrics.serve(args.metrics_port)
    runner = ProducerRunner(args)
    signal.signal(signal.SIGTERM, runner.stop)
    signal.signal(signal.SIGINT, runner.stop)

    report = runner.run()
    print(json.dumps(report, indent=2))

    if args.report:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        with open(args.report, "w") as fh:
            json.dump(report, fh, indent=2)
        log.info("report -> %s", args.report)

    # Non-zero exit if the producer could not hold its target: a sweep step
    # that silently under-delivered must not be read as a sustainable rate.
    if args.rate > 0 and report["rate_attainment_pct"] is not None \
            and report["rate_attainment_pct"] < 95.0:
        log.warning("target rate not attained: %.1f%%", report["rate_attainment_pct"])
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
