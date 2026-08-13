"""Throughput sweep (Section 7.1): find the SUSTAINABLE rate, not the peak.

The sustainable rate is the highest target at which consumer lag stays flat.
It is not the highest rate the producer can emit, and it is not the rate at
which the largest number appeared on screen once.

A step counts as SUSTAINED only if all of these hold:

  1. The producer actually attained its target (>= 95%). A step where the
     producer under-delivered says nothing about the consumer.
  2. Lag did not grow monotonically for longer than `--max-lag-growth-run`
     consecutive samples. This is the same condition the Grafana alert fires
     on, so the benchmark and the alert agree on what "unhealthy" means.
  3. The backlog fully drained within the drain timeout.
  4. Reconciliation shows zero unaccounted events.
  5. THE CONSUMER KEPT PACE: the post-production drain took no longer than
     `--max-drain-ratio` of the production window.

Criterion 5 exists because criteria 2 and 3 were not sufficient, and the first
run of this sweep proved it. With `maxOffsetsPerTrigger` set large, Spark
claims every available offset on each trigger, so `offsetsBehindLatest` reads
~0 even while a single batch takes 52 seconds to process. Lag looked perfect at
2000/s while the run needed 90s of production plus 70s of drain - the consumer
was 1.8x behind and the lag gauge could not see it.

The drain ratio catches exactly that: if the consumer needed a long tail after
the producer stopped, it was never keeping up. Effective consumer throughput,
`events / (produce_window + drain)`, is reported per step for the same reason.

The reported number is the highest rate where every step at or below it
sustained. Reporting the highest *individual* success would let a lucky step
above a failing one set the headline.

Throughput without topology is meaningless, so partition count, executor
count and write amplification are reported alongside.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(REPO, "results", "raw")


def run_step(rate: float, args: argparse.Namespace, idx: int) -> Dict[str, Any]:
    run_id = f"{args.prefix}_r{int(rate)}"
    cmd = [
        sys.executable, os.path.join(REPO, "bench", "run_scenario.py"),
        "--run-id", run_id,
        "--scenario", f"throughput_sweep_step_{idx}",
        "--rate", str(rate),
        "--duration", str(args.duration),
        "--tables", args.tables,
        "--total-cores", str(args.total_cores),
        "--executor-cores", str(args.executor_cores),
        "--executor-memory", args.executor_memory,
        "--latency-sample-rate", str(args.latency_sample_rate),
        "--drain-timeout", str(args.drain_timeout),
    ]
    print(f"\n{'=' * 72}\n  STEP {idx}: target {rate:g} events/sec\n{'=' * 72}")
    proc = subprocess.run(cmd, timeout=int(args.duration) + args.drain_timeout + 900)

    path = os.path.join(RESULTS, f"{run_id}_scenario.json")
    try:
        with open(path) as fh:
            report = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        return {"rate": rate, "run_id": run_id, "sustained": False,
                "error": f"no scenario report: {exc}",
                "exit_code": proc.returncode}

    prod = report.get("producer") or {}
    lag = report.get("lag") or {}
    rec = report.get("reconciliation") or {}
    stream = report.get("stream") or {}

    attainment = prod.get("rate_attainment_pct") or 0
    growth_run = lag.get("longest_monotonic_growth_run", 0)
    drained = bool(lag.get("drained"))
    unaccounted = rec.get("unaccounted_silent_loss", 0)

    duration = float(args.duration)
    drain_s = lag.get("drain_seconds") or 0.0
    written = rec.get("written_distinct_trips") or 0
    effective = written / (duration + drain_s) if (duration + drain_s) else 0.0
    drain_ratio = drain_s / duration if duration else 0.0

    reasons = []
    if drain_ratio > args.max_drain_ratio:
        reasons.append(
            f"consumer did not keep pace: drain took {drain_s:.0f}s after a "
            f"{duration:.0f}s window (ratio {drain_ratio:.2f} > "
            f"{args.max_drain_ratio})")
    if attainment < 95:
        reasons.append(f"producer attained only {attainment}% of target")
    if growth_run > args.max_lag_growth_run:
        reasons.append(f"lag grew monotonically for {growth_run} samples "
                       f"(limit {args.max_lag_growth_run})")
    if not drained:
        reasons.append("backlog did not drain within timeout")
    if unaccounted and unaccounted > 0:
        reasons.append(f"{unaccounted} events unaccounted for")

    return {
        "rate": rate,
        "run_id": run_id,
        "sustained": not reasons,
        "failed_because": reasons,
        "achieved_rate": prod.get("achieved_rate"),
        "rate_attainment_pct": attainment,
        "acked": prod.get("acked"),
        "written": rec.get("written_distinct_trips"),
        "dlq": rec.get("dlq_distinct_events"),
        "unaccounted": unaccounted,
        "lag_max": lag.get("max_observed"),
        "lag_final": lag.get("final"),
        "lag_growth_run": growth_run,
        "drain_seconds": drain_s,
        "drain_ratio": round(drain_ratio, 3),
        "effective_consumer_rate": round(effective, 1),
        "rows_written_cassandra": stream.get("rows_written_cassandra"),
        "write_amplification": stream.get("write_amplification"),
        "batch_seconds_p95": stream.get("batch_seconds_p95"),
        "mean_latency_ms_sampled": stream.get("mean_e2e_latency_ms_sampled"),
    }


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Throughput sweep to the lag-flat rate")
    p.add_argument("--rates", default="500,1000,1500,2000,3000,4000",
                   help="comma-separated target rates, ascending")
    p.add_argument("--duration", type=float, default=120)
    p.add_argument("--prefix", default="sweep")
    p.add_argument("--tables", choices=["all", "core"], default="all")
    p.add_argument("--total-cores", type=int, default=6)
    p.add_argument("--executor-cores", type=int, default=3)
    p.add_argument("--executor-memory", default="1g")
    p.add_argument("--latency-sample-rate", type=float, default=0.05)
    p.add_argument("--drain-timeout", type=int, default=300)
    p.add_argument("--max-lag-growth-run", type=int, default=5,
                   help="consecutive rising lag samples tolerated per step")
    p.add_argument("--max-drain-ratio", type=float, default=0.25,
                   help="drain time / produce window above which the consumer "
                        "is judged not to have kept pace")
    p.add_argument("--stop-after-failures", type=int, default=2)
    p.add_argument("--settle", type=float, default=20,
                   help="seconds between steps for the stack to settle")
    p.add_argument("--report", default=os.path.join(RESULTS, "throughput_sweep.json"))
    args = p.parse_args(argv)

    rates = [float(r) for r in args.rates.split(",")]
    steps: List[Dict[str, Any]] = []
    consecutive_failures = 0

    for i, rate in enumerate(rates, 1):
        step = run_step(rate, args, i)
        steps.append(step)
        status = "SUSTAINED" if step["sustained"] else "NOT sustained"
        print(f"\n  -> {rate:g}/s {status}"
              + ("" if step["sustained"] else f": {'; '.join(step['failed_because'])}"))

        consecutive_failures = 0 if step["sustained"] else consecutive_failures + 1
        if consecutive_failures >= args.stop_after_failures:
            print(f"\n  stopping sweep after {consecutive_failures} consecutive "
                  f"failures - the ceiling is below {rate:g}/s")
            break
        if i < len(rates):
            time.sleep(args.settle)

    # Highest rate such that every step at or below it sustained.
    sustainable = None
    for step in sorted(steps, key=lambda s: s["rate"]):
        if step["sustained"]:
            sustainable = step
        else:
            break

    report = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "protocol": {
            "definition": ("highest target rate at which lag stays flat, the "
                           "producer attains its target, the backlog drains, "
                           "and reconciliation shows no unaccounted events"),
            "step_duration_sec": args.duration,
            "max_lag_growth_run": args.max_lag_growth_run,
        },
        "topology": {
            "kafka_brokers": 3,
            "topic_partitions": int(os.environ.get("TOPIC_PARTITIONS", 6)),
            "replication_factor": 3,
            "min_insync_replicas": 2,
            "cassandra_nodes": 3,
            "cassandra_rf": 3,
            "write_consistency": os.environ.get("CASSANDRA_WRITE_CONSISTENCY", "QUORUM"),
            "spark_total_cores": args.total_cores,
            "spark_executor_cores": args.executor_cores,
            "spark_executor_memory": args.executor_memory,
            "cassandra_tables_written": args.tables,
            "host": "single host, Docker Compose",
        },
        "steps": steps,
        "sustained_rate": sustainable["rate"] if sustainable else None,
        "sustained_step": sustainable,
        "first_failing_rate": next((s["rate"] for s in steps if not s["sustained"]), None),
    }

    print("\n" + "=" * 72)
    print("  THROUGHPUT SWEEP")
    print("=" * 72)
    print(f"  {'target':>8} {'achieved':>9} {'effective':>10} {'drain':>7} "
          f"{'ratio':>6} {'p95 batch':>10}  verdict")
    for s in steps:
        print(f"  {s['rate']:>8.0f} {s.get('achieved_rate') or 0:>9.0f} "
              f"{s.get('effective_consumer_rate') or 0:>10.0f} "
              f"{s.get('drain_seconds') or 0:>7.1f} "
              f"{s.get('drain_ratio') or 0:>6.2f} "
              f"{s.get('batch_seconds_p95') or 0:>10.1f}  "
              f"{'sustained' if s['sustained'] else 'FAILED'}")
    print("-" * 72)
    print(f"  SUSTAINED RATE: {report['sustained_rate']} events/sec")
    print(f"  topology: {report['topology']['topic_partitions']} partitions, "
          f"{args.total_cores} executor cores, RF=3, CL=QUORUM, "
          f"{args.tables} Cassandra tables")
    print("=" * 72)

    if args.report:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        with open(args.report, "w") as fh:
            json.dump(report, fh, indent=2)
        print(f"report -> {args.report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
