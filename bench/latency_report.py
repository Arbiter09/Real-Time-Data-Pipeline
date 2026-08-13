"""End-to-end latency distribution from the sampled writes (Section 7.2).

Latency here is `cassandra_write_ms - produced_at_ms`:

    produced_at_ms   wall clock at producer.produce()
    cassandra_write_ms   taken on the executor immediately before the write is
                         issued for that record

It is NOT measured against `event_time`. `event_time` is the business
timestamp and is deliberately back-dated for a fraction of events to exercise
watermarking; measuring against it would report a fabricated multi-minute
latency for every late event.

CAVEAT, stated here and in the README: producer and consumer are containers on
one host and share a clock, so this number contains no clock skew. That makes
it a clean measure of pipeline latency and NOT evidence about clock discipline
in a distributed deployment.

Percentiles are computed over raw samples with linear interpolation, not
estimated from histogram buckets, so the tail is exact for the sampled
population rather than rounded to a bucket edge.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

from common.config import CassandraConfig


def percentile(sorted_vals: List[float], p: float) -> float:
    """Linear-interpolated percentile. p is 0-100."""
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    k = (len(sorted_vals) - 1) * (p / 100.0)
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return float(sorted_vals[int(k)])
    return float(sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo))


def fetch(run_id: str, cfg: CassandraConfig) -> List[int]:
    cluster = Cluster(cfg.hosts)
    try:
        session = cluster.connect(cfg.keyspace)
        # Partition key is (run_id, minute_bucket), so the buckets present for
        # this run have to be discovered before they can be read by key. This
        # one query is an unavoidable scan; everything after it is keyed.
        buckets = session.execute(SimpleStatement(
            "SELECT DISTINCT run_id, minute_bucket FROM latency_samples",
            consistency_level=ConsistencyLevel.QUORUM, fetch_size=5000))
        mine = [r.minute_bucket for r in buckets if r.run_id == run_id]

        samples: List[int] = []
        stmt = session.prepare(
            "SELECT latency_ms FROM latency_samples "
            "WHERE run_id=? AND minute_bucket=?")
        stmt.consistency_level = ConsistencyLevel.QUORUM
        stmt.fetch_size = 5000
        for b in sorted(mine):
            for row in session.execute(stmt, (run_id, b), timeout=120):
                if row.latency_ms is not None and row.latency_ms >= 0:
                    samples.append(int(row.latency_ms))
        return samples
    finally:
        cluster.shutdown()


def build_report(run_id: str, samples: List[int],
                 sample_rate: Optional[float]) -> Dict[str, Any]:
    s = sorted(samples)
    n = len(s)
    if n == 0:
        return {"run_id": run_id, "samples": 0,
                "error": "no latency samples found - was LATENCY_SAMPLE_RATE > 0?"}

    p50, p95, p99 = percentile(s, 50), percentile(s, 95), percentile(s, 99)
    mean = sum(s) / n
    report = {
        "run_id": run_id,
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "samples": n,
        "sample_rate": sample_rate,
        "definition": "produced_at_ms -> cassandra write, milliseconds",
        "min_ms": s[0],
        "p50_ms": round(p50, 1),
        "p90_ms": round(percentile(s, 90), 1),
        "p95_ms": round(p95, 1),
        "p99_ms": round(p99, 1),
        "p999_ms": round(percentile(s, 99.9), 1),
        "max_ms": s[-1],
        "mean_ms": round(mean, 1),
        # The mean is reported LAST and flagged, because quoting it is how a
        # latency claim hides its tail. p95 is the number worth claiming.
        "mean_is_not_the_headline": (
            "p95 is the figure to quote; the mean sits below it and hides the "
            "tail a reliability reviewer cares about"),
        "sub_second_at": (
            "p99" if p99 < 1000 else
            "p95" if p95 < 1000 else
            "p50" if p50 < 1000 else "none"),
        "pct_under_1s": round(100 * sum(1 for v in s if v < 1000) / n, 3),
        "pct_under_500ms": round(100 * sum(1 for v in s if v < 500) / n, 3),
        "clock_caveat": ("producer and consumer share a host clock; this "
                         "measurement contains no clock skew and is not "
                         "evidence about cross-host clock discipline"),
    }
    return report


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="E2E latency percentiles")
    p.add_argument("--run-id", required=True)
    p.add_argument("--sample-rate", type=float, default=None)
    p.add_argument("--report", default=None)
    args = p.parse_args(argv)

    cfg = CassandraConfig()
    samples = fetch(args.run_id, cfg)
    report = build_report(args.run_id, samples, args.sample_rate)

    print(json.dumps(report, indent=2))
    if report.get("samples", 0):
        print(f"\n  p50 {report['p50_ms']:>9.1f} ms")
        print(f"  p95 {report['p95_ms']:>9.1f} ms   <- the number to quote")
        print(f"  p99 {report['p99_ms']:>9.1f} ms")
        print(f"  sub-second holds at: {report['sub_second_at']}\n")

    if args.report:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        with open(args.report, "w") as fh:
            json.dump(report, fh, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
