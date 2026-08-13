"""Baseline (wide table) vs treatment (star schema): the Section 8 experiment.

Protocol, chosen so the result cannot be an artifact of the harness:

  1. EQUIVALENCE FIRST. Each query pair is executed once and the result sets
     compared. A pair that disagrees is reported as MISMATCH and its timings
     are withheld - a faster query returning different rows is not a result.

  2. WARM CACHE. Every query runs `--warmups` times before timing starts, per
     arm. Cold-cache timings would measure the page cache, not the schema.

  3. INTERLEAVED ITERATIONS. Timed runs alternate baseline, star, baseline,
     star... rather than running all of one arm and then the other. If the
     machine drifts - a compaction kicks in, a background job wakes - the
     drift hits both arms roughly equally instead of landing entirely on
     whichever ran second.

  4. MEDIAN, NOT MEAN. One slow outlier from an unrelated process should not
     decide the reported ratio. Min and p95 are reported alongside so the
     spread is visible.

  5. PER-QUERY RATIOS. No blended headline number. If one query gets 6x and
     another gets 0.8x, both appear.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

from bench.analytics_queries import QUERIES
from common.config import PostgresConfig


def connect(cfg: PostgresConfig):
    conn = psycopg2.connect(cfg.dsn())
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute("SET work_mem = '64MB'")
    return conn


def _normalize(rows: List[Tuple]) -> List[Tuple]:
    """Make result sets comparable across arms.

    numeric vs float and date vs datetime differ by type between the two
    schemas even when the values agree, so compare on rounded string forms.
    """
    out = []
    for r in rows:
        norm = []
        for v in r:
            if isinstance(v, (Decimal, float)):
                norm.append(f"{float(v):.2f}")
            elif isinstance(v, datetime):
                norm.append(v.replace(tzinfo=None).isoformat())
            elif isinstance(v, date):
                norm.append(v.isoformat())
            else:
                norm.append(str(v))
        out.append(tuple(norm))
    return sorted(out)


def run_query(conn, sql: str, params: Dict[str, Any]) -> Tuple[float, List[Tuple]]:
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute(sql, params)
        rows = cur.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000.0
    return elapsed, rows


def explain(conn, sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params)
        plan = cur.fetchone()[0]
    root = plan[0]["Plan"]
    return {
        "node": root.get("Node Type"),
        "total_ms": plan[0].get("Execution Time"),
        "planning_ms": plan[0].get("Planning Time"),
        "shared_hit": root.get("Shared Hit Blocks"),
        "shared_read": root.get("Shared Read Blocks"),
        "rows": root.get("Actual Rows"),
    }


def summarize(samples: List[float]) -> Dict[str, float]:
    s = sorted(samples)
    return {
        "runs": len(s),
        "median_ms": round(statistics.median(s), 3),
        "mean_ms": round(statistics.fmean(s), 3),
        "min_ms": round(s[0], 3),
        "max_ms": round(s[-1], 3),
        "p95_ms": round(s[min(len(s) - 1, int(round(0.95 * (len(s) - 1))))], 3),
        "stdev_ms": round(statistics.stdev(s), 3) if len(s) > 1 else 0.0,
    }


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Star schema vs wide table benchmark")
    p.add_argument("--iterations", type=int, default=9)
    p.add_argument("--warmups", type=int, default=3)
    p.add_argument("--start-date", type=lambda s: date.fromisoformat(s),
                   default=date(2026, 6, 1))
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--only", default=None, help="run a single query by name")
    p.add_argument("--report", default="/opt/app/results/raw/analytics_bench.json")
    args = p.parse_args(argv)

    end_date = args.start_date + timedelta(days=args.days)
    params = {
        "start_date": args.start_date,
        "end_date": end_date,
        "start_ts": datetime.combine(args.start_date, datetime.min.time(),
                                     tzinfo=timezone.utc),
        "end_ts": datetime.combine(end_date, datetime.min.time(),
                                   tzinfo=timezone.utc),
    }

    cfg = PostgresConfig()
    conn = connect(cfg)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM trips_wide")
        wide_rows = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fact_trip")
        fact_rows = cur.fetchone()[0]
    if wide_rows != fact_rows:
        print(f"REFUSING TO BENCHMARK: trips_wide has {wide_rows:,} rows but "
              f"fact_trip has {fact_rows:,}. The arms must hold identical data.",
              file=sys.stderr)
        return 2
    print(f"corpus: {wide_rows:,} rows in both arms, window "
          f"{args.start_date} .. {end_date}\n")

    names = [args.only] if args.only else list(QUERIES)
    results: Dict[str, Any] = {}

    for name in names:
        q = QUERIES[name]
        print(f"--- {name}")

        # 1. equivalence
        _, base_rows = run_query(conn, q["baseline"], params)
        _, star_rows = run_query(conn, q["star"], params)
        nb, ns = _normalize(base_rows), _normalize(star_rows)
        equivalent = nb == ns
        if not equivalent:
            print(f"    MISMATCH: baseline {len(nb)} rows, star {len(ns)} rows "
                  f"- timings withheld")
            diff = [(b, s) for b, s in zip(nb, ns) if b != s][:3]
            results[name] = {
                "description": q["description"],
                "equivalent": False,
                "baseline_rows": len(nb), "star_rows": len(ns),
                "first_differences": diff,
            }
            continue

        # 2. warm both arms
        for _ in range(args.warmups):
            run_query(conn, q["baseline"], params)
            run_query(conn, q["star"], params)

        # 3. interleaved timed runs
        base_t: List[float] = []
        star_t: List[float] = []
        for _ in range(args.iterations):
            base_t.append(run_query(conn, q["baseline"], params)[0])
            star_t.append(run_query(conn, q["star"], params)[0])

        b, s = summarize(base_t), summarize(star_t)
        ratio = b["median_ms"] / s["median_ms"] if s["median_ms"] else None

        results[name] = {
            "description": q["description"],
            "equivalent": True,
            "result_rows": len(nb),
            "baseline": b,
            "star": s,
            "speedup_x": round(ratio, 3) if ratio else None,
            "faster_arm": ("star" if ratio and ratio > 1 else
                           "baseline" if ratio else None),
            "baseline_plan": explain(conn, q["baseline"], params),
            "star_plan": explain(conn, q["star"], params),
        }
        verdict = (f"star {ratio:.2f}x faster" if ratio and ratio >= 1
                   else f"baseline {1/ratio:.2f}x faster" if ratio else "?")
        print(f"    baseline {b['median_ms']:>9.2f} ms   "
              f"star {s['median_ms']:>9.2f} ms   -> {verdict}")

    timed = [r for r in results.values() if r.get("equivalent") and r.get("speedup_x")]
    ratios = [r["speedup_x"] for r in timed]

    report = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "corpus_rows": wide_rows,
        "window": {"start": args.start_date.isoformat(), "end": end_date.isoformat()},
        "protocol": {
            "iterations": args.iterations,
            "warmups": args.warmups,
            "interleaved": True,
            "statistic": "median of interleaved timed runs",
        },
        "queries": results,
        "summary": {
            "queries_timed": len(timed),
            "queries_mismatched": sum(1 for r in results.values()
                                      if not r.get("equivalent")),
            # Geometric mean, because these are ratios. An arithmetic mean of
            # speedups over-weights the query that happened to win biggest.
            "geometric_mean_speedup": (
                round(statistics.geometric_mean(ratios), 3) if ratios else None),
            "median_speedup": round(statistics.median(ratios), 3) if ratios else None,
            "best": (max(timed, key=lambda r: r["speedup_x"])["speedup_x"]
                     if timed else None),
            "worst": (min(timed, key=lambda r: r["speedup_x"])["speedup_x"]
                      if timed else None),
        },
    }

    print("\n" + "=" * 72)
    print(f"  {len(timed)} queries timed | geomean "
          f"{report['summary']['geometric_mean_speedup']}x | "
          f"best {report['summary']['best']}x | worst {report['summary']['worst']}x")
    print("=" * 72)

    if args.report:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        with open(args.report, "w") as fh:
            json.dump(report, fh, indent=2, default=str)
        print(f"report -> {args.report}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
