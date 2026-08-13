"""Load an identical corpus into both the baseline and the star schema.

WHY THE CORPUS IS GENERATED RATHER THAN STREAMED:
The analytical query set spans weeks of history. Producing that through the
streaming path would take weeks of wall clock. So this loader generates the
corpus with the SAME `TripGenerator` the producer uses, spread across a date
range, and bulk-loads it into both schemas via COPY.

The production path (Cassandra -> Postgres) is real and is exercised by the
Airflow `trips_batch_rollup` DAG on live streamed data. This loader exists to
give the Section 8 experiment enough history to be meaningful, and it is
stated as such in the README rather than implied to be the streaming path.

FAIRNESS: both schemas are loaded from the same generated rows in the same
pass, then both are ANALYZEd. Neither gets data the other does not have.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import psycopg2

from common.config import PostgresConfig
from producer.events import (CITY_IDS, N_DRIVERS, PAYMENT_TYPES, VEHICLE_CLASSES,
                             TripGenerator)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CITY_META = {
    "nyc": ("New York",     "Northeast", "USA"),
    "chi": ("Chicago",      "Midwest",   "USA"),
    "sfo": ("San Francisco", "West",     "USA"),
    "lax": ("Los Angeles",  "West",      "USA"),
    "bos": ("Boston",       "Northeast", "USA"),
    "sea": ("Seattle",      "West",      "USA"),
    "aus": ("Austin",       "South",     "USA"),
    "mia": ("Miami",        "South",     "USA"),
}
VEHICLE_TIER = {"economy": "standard", "comfort": "standard",
                "xl": "premium", "black": "premium"}
CASHLESS = {"card": True, "wallet": True, "cash": False}
TENURE_BANDS = ["new", "established", "veteran"]


def connect(cfg: PostgresConfig):
    conn = psycopg2.connect(cfg.dsn())
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    return conn


def apply_schema(conn, path: str) -> None:
    with open(path) as fh:
        sql = fh.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"  applied {os.path.basename(path)}")


def load_dimensions(conn, start: date, days: int) -> Dict[str, Dict[Any, int]]:
    maps: Dict[str, Dict[Any, int]] = {}
    with conn.cursor() as cur:
        # dim_date - padded either side so a trip that lands just outside the
        # generated window still finds its key instead of violating the FK.
        rows = []
        for i in range(-2, days + 3):
            d = start + timedelta(days=i)
            iso_year, iso_week, _ = d.isocalendar()
            rows.append((
                int(d.strftime("%Y%m%d")), d, d.year, (d.month - 1) // 3 + 1,
                d.month, d.strftime("%B"), d.day,
                (d.weekday() + 1) % 7,          # 0 = Sunday, matching EXTRACT(DOW)
                d.strftime("%A"), iso_week, iso_year,
                d.weekday() >= 5,
            ))
        cur.executemany(
            "INSERT INTO dim_date (date_key, full_date, year, quarter, month, "
            "month_name, day_of_month, day_of_week, day_name, iso_week, "
            "iso_year, is_weekend) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING", rows)

        city_map = {}
        for i, cid in enumerate(CITY_IDS):
            name, region, country = CITY_META[cid]
            cur.execute("INSERT INTO dim_city VALUES (%s,%s,%s,%s,%s) "
                        "ON CONFLICT DO NOTHING", (i + 1, cid, name, region, country))
            city_map[cid] = i + 1
        maps["city"] = city_map

        veh_map = {}
        for i, v in enumerate(VEHICLE_CLASSES):
            cur.execute("INSERT INTO dim_vehicle VALUES (%s,%s,%s) "
                        "ON CONFLICT DO NOTHING", (i + 1, v, VEHICLE_TIER[v]))
            veh_map[v] = i + 1
        maps["vehicle"] = veh_map

        pay_map = {}
        for i, p in enumerate(PAYMENT_TYPES):
            cur.execute("INSERT INTO dim_payment VALUES (%s,%s,%s) "
                        "ON CONFLICT DO NOTHING", (i + 1, p, CASHLESS[p]))
            pay_map[p] = i + 1
        maps["payment"] = pay_map

        rng = random.Random(7)
        drivers = [(k, f"d-{k:06d}", rng.choice(TENURE_BANDS))
                   for k in range(N_DRIVERS)]
        cur.executemany("INSERT INTO dim_driver VALUES (%s,%s,%s) "
                        "ON CONFLICT DO NOTHING", drivers)
    conn.commit()
    print(f"  dimensions loaded (dates={days + 5}, cities={len(CITY_IDS)}, "
          f"drivers={N_DRIVERS})")
    return maps


def generate_and_load(conn, args: argparse.Namespace,
                      maps: Dict[str, Dict[Any, int]]) -> Dict[str, Any]:
    gen = TripGenerator(seed=args.seed)
    rng = random.Random(args.seed + 1)
    start_dt = datetime.combine(args.start_date, datetime.min.time(),
                                tzinfo=timezone.utc)
    window_sec = args.days * 86400

    wide_buf = io.StringIO()
    fact_buf = io.StringIO()
    wide_w = csv.writer(wide_buf)
    fact_w = csv.writer(fact_buf)

    city_map, veh_map, pay_map = maps["city"], maps["vehicle"], maps["payment"]
    t0 = time.time()
    written = 0

    def flush() -> None:
        nonlocal wide_buf, fact_buf, wide_w, fact_w
        wide_buf.seek(0)
        fact_buf.seek(0)
        with conn.cursor() as cur:
            cur.copy_expert(
                "COPY trips_wide (trip_id, event_id, event_time, city_id, "
                "city_name, region, driver_id, rider_id, pickup_zone_id, "
                "dropoff_zone_id, vehicle_class, payment_type, distance_km, "
                "duration_sec, fare_amount, surge_multiplier, status) "
                "FROM STDIN WITH CSV", wide_buf)
            cur.copy_expert(
                "COPY fact_trip (trip_id, date_key, driver_key, duration_sec, "
                "pickup_zone_id, fare_amount, distance_km, surge_multiplier, "
                "city_key, vehicle_key, payment_key, hour_of_day, is_completed, "
                "is_surged) FROM STDIN WITH CSV", fact_buf)
        conn.commit()
        wide_buf = io.StringIO()
        fact_buf = io.StringIO()
        wide_w = csv.writer(wide_buf)
        fact_w = csv.writer(fact_buf)

    for i in range(args.rows):
        ev = gen.next_event()
        # Spread events across the window with a diurnal shape, so the
        # hour-of-day query has something real to find rather than noise.
        day_offset = rng.randrange(args.days)
        hour = rng.choices(range(24), weights=DIURNAL_WEIGHTS, k=1)[0]
        secs = day_offset * 86400 + hour * 3600 + rng.randrange(3600)
        et = start_dt + timedelta(seconds=secs)

        cid = ev["city_id"]
        name, region, _ = CITY_META[cid]
        driver_key = int(ev["driver_id"].split("-")[1])
        completed = ev["status"] == "completed"

        wide_w.writerow([
            ev["trip_id"], ev["event_id"], et.isoformat(), cid, name, region,
            ev["driver_id"], ev["rider_id"], ev["pickup_zone_id"],
            ev["dropoff_zone_id"], ev["vehicle_class"], ev["payment_type"],
            ev["distance_km"], ev["duration_sec"], ev["fare_amount"],
            ev["surge_multiplier"], ev["status"]])
        fact_w.writerow([
            ev["trip_id"], int(et.strftime("%Y%m%d")), driver_key,
            ev["duration_sec"], ev["pickup_zone_id"], ev["fare_amount"],
            ev["distance_km"], ev["surge_multiplier"], city_map[cid],
            veh_map[ev["vehicle_class"]], pay_map[ev["payment_type"]],
            et.hour, completed, ev["surge_multiplier"] > 1.0])

        written += 1
        if written % args.batch == 0:
            flush()
            elapsed = time.time() - t0
            print(f"  {written:,}/{args.rows:,} rows ({written/elapsed:,.0f}/s)",
                  flush=True)

    flush()
    elapsed = time.time() - t0
    print(f"  loaded {written:,} rows in {elapsed:.1f}s")
    return {"rows": written, "load_seconds": round(elapsed, 2)}


# Rough diurnal demand curve: morning and evening peaks, 3am trough.
DIURNAL_WEIGHTS = [3, 2, 1.5, 1, 1, 2, 4, 7, 9, 7, 5, 5,
                   6, 6, 5, 6, 8, 10, 9, 8, 7, 6, 5, 4]


def post_load(conn) -> Dict[str, Any]:
    print("  ANALYZE + size report")
    stats: Dict[str, Any] = {}
    with conn.cursor() as cur:
        for t in ("trips_wide", "fact_trip", "dim_date", "dim_city",
                  "dim_driver", "dim_vehicle", "dim_payment"):
            cur.execute(f"ANALYZE {t}")
        conn.commit()
        # Table size is the mechanism behind any speedup, so it is reported
        # alongside the timings rather than left as an assertion.
        cur.execute("""
            SELECT relname,
                   pg_total_relation_size(c.oid)  AS total_bytes,
                   pg_relation_size(c.oid)        AS heap_bytes,
                   reltuples::bigint              AS est_rows
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND relkind = 'r'
              AND relname IN ('trips_wide','fact_trip','dim_date','dim_city',
                              'dim_driver','dim_vehicle','dim_payment')
            ORDER BY total_bytes DESC""")
        for name, total, heap, rows in cur.fetchall():
            stats[name] = {"total_bytes": int(total), "heap_bytes": int(heap),
                           "total_mb": round(total / 1048576, 2),
                           "heap_mb": round(heap / 1048576, 2),
                           "est_rows": int(rows)}
    return stats


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load the analytics corpus")
    p.add_argument("--rows", type=int, default=3_000_000)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--start-date", type=lambda s: date.fromisoformat(s),
                   default=date(2026, 6, 1))
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--batch", type=int, default=250_000)
    p.add_argument("--report", default="/opt/app/results/raw/analytics_load.json")
    args = p.parse_args(argv)

    cfg = PostgresConfig()
    conn = connect(cfg)
    print(f"loading {args.rows:,} rows over {args.days} days from {args.start_date}")

    apply_schema(conn, os.path.join(REPO, "sql", "analytics", "01_baseline.sql"))
    apply_schema(conn, os.path.join(REPO, "sql", "analytics", "02_star.sql"))
    maps = load_dimensions(conn, args.start_date, args.days)
    load_stats = generate_and_load(conn, args, maps)
    sizes = post_load(conn)

    report = {
        "loaded_at": datetime.now(timezone.utc).isoformat(),
        "rows": load_stats["rows"],
        "days": args.days,
        "start_date": args.start_date.isoformat(),
        "seed": args.seed,
        "load_seconds": load_stats["load_seconds"],
        "table_sizes": sizes,
        "fact_vs_wide_heap_ratio": (
            round(sizes["fact_trip"]["heap_bytes"] / sizes["trips_wide"]["heap_bytes"], 4)
            if "fact_trip" in sizes and "trips_wide" in sizes else None),
    }
    print(json.dumps(report, indent=2))
    if args.report:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        with open(args.report, "w") as fh:
            json.dump(report, fh, indent=2)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
