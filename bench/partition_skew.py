"""Measure partition balance under each candidate Kafka partition key.

The README claims `city_id` was rejected as a partition key because it skews.
This harness is why that sentence is allowed to exist: it replays the real
generator through Kafka's own partitioner (murmur2 over the key bytes, modulo
partition count) and reports the resulting distribution.

No Kafka connection needed - the partitioner is deterministic, so the answer is
computable offline and the result is reproducible by anyone with the repo.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from typing import Dict, List

from producer.events import TripGenerator


def murmur2(data: bytes) -> int:
    """Kafka's murmur2, ported exactly.

    This is org.apache.kafka.common.utils.Utils.murmur2. The default partitioner
    computes `toPositive(murmur2(keyBytes)) % numPartitions`, so reproducing it
    here gives the same partition assignment the broker-side view would show.
    """
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24

    h = (seed ^ length) & 0xFFFFFFFF
    length4 = length // 4

    for i in range(length4):
        i4 = i * 4
        k = (data[i4] & 0xFF) | ((data[i4 + 1] & 0xFF) << 8) | \
            ((data[i4 + 2] & 0xFF) << 16) | ((data[i4 + 3] & 0xFF) << 24)
        k = (k * m) & 0xFFFFFFFF
        k ^= (k >> r)
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k

    rem = length % 4
    if rem == 3:
        h ^= (data[(length & ~3) + 2] & 0xFF) << 16
    if rem >= 2:
        h ^= (data[(length & ~3) + 1] & 0xFF) << 8
    if rem >= 1:
        h ^= (data[length & ~3] & 0xFF)
        h = (h * m) & 0xFFFFFFFF

    h ^= (h >> 13)
    h = (h * m) & 0xFFFFFFFF
    h ^= (h >> 15)
    return h & 0xFFFFFFFF


def to_positive(x: int) -> int:
    return x & 0x7FFFFFFF


def partition_for(key: str, num_partitions: int) -> int:
    return to_positive(murmur2(key.encode("utf-8"))) % num_partitions


def analyse(counts: Counter, num_partitions: int) -> Dict:
    total = sum(counts.values())
    per = [counts.get(p, 0) for p in range(num_partitions)]
    ideal = total / num_partitions if num_partitions else 0
    hottest = max(per) if per else 0
    coldest = min(per) if per else 0
    return {
        "total_events": total,
        "partitions": num_partitions,
        "per_partition": per,
        "per_partition_pct": [round(100 * c / total, 2) if total else 0 for c in per],
        "ideal_per_partition": round(ideal, 1),
        # Imbalance ratio: hottest partition / perfectly even share. 1.00 is
        # perfect. This is the number that matters, because consumer
        # parallelism is bounded by the SLOWEST partition's backlog - a
        # partition carrying 3x its share sets the pipeline's pace.
        "imbalance_ratio": round(hottest / ideal, 3) if ideal else 0,
        "hottest_partition_pct": round(100 * hottest / total, 2) if total else 0,
        "coldest_partition_pct": round(100 * coldest / total, 2) if total else 0,
        "empty_partitions": sum(1 for c in per if c == 0),
    }


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Kafka partition skew by key choice")
    p.add_argument("--events", type=int, default=200_000)
    p.add_argument("--partitions", type=int,
                   default=int(os.environ.get("TOPIC_PARTITIONS", 6)))
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--report", default=None)
    args = p.parse_args(argv)

    gen = TripGenerator(seed=args.seed)
    strategies = ["trip_id", "city_id", "driver_id", "rider_id"]
    counters = {s: Counter() for s in strategies}
    distinct_keys = {s: set() for s in strategies}

    for _ in range(args.events):
        ev = gen.next_event()
        for s in strategies:
            key = str(ev[s])
            counters[s][partition_for(key, args.partitions)] += 1
            if len(distinct_keys[s]) < 200_000:
                distinct_keys[s].add(key)

    result = {
        "events": args.events,
        "partitions": args.partitions,
        "seed": args.seed,
        "strategies": {
            s: {**analyse(counters[s], args.partitions),
                "distinct_keys": len(distinct_keys[s])}
            for s in strategies
        },
    }

    chosen = result["strategies"]["trip_id"]["imbalance_ratio"]
    rejected = result["strategies"]["city_id"]["imbalance_ratio"]
    result["verdict"] = {
        "chosen_key": "trip_id",
        "chosen_imbalance": chosen,
        "rejected_key": "city_id",
        "rejected_imbalance": rejected,
        "skew_multiple": round(rejected / chosen, 2) if chosen else None,
        "note": ("city_id has only 8 distinct values across 6 partitions, so "
                 "partition assignment is decided by 8 hash values and cannot "
                 "balance regardless of volume."),
    }

    print(json.dumps(result, indent=2))
    if args.report:
        os.makedirs(os.path.dirname(args.report), exist_ok=True)
        with open(args.report, "w") as fh:
            json.dump(result, fh, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
