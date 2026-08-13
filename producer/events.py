"""Synthetic ride-hailing trip generator.

Design goals, in priority order:
  1. Deterministic under a seed, so a benchmark can be re-run and compared.
  2. Realistically skewed, so partition-key choice is a real decision.
  3. Cheap - the generator must not become the throughput bottleneck. At the
     rates in Section 7.1 the producer has ~1ms per event for everything;
     anything fancy here would be measuring the generator, not the pipeline.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

SCHEMA_VERSION = 1

# Zipf-ish city weights. NYC and CHI dominate on purpose: this is what makes
# keying Kafka partitions on city_id a measurably bad idea.
CITIES: List[tuple[str, float]] = [
    ("nyc", 0.34), ("chi", 0.21), ("sfo", 0.14), ("lax", 0.11),
    ("bos", 0.08), ("sea", 0.06), ("aus", 0.04), ("mia", 0.02),
]
CITY_IDS = [c for c, _ in CITIES]
CITY_WEIGHTS = [w for _, w in CITIES]

VEHICLE_CLASSES = ["economy", "comfort", "xl", "black"]
VEHICLE_WEIGHTS = [0.55, 0.26, 0.13, 0.06]
# Per-class fare shape: (base fare, per-km rate, per-minute rate)
VEHICLE_PRICING = {
    "economy": (2.50, 1.10, 0.22),
    "comfort": (3.50, 1.55, 0.30),
    "xl":      (4.25, 1.85, 0.36),
    "black":   (7.00, 3.10, 0.55),
}

PAYMENT_TYPES = ["card", "wallet", "cash"]
PAYMENT_WEIGHTS = [0.62, 0.29, 0.09]

N_DRIVERS = 5_000
N_RIDERS = 50_000
ZONES_PER_CITY = 40


class TripGenerator:
    """Emits trip dicts. One instance per producer thread (not thread-safe)."""

    def __init__(
        self,
        seed: int = 42,
        producer_id: str = "p-1",
        late_event_ratio: float = 0.0,
        late_event_max_sec: int = 600,
        malformed_ratio: float = 0.0,
    ):
        self.rng = random.Random(seed)
        self.producer_id = producer_id
        self.late_event_ratio = late_event_ratio
        self.late_event_max_sec = late_event_max_sec
        self.malformed_ratio = malformed_ratio
        self._late_count = 0
        self._malformed_count = 0

    @property
    def late_count(self) -> int:
        return self._late_count

    @property
    def malformed_count(self) -> int:
        return self._malformed_count

    def next_event(self, now_ms: int | None = None) -> Dict[str, Any]:
        rng = self.rng
        now_ms = now_ms if now_ms is not None else int(
            datetime.now(timezone.utc).timestamp() * 1000)

        city = rng.choices(CITY_IDS, weights=CITY_WEIGHTS, k=1)[0]
        vclass = rng.choices(VEHICLE_CLASSES, weights=VEHICLE_WEIGHTS, k=1)[0]
        base, per_km, per_min = VEHICLE_PRICING[vclass]

        # Log-normal-ish trip length: many short hops, a long tail of airport runs.
        distance_km = round(min(85.0, max(0.4, rng.lognormvariate(1.05, 0.75))), 3)
        # Average speed varies by city density; duration follows from distance.
        avg_speed_kmh = rng.uniform(14.0, 38.0)
        duration_sec = int(max(60, (distance_km / avg_speed_kmh) * 3600
                               + rng.uniform(-90, 240)))

        surge = 1.0
        if rng.random() < 0.18:
            surge = round(rng.uniform(1.2, 3.0), 2)

        fare = (base + distance_km * per_km + (duration_sec / 60.0) * per_min) * surge
        fare = round(max(3.0, fare), 2)

        cancelled = rng.random() < 0.055
        status = "cancelled" if cancelled else "completed"
        if cancelled:
            # Cancellations bill a flat fee, not the metered fare. Without this
            # the star-schema revenue queries would show a distribution that no
            # real ride-hailing dataset has.
            fare = round(rng.choice([0.0, 5.0]), 2)
            duration_sec = int(duration_sec * rng.uniform(0.05, 0.3))

        # --- event_time: business time, occasionally back-dated ---------------
        event_dt = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc)
        if self.late_event_ratio > 0 and rng.random() < self.late_event_ratio:
            lateness = rng.uniform(5, self.late_event_max_sec)
            event_dt -= timedelta(seconds=lateness)
            self._late_count += 1
        else:
            # Even "on time" events carry a small natural lag: a trip's event
            # time is when the trip ended, not when the message was built.
            event_dt -= timedelta(seconds=rng.uniform(0.0, 3.0))

        trip_id = str(uuid.uuid4())
        event = {
            "event_id": str(uuid.uuid4()),
            "trip_id": trip_id,
            "event_time": event_dt.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "produced_at_ms": now_ms,
            "city_id": city,
            "driver_id": f"d-{rng.randrange(N_DRIVERS):06d}",
            "rider_id": f"r-{rng.randrange(N_RIDERS):06d}",
            "pickup_zone_id": rng.randrange(ZONES_PER_CITY),
            "dropoff_zone_id": rng.randrange(ZONES_PER_CITY),
            "vehicle_class": vclass,
            "payment_type": rng.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS, k=1)[0],
            "distance_km": distance_km,
            "duration_sec": duration_sec,
            "fare_amount": fare,
            "surge_multiplier": surge,
            "status": status,
            "producer_id": self.producer_id,
            "schema_version": SCHEMA_VERSION,
        }

        # Poison-pill injection for the DLQ path. These are PERMANENT failures:
        # no amount of retrying fixes a null fare, so they should reach the DLQ
        # on the first attempt, not after burning the full backoff schedule.
        if self.malformed_ratio > 0 and rng.random() < self.malformed_ratio:
            self._malformed_count += 1
            event["fare_amount"] = None
            event["_injected_defect"] = "null_fare_amount"

        return event


def duplicate_of(event: Dict[str, Any]) -> Dict[str, Any]:
    """Byte-identical replay of an event.

    Used by the idempotency test: re-sending this must NOT change any Cassandra
    row count, because the primary keys are derived entirely from event fields.
    """
    return dict(event)
