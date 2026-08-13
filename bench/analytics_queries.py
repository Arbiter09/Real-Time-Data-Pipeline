"""The analytical query set, in matched baseline/star pairs.

Each pair MUST return an identical result set. The harness asserts that before
it reports any timing, because a "2x faster" query that quietly returns
different rows is not faster, it is wrong. Equivalence is checked by comparing
sorted, rounded result tuples.

All queries run with the session time zone pinned to UTC so that
`event_time::date` in the baseline and `dim_date.full_date` in the star agree
on which day a midnight-adjacent trip belongs to.
"""
from __future__ import annotations

from typing import Dict

QUERIES: Dict[str, Dict[str, str]] = {}


QUERIES["q1_revenue_by_region_day"] = {
    "description": "Revenue and trip count by region by day over the window",
    "baseline": """
        SELECT region,
               (event_time AT TIME ZONE 'UTC')::date AS day,
               COUNT(*)                              AS trips,
               ROUND(SUM(fare_amount), 2)            AS revenue
        FROM trips_wide
        WHERE status = 'completed'
          AND event_time >= %(start_ts)s
          AND event_time <  %(end_ts)s
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
    "star": """
        SELECT c.region,
               d.full_date                    AS day,
               COUNT(*)                       AS trips,
               ROUND(SUM(f.fare_amount), 2)   AS revenue
        FROM fact_trip f
        JOIN dim_city c ON c.city_key = f.city_key
        JOIN dim_date d ON d.date_key = f.date_key
        WHERE f.is_completed
          AND d.full_date >= %(start_date)s
          AND d.full_date <  %(end_date)s
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
}


QUERIES["q2_top_drivers_by_revenue"] = {
    "description": "Top 25 drivers by revenue in the window",
    "baseline": """
        SELECT driver_id,
               COUNT(*)                   AS trips,
               ROUND(SUM(fare_amount), 2) AS revenue
        FROM trips_wide
        WHERE status = 'completed'
          AND event_time >= %(start_ts)s
          AND event_time <  %(end_ts)s
        GROUP BY 1
        ORDER BY revenue DESC, driver_id
        LIMIT 25
    """,
    "star": """
        SELECT dr.driver_id,
               COUNT(*)                     AS trips,
               ROUND(SUM(f.fare_amount), 2) AS revenue
        FROM fact_trip f
        JOIN dim_driver dr ON dr.driver_key = f.driver_key
        JOIN dim_date  d  ON d.date_key    = f.date_key
        WHERE f.is_completed
          AND d.full_date >= %(start_date)s
          AND d.full_date <  %(end_date)s
        GROUP BY 1
        ORDER BY revenue DESC, dr.driver_id
        LIMIT 25
    """,
}


QUERIES["q3_weekend_split_by_vehicle"] = {
    "description": "Weekend vs weekday revenue and mean fare by vehicle class",
    "baseline": """
        SELECT vehicle_class,
               (EXTRACT(DOW FROM event_time AT TIME ZONE 'UTC') IN (0, 6)) AS is_weekend,
               COUNT(*)                        AS trips,
               ROUND(SUM(fare_amount), 2)      AS revenue,
               ROUND(AVG(fare_amount), 2)      AS avg_fare
        FROM trips_wide
        WHERE status = 'completed'
          AND event_time >= %(start_ts)s
          AND event_time <  %(end_ts)s
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
    "star": """
        SELECT v.vehicle_class,
               d.is_weekend,
               COUNT(*)                       AS trips,
               ROUND(SUM(f.fare_amount), 2)   AS revenue,
               ROUND(AVG(f.fare_amount), 2)   AS avg_fare
        FROM fact_trip f
        JOIN dim_vehicle v ON v.vehicle_key = f.vehicle_key
        JOIN dim_date    d ON d.date_key    = f.date_key
        WHERE f.is_completed
          AND d.full_date >= %(start_date)s
          AND d.full_date <  %(end_date)s
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
}


QUERIES["q4_hourly_demand_by_city"] = {
    "description": "Hour-of-day demand curve and mean distance per city",
    "baseline": """
        SELECT city_name,
               EXTRACT(HOUR FROM event_time AT TIME ZONE 'UTC')::smallint AS hour_of_day,
               COUNT(*)                     AS trips,
               ROUND(AVG(distance_km)::numeric, 3) AS avg_distance,
               ROUND(SUM(fare_amount), 2)   AS revenue
        FROM trips_wide
        WHERE status = 'completed'
          AND event_time >= %(start_ts)s
          AND event_time <  %(end_ts)s
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
    "star": """
        SELECT c.city_name,
               f.hour_of_day,
               COUNT(*)                       AS trips,
               ROUND(AVG(f.distance_km)::numeric, 3) AS avg_distance,
               ROUND(SUM(f.fare_amount), 2)   AS revenue
        FROM fact_trip f
        JOIN dim_city c ON c.city_key = f.city_key
        JOIN dim_date d ON d.date_key = f.date_key
        WHERE f.is_completed
          AND d.full_date >= %(start_date)s
          AND d.full_date <  %(end_date)s
        GROUP BY 1, 2
        ORDER BY 1, 2
    """,
}


QUERIES["q5_surge_share_by_city_week"] = {
    "description": "Share of surged trips and surge revenue premium, by city by ISO week",
    "baseline": """
        SELECT city_name,
               EXTRACT(ISOYEAR FROM event_time AT TIME ZONE 'UTC')::int  AS iso_year,
               EXTRACT(WEEK    FROM event_time AT TIME ZONE 'UTC')::int  AS iso_week,
               COUNT(*)                                                  AS trips,
               COUNT(*) FILTER (WHERE surge_multiplier > 1.0)            AS surged_trips,
               ROUND(100.0 * COUNT(*) FILTER (WHERE surge_multiplier > 1.0)
                     / NULLIF(COUNT(*), 0), 2)                           AS surge_pct,
               ROUND(SUM(fare_amount), 2)                                AS revenue
        FROM trips_wide
        WHERE status = 'completed'
          AND event_time >= %(start_ts)s
          AND event_time <  %(end_ts)s
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """,
    "star": """
        SELECT c.city_name,
               d.iso_year::int  AS iso_year,
               d.iso_week::int  AS iso_week,
               COUNT(*)                                       AS trips,
               COUNT(*) FILTER (WHERE f.is_surged)             AS surged_trips,
               ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_surged)
                     / NULLIF(COUNT(*), 0), 2)                 AS surge_pct,
               ROUND(SUM(f.fare_amount), 2)                    AS revenue
        FROM fact_trip f
        JOIN dim_city c ON c.city_key = f.city_key
        JOIN dim_date d ON d.date_key = f.date_key
        WHERE f.is_completed
          AND d.full_date >= %(start_date)s
          AND d.full_date <  %(end_date)s
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """,
}
