"""
NYC Yellow Taxi Analysis — Bedrock Job Definition
==================================================
Queries the NY taxi Iceberg dataset through the Bedrock query engine
(ABAC enforced), materialises summary Parquet files for the Evidence
dashboard, and emits structured progress events visible in the Bedrock UI.
"""

import os
import sys

sys.path.insert(0, "/")
from bedrock_sdk import BedrockJob

job = BedrockJob()
conn = job.connect()

year = int(os.environ.get("PARAM_YEAR", 2022))
min_trips = int(os.environ.get("PARAM_MIN_TRIPS", 500))

# ── 1. Fetch data from Iceberg (ABAC enforced via query engine) ──────────────
job.update_progress("running_analysis", progress_pct=5,
                    progress_message="Connecting to query engine…")

job.fetch("taxi_trips", f"""
    SELECT tpep_pickup_datetime, tpep_dropoff_datetime,
           pu_location_id, do_location_id,
           trip_distance, total_amount, tip_amount
    FROM catalog.transportation.nyc_taxi_trips
    WHERE EXTRACT(year FROM tpep_pickup_datetime) = {year}
""")

job.update_progress("running_analysis", progress_pct=10,
                    progress_message=f"Analysing {year} taxi data…")

# ── 2. Hourly trip volume ─────────────────────────────────────────────────────
job.progress(30, "Computing hourly trends…")

# ── 3. Top pickup zones ──────────────────────────────────────────────────────
job.progress(55, "Computing zone statistics…")

# ── 4. Tip distribution ──────────────────────────────────────────────────────
job.progress(75, "Computing tip distribution…")

# ── 5. Daily revenue trend ───────────────────────────────────────────────────
job.progress(85, "Computing daily revenue trend…")

# ── 6. Write Parquet files via presigned URLs (no R2 creds needed) ────────────
job.progress(90, "Writing result files…")

job.write_parquet("hourly_trips", """
    SELECT
        EXTRACT(hour FROM tpep_pickup_datetime)::INT AS hour_of_day,
        COUNT(*)                                      AS trips,
        ROUND(AVG(total_amount), 2)                   AS avg_fare,
        ROUND(AVG(trip_distance), 2)                  AS avg_distance_miles
    FROM taxi_trips
    GROUP BY hour_of_day
    ORDER BY hour_of_day
""")

job.write_parquet("top_zones", f"""
    SELECT
        pu_location_id                        AS zone_id,
        COUNT(*)                              AS pickups,
        ROUND(AVG(total_amount), 2)           AS avg_fare,
        ROUND(AVG(tip_amount / NULLIF(total_amount,0)) * 100, 1) AS tip_pct
    FROM taxi_trips
    GROUP BY pu_location_id
    HAVING COUNT(*) >= {min_trips}
    ORDER BY pickups DESC
    LIMIT 50
""")

job.write_parquet("tip_buckets", """
    SELECT
        CASE
            WHEN tip_amount = 0                              THEN 'No tip'
            WHEN tip_amount / total_amount < 0.10            THEN '< 10%'
            WHEN tip_amount / total_amount < 0.15            THEN '10-15%'
            WHEN tip_amount / total_amount < 0.20            THEN '15-20%'
            WHEN tip_amount / total_amount < 0.25            THEN '20-25%'
            ELSE                                                  '25%+'
        END                                                  AS tip_bucket,
        COUNT(*)                                             AS trips,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)  AS pct_of_total
    FROM taxi_trips
    WHERE total_amount > 0
    GROUP BY tip_bucket
    ORDER BY MIN(tip_amount / NULLIF(total_amount,1))
""")

job.write_parquet("daily_revenue", """
    SELECT
        tpep_pickup_datetime::DATE            AS trip_date,
        COUNT(*)                              AS trips,
        ROUND(SUM(total_amount), 0)           AS total_revenue,
        ROUND(AVG(total_amount), 2)           AS avg_fare
    FROM taxi_trips
    GROUP BY trip_date
    ORDER BY trip_date
""")

# ── 7. Emit structured report ────────────────────────────────────────────────
out_prefix = f"analytics/bedrock/{job.job_id}/data"
job.update_progress("running_analysis", progress_pct=95,
                    progress_message="Finalising report…",
                    lineage={
                        "inputs":  ["bedrock.transportation.nyc_taxi_trips"],
                        "outputs": [f"{out_prefix}/hourly_trips.parquet",
                                    f"{out_prefix}/top_zones.parquet",
                                    f"{out_prefix}/tip_buckets.parquet",
                                    f"{out_prefix}/daily_revenue.parquet"]
                    })

hourly = conn.execute("""
    SELECT hour_of_day, trips, avg_fare, avg_distance_miles
    FROM read_parquet('/tmp/hourly_trips.parquet')
""").fetchall()

tips = conn.execute("""
    SELECT tip_bucket, trips, pct_of_total
    FROM read_parquet('/tmp/tip_buckets.parquet')
""").fetchall()

job.table(
    id="hourly_summary",
    title=f"Trip Volume by Hour — {year}",
    headers=["Hour", "Trips", "Avg Fare ($)", "Avg Distance (mi)"],
    rows=[[r[0], f"{r[1]:,}", r[2], r[3]] for r in hourly],
)

job.table(
    id="tip_summary",
    title="Tip Distribution",
    headers=["Tip Bucket", "Trips", "% of Total"],
    rows=[[r[0], f"{r[1]:,}", f"{r[2]}%"] for r in tips],
)

job.complete()
