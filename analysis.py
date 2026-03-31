"""
NYC Yellow Taxi Analysis — Bedrock Job Definition
==================================================
Queries the NY taxi Iceberg dataset through the Bedrock query engine (gRPC/ADBC),
materialises summary Parquet files for the Evidence dashboard, and emits
structured progress events visible in the Bedrock UI.
"""

import os
import sys

# Bedrock SDK — injected into the container via the base image
sys.path.insert(0, "/bedrock_sdk")
from bedrock_sdk import BedrockJob

job = BedrockJob()

# ── 1. Connect via Arrow Flight (ABAC enforced by the query engine) ───────────
job.update_progress("running_analysis", progress_pct=5,
                    progress_message="Connecting to query engine…")
conn = job.connect()   # ADBC → grpc://bedrock-query-engine:7778

year = int(os.environ.get("PARAM_YEAR", 2023))
min_trips = int(os.environ.get("PARAM_MIN_TRIPS", 500))

job.update_progress("running_analysis", progress_pct=10,
                    progress_message=f"Analysing {year} taxi data…")

# ── 2. Hourly trip volume ─────────────────────────────────────────────────────
hourly = conn.execute(f"""
    SELECT
        EXTRACT(hour FROM tpep_pickup_datetime)::INT AS hour_of_day,
        COUNT(*)                                      AS trips,
        ROUND(AVG(total_amount), 2)                   AS avg_fare,
        ROUND(AVG(trip_distance), 2)                  AS avg_distance_miles
    FROM catalog.nyc.yellow_taxi
    WHERE EXTRACT(year FROM tpep_pickup_datetime) = {year}
    GROUP BY hour_of_day
    ORDER BY hour_of_day
""").fetchall()

job.update_progress("running_analysis", progress_pct=30,
                    progress_message="Computing zone statistics…")

# ── 3. Top pickup zones ───────────────────────────────────────────────────────
zones = conn.execute(f"""
    SELECT
        PULocationID                          AS zone_id,
        COUNT(*)                              AS pickups,
        ROUND(AVG(total_amount), 2)           AS avg_fare,
        ROUND(AVG(tip_amount / NULLIF(total_amount,0)) * 100, 1) AS tip_pct
    FROM catalog.nyc.yellow_taxi
    WHERE EXTRACT(year FROM tpep_pickup_datetime) = {year}
    GROUP BY PULocationID
    HAVING COUNT(*) >= {min_trips}
    ORDER BY pickups DESC
    LIMIT 50
""").fetchall()

job.update_progress("running_analysis", progress_pct=55,
                    progress_message="Computing tip distribution…")

# ── 4. Tip percentage distribution ───────────────────────────────────────────
tips = conn.execute(f"""
    SELECT
        CASE
            WHEN tip_amount = 0                              THEN 'No tip'
            WHEN tip_amount / total_amount < 0.10            THEN '< 10%'
            WHEN tip_amount / total_amount < 0.15            THEN '10–15%'
            WHEN tip_amount / total_amount < 0.20            THEN '15–20%'
            WHEN tip_amount / total_amount < 0.25            THEN '20–25%'
            ELSE                                                  '25%+'
        END                                                  AS tip_bucket,
        COUNT(*)                                             AS trips,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)  AS pct_of_total
    FROM catalog.nyc.yellow_taxi
    WHERE EXTRACT(year FROM tpep_pickup_datetime) = {year}
      AND total_amount > 0
    GROUP BY tip_bucket
    ORDER BY MIN(tip_amount / total_amount)
""").fetchall()

job.update_progress("running_analysis", progress_pct=75,
                    progress_message="Computing daily revenue trend…")

# ── 5. Daily revenue trend ────────────────────────────────────────────────────
revenue = conn.execute(f"""
    SELECT
        tpep_pickup_datetime::DATE            AS trip_date,
        COUNT(*)                              AS trips,
        ROUND(SUM(total_amount), 0)           AS total_revenue,
        ROUND(AVG(total_amount), 2)           AS avg_fare
    FROM catalog.nyc.yellow_taxi
    WHERE EXTRACT(year FROM tpep_pickup_datetime) = {year}
    GROUP BY trip_date
    ORDER BY trip_date
""").fetchall()

job.update_progress("running_analysis", progress_pct=85,
                    progress_message="Writing result files…")

# ── 6. Write Parquet files for Evidence dashboard ─────────────────────────────
import duckdb
write_conn = duckdb.connect(":memory:")
write_conn.execute("INSTALL httpfs; LOAD httpfs;")
write_conn.execute(f"""
    CREATE SECRET r2 (
        TYPE S3,
        KEY_ID '{os.environ["BEDROCK_R2_ACCESS_KEY"]}',
        SECRET '{os.environ["BEDROCK_R2_SECRET_KEY"]}',
        ENDPOINT '{os.environ["BEDROCK_R2_ACCOUNT_ID"]}.r2.cloudflarestorage.com',
        REGION 'auto', URL_STYLE 'path'
    )
""")

out = job.output_path  # e.g. s3://bedrock-lake/analytics/bedrock/<job_id>/data

def write_parquet(name, rows, columns):
    import json
    col_defs = ", ".join(f"v[{i}] AS {c}" for i, c in enumerate(columns))
    vals = json.dumps(rows)
    write_conn.execute(f"""
        COPY (
            SELECT {col_defs}
            FROM (SELECT unnest({vals!r}::JSON[]) AS v)
        ) TO '{out}/{name}.parquet' (FORMAT PARQUET)
    """)
    print(f"  wrote {name}.parquet ({len(rows)} rows)", flush=True)

write_parquet("hourly_trips",   hourly,  ["hour_of_day", "trips", "avg_fare", "avg_distance_miles"])
write_parquet("top_zones",      zones,   ["zone_id", "pickups", "avg_fare", "tip_pct"])
write_parquet("tip_buckets",    tips,    ["tip_bucket", "trips", "pct_of_total"])
write_parquet("daily_revenue",  revenue, ["trip_date", "trips", "total_revenue", "avg_fare"])

# ── 7. Emit structured report card (visible in Bedrock UI) ────────────────────
job.update_progress("running_analysis", progress_pct=95,
                    progress_message="Finalising report…",
                    lineage={
                        "inputs":  ["bedrock.nyc.yellow_taxi"],
                        "outputs": [f"{out}/hourly_trips.parquet",
                                    f"{out}/top_zones.parquet",
                                    f"{out}/tip_buckets.parquet",
                                    f"{out}/daily_revenue.parquet"]
                    })

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
