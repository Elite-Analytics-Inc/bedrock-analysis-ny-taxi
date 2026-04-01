---
title: NYC Yellow Taxi — Trip Analysis
---

```sql hourly_trips
SELECT hour_of_day::INT AS hour_of_day,
       trips::BIGINT AS trips,
       avg_fare::DOUBLE AS avg_fare,
       avg_distance_miles::DOUBLE AS avg_distance_miles
FROM results.hourly_trips ORDER BY hour_of_day
```

```sql top_zones
SELECT zone_id::VARCHAR AS zone_id,
       pickups::BIGINT AS pickups,
       avg_fare::DOUBLE AS avg_fare,
       tip_pct::DOUBLE AS tip_pct
FROM results.top_zones ORDER BY pickups DESC
```

```sql tip_buckets
SELECT REPLACE(tip_bucket, '"', '') AS tip_bucket,
       trips::BIGINT AS trips,
       pct_of_total::DOUBLE AS pct_of_total
FROM results.tip_buckets
```

```sql daily_revenue
SELECT REPLACE(trip_date, '"', '')::DATE AS trip_date,
       trips::BIGINT AS trips,
       total_revenue::DOUBLE AS total_revenue,
       avg_fare::DOUBLE AS avg_fare
FROM results.daily_revenue ORDER BY trip_date
```

```sql summary
SELECT SUM(trips::BIGINT)                          AS total_trips,
       ROUND(SUM(total_revenue::DOUBLE), 0)::BIGINT AS total_revenue,
       ROUND(AVG(avg_fare::DOUBLE), 2)              AS avg_fare
FROM results.daily_revenue
```

```sql tip_summary
SELECT ROUND(SUM(CASE WHEN REPLACE(tip_bucket, '"', '') != 'No tip' THEN pct_of_total::DOUBLE ELSE 0 END), 1) AS tipped_pct
FROM results.tip_buckets
```

```sql revenue_filtered
SELECT REPLACE(trip_date, '"', '')::DATE AS trip_date,
       trips::BIGINT AS trips,
       total_revenue::DOUBLE AS total_revenue,
       avg_fare::DOUBLE AS avg_fare
FROM results.daily_revenue
WHERE REPLACE(trip_date, '"', '')::DATE >= '${inputs.date_range.start}'::DATE
  AND REPLACE(trip_date, '"', '')::DATE <= '${inputs.date_range.end}'::DATE
ORDER BY trip_date
```

```sql top_zones_filtered
SELECT zone_id::VARCHAR AS zone_id,
       pickups::BIGINT AS pickups,
       avg_fare::DOUBLE AS avg_fare,
       tip_pct::DOUBLE AS tip_pct
FROM results.top_zones
WHERE pickups::BIGINT >= ${inputs.min_pickups}
ORDER BY pickups::BIGINT DESC

```

<BigValue data={summary} value="total_trips" title="Total Trips" fmt="num0" />
<BigValue data={summary} value="total_revenue" title="Total Revenue" fmt="usd0" />
<BigValue data={summary} value="avg_fare" title="Avg Fare" fmt="usd2" />
<BigValue data={tip_summary} value="tipped_pct" title="Trips With Tip" fmt="num1" suffix="%" />

## Hourly Trip Patterns

<Grid cols=2>
  <BarChart
    data={hourly_trips}
    x="hour_of_day"
    y="trips"
    title="Trips by Hour"
    xAxisTitle="Hour"
    colorPalette={["#3B82F6"]}
  />
  <LineChart
    data={hourly_trips}
    x="hour_of_day"
    y={["avg_fare", "avg_distance_miles"]}
    y2="avg_distance_miles"
    title="Avg Fare & Distance by Hour"
    xAxisTitle="Hour"
    yAxisTitle="Fare ($)"
    y2AxisTitle="Miles"
  />
</Grid>

## Daily Revenue

<DateRange name="date_range" data={daily_revenue} dates="trip_date" />

<Grid cols=2>
  <LineChart
    data={revenue_filtered}
    x="trip_date"
    y="total_revenue"
    title="Daily Revenue"
    yAxisTitle="Revenue ($)"
    colorPalette={["#10B981"]}
  />
  <BarChart
    data={revenue_filtered}
    x="trip_date"
    y="trips"
    title="Daily Trip Count"
    yAxisTitle="Trips"
    colorPalette={["#6366F1"]}
  />
</Grid>

## Top Pickup Zones

<Slider name="min_pickups" title="Minimum Pickups" min=10000 max=2000000 step=10000 defaultValue=500000 />

<Grid cols=2>
  <BarChart
    data={top_zones_filtered}
    x="zone_id"
    y="pickups"
    swapXY=true
    title="Pickups by Zone"
    colorPalette={["#F59E0B"]}
  />
  <DataTable data={top_zones_filtered} rows=20>
    <Column id="zone_id" title="Zone" />
    <Column id="pickups" title="Pickups" fmt="num0" />
    <Column id="avg_fare" title="Avg Fare" fmt="usd2" />
    <Column id="tip_pct" title="Tip %" fmt="num1" suffix="%" />
  </DataTable>
</Grid>

## Tip Distribution

<Grid cols=2>
  <BarChart
    data={tip_buckets}
    x="tip_bucket"
    y="trips"
    title="Trips by Tip Range"
    xAxisTitle="Tip Range"
    yAxisTitle="Trips"
    colorPalette={["#3B82F6"]}
  />
  <DataTable data={tip_buckets}>
    <Column id="tip_bucket" title="Tip Range" />
    <Column id="trips" title="Trips" fmt="num0" />
    <Column id="pct_of_total" title="% of Total" fmt="num1" suffix="%" />
  </DataTable>
</Grid>
