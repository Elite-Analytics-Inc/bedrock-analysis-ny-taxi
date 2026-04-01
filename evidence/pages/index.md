---
title: NYC Yellow Taxi — Trip Analysis
hide_title: true
---

```sql hourly_trips
SELECT * FROM results.hourly_trips ORDER BY hour_of_day
```

```sql top_zones
SELECT * FROM results.top_zones ORDER BY pickups DESC
```

```sql tip_buckets
SELECT * FROM results.tip_buckets
```

```sql daily_revenue
SELECT * FROM results.daily_revenue ORDER BY trip_date
```

```sql summary
SELECT
  SUM(trips::BIGINT)                          AS total_trips,
  ROUND(SUM(total_revenue::DOUBLE), 0)::BIGINT AS total_revenue,
  ROUND(AVG(avg_fare::DOUBLE), 2)              AS avg_fare
FROM results.daily_revenue
```

```sql tip_summary
SELECT ROUND(SUM(CASE WHEN tip_bucket != 'No tip' THEN pct_of_total::DOUBLE ELSE 0 END), 1) AS tipped_pct
FROM results.tip_buckets
```

```sql revenue_filtered
SELECT *
FROM results.daily_revenue
WHERE trip_date >= '${inputs.date_range.start}'
  AND trip_date <= '${inputs.date_range.end}'
ORDER BY trip_date
```

```sql top_zones_filtered
SELECT *
FROM results.top_zones
WHERE pickups::INT >= ${inputs.min_pickups}
ORDER BY pickups::INT DESC
LIMIT 20
```

<h1 style="margin-bottom:0.25rem;">NYC Yellow Taxi</h1>
<p style="color:#64748b; margin-top:0;">2022 trip analysis — fare trends, zone popularity, tip behaviour</p>

<Grid cols=4 gapSize="md">
  <BigValue data={summary} value="total_trips" title="Total Trips" fmt="num0" />
  <BigValue data={summary} value="total_revenue" title="Total Revenue" fmt="usd0" />
  <BigValue data={summary} value="avg_fare" title="Avg Fare" fmt="usd2" />
  <BigValue data={tip_summary} value="tipped_pct" title="Trips With Tip" fmt="num1" suffix="%" />
</Grid>

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

<DateRange name="date_range" title="Date Range" />

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

<Slider name="min_pickups" title="Minimum Pickups" min=100 max=5000 step=100 defaultValue=500 />

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
  <ECharts config={
    {
      tooltip: { trigger: 'item', formatter: '{b}: {c} trips ({d}%)' },
      series: [{
        type: 'pie',
        radius: ['40%', '75%'],
        itemStyle: { borderRadius: 6, borderColor: '#fff', borderWidth: 2 },
        label: { formatter: '{b}\n{d}%' },
        data: tip_buckets.map(r => ({ name: r.tip_bucket, value: Number(r.trips) })),
        color: ['#EF4444','#F59E0B','#10B981','#3B82F6','#8B5CF6','#EC4899']
      }]
    }
  } height=320 />
  <DataTable data={tip_buckets}>
    <Column id="tip_bucket" title="Tip Range" />
    <Column id="trips" title="Trips" fmt="num0" />
    <Column id="pct_of_total" title="% of Total" fmt="num1" suffix="%" />
  </DataTable>
</Grid>
