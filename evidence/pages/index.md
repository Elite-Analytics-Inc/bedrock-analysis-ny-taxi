---
title: NYC Yellow Taxi — Trip Analysis
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
WHERE pickups >= ${inputs.min_pickups}
ORDER BY pickups DESC
LIMIT 20
```

# NYC Yellow Taxi — Trip Analysis

<BigValue
  data={hourly_trips.reduce((a, r) => [{trips: (a[0]?.trips || 0) + r.trips}], [])}
  value="trips"
  title="Total Trips"
  fmt="num0"
/>
<BigValue
  data={top_zones}
  value="avg_fare"
  title="Avg Fare (Top Zones)"
  fmt="usd2"
  agg="mean"
/>
<BigValue
  data={tip_buckets.filter(r => r.tip_bucket !== 'No tip')}
  value="pct_of_total"
  title="Trips With a Tip"
  fmt="num1"
  agg="sum"
  suffix="%"
/>
<BigValue
  data={daily_revenue.reduce((a, r) => [{total_revenue: (a[0]?.total_revenue || 0) + r.total_revenue}], [])}
  value="total_revenue"
  title="Total Revenue"
  fmt="usd0"
/>

---

## Hourly Trip Patterns

<BarChart
  data={hourly_trips}
  x="hour_of_day"
  y="trips"
  title="Trips by Hour of Day"
  xAxisTitle="Hour"
  yAxisTitle="Trips"
  colorPalette={["#3B82F6"]}
  labels=true
/>

<LineChart
  data={hourly_trips}
  x="hour_of_day"
  y={["avg_fare", "avg_distance_miles"]}
  title="Avg Fare & Distance by Hour"
  xAxisTitle="Hour"
  yAxisTitle="Value"
  labels=false
/>

---

## Daily Revenue Trend

<DateRange
  name="date_range"
  title="Date Range"
/>

<LineChart
  data={revenue_filtered}
  x="trip_date"
  y="total_revenue"
  title="Daily Revenue"
  xAxisTitle="Date"
  yAxisTitle="Revenue ($)"
  colorPalette={["#10B981"]}
  labels=false
/>

<BarChart
  data={revenue_filtered}
  x="trip_date"
  y="trips"
  title="Daily Trip Count"
  xAxisTitle="Date"
  yAxisTitle="Trips"
  colorPalette={["#6366F1"]}
  labels=false
/>

---

## Top Pickup Zones

Adjust the minimum trip threshold to focus on the busiest zones:

<Slider
  name="min_pickups"
  title="Minimum Pickups"
  min=100
  max=5000
  step=100
  defaultValue=500
/>

<BarChart
  data={top_zones_filtered}
  x="zone_id"
  y="pickups"
  swapXY=true
  title="Top Pickup Zones by Volume"
  xAxisTitle="Pickups"
  yAxisTitle="Zone ID"
  colorPalette={["#F59E0B"]}
  labels=true
/>

<DataTable
  data={top_zones_filtered}
  rows=20
>
  <Column id="zone_id" title="Zone" />
  <Column id="pickups" title="Pickups" fmt="num0" />
  <Column id="avg_fare" title="Avg Fare" fmt="usd2" />
  <Column id="tip_pct" title="Tip %" fmt="num1" suffix="%" />
</DataTable>

---

## Tip Distribution

<ECharts config={
  {
    tooltip: { trigger: 'item' },
    legend: { orient: 'vertical', left: 'left' },
    series: [{
      name: 'Tip Distribution',
      type: 'pie',
      radius: ['40%', '70%'],
      data: tip_buckets.map(r => ({ name: r.tip_bucket, value: r.trips })),
      emphasis: {
        itemStyle: {
          shadowBlur: 10,
          shadowOffsetX: 0,
          shadowColor: 'rgba(0,0,0,0.5)'
        }
      }
    }]
  }
}
  height=350
/>

<DataTable data={tip_buckets}>
  <Column id="tip_bucket" title="Tip Range" />
  <Column id="trips" title="Trips" fmt="num0" />
  <Column id="pct_of_total" title="% of Total" fmt="num1" suffix="%" />
</DataTable>
