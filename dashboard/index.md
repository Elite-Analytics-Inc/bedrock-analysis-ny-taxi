---
title: NYC Yellow Taxi — Trip Analysis
---

```sql hourly_trips
SELECT hour_of_day, trips, avg_fare, avg_distance_miles
FROM hourly_trips ORDER BY hour_of_day
```

```sql top_zones
SELECT zone_id, pickups, avg_fare, tip_pct
FROM top_zones ORDER BY pickups DESC
```

```sql tip_buckets
SELECT tip_bucket, trips, pct_of_total
FROM tip_buckets
```

```sql daily_revenue
SELECT trip_date::DATE AS trip_date, trips, total_revenue, avg_fare
FROM daily_revenue ORDER BY trip_date
```

```sql summary
SELECT SUM(trips) AS total_trips,
       ROUND(SUM(total_revenue), 0) AS total_revenue,
       ROUND(AVG(avg_fare), 2) AS avg_fare
FROM daily_revenue
```

```sql tip_summary
SELECT ROUND(SUM(CASE WHEN tip_bucket != 'No tip' THEN pct_of_total ELSE 0 END), 1) AS tipped_pct
FROM tip_buckets
```

```sql revenue_filtered
SELECT trip_date::DATE AS trip_date, trips, total_revenue, avg_fare
FROM daily_revenue
WHERE ('${inputs.date_range.start}' = '' OR trip_date::DATE >= '${inputs.date_range.start}'::DATE)
  AND ('${inputs.date_range.end}' = '' OR trip_date::DATE <= '${inputs.date_range.end}'::DATE)
ORDER BY trip_date
```

```sql top_zones_filtered
SELECT zone_id, pickups, avg_fare, tip_pct
FROM top_zones
WHERE pickups >= ${inputs.min_pickups}
ORDER BY pickups DESC
```

{% big_value data="$summary" value="total_trips" title="Total Trips" fmt="num0" /%}
{% big_value data="$summary" value="total_revenue" title="Total Revenue" fmt="usd0" /%}
{% big_value data="$summary" value="avg_fare" title="Avg Fare" fmt="usd2" /%}
{% big_value data="$tip_summary" value="tipped_pct" title="Trips With Tip" fmt="num1" suffix="%" /%}

## Hourly Trip Patterns

{% grid cols=2 %}
  {% bar_chart data="$hourly_trips" x="hour_of_day" y="trips" title="Trips by Hour" xAxisTitle="Hour" colors=["#3B82F6"] /%}
  {% line_chart data="$hourly_trips" x="hour_of_day" y=["avg_fare","avg_distance_miles"] y2="avg_distance_miles" title="Avg Fare & Distance by Hour" xAxisTitle="Hour" yAxisTitle="Fare ($)" y2AxisTitle="Miles" /%}
{% /grid %}

## Daily Revenue

{% date_range name="date_range" data="$daily_revenue" dates="trip_date" /%}

{% grid cols=2 %}
  {% line_chart data="$revenue_filtered" x="trip_date" y=["total_revenue"] title="Daily Revenue" yAxisTitle="Revenue ($)" colors=["#10B981"] /%}
  {% bar_chart data="$revenue_filtered" x="trip_date" y="trips" title="Daily Trip Count" yAxisTitle="Trips" colors=["#6366F1"] /%}
{% /grid %}

## Top Pickup Zones

{% slider name="min_pickups" title="Minimum Pickups" min=10000 max=2000000 step=10000 default=500000 /%}

{% grid cols=2 %}
  {% bar_chart data="$top_zones_filtered" x="zone_id" y="pickups" swapXY=true title="Pickups by Zone" colors=["#F59E0B"] /%}
  {% data_table data="$top_zones_filtered" rows=20 %}
    {% column id="zone_id" title="Zone" /%}
    {% column id="pickups" title="Pickups" fmt="num0" /%}
    {% column id="avg_fare" title="Avg Fare" fmt="usd2" /%}
    {% column id="tip_pct" title="Tip %" fmt="num1" suffix="%" /%}
  {% /data_table %}
{% /grid %}

## Tip Distribution

{% grid cols=2 %}
  {% bar_chart data="$tip_buckets" x="tip_bucket" y="trips" title="Trips by Tip Range" xAxisTitle="Tip Range" yAxisTitle="Trips" colors=["#3B82F6"] /%}
  {% data_table data="$tip_buckets" %}
    {% column id="tip_bucket" title="Tip Range" /%}
    {% column id="trips" title="Trips" fmt="num0" /%}
    {% column id="pct_of_total" title="% of Total" fmt="num1" suffix="%" /%}
  {% /data_table %}
{% /grid %}
