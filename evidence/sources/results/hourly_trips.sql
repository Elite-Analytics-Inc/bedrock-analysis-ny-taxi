SELECT * FROM read_parquet('hourly_trips.parquet')
ORDER BY hour_of_day
