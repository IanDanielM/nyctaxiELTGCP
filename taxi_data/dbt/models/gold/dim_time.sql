{{
    config(
        materialized = 'table',
        unique_key = 'time_id'
    )
}}

WITH time_series AS (
  SELECT UNNEST(generate_series(
    TIMESTAMP '2024-01-01 00:00:00',
    TIMESTAMP '2024-01-01 23:00:00',
    INTERVAL '1 hour'
  )) AS timestamp_value
)
SELECT 
  ROW_NUMBER() OVER() + 2000 AS time_id, 
  CAST(timestamp_value AS TIME) AS time_value, 
  DATE_PART('hour', timestamp_value) AS hour,
  CASE
    WHEN DATE_PART('hour', timestamp_value) BETWEEN 5 AND 11 THEN 'Morning'
    WHEN DATE_PART('hour', timestamp_value) BETWEEN 12 AND 16 THEN 'Afternoon'
    WHEN DATE_PART('hour', timestamp_value) BETWEEN 17 AND 20 THEN 'Evening'
    WHEN DATE_PART('hour', timestamp_value) BETWEEN 21 AND 23 THEN 'Night'
    ELSE 'Midnight'
  END AS time_of_day_category
FROM time_series
