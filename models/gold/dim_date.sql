{{
    config(
        materialized = 'table',
        unique_key = 'date_id'
    )
}}

WITH date_series AS (
  SELECT unnest(generate_series(
    '2015-01-01'::DATE, 
    '2025-12-31'::DATE, 
    INTERVAL '1 day'
  )) AS full_date
)
SELECT
  ROW_NUMBER() OVER() + 1000 AS date_id,
  strftime(full_date, '%Y-%m-%d') as full_date,
  EXTRACT(YEAR FROM full_date) AS year,
  'Q' || EXTRACT(QUARTER FROM full_date) AS quarter,
  EXTRACT(MONTH FROM full_date) AS month,
  EXTRACT(DAY FROM full_date) AS day,
  strftime('%A', full_date) AS day_of_week,
  CASE 
    WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_category
FROM date_series


