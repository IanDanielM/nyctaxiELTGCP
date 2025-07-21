{{
    config(
        materialized = 'table',
        unique_key = 'date_id',
        tags=['gold']
    )
}}

WITH date_series AS (
  SELECT full_date
  FROM UNNEST ( 
    GENERATE_DATE_ARRAY(
      DATE '2010-01-01',
      DATE '2030-12-31',
      INTERVAL 1 DAY
    )
  ) AS full_date
)
SELECT
  ROW_NUMBER() OVER() + 1000 AS date_id,
  full_date,
  EXTRACT(YEAR FROM full_date) AS year,
  'Q' || EXTRACT(QUARTER FROM full_date) AS quarter,
  EXTRACT(MONTH FROM full_date) AS month,
  EXTRACT(DAY FROM full_date) AS day,
  FORMAT_DATE('%A', full_date) AS day_of_week,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_category
FROM date_series
ORDER BY full_date


