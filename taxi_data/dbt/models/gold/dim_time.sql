{{
    config(
        materialized = 'table',
        unique_key = 'time_id',
        tags=['gold']
    )
}}

WITH time_series AS (
    SELECT timestamp_value
    FROM UNNEST(
        GENERATE_TIMESTAMP_ARRAY(
            TIMESTAMP('2024-01-01 00:00:00'),
            TIMESTAMP('2024-01-01 23:00:00'),
            INTERVAL 1 HOUR
        )
    ) AS timestamp_value
)
SELECT
    ROW_NUMBER() OVER() + 2000 AS time_id,
    CAST(timestamp_value AS TIME) AS time_value,
    EXTRACT(HOUR FROM timestamp_value) AS hour,
    CASE
        WHEN EXTRACT(HOUR FROM timestamp_value) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM timestamp_value) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM timestamp_value) BETWEEN 17 AND 20 THEN 'Evening'
        WHEN EXTRACT(HOUR FROM timestamp_value) BETWEEN 21 AND 23 THEN 'Night'
        ELSE 'Midnight'
    END AS time_of_day_category
FROM time_series
ORDER BY time_value