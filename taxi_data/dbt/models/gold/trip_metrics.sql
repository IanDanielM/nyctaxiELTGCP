{{
    config(
        materialized = 'table',
        unique_key = 'trip_id'
    )
}}

WITH trip_data AS (
    SELECT 
        trip_id,
        fare_amount,
        tip_amount,
        trip_duration_minutes,
        trip_distance
    FROM {{ ref('fct_trip') }}
),
trip_measures AS (
    SELECT 
        trip_id,
        CASE 
            WHEN trip_duration_minutes < 10 THEN 'short'
            WHEN trip_duration_minutes BETWEEN 10 AND 30 THEN 'medium'
            ELSE 'long'
        END AS trip_duration_bin,
        CASE 
            WHEN trip_distance < 2 THEN '0-2 miles'
            WHEN trip_distance BETWEEN 2 AND 5 THEN '2-5 miles'
            ELSE '5+ miles'
        END AS distance_bin,
        CASE 
            WHEN fare_amount < 10 THEN 'low'
            WHEN fare_amount BETWEEN 10 AND 30 THEN 'medium'
            ELSE 'high'
        END AS fare_amount_bin,
        CASE 
            WHEN tip_amount = 0 THEN 'no tip'
            WHEN tip_amount BETWEEN 0.01 AND 2 THEN 'low'
            WHEN tip_amount BETWEEN 2.01 AND 5 THEN 'standard'
            ELSE 'generous'
        END AS tip_amount_bin,
        CASE 
            WHEN fare_amount > 0 THEN ROUND(tip_amount / fare_amount, 2) 
            ELSE NULL 
        END AS tip_percentage
    FROM trip_data
)

SELECT * FROM trip_measures
