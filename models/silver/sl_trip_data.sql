{{ config(
    materialized = 'incremental',
    unique_key = 'trip_id',
) }}


WITH green_taxi AS (
    SELECT 
        trip_id,
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID,
        PULocationID,
        DOLocationID,
        payment_type,
        tip_amount,
        ABS(fare_amount) AS fare_amount,
        total_amount,
        1 AS taxi_type
    FROM {{ ref('green_taxi_raw') }}
),

yellow_taxi AS (
    SELECT 
        trip_id,
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID,
        PULocationID,
        DOLocationID,
        payment_type,
        tip_amount,
        ABS(fare_amount) AS fare_amount,
        total_amount,
        2 as taxi_type
    FROM {{ ref('yellow_taxi_raw') }}
),

-- Union the green and yellow taxi data together
combined_taxi_data AS (
    SELECT * FROM green_taxi
    UNION ALL
    SELECT * FROM yellow_taxi
),

-- Remove negative and zero values
filtered_values AS (
    SELECT *
    FROM combined_taxi_data
    WHERE passenger_count > 0
        AND fare_amount > 0
        AND trip_distance > 0
),

updated_values AS (
    SELECT 
        *,
        COALESCE(RatecodeID, 99) AS RatecodeID,
        COALESCE(payment_type, 0) AS payment_type
    FROM filtered_values
),

-- Calculate trip duration
calculated_trip_duration AS (
    SELECT *,
        round((
            EXTRACT(EPOCH FROM dropoff_datetime) - EXTRACT(EPOCH FROM pickup_datetime)
        ) / 60, 2) AS trip_duration_minutes
    FROM updated_values
    WHERE dropoff_datetime > pickup_datetime
)

-- Final cleaned and transformed data
SELECT *
FROM calculated_trip_duration
{% if is_incremental() %}
WHERE trip_id > (SELECT MAX(trip_id) FROM {{ this }})
{% endif %}
