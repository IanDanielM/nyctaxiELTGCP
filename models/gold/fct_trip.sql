{{
    config(
        materialized = 'incremental',
        unique_key = 'trip_id'
    )
}}

WITH trip_data AS (
    SELECT
        trip_id,
        taxi_type AS taxi_type_id,
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        tip_amount,
        fare_amount,
        total_amount,
        payment_type AS payment_id,
        RatecodeID AS rate_id,
        CAST(pickup_datetime AS DATE) AS pickup_date,
        CAST(pickup_datetime AS TIME) AS pickup_time,
        CAST(dropoff_datetime AS DATE) AS dropoff_date,
        CAST(dropoff_datetime AS TIME) AS dropoff_time,
        PULocationID AS pickup_location_id,
        DOLocationID AS dropoff_location_id
    FROM {{ ref('sl_trip_data') }} td
),
final_data AS (
    SELECT
        td.trip_id,
        td.taxi_type_id,
        td.passenger_count,
        td.trip_distance,
        td.trip_duration_minutes,
        td.tip_amount,
        td.fare_amount,
        td.total_amount,
        td.payment_id,
        td.rate_id,
        pd.date_id AS pickup_date_id,
        pt.time_id AS pickup_time_id,
        dd.date_id AS dropoff_date_id,
        dt.time_id AS dropoff_time_id,
        td.pickup_location_id,
        td.dropoff_location_id
    FROM trip_data td
    LEFT JOIN {{ ref('dim_date') }} pd
    ON td.pickup_date = pd.full_date
    LEFT JOIN {{ ref('dim_time') }} pt
    ON EXTRACT(HOUR FROM td.pickup_time) = pt.hour
    LEFT JOIN {{ ref('dim_date') }} dd
    ON td.dropoff_date = dd.full_date
    LEFT JOIN {{ ref('dim_time') }} dt
    ON EXTRACT(HOUR FROM td.dropoff_time) = dt.hour
)


SELECT * FROM final_data
{% if is_incremental() %}
    WHERE trip_id > (SELECT MAX(trip_id) FROM {{ this }})
{% endif %}
