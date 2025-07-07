{{
    config(
        materialized = 'incremental',
        unique_key = 'trip_id',
        tags=['gold'],
        partition_by={
            "field": "pickup_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['pickup_location_id', 'dropoff_location_id', 'payment_id']
    )
}}

WITH trip_data AS (
    SELECT
        trip_id,
        vendor_id,
        taxi_type AS taxi_type_id,
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        tip_amount,
        fare_amount,
        total_amount,
        payment_type AS payment_id,
        rate_code_id,
        pickup_datetime,
        CAST(pickup_datetime AS DATE) AS pickup_date,
        CAST(pickup_datetime AS TIME) AS pickup_time,
        CAST(dropoff_datetime AS DATE) AS dropoff_date,
        CAST(dropoff_datetime AS TIME) AS dropoff_time,
        pickup_location_id,
        dropoff_location_id
    FROM {{ ref('sl_trip_data') }} td
),
final_data AS (
    SELECT
        td.trip_id,
        td.vendor_id,
        td.taxi_type_id,
        td.passenger_count,
        td.trip_distance,
        td.trip_duration_minutes,
        td.tip_amount,
        td.fare_amount,
        td.total_amount,
        td.payment_id,
        td.rate_code_id,
        pd.date_id AS pickup_date_id,
        pt.time_id AS pickup_time_id,
        dd.date_id AS dropoff_date_id,
        dt.time_id AS dropoff_time_id,
        td.pickup_location_id,
        td.dropoff_location_id,
        td.pickup_datetime
    FROM trip_data td
    LEFT JOIN {{ ref('dim_date') }} pd
    ON td.pickup_date = pd.full_date
    LEFT JOIN {{ ref('dim_time') }} pt
    ON TIME_TRUNC(td.pickup_time, HOUR) = pt.time_value
    LEFT JOIN {{ ref('dim_date') }} dd
    ON td.dropoff_date = dd.full_date
    LEFT JOIN {{ ref('dim_time') }} dt
    ON TIME_TRUNC(td.dropoff_time, HOUR) = dt.time_value
)

SELECT
    trip_id,
    vendor_id,
    taxi_type_id,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    tip_amount,
    fare_amount,
    total_amount,
    payment_id,
    rate_code_id,
    pickup_date_id,
    pickup_time_id,
    dropoff_date_id,
    dropoff_time_id,
    pickup_location_id,
    dropoff_location_id,
    CAST(pickup_datetime AS DATE) AS pickup_date
FROM final_data
{% if is_incremental() %}
    WHERE pickup_datetime > (SELECT MAX(pickup_datetime) FROM {{ this }})
{% endif %}
