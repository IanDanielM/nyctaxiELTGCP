{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        partition_by={
            "field": "pickup_datetime",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['pickup_location_id', 'payment_type'],
        tags=['silver']
    )
}}

WITH combined_taxi_data AS (
    SELECT
        SAFE_CAST(VendorID  AS INT64) AS vendor_id,
        SAFE_CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        SAFE_CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        SAFE_CAST(RatecodeID AS INT64) AS rate_code_id,
        SAFE_CAST(PULocationID AS INT64) AS pickup_location_id,
        SAFE_CAST(DOLocationID AS INT64) AS dropoff_location_id,
        SAFE_CAST(SAFE_CAST(passenger_count AS STRING) AS INT64) AS passenger_count,
        SAFE_CAST(trip_distance AS FLOAT64) AS trip_distance,
        SAFE_CAST(fare_amount AS NUMERIC) AS fare_amount,
        SAFE_CAST(SAFE_CAST(payment_type AS STRING) AS INT64) AS payment_type,
        SAFE_CAST(tip_amount AS NUMERIC) AS tip_amount,
        SAFE_CAST(total_amount AS NUMERIC) AS total_amount,
        SAFE_CAST('Green' AS STRING) AS taxi_type,
        SAFE_CAST(trip_type AS INTEGER) AS trip_type
    FROM {{ ref('green_taxi_raw') }}
    WHERE VendorID IS NOT NULL and passenger_count != floor(passenger_count)

    UNION ALL

    SELECT
        SAFE_CAST(VendorID  AS INT64) AS vendor_id,
        SAFE_CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        SAFE_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        SAFE_CAST(RatecodeID AS INT64) AS rate_code_id,
        SAFE_CAST(PULocationID AS INT64) AS pickup_location_id,
        SAFE_CAST(DOLocationID AS INT64) AS dropoff_location_id,
        SAFE_CAST(SAFE_CAST(passenger_count AS STRING) AS INT64) AS passenger_count,
        SAFE_CAST(trip_distance AS FLOAT64) AS trip_distance,
        SAFE_CAST(fare_amount AS NUMERIC) AS fare_amount,
        SAFE_CAST(SAFE_CAST(payment_type AS STRING) AS INT64) AS payment_type,
        SAFE_CAST(tip_amount AS NUMERIC) AS tip_amount,
        SAFE_CAST(total_amount AS NUMERIC) AS total_amount,
        SAFE_CAST('Yellow' AS STRING) AS taxi_type,
        NULL AS trip_type
    FROM {{ ref('yellow_taxi_raw') }}
    WHERE VendorID IS NOT NULL and passenger_count != floor(passenger_count)
),

cleaned_data AS (
    SELECT
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        taxi_type,
        trip_type,
        COALESCE(rate_code_id, 99) AS rate_code_id,
        COALESCE(payment_type, 0) AS payment_type,
        SAFE_CAST(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS FLOAT64) AS trip_duration_minutes
    FROM combined_taxi_data
    WHERE
        passenger_count > 0
        AND fare_amount > 0
        AND trip_distance > 0
        AND dropoff_datetime > pickup_datetime -- Ensures duration is positive
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'vendor_id',
        'pickup_datetime',
        'dropoff_datetime',
        'pickup_location_id',
        'dropoff_location_id',
        'total_amount'
    ]) }} AS trip_id,
    *
FROM cleaned_data
{% if is_incremental() %}
    WHERE pickup_datetime > (SELECT MAX(pickup_datetime) FROM {{ this }})
{% endif %}