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

WITH cleaned_data AS (
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
    FROM {{ source('nyctaxi', 'standardized_trips') }}
    WHERE
        passenger_count > 0
        AND fare_amount > 0
        AND trip_distance > 0
        AND dropoff_datetime > pickup_datetime
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