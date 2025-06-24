{{ config(
    materialized='table',
    tags=['bronze']
) }}

SELECT
    *,
    'yellow' AS taxi_type
FROM {{ source('nyctaxi', 'yellow_taxi_trips') }}