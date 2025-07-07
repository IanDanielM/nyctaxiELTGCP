{{ config(
    materialized='table',
    tags=['bronze']
) }}

SELECT
    *,
    'green' AS taxi_type
FROM {{ source('nyctaxi', 'green_taxi_trips') }}
      


