{{
    config(
        materialized = 'table',
        unique_key = 'location_id',
        tags=['gold']
    )
}}

SELECT
    LocationID AS location_id,
    Borough AS borough,
    Zone AS zone,
    service_zone
FROM
    {{ ref('taxi_zone_lookup') }}
