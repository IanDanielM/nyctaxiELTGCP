{{ config(
    materialized='table',
    unique_key='trip_id'
) }}

WITH green_data AS (
    SELECT 
        *
    FROM parquet_scan('data/raw/green_tripdata_*.parquet')
),
numbered_data AS (
    SELECT 
        row_number() OVER () AS row_num, 
        *
    FROM green_data
),
last_trip_id AS (
    SELECT COALESCE(MAX(trip_id), 0) AS last_id FROM {{ this }}
)

SELECT 
    (n.row_num + l.last_id) AS trip_id, 
    n.* EXCLUDE (row_num)
FROM numbered_data n, last_trip_id l
{% if is_incremental() %}
WHERE lpep_pickup_datetime > (SELECT MAX(lpep_pickup_datetime) FROM {{ this }})
{% endif %}
