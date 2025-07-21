{{
    config(
        materialized = 'table',
        tags=['gold']
    )
}}

SELECT 1 AS taxi_type_id, 'Green Taxi' AS taxi_type
UNION ALL
SELECT 2 AS taxi_type_id, 'Yellow Taxi' AS taxi_type

