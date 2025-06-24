{{
    config(
        materialized = 'table',
        tags=['gold']
    )
}}

SELECT DISTINCT
    COALESCE(rate_code_id, 0) AS rate_id,
    CASE COALESCE(rate_code_id, 0)
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        WHEN 0 THEN 'Unknown'
        ELSE 'Other'
    END AS rate_type
FROM {{ ref('sl_trip_data') }}