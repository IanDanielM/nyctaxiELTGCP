{{
    config(
        materialized = 'table',
        unique_key = 'rate_id'
    )
}}

SELECT DISTINCT RatecodeID AS rate_id,
CASE RatecodeID
    WHEN 1 THEN 'Standard rate'
    WHEN 2 THEN 'JFK'
    WHEN 3 THEN 'Newark'
    WHEN 4 THEN 'Nassau or Westchester'
    WHEN 5 THEN 'Negotiated fare'
    WHEN 6 THEN 'Group ride'
    ELSE 'Unknown'
END AS rate_type
FROM {{ ref('sl_trip_data') }}

