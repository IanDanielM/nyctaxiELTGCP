{{
    config(
        materialized = 'table',
        unique_key = 'payment_id',
        tags=['gold']
    )
}}

SELECT DISTINCT  COALESCE(payment_type, 0) AS payment_id,
CASE COALESCE(payment_type, 0)
    WHEN 0 THEN 'Unknown'
    WHEN 1 THEN 'Credit card'
    WHEN 2 THEN 'Cash'
    WHEN 3 THEN 'No charge'
    WHEN 4 THEN 'Dispute'
    WHEN 5 THEN 'Unknown'
    WHEN 6 THEN 'Voided trip'
    ELSE 'Other'
END AS payment_method
FROM {{ ref('sl_trip_data') }}

