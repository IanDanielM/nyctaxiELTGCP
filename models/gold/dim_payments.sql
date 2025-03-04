{{
    config(
        materialized = 'table',
        unique_key = 'payment_id'
    )
}}

SELECT DISTINCT payment_type AS payment_id,
CASE payment_type
    WHEN 1 THEN 'Credit card'
    WHEN 2 THEN 'Cash'
    WHEN 3 THEN 'No charge'
    WHEN 4 THEN 'Dispute'
    WHEN 5 THEN 'Unknown'
    WHEN 6 THEN 'Voided trip'
    ELSE 'Other'
END AS payment_method
FROM {{ ref('sl_trip_data') }}

