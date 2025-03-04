{{
    config(
        materialized = 'table',
        unique_key = 'location_id'
    )
}}

SELECT LocationID AS location_id, Borough AS borough, Zone AS zone, service_zone FROM read_csv_auto('data/ext/taxi_zone_lookup.csv')
