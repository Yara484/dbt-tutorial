{{
    config(
        materialized='table'
    )
}}

with yellow_cab_data as (
    select *,
    'Yellow' as service_type
    from {{ ref('stg_staging__yellow_cab_data') }}
)

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

 
select 
    yellow_cab_data.tripid, 
    yellow_cab_data.vendorid, 
    yellow_cab_data.service_type,
    yellow_cab_data.ratecodeid, 
    yellow_cab_data.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    yellow_cab_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    yellow_cab_data.pickup_datetime, 
    yellow_cab_data.dropoff_datetime, 
    yellow_cab_data.store_and_fwd_flag, 
    yellow_cab_data.passenger_count, 
    yellow_cab_data.trip_distance, 
    yellow_cab_data.trip_type, 
    yellow_cab_data.fare_amount, 
    yellow_cab_data.extra, 
    yellow_cab_data.mta_tax, 
    yellow_cab_data.tip_amount, 
    yellow_cab_data.tolls_amount, 
    yellow_cab_data.ehail_fee, 
    yellow_cab_data.improvement_surcharge, 
    yellow_cab_data.total_amount, 
    yellow_cab_data.payment_type, 
    yellow_cab_data.payment_type_description
from yellow_cab_data
inner join dim_zones as pickup_zone
on yellow_cab_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on yellow_cab_data.pickup_locationid = dropoff_zone.locationid