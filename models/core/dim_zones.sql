select 
locationid,
borough,
zone,
service_zone
from {{ ref('taxi_zones_lookup') }}