with taxi_zone as (
    select * from {{ ref('taxi_zone_lookup') }}
)
select 
    locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone
from taxi_zone