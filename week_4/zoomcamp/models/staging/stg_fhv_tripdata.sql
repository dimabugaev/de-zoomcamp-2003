with raw_fhv_tripdata as (
    select 
        *
        -- row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
    from {{ source('staging', 'external_fhv_tripdata') }}     
)
select
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num as dispatching_base_num,
    
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    
    sr_flag,
    affiliated_base_number
    
from
    raw_fhv_tripdata
-- where
--    rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
--{% if var('is_test_run', default=true) %}

--  limit 100

--{% endif %}