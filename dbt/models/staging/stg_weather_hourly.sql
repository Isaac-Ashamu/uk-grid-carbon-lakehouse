{{ config(materialized='view') }}

with ranked as (
    select
        region_id::integer as region_id,
        observed_at::timestamptz as observed_at,
        temperature_2m as temperature_c,
        wind_speed_10m as wind_speed_10m_ms,
        wind_speed_100m as wind_speed_100m_ms,
        shortwave_radiation as shortwave_radiation_wm2,
        cloud_cover as cloud_cover_pct,
        precipitation as precipitation_mm,
        partition_date::date as partition_date,
        fetched_at::timestamptz as fetched_at,
        row_number() over (partition by region_id, observed_at order by fetched_at desc) as rn
    from {{ source('silver', 'weather_hourly') }}
)
select * exclude (rn)
from ranked
where rn = 1
