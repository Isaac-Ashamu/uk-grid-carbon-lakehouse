{{ config(materialized='table') }}

-- How the share of wind generation and the carbon intensity respond to wind
-- speed at hub height (100 m), in 2 m/s bins, per DNO region.
with binned as (
    select
        region_id,
        region_name,
        least(floor(wind_speed_100m_ms / 2) * 2, 20) as wind_speed_bin_floor_ms,
        wind_pct,
        renewable_pct,
        forecast_gco2_kwh
    from {{ ref('fct_intensity_regional_halfhourly') }}
    where not is_aggregate and wind_speed_100m_ms is not null
)

select
    region_id,
    region_name,
    wind_speed_bin_floor_ms::integer as wind_speed_bin_floor_ms,
    case when wind_speed_bin_floor_ms >= 20 then '20+'
         else wind_speed_bin_floor_ms::integer || '-' || (wind_speed_bin_floor_ms + 2)::integer end as wind_speed_bin_label,
    count(*) as periods,
    round(avg(wind_pct), 2) as mean_wind_pct,
    round(avg(renewable_pct), 2) as mean_renewable_pct,
    round(avg(forecast_gco2_kwh), 1) as mean_forecast_gco2_kwh
from binned
group by 1, 2, 3, 4
