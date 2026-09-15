{{ config(materialized='table') }}

select
    f.date_day,
    f.region_id,
    f.region_name,
    f.nation,
    f.is_aggregate,
    count(*) as periods,
    count(*) = 48 as is_complete_day,
    round(avg(f.forecast_gco2_kwh), 1) as mean_forecast_gco2_kwh,
    min(f.forecast_gco2_kwh) as min_forecast_gco2_kwh,
    max(f.forecast_gco2_kwh) as max_forecast_gco2_kwh,
    arg_min(f.period_start, f.forecast_gco2_kwh) as cleanest_period_start,
    arg_max(f.period_start, f.forecast_gco2_kwh) as dirtiest_period_start,
    round(avg(f.renewable_pct), 2) as mean_renewable_pct,
    round(avg(f.low_carbon_pct), 2) as mean_low_carbon_pct,
    round(avg(f.fossil_pct), 2) as mean_fossil_pct,
    round(avg(f.wind_pct), 2) as mean_wind_pct,
    round(avg(f.solar_pct), 2) as mean_solar_pct,
    round(avg(f.gas_pct), 2) as mean_gas_pct,
    round(avg(f.nuclear_pct), 2) as mean_nuclear_pct,
    round(avg(f.imports_pct), 2) as mean_imports_pct,
    round(avg(f.temperature_c), 2) as mean_temperature_c,
    round(avg(f.wind_speed_100m_ms), 2) as mean_wind_speed_100m_ms,
    round(max(f.wind_speed_100m_ms), 2) as max_wind_speed_100m_ms,
    round(avg(f.shortwave_radiation_wm2), 1) as mean_shortwave_radiation_wm2,
    round(avg(f.cloud_cover_pct), 1) as mean_cloud_cover_pct,
    round(sum(f.precipitation_mm), 2) as total_precipitation_mm,
    max(f.fetched_at) as last_fetched_at
from {{ ref('fct_intensity_regional_halfhourly') }} as f
group by 1, 2, 3, 4, 5
