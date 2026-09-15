{{ config(materialized='table') }}

-- The "when should I run the dishwasher" mart: average intensity and clean
-- share by region and hour of day (UTC) across the loaded history.
select
    region_id,
    region_name,
    nation,
    is_aggregate,
    hour(period_start) as hour_of_day_utc,
    count(*) as periods,
    round(avg(forecast_gco2_kwh), 1) as mean_forecast_gco2_kwh,
    round(avg(renewable_pct), 2) as mean_renewable_pct,
    round(avg(low_carbon_pct), 2) as mean_low_carbon_pct,
    round(avg(fossil_pct), 2) as mean_fossil_pct,
    round(avg(wind_pct), 2) as mean_wind_pct,
    round(avg(solar_pct), 2) as mean_solar_pct,
    rank() over (partition by region_id order by avg(forecast_gco2_kwh)) as cleanliness_rank
from {{ ref('fct_intensity_regional_halfhourly') }}
group by 1, 2, 3, 4, 5
