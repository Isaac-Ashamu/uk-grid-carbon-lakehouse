{{ config(materialized='table') }}

-- Daily national summary and forecast accuracy. A day is "complete" when all
-- 48 settlement periods are present; the trailing day of a backfill often is not.
select
    date_day,
    count(*) as periods,
    count(*) = 48 as is_complete_day,
    count(actual_gco2_kwh) as periods_with_actual,
    round(avg(forecast_gco2_kwh), 1) as mean_forecast_gco2_kwh,
    round(avg(actual_gco2_kwh), 1) as mean_actual_gco2_kwh,
    min(actual_gco2_kwh) as min_actual_gco2_kwh,
    max(actual_gco2_kwh) as max_actual_gco2_kwh,
    arg_min(period_start, actual_gco2_kwh) as cleanest_period_start,
    arg_max(period_start, actual_gco2_kwh) as dirtiest_period_start,
    round(avg(abs_forecast_error_gco2_kwh), 2) as forecast_mae_gco2_kwh,
    round(avg(forecast_error_gco2_kwh), 2) as forecast_bias_gco2_kwh,
    round(avg(abs(forecast_error_pct)), 2) as forecast_mape_pct,
    round(sqrt(avg(forecast_error_gco2_kwh * forecast_error_gco2_kwh)), 2) as forecast_rmse_gco2_kwh,
    round(avg(renewable_pct), 2) as mean_renewable_pct,
    round(avg(low_carbon_pct), 2) as mean_low_carbon_pct,
    round(avg(fossil_pct), 2) as mean_fossil_pct,
    round(avg(wind_pct), 2) as mean_wind_pct,
    round(avg(solar_pct), 2) as mean_solar_pct,
    round(avg(gas_pct), 2) as mean_gas_pct,
    round(avg(nuclear_pct), 2) as mean_nuclear_pct,
    round(avg(imports_pct), 2) as mean_imports_pct,
    max(fetched_at) as last_fetched_at
from {{ ref('fct_intensity_national_halfhourly') }}
group by date_day
