{{ config(materialized='table') }}

-- When in the day is ESO's day-ahead intensity forecast least reliable?
select
    settlement_period,
    (settlement_period - 1) // 2 as hour_of_day,
    count(actual_gco2_kwh) as periods,
    round(avg(abs_forecast_error_gco2_kwh), 2) as mae_gco2_kwh,
    round(avg(forecast_error_gco2_kwh), 2) as bias_gco2_kwh,
    round(avg(abs(forecast_error_pct)), 2) as mape_pct,
    round(sqrt(avg(forecast_error_gco2_kwh * forecast_error_gco2_kwh)), 2) as rmse_gco2_kwh,
    round(quantile_cont(abs_forecast_error_gco2_kwh, 0.5), 1) as median_abs_error_gco2_kwh,
    round(quantile_cont(abs_forecast_error_gco2_kwh, 0.9), 1) as p90_abs_error_gco2_kwh,
    round(avg(actual_gco2_kwh), 1) as mean_actual_gco2_kwh
from {{ ref('fct_intensity_national_halfhourly') }}
where actual_gco2_kwh is not null
group by settlement_period
