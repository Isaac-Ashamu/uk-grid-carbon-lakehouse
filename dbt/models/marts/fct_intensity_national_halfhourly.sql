{{ config(materialized='table') }}

-- The national fact grain: one row per half-hour settlement period.
select
    i.period_start,
    i.period_end,
    i.period_start::date as date_day,
    (hour(i.period_start) * 2 + minute(i.period_start) / 30 + 1)::smallint as settlement_period,
    i.forecast_gco2_kwh,
    i.actual_gco2_kwh,
    i.intensity_index,
    i.forecast_gco2_kwh - i.actual_gco2_kwh as forecast_error_gco2_kwh,
    abs(i.forecast_gco2_kwh - i.actual_gco2_kwh) as abs_forecast_error_gco2_kwh,
    case when i.actual_gco2_kwh > 0
         then 100.0 * (i.forecast_gco2_kwh - i.actual_gco2_kwh) / i.actual_gco2_kwh
    end as forecast_error_pct,
    g.biomass_pct,
    g.coal_pct,
    g.imports_pct,
    g.gas_pct,
    g.nuclear_pct,
    g.other_pct,
    g.hydro_pct,
    g.solar_pct,
    g.wind_pct,
    g.renewable_pct,
    g.low_carbon_pct,
    g.fossil_pct,
    i.partition_date,
    i.fetched_at,
    i.run_id,
    i.bronze_sha256
from {{ ref('stg_intensity_national') }} as i
left join {{ ref('int_generation_national_wide') }} as g
    on i.period_start = g.period_start
