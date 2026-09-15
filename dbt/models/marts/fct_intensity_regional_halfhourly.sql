{{ config(materialized='table') }}

-- The regional fact grain: one row per region per half-hour, with the
-- generation mix pivoted wide and the weather at that region's point joined
-- on (interpolated to the half hour for the 14 DNO regions; null for the
-- four aggregate regions, which have no location).
select
    i.period_start,
    i.period_end,
    i.period_start::date as date_day,
    (hour(i.period_start) * 2 + minute(i.period_start) / 30 + 1)::smallint as settlement_period,
    i.region_id,
    r.region_name,
    r.nation,
    r.is_aggregate,
    i.forecast_gco2_kwh,
    i.intensity_index,
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
    w.temperature_c,
    w.wind_speed_10m_ms,
    w.wind_speed_100m_ms,
    w.shortwave_radiation_wm2,
    w.cloud_cover_pct,
    w.precipitation_mm,
    w.weather_method,
    i.partition_date,
    i.fetched_at,
    i.run_id,
    i.bronze_sha256
from {{ ref('stg_intensity_regional') }} as i
inner join {{ ref('dim_region') }} as r
    on i.region_id = r.region_id
left join {{ ref('int_generation_regional_wide') }} as g
    on i.period_start = g.period_start and i.region_id = g.region_id
left join {{ ref('int_weather_halfhourly') }} as w
    on i.period_start = w.period_start and i.region_id = w.region_id
