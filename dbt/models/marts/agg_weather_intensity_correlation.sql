{{ config(materialized='table') }}

-- Pearson correlations between the weather at each region's point and what
-- the grid did there. Expect wind speed to correlate positively with wind
-- share and negatively with intensity; solar radiation likewise with solar.
select
    region_id,
    region_name,
    nation,
    count(*) as periods,
    min(date_day) as first_day,
    max(date_day) as last_day,
    {{ safe_corr('wind_speed_100m_ms', 'wind_pct') }} as corr_wind_speed_vs_wind_pct,
    {{ safe_corr('wind_speed_100m_ms', 'forecast_gco2_kwh') }} as corr_wind_speed_vs_intensity,
    {{ safe_corr('shortwave_radiation_wm2', 'solar_pct') }} as corr_radiation_vs_solar_pct,
    {{ safe_corr('shortwave_radiation_wm2', 'forecast_gco2_kwh') }} as corr_radiation_vs_intensity,
    {{ safe_corr('temperature_c', 'forecast_gco2_kwh') }} as corr_temperature_vs_intensity,
    {{ safe_corr('renewable_pct', 'forecast_gco2_kwh') }} as corr_renewable_pct_vs_intensity
from {{ ref('fct_intensity_regional_halfhourly') }}
where not is_aggregate
  and wind_speed_100m_ms is not null
group by 1, 2, 3
