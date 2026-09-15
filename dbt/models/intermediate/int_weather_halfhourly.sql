{{ config(materialized='view') }}

-- Weather arrives hourly; the grid reports half-hourly. The :00 period takes the
-- hour's value; the :30 period takes the mean of this hour and the next when
-- both exist (linear interpolation), otherwise this hour's value.
with hourly as (
    select
        region_id,
        observed_at,
        temperature_c,
        wind_speed_10m_ms,
        wind_speed_100m_ms,
        shortwave_radiation_wm2,
        cloud_cover_pct,
        precipitation_mm,
        lead(temperature_c) over w as next_temperature_c,
        lead(wind_speed_10m_ms) over w as next_wind_speed_10m_ms,
        lead(wind_speed_100m_ms) over w as next_wind_speed_100m_ms,
        lead(shortwave_radiation_wm2) over w as next_shortwave_radiation_wm2,
        lead(cloud_cover_pct) over w as next_cloud_cover_pct,
        lead(observed_at) over w as next_observed_at
    from {{ ref('stg_weather_hourly') }}
    window w as (partition by region_id order by observed_at)
),

on_the_hour as (
    select
        region_id,
        observed_at as period_start,
        temperature_c,
        wind_speed_10m_ms,
        wind_speed_100m_ms,
        shortwave_radiation_wm2,
        cloud_cover_pct,
        precipitation_mm,
        'observed' as weather_method
    from hourly
),

on_the_half_hour as (
    select
        region_id,
        observed_at + interval 30 minute as period_start,
        case when next_observed_at = observed_at + interval 1 hour
             then (temperature_c + next_temperature_c) / 2 else temperature_c end as temperature_c,
        case when next_observed_at = observed_at + interval 1 hour
             then (wind_speed_10m_ms + next_wind_speed_10m_ms) / 2 else wind_speed_10m_ms end as wind_speed_10m_ms,
        case when next_observed_at = observed_at + interval 1 hour
             then (wind_speed_100m_ms + next_wind_speed_100m_ms) / 2 else wind_speed_100m_ms end as wind_speed_100m_ms,
        case when next_observed_at = observed_at + interval 1 hour
             then (shortwave_radiation_wm2 + next_shortwave_radiation_wm2) / 2 else shortwave_radiation_wm2 end as shortwave_radiation_wm2,
        case when next_observed_at = observed_at + interval 1 hour
             then (cloud_cover_pct + next_cloud_cover_pct) / 2 else cloud_cover_pct end as cloud_cover_pct,
        precipitation_mm / 2 as precipitation_mm,
        case when next_observed_at = observed_at + interval 1 hour then 'interpolated' else 'carried_forward' end as weather_method
    from hourly
)

select * from on_the_hour
union all
select * from on_the_half_hour
