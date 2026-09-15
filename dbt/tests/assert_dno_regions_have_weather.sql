-- Every DNO region period on a complete day must have weather attached.
with complete_days as (
    select date_day from {{ ref('agg_daily_national') }} where is_complete_day
)
select f.period_start, f.region_id
from {{ ref('fct_intensity_regional_halfhourly') }} as f
inner join complete_days as d on f.date_day = d.date_day
where not f.is_aggregate
  and f.wind_speed_100m_ms is null
  and f.date_day < (select max(date_day) from complete_days)
