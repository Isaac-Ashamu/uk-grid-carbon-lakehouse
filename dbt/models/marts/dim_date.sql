{{ config(materialized='table') }}

-- One row per calendar day spanning the loaded history (UTC).
with bounds as (
    select
        min(period_start)::date as first_day,
        max(period_start)::date as last_day
    from {{ ref('stg_intensity_national') }}
),

days as (
    select unnest(generate_series(first_day, last_day, interval 1 day))::date as date_day
    from bounds
)

select
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day_of_month,
    isodow(date_day) as iso_day_of_week,
    dayname(date_day) as day_name,
    isodow(date_day) >= 6 as is_weekend,
    weekofyear(date_day) as iso_week,
    case
        when month(date_day) in (12, 1, 2) then 'winter'
        when month(date_day) in (3, 4, 5) then 'spring'
        when month(date_day) in (6, 7, 8) then 'summer'
        else 'autumn'
    end as season
from days
