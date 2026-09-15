-- Every fully loaded day (i.e. not the most recent) must have all 48 periods.
with per_day as (
    select date_day, count(*) as periods
    from {{ ref('fct_intensity_national_halfhourly') }}
    group by 1
),
latest as (select max(date_day) as max_day from per_day)
select p.date_day, p.periods
from per_day as p, latest as l
where p.date_day < l.max_day and p.periods <> 48
