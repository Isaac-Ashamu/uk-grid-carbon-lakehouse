{{ config(materialized='view') }}

with ranked as (
    select
        period_start,
        period_end,
        fuel,
        perc,
        partition_date::date as partition_date,
        fetched_at,
        row_number() over (partition by period_start, fuel order by fetched_at desc) as rn
    from {{ source('silver', 'generation_national') }}
)
select * exclude (rn)
from ranked
where rn = 1
