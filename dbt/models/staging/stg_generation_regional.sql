{{ config(materialized='view') }}

with ranked as (
    select
        period_start::timestamptz as period_start,
        period_end::timestamptz as period_end,
        region_id::integer as region_id,
        fuel,
        perc,
        partition_date::date as partition_date,
        fetched_at::timestamptz as fetched_at,
        row_number() over (partition by period_start, region_id, fuel order by fetched_at desc) as rn
    from {{ source('silver', 'generation_regional') }}
)
select * exclude (rn)
from ranked
where rn = 1
