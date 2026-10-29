{{ config(materialized='view') }}

with ranked as (
    select
        period_start::timestamptz as period_start,
        period_end::timestamptz as period_end,
        region_id::integer as region_id,
        forecast as forecast_gco2_kwh,
        index as intensity_index,
        partition_date::date as partition_date,
        fetched_at::timestamptz as fetched_at,
        run_id,
        bronze_sha256,
        row_number() over (partition by period_start, region_id order by fetched_at desc) as rn
    from {{ source('silver', 'intensity_regional') }}
)
select * exclude (rn)
from ranked
where rn = 1
