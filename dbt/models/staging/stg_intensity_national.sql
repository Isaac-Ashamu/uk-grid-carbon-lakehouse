{{ config(materialized='view') }}

-- Latest successful fetch wins if a partition was ever re-extracted.
-- Timestamps are cast to timestamptz (microseconds): DuckDB 2.0 reads Parquet
-- nanosecond timestamps as TIMESTAMPTZ_NS, which hour() and friends reject.
with ranked as (
    select
        period_start::timestamptz as period_start,
        period_end::timestamptz as period_end,
        forecast as forecast_gco2_kwh,
        actual as actual_gco2_kwh,
        index as intensity_index,
        partition_date::date as partition_date,
        fetched_at::timestamptz as fetched_at,
        run_id,
        bronze_sha256,
        row_number() over (partition by period_start order by fetched_at desc) as rn
    from {{ source('silver', 'intensity_national') }}
)
select * exclude (rn)
from ranked
where rn = 1
