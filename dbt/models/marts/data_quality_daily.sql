{{ config(materialized='table') }}

-- Operational view: per partition day and dataset, what the pipeline did,
-- how much it rejected, and whether the day is complete in the marts.
with runs as (
    select
        partition::date as date_day,
        dataset,
        stage,
        status,
        rows_in,
        rows_out,
        rows_quarantined,
        duration_ms,
        finished_at::timestamptz as finished_at,
        row_number() over (partition by partition, dataset, stage order by finished_at desc) as rn
    from {{ source('audit', 'pipeline_runs') }}
    where partition is not null and dataset is not null
),

latest_transform as (
    select * from runs where stage = 'transform' and rn = 1
),

latest_extract as (
    select * from runs where stage = 'extract' and rn = 1
),

quarantined as (
    select
        partition_date::date as date_day,
        "table",
        count(*) as quarantine_rows
    from {{ source('silver', 'quarantine') }}
    group by 1, 2
),

national_periods as (
    select date_day, count(*) as national_periods
    from {{ ref('fct_intensity_national_halfhourly') }}
    group by 1
)

select
    t.date_day,
    t.dataset,
    e.status as extract_status,
    t.status as transform_status,
    t.rows_in,
    t.rows_out,
    coalesce(t.rows_quarantined, 0) as rows_quarantined,
    case when t.rows_in > 0 then round(100.0 * coalesce(t.rows_quarantined, 0) / t.rows_in, 3) end as quarantine_rate_pct,
    t.duration_ms as transform_duration_ms,
    e.duration_ms as extract_duration_ms,
    t.finished_at as transform_finished_at,
    n.national_periods,
    case when n.national_periods is null then null else n.national_periods = 48 end as national_day_complete
from latest_transform as t
left join latest_extract as e
    on t.date_day = e.date_day and t.dataset = e.dataset
left join national_periods as n
    on t.date_day = n.date_day
