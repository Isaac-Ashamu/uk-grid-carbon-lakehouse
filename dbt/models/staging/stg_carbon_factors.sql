{{ config(materialized='view') }}

-- One row per fuel label from the most recent snapshot.
with ranked as (
    select
        fuel_label,
        gco2_per_kwh,
        partition_date::date as snapshot_date,
        fetched_at,
        row_number() over (partition by fuel_label order by fetched_at desc) as rn
    from {{ source('silver', 'carbon_factors') }}
)
select * exclude (rn)
from ranked
where rn = 1
