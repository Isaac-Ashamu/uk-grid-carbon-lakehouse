{{ config(materialized='table') }}

-- The fuel dimension joined to the latest ESO emission factor for that fuel.
-- Imports have no single factor (the API publishes Dutch, French and Irish
-- separately) so it is left null rather than guessed.
select
    f.fuel,
    f.fuel_group,
    f.is_renewable,
    f.is_low_carbon,
    f.is_fossil,
    f.factor_label,
    c.gco2_per_kwh as emission_factor_gco2_kwh,
    c.snapshot_date as factor_snapshot_date
from {{ ref('dim_fuel') }} as f
left join {{ ref('stg_carbon_factors') }} as c
    on f.factor_label = c.fuel_label
