-- Every pivoted period's fuel shares must sum to 100 within tolerance.
-- (Silver already quarantines bad mixes; this guards the pivot itself.)
select period_start, region_id, total
from (
    select
        period_start,
        region_id,
        biomass_pct + coal_pct + imports_pct + gas_pct + nuclear_pct + other_pct + hydro_pct + solar_pct + wind_pct as total
    from {{ ref('int_generation_regional_wide') }}
)
where abs(total - 100) > 2.5
