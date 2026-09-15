{#- Pivot the long (period, [region], fuel, perc) shape into one wide row per period.
    Fuel groupings follow dim_fuel: renewable = wind + solar + hydro + biomass,
    low carbon = renewable + nuclear, fossil = gas + coal. -#}
{% macro pivot_generation_mix(relation, extra_keys=[]) %}
select
    period_start,
    period_end,
    {% for key in extra_keys %}{{ key }},{% endfor %}
    {% for fuel in ['biomass', 'coal', 'imports', 'gas', 'nuclear', 'other', 'hydro', 'solar', 'wind'] %}
    coalesce(max(case when fuel = '{{ fuel }}' then perc end), 0)::double as {{ fuel }}_pct,
    {% endfor %}
    coalesce(sum(case when fuel in ('wind', 'solar', 'hydro', 'biomass') then perc end), 0)::double as renewable_pct,
    coalesce(sum(case when fuel in ('wind', 'solar', 'hydro', 'biomass', 'nuclear') then perc end), 0)::double as low_carbon_pct,
    coalesce(sum(case when fuel in ('gas', 'coal') then perc end), 0)::double as fossil_pct,
    count(*) as fuel_rows,
    max(fetched_at) as fetched_at
from {{ relation }}
group by period_start, period_end{% for key in extra_keys %}, {{ key }}{% endfor %}
{% endmacro %}
