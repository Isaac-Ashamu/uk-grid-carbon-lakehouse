{#- Pearson correlation that returns NULL instead of NaN when either side has zero variance
    (e.g. a region that ran on 100% wind at 0 gCO2/kWh for the whole window). -#}
{% macro safe_corr(x, y, digits=3) -%}
    case when isnan(corr({{ x }}, {{ y }})) then null else round(corr({{ x }}, {{ y }}), {{ digits }}) end
{%- endmacro %}
