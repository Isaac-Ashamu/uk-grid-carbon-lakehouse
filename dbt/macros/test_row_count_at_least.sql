{#- Generic test: the model has at least `minimum` rows. -#}
{% test row_count_at_least(model, minimum) %}
select count(*) as n
from {{ model }}
having count(*) < {{ minimum }}
{% endtest %}
