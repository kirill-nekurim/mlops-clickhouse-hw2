{% macro amount_bucket(column) -%}
    case
        when {{ column }} >= {{ var('large_amount', 1000) }} then 'LARGE'
        when {{ column }} >= 100 then 'MEDIUM'
        else 'SMALL'
    end
{%- endmacro %}
