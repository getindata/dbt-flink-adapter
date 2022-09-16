{% macro get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
  {{ adapter.dispatch('get_test_sql', 'dbt')(main_sql, fail_calc, warn_if, error_if, limit) }}
{%- endmacro %}

{% macro default__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
select
    {{ fail_calc }} as failures,
    {{ fail_calc }} <> 0 as should_warn,
    {{ fail_calc }} <> 0 as should_error
from (
    {{ main_sql }}
    {{ "limit " ~ limit if limit != none }}
    ) dbt_internal_test
    {%- endmacro %}
