{% macro get_create_table_as_sql(temporary, relation, sql) -%}
  {{ adapter.dispatch('get_create_table_as_sql', 'dbt')(temporary, relation, sql) }}
{%- endmacro %}

{% macro flink__get_create_table_as_sql(temporary, relation, sql) -%}
  {{ return(create_table_as(temporary, relation, sql)) }}
{% endmacro %}


/* {# keep logic under old macro name for backwards compatibility #} */
{% macro create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {# backward compatibility for create_table_as that does not support language #}
  {% if language == "sql" %}
    {{ adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code)}}
  {% else %}
    {{ adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code, language) }}
  {% endif %}

{%- endmacro %}

{% macro flink__create_table_as(temporary, relation, sql) -%}
  {% set type = config.get('type', None) %}
  {%- set sql_header = config.get('sql_header', none) -%}
  {% set connector_properties = config.get('default_connector_properties', {}) %}
  {% set _dummy = connector_properties.update(config.get('connector_properties', {})) %}
  {% set execution_config = config.get('default_execution_config', {}) %}
  {% set _dummy = execution_config.update(config.get('execution_config', {})) %}
  {% set upgrade_mode = config.get('upgrade_mode', 'stateless') %}
  {% set job_state = config.get('job_state', 'running') %}
  {% set statement_set_group = config.get('statement_set_group', none) %}
  {% set statement_set_leader = config.get('statement_set_leader', false) %}
  {% set persist_relation_docs = config.persist_relation_docs() %}
  {% set persist_column_docs = config.persist_column_docs() %}
  {% set model_columns = model.columns.values() if model is defined else [] %}
  {% set relation_comment = none %}
  {% if persist_relation_docs and model is defined and model.description %}
    {% set relation_comment = model.description | replace("'", "''") %}
  {% endif %}
  {# Flink: quote table id with backticks (reserved words e.g. TREAT). Alias must be plain TREAT — no backticks in alias (drop hint also wraps). #}
  {% set flink_backtick_table = '`' ~ (this.render() | replace('`', '``')) ~ '`' %}

  {{ sql_header if sql_header is not none }}
  /** upgrade_mode('{{upgrade_mode}}') */ /** job_state('{{job_state}}') */{% if statement_set_group is not none %} /** statement_set_group('{{statement_set_group}}') */ /** statement_set_leader('{{statement_set_leader}}') */{% endif %}
  {% if execution_config %}/** execution_config('{% for cfg_name in execution_config %}{{cfg_name}}={{execution_config[cfg_name]}}{% if not loop.last %};{% endif %}{% endfor %}') */{% endif %}
  /** drop_statement('drop {% if temporary: -%}temporary {%- endif %}table if exists {{ flink_backtick_table }}') */
  {% if statement_set_group is not none %}
  /** statement_set_create */
  create {% if temporary: -%}temporary {%- endif %}table
    {{ flink_backtick_table }}
    {% if type %}/** mode('{{type}}')*/{% endif %}
  (
    {% for column in model_columns -%}
      {% set col_type = column.data_type | default('STRING', true) %}
      `{{ column.name }}` {% if col_type | lower == 'text' %}STRING{% else %}{{ col_type }}{% endif %}{% if persist_column_docs and column.description %} COMMENT '{{ column.description | replace("'", "''") }}'{% endif %}{% if not loop.last %},{% endif %}
    {%- endfor %}
  )
  {% if relation_comment %} COMMENT '{{ relation_comment }}'{% endif %}
  with (
    {% for property_name in connector_properties %} '{{ property_name }}' = '{{ connector_properties[property_name] }}'{% if not loop.last %},{% endif %}
    {% endfor %}
  );
  /** statement_set_insert */
  insert into {{ flink_backtick_table }} (
    {{ sql }}
  );
  {% else %}
  create {% if temporary: -%}temporary {%- endif %}table
    {{ flink_backtick_table }}
    {% if type %}/** mode('{{type}}')*/{% endif %}
  {% if persist_column_docs and model_columns %}
  (
    {% for column in model_columns -%}
      {% set col_type = column.data_type | default('STRING', true) %}
      `{{ column.name }}` {% if col_type | lower == 'text' %}STRING{% else %}{{ col_type }}{% endif %}{% if column.description %} COMMENT '{{ column.description | replace("'", "''") }}'{% endif %}{% if not loop.last %},{% endif %}
    {%- endfor %}
  )
  {% endif %}
  {% if relation_comment %} COMMENT '{{ relation_comment }}'{% endif %}
  with (
    {% for property_name in connector_properties %} '{{ property_name }}' = '{{ connector_properties[property_name] }}'{% if not loop.last %},{% endif %}
    {% endfor %}
  )
  as (
    {{ sql }}
  );
  {% endif %}
{%- endmacro %}
