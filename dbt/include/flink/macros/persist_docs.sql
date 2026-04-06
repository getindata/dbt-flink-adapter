{% macro flink__persist_docs(relation, model, for_relation, for_columns) -%}
  {#
    Flink/StarRocks comments are emitted at CREATE TABLE time in this adapter.
    Persist docs is intentionally a no-op to avoid runtime failures on
    alter_relation_comment / alter_column_comment for backends without
    post-create comment DDL support.
  #}
  {{ return(none) }}
{%- endmacro %}

{% macro flink__alter_relation_comment(relation, relation_comment) -%}
  {{ return(none) }}
{%- endmacro %}

{% macro flink__alter_column_comment(relation, column_dict) -%}
  {{ return(none) }}
{%- endmacro %}
