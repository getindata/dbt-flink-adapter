{% macro create_sources() %}
{% if execute %}
{% for node in graph.sources.values() -%}
{% set flink_source_sql %}
{% set connector_properties = node.config.get('connector_properties') %}
{% set table_column_ids = node.columns.keys() %}
CREATE TABLE IF NOT EXISTS {{ node.identifier }} (
{% for column_id in table_column_ids %} `{{ node.columns[column_id]["name"] }}`: {{ node.columns[column_id]["data_type"] }}{% if not loop.last %},{% endif %}
{% endfor %}
)
with (
{% for property_name in connector_properties %} '{{ property_name }}' = '{{ connector_properties[property_name] }}'{% if not loop.last %},{% endif %}
{% endfor %}
);
{% endset %}
{{ log("Source " ~ node.identifier ~ " creation ... ") }}
{% set source_creation_results = run_query(flink_source_sql) %}
{{ log("Source " ~ node.identifier ~ " creation result " ~ source_creation_results) }}
{%- endfor %}
{% endif %}
{% endmacro %}
