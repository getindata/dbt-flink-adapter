# seeds/my_seed.csv
my_seed_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
""".lstrip()

# https://docs.getdbt.com/reference/dbt-jinja-functions/run_query
# https://aiven.io/blog/build-a-streaming-sql-pipeline-with-flink-and-kafka
# https://github.com/dbt-labs/dbt-codegen#generate_source-source

# for later use in source materialization
# {% if execute %}
#   {% for node in graph.sources.values() -%}
#     {% do log("SOURCE: " ~ node.identifier ~ " with config: " ~ node.config.connector_properties ~ ", COLUMNS: " ~ node.columns, info=true) %}
#   {%- endfor %}
#   {% set results = run_query('select 1 as id') %}
#   {% do results.print_table() %}
# {% endif %}


# models/my_model.sql
my_model_sql = """
select * from {{ source('my_source', 'input_topic') }}
"""

# models/my_model.yml
my_model_yml = """
version: 2
models:
  - name: my_model
    config:
      database: my_model_database
      schema: my_model_schema
      connector_properties:
        connector: 'kafka'
        'properties.bootstrap.servers': '127.0.0.1:9092'
        'topic': 'customers'
        'scan.startup.mode': 'earliest-offset'
        'value.format': 'json'
        'properties.group.id': 'my-working-group'
    columns:
      - name: id
      - name: name
"""

my_source_yml = """
version: 2
sources:
  - name: my_source
    tables:
      - name: input_topic
        config:
          connector_properties:
            connector: 'kafka'
            'properties.bootstrap.servers': '127.0.0.1:9092'
            'topic': 'input_topic'
            'scan.startup.mode': 'earliest-offset'
            'value.format': 'json'
            'properties.group.id': 'my-working-group'
        columns:
          - name: id
            description: Primary key of the table
            data_type: STRING
          - name: name
            data_type: STRING
      - name: output_topic
        config:
          connector_properties:
            connector: 'kafka'
            'properties.bootstrap.servers': '127.0.0.1:9092'
            'topic': 'output_topic'
            'scan.startup.mode': 'earliest-offset'
            'value.format': 'json'
            'properties.group.id': 'my-working-group'
        columns:
          - name: id
            description: Primary key of the table
            data_type: STRING
          - name: name
            data_type: STRING
"""