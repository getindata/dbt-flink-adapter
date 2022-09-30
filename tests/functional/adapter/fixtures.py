# seeds/my_seed.csv
my_seed_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
""".lstrip()

# models/my_model.sql
my_model_sql = """
select * from {{ source('my_source', 'customers') }}
"""

# models/my_model.yml
my_model_yml = """
version: 2
models:
  - name: my_model
    config:
      database: my_model_database
      schema: my_model_schema
    columns:
      - name: id
      - name: name
sources:
  - name: my_source
    config:
      my_source_config:
        - my_config_property_1: 2
        - my_config_property_2: 2
    tables:
      - name: customers
        config:
          customers_table_config:
            - my_config_property_3: 3
            - my_config_property_4: "{{ var('my_config_property_4', 4) }}"
        columns:
          - name: id
            description: Primary key of the table
          - name: name
"""