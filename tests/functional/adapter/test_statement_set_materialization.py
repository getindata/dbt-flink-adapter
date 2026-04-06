import pytest
from dbt.tests.util import run_dbt

model_a_sql = """
select cast('1' as string) as id, cast('model_a' as string) as name
"""

model_b_sql = """
select cast('2' as string) as id, cast('model_b' as string) as name
"""

statement_set_models_yml = """
version: 2
models:
  - name: model_a
    config:
      type: streaming
      connector_properties:
        connector: 'kafka'
        'properties.bootstrap.servers': 'kafka:29092'
        'topic': 'statement_set_model_a'
        'scan.startup.mode': 'earliest-offset'
        'value.format': 'json'
        'properties.group.id': 'my-working-group'
      statement_set_group: 'statement_set_job'
      statement_set_leader: false
    columns:
      - name: id
        data_type: STRING
      - name: name
        data_type: STRING
  - name: model_b
    config:
      type: streaming
      connector_properties:
        connector: 'kafka'
        'properties.bootstrap.servers': 'kafka:29092'
        'topic': 'statement_set_model_b'
        'scan.startup.mode': 'earliest-offset'
        'value.format': 'json'
        'properties.group.id': 'my-working-group'
      statement_set_group: 'statement_set_job'
      statement_set_leader: true
      execution_config:
        pipeline.name: statement_set_job
        parallelism.default: 2
    columns:
      - name: id
        data_type: STRING
      - name: name
        data_type: STRING
"""


class TestStatementSetMaterialization:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {"+materialized": "table"},
            "on-run-start": [
                "drop view if exists `model_a`",
                "drop table if exists `model_a`",
                "drop view if exists `model_b`",
                "drop table if exists `model_b`",
            ],
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": model_a_sql,
            "model_b.sql": model_b_sql,
            "models.yml": statement_set_models_yml,
        }

    def test_statement_set_materialization(self, project):
        results = run_dbt(["run"])
        model_results = [r for r in results if getattr(r.node, "resource_type", None) == "model"]
        assert len(model_results) == 2
        assert {r.node.name for r in model_results} == {"model_a", "model_b"}
