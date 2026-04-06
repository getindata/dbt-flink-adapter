from pathlib import Path

import pytest
from dbt.flags import get_flags
from dbt.tests.util import run_dbt


persist_docs_model_sql = """
select cast('1' as string) as id, cast('name' as string) as name
"""

persist_docs_no_columns_sql = """
select cast('1' as string) as id, cast('name' as string) as name
"""

persist_docs_columns_only_sql = """
select cast('1' as string) as id, cast('name' as string) as name
"""

persist_docs_models_yml = """
version: 2
models:
  - name: persist_docs_model
    description: "Model's relation comment"
    config:
      type: streaming
      connector_properties:
        connector: 'kafka'
        'properties.bootstrap.servers': 'kafka:29092'
        'topic': 'persist_docs_model'
        'scan.startup.mode': 'earliest-offset'
        'value.format': 'json'
        'properties.group.id': 'my-working-group'
    columns:
      - name: id
        data_type: STRING
        description: "Identifier's value"
      - name: name
        data_type: STRING
        description: "Person name"
"""


class TestPersistDocs:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {
                "+materialized": "table",
                "+persist_docs": {"relation": True, "columns": True},
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "persist_docs_model.sql": persist_docs_model_sql,
            "persist_docs_model.yml": persist_docs_models_yml,
        }

    def test_persist_docs_rendered_to_comments(self, project):
        results = run_dbt(["run"], expect_pass=None)
        model_results = [r for r in results if getattr(r.node, "resource_type", None) == "model"]
        assert len(model_results) == 1

        compiled_sql_path = (
            Path(get_flags().PROJECT_DIR)
            / "target"
            / "run"
            / "example"
            / "models"
            / "persist_docs_model.sql"
        )
        compiled_sql = compiled_sql_path.read_text()

        assert "COMMENT 'Model''s relation comment'" in compiled_sql
        assert "`id` STRING COMMENT 'Identifier''s value'" in compiled_sql
        assert "`name` STRING COMMENT 'Person name'" in compiled_sql


persist_docs_no_columns_models_yml = """
version: 2
models:
  - name: persist_docs_no_columns_model
    description: "Only relation comment"
    config:
      type: streaming
      connector_properties:
        connector: 'kafka'
        'properties.bootstrap.servers': 'kafka:29092'
        'topic': 'persist_docs_no_columns_model'
        'scan.startup.mode': 'earliest-offset'
        'value.format': 'json'
        'properties.group.id': 'my-working-group'
    columns:
      - name: id
        data_type: STRING
        description: "Must not be persisted"
      - name: name
        data_type: STRING
        description: "Must not be persisted"
"""


class TestPersistDocsRelationOnly:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {
                "+materialized": "table",
                "+persist_docs": {"relation": True, "columns": False},
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "persist_docs_no_columns_model.sql": persist_docs_no_columns_sql,
            "persist_docs_no_columns_model.yml": persist_docs_no_columns_models_yml,
        }

    def test_persist_only_relation_comment(self, project):
        results = run_dbt(["run"], expect_pass=None)
        model_results = [r for r in results if getattr(r.node, "resource_type", None) == "model"]
        assert len(model_results) == 1

        compiled_sql_path = (
            Path(get_flags().PROJECT_DIR)
            / "target"
            / "run"
            / "example"
            / "models"
            / "persist_docs_no_columns_model.sql"
        )
        compiled_sql = compiled_sql_path.read_text()

        assert "COMMENT 'Only relation comment'" in compiled_sql
        assert "COMMENT 'Must not be persisted'" not in compiled_sql


persist_docs_columns_only_models_yml = """
version: 2
models:
  - name: persist_docs_columns_only_model
    description: "Must not be relation comment"
    config:
      type: streaming
      connector_properties:
        connector: 'kafka'
        'properties.bootstrap.servers': 'kafka:29092'
        'topic': 'persist_docs_columns_only_model'
        'scan.startup.mode': 'earliest-offset'
        'value.format': 'json'
        'properties.group.id': 'my-working-group'
    columns:
      - name: id
        data_type: STRING
        description: "Identifier should be persisted"
      - name: name
        data_type: STRING
        description: "Name should be persisted"
"""


class TestPersistDocsColumnsOnly:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {
                "+materialized": "table",
                "+persist_docs": {"relation": False, "columns": True},
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "persist_docs_columns_only_model.sql": persist_docs_columns_only_sql,
            "persist_docs_columns_only_model.yml": persist_docs_columns_only_models_yml,
        }

    def test_persist_only_column_comments(self, project):
        results = run_dbt(["run"], expect_pass=None)
        model_results = [r for r in results if getattr(r.node, "resource_type", None) == "model"]
        assert len(model_results) == 1

        compiled_sql_path = (
            Path(get_flags().PROJECT_DIR)
            / "target"
            / "run"
            / "example"
            / "models"
            / "persist_docs_columns_only_model.sql"
        )
        compiled_sql = compiled_sql_path.read_text()

        assert "COMMENT 'Must not be relation comment'" not in compiled_sql
        assert "`id` STRING COMMENT 'Identifier should be persisted'" in compiled_sql
        assert "`name` STRING COMMENT 'Name should be persisted'" in compiled_sql
