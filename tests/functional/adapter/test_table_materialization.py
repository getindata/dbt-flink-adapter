import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.fixtures import (
    my_model_sql,
    my_model_yml,
    my_source_yml,
)


class TestTableMaterialization:
    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "example", "models": {"+materialized": "table"}}

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model.yml": my_model_yml,
            "my_source.yml": my_source_yml,
        }

    def test_materialize_tables(self, project):
        # run models
        results = run_dbt(["run"])
        assert len(results) == 1
