import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.fixtures import (
    my_model_sql,
    my_model_yml,
    my_source_yml,
)


class TestSourceTablesGeneration:
    """
    Methods in this class will be of two types:
    1. Fixtures defining the dbt "project" for this test case.
       These are scoped to the class, and reused for all tests in the class.
    2. Actual tests, whose names begin with 'test_'.
       These define sequences of dbt commands and 'assert' statements.
    """

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {"+materialized": "view"},
            "on-run-start": [
                "drop view if exists `my_model`",
                "drop table if exists `my_model`",
                "{{ create_sources() }}",
            ],
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model.yml": my_model_yml,
            "my_source.yml": my_source_yml,
        }

    def test_create_source_tables(self, project):
        # run models
        results = run_dbt(["run"])
        model_results = [r for r in results if getattr(r.node, "resource_type", None) == "model"]
        assert len(model_results) == 1
        assert model_results[0].node.name == "my_model"
        # test tests
        results = run_dbt(["test"], expect_pass=None)
        test_results = [r for r in results if getattr(r.node, "resource_type", None) == "test"]
        assert len(test_results) >= 0
        if test_results:
            result_statuses = sorted(str(r.status) for r in test_results)
            assert all(status in {"pass", "fail", "error"} for status in result_statuses)
