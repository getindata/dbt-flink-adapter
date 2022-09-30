import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.fixtures import (
    my_model_sql,
    my_model_yml,
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
            "models": {"+materialized": "view"}
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model.yml": my_model_yml,
        }

    def test_run_source_test(self, project):
        # run models
        results = run_dbt(["run"])
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=False)  # expect failing test
        assert len(results) == 2
        # validate that the results include one pass and one failure
        result_statuses = sorted(r.status for r in results)
        assert result_statuses == ["fail", "pass"]

    @pytest.mark.xfail
    def test_build(self, project):
        """Expect a failing test"""
        # do it all
        results = run_dbt(["build"])
