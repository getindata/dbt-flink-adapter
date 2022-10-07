import pytest

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
    relation_from_name,
    check_relation_types,
    check_relations_equal,
)

seeds_base_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
10,Nora,1976-03-01T16:51:39
""".lstrip()

seeds_base_yml = """
version: 2

seeds:
  - name: base
    config:
      connector_properties:
        connector: 'kafka'
        'properties.bootstrap.servers': 'kafka:29092'
        'topic': 'base'
        'scan.startup.mode': 'earliest-offset'
        'value.format': 'json'
        'properties.group.id': 'my-working-group'
        'value.json.encode.decimal-as-plain-number': 'true'
"""

test_passing_sql = """
select /** fetch_timeout_ms(10000) */ /** fetch_mode('streaming') */ * from base where id = 11
"""

test_failing_sql = """
select /** fetch_timeout_ms(10000) */ /** fetch_mode('streaming') */ * from base where id = 10
"""

class TestSeeds():
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "base.yml": seeds_base_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "passing.sql": test_passing_sql,
            "failing.sql": test_failing_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
        }

    def test_seed(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # TODO: This does not assert anything useful. In order to assert we need to execute test but test will need to first materialize source as between `run_dbt` we have different sessions and base does not persist.

        # # test command
        # results = run_dbt(["test"], expect_pass=False)
        # assert len(results) == 2
        #
        # # We have the right result nodes
        # check_result_nodes_by_name(results, ["passing", "failing"])
        #
        # # Check result status
        # for result in results:
        #     if result.node.name == "passing":
        #         assert result.status == "pass"
        #     elif result.node.name == "failing":
        #         assert result.status == "fail"
