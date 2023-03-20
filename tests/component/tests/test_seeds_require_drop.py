import os
from typing import List

import pytest

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
    relation_from_name,
    check_relation_types,
    check_relations_equal,
)

from tests.component.mock_gw.assert_utils import AssertUtils
from tests.component.mock_gw.mock_gw import MockSqlGateway

seeds_base_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
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

seed_expect_statements = [
    "SET 'execution.runtime-mode' = 'batch'",
    """
    /* {"app": "dbt", "dbt_version": "1.3.3", "profile_name": "test", "target_name": "default", "node_id": "seed.base.base"} */
    show views
    """,

    "SET 'execution.runtime-mode' = 'batch'",
    """
    /* {"app": "dbt", "dbt_version": "1.3.3", "profile_name": "test", "target_name": "default", "node_id": "seed.base.base"} */
    show tables like 'base'
    """,

    "SET 'execution.runtime-mode' = 'batch'",
    """
    /* {"app": "dbt", "dbt_version": "1.3.3", "profile_name": "test", "target_name": "default", "node_id": "seed.base.base"} */
    drop table if exists base
    """,

    "SET 'execution.runtime-mode' = 'batch'",
    """
    /* {"app": "dbt", "dbt_version": "1.3.3", "profile_name": "test", "target_name": "default", "node_id": "seed.base.base"} */
        create table if not exists base (id DECIMAL,name STRING,some_date TIMESTAMP) with (
           'connector' = 'kafka',
           'properties.bootstrap.servers' = 'kafka:29092',
           'topic' = 'base',
           'scan.startup.mode' = 'earliest-offset',
           'value.format' = 'json',
           'properties.group.id' = 'my-working-group',
           'value.json.encode.decimal-as-plain-number' = 'true'
        )
    """,


    "SET 'execution.runtime-mode' = 'batch'",
    """
    insert into base (id, name, some_date) values
    (1,'Easton',TIMESTAMP '1981-05-20 06:46:51'),
    (2,'Lillian',TIMESTAMP '1978-09-03 18:10:33')
    """
]


class TestSeeds:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "base.yml": seeds_base_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
        }

    def test_seed(self, project):
        # MUST set up gateway before run any dbt command

        # manual create "base" table, so it should drop it first
        gw_config = {
            "host": "127.0.0.1",
            "port": "8083",
            "schemas": [
                {"catalog": "default_catalog", "database": "default_database", "tables": ["base"], "views": ["v0"]},
            ],
            "current_catalog": "default_catalog",
            "current_database": "default_database",
        }
        gw = MockSqlGateway(gw_config)  # all http request to {host_port} will cast to MockSqlGateway

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # assert sql received by MockSqlGateway, sql will be compared ignore \n \t \s, feel free to edit
        AssertUtils.assert_sql_equals(seed_expect_statements, gw.all_statements())

        # in case run dbt cmd a second time, MUST clean
        gw.clear_statements()
        _ = run_dbt(["seed"])
        AssertUtils.assert_sql_equals(seed_expect_statements, gw.all_statements())
