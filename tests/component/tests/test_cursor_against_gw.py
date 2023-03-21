import unittest

from dbt.adapters.flink.handler import FlinkCursor
from flink.sqlgateway.client import FlinkSqlGatewayClient
from tests.component.mock_gw.assert_utils import AssertUtils
from tests.component.mock_gw.mock_gw import MockSqlGateway, default_config


class TestTmp(unittest.TestCase):

    def test_cursor_against_gw(self):
        # MUST set up gateway before run any dbt command
        gw = MockSqlGateway(default_config)

        session = FlinkSqlGatewayClient.create_session(
            host="127.0.0.1",
            port=8083,
            session_name="some_session",
        )
        cursor = FlinkCursor(session)
        sql = "select * /** fetch_max(10) fetch_mode('streaming') fetch_timeout_ms(5000) */ from input2"
        cursor.execute(sql)
        # check sql gw received
        seed_expect_statements = ["SET 'execution.runtime-mode' = 'batch'", sql]
        AssertUtils.assert_sql_equals(seed_expect_statements, gw.all_statements())
