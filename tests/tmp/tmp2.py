import unittest

from dbt.adapters.flink.handler import FlinkCursor
from tests.sqlgateway.mock.mock_client import MockFlinkSqlGatewayClient


class TestTmp(unittest.TestCase):

    def test_tmp(self):
        session = MockFlinkSqlGatewayClient.create_session(
            host="127.0.0.1",
            port=8083,
            session_name="some_session",
        )
        cursor = FlinkCursor(session)
        sql = "select * /** fetch_max(10) fetch_mode('streaming') fetch_timeout_ms(5000) */ from input2"
        cursor.execute(sql)
        # check sql received
        stats = MockFlinkSqlGatewayClient.all_statements(session)
        self.assertTrue("SET 'execution.runtime-mode' = 'batch'", stats[0])
        self.assertTrue(sql, stats[1])
