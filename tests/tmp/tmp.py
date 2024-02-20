import pytest

from dbt.adapters.flink.handler import FlinkCursor
from flink.sqlgateway.client import FlinkSqlGatewayClient


class TestTmp:
    def test_tmp(self):
        session = FlinkSqlGatewayClient.create_session(
            host="127.0.0.1",
            port=8083,
            session_name="some_session",
        )
        cursor = FlinkCursor(session)
        cursor.execute(
            "CREATE TABLE input2 (`id` BIGINT,  `content` STRING) WITH ( 'connector' = 'kafka', 'topic' = 'input', 'properties.bootstrap.servers' = 'kafka:29092', 'properties.group.id' = 'flink', 'scan.startup.mode' = 'earliest-offset',  'format' = 'json')"
        )
        cursor.execute(
            "select * /** fetch_max(10) fetch_mode('streaming') fetch_timeout_ms(5000) */ from input2"
        )
        print(cursor.fetchall())
