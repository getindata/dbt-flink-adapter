import os
from typing import List

from flink.sqlgateway.operation import SqlGatewayOperation
from flink.sqlgateway.session import SqlGatewaySession
from flink.sqlgateway.config import SqlGatewayConfig
from tests.sqlgateway.mock.gw_router import GwRouter
import requests
import json


class MockFlinkSqlGatewayClient:
    def __init__(self):
        self.router = None
        self.session = None

    def create_session(self, host: str, port: int, session_name: str) -> SqlGatewaySession:
        host_port = f"http://{host}:{port}"
        test_config = {
            "host_port": host_port,
            "schemas": [
                {"catalog": "default_catalog", "database": "default_database", "tables": [], "views": []}
            ],
            "current_catalog": "default_catalog",
            "current_database": "default_database",
        }
        self.router = GwRouter(test_config)
        self.router.start()
        # create session
        r = requests.post(f"{host_port}/v1/sessions", json.dumps({"sessionName": f"{session_name}"}))
        session_handle = r.json()['sessionHandle']
        session = SqlGatewaySession(SqlGatewayConfig(host, port, session_name), session_handle)
        self.session = session
        return session

    def execute_statement(self, sql: str) -> SqlGatewayOperation:
        if self.session.session_handle is None:
            raise Exception(
                f"Session '{self.session.config.session_name}' is not created. Call create() method first"
            )
        host_port = f"http://{self.session.config.host}:{self.session.config.port}"
        session_handle = self.session.session_handle
        data = {"statement": sql}
        r = requests.post(f"{host_port}/v1/sessions/{session_handle}/statements", json.dumps(data))
        operation_handle = r.json()['operationHandle']
        return SqlGatewayOperation(session=self.session, operation_handle=operation_handle)

    def clear_statements(self):
        return self.router.clear_statements()

    def all_statements(self) -> List[str]:
        return self.router.all_statements()

    @staticmethod
    def setup(host: str = None, port: str | int = None, session_name: str = None):
        client = MockFlinkSqlGatewayClient()
        _ = client.create_session(
            host if host else os.getenv('FLINK_SQL_GATEWAY_HOST', '127.0.0.1'),
            port if port else int(os.getenv('FLINK_SQL_GATEWAY_PORT', '8083')),
            session_name if session_name else os.getenv('SESSION_NAME', 'test_session'),
        )
        return client
