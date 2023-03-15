from typing import List

from flink.sqlgateway.client import FlinkSqlGatewayClient
from flink.sqlgateway.operation import SqlGatewayOperation
from flink.sqlgateway.session import SqlGatewaySession
from flink.sqlgateway.config import SqlGatewayConfig
from tests.sqlgateway.mock.gw_router import GwRouter
import requests
import json


class MockFlinkSqlGatewayClient(FlinkSqlGatewayClient):
    router: GwRouter

    @staticmethod
    def create_session(host: str, port: int, session_name: str) -> SqlGatewaySession:
        host_port = f"http://{host}:{port}"
        test_config = {
            "host_port": host_port,
            "schemas": [
                {"catalog": "default_catalog", "database": "default_database", "tables": [], "views": []}
            ],
            "current_catalog": "default_catalog",
            "current_database": "default_database",
        }
        MockFlinkSqlGatewayClient.router = GwRouter(test_config)
        MockFlinkSqlGatewayClient.router.start()
        # create session
        r = requests.post(f"{host_port}/v1/sessions", json.dumps({"sessionName": f"{session_name}"}))
        session_handle = r.json()['sessionHandle']
        import threading
        print(f"thread_id_AA={threading.get_native_id()}")
        return SqlGatewaySession(SqlGatewayConfig(host, port, session_name), session_handle)

    @staticmethod
    def execute_statement(session: SqlGatewaySession, sql: str) -> SqlGatewayOperation:
        if session.session_handle is None:
            raise Exception(
                f"Session '{session.config.session_name}' is not created. Call create() method first"
            )
        host_port = f"http://{session.config.host}:{session.config.port}"
        session_handle = session.session_handle
        data = {"statement": sql}
        r = requests.post(f"{host_port}/v1/sessions/{session_handle}/statements", json.dumps(data))
        operation_handle = r.json()['operationHandle']
        return SqlGatewayOperation(session=session, operation_handle=operation_handle)

    @staticmethod
    def clear_statements(session: SqlGatewaySession):
        return MockFlinkSqlGatewayClient.router.clear_statements()

    @staticmethod
    def all_statements(session: SqlGatewaySession) -> List[str]:
        return MockFlinkSqlGatewayClient.router.all_statements()
