import json
import requests
from typing import Optional
from flink.sqlgateway.operation import SqlGatewayOperation
from flink.sqlgateway.config import SqlGatewayConfig


class SqlGatewaySession:
    config: SqlGatewayConfig
    session_handle: Optional[str] = None

    def __init__(self, config: SqlGatewayConfig, session_handle: str):
        self.config = config
        self.session_handle = session_handle

    @staticmethod
    def create(config: SqlGatewayConfig) -> "SqlGatewaySession":
        session_request = {"sessionName": config.session_name}

        response = requests.post(
            url=f"${config.gateway_url()}/v1/sessions",
            data=json.dumps(session_request),
            headers={
                "Content-Type": "application/json",
            },
        )

        if response.status_code == 200:
            session_handle = response.json()["sessionHandle"]
            return SqlGatewaySession(config, session_handle)
        else:
            raise Exception("SQL gateway error: ", response.status_code)

    def session_endpoint_url(self) -> str:
        return f"{self.config.gateway_url()}/v1/sessions/{self.session_handle}"

    def execute_statement(self, sql: str) -> SqlGatewayOperation:
        if self.session_handle is None:
            raise Exception(
                f"Session '${self.config.session_name}' is not created. Call create() method first"
            )

        return SqlGatewayOperation.execute_statement(session=self, sql=sql)
