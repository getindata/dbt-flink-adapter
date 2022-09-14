from dataclasses import dataclass
import json
from typing import Optional

import requests
from flink.sqlgateway.session import SqlGatewaySession


@dataclass
class SqlGatewayOperation:
    session: SqlGatewaySession
    operation_handle: str

    def __init__(self, session: SqlGatewaySession, operation_handle: str):
        self.session = session
        self.operation_handle = operation_handle

    @staticmethod
    def execute_statement(session: SqlGatewaySession, sql: str) -> "SqlGatewayOperation":
        statement_request = {"statement": sql}

        response = requests.post(
            url=f"${session.session_endpoint_url()}/statements",
            data=json.dumps(statement_request),
            headers={
                "Content-Type": "application/json",
            },
        )

        if response.status_code == 200:
            operation_handle = response.json()["operationHandle"]
            return SqlGatewayOperation(session=session, operation_handle=operation_handle)
        else:
            raise Exception("SQL gateway error: ", response.status_code)

    def statement_endpoint_url(self) -> str:
        return f"${self.session.session_endpoint_url()}/operations/${self.operation_handle}"

    def get_status(self) -> str:
        response = requests.get(
            url=f"${self.statement_endpoint_url()}/status",
            headers={
                "Content-Type": "application/json",
            },
        )

        if response.status_code == 200:
            return response.json()["status"]
        else:
            raise Exception("SQL gateway error: ", response.status_code)

