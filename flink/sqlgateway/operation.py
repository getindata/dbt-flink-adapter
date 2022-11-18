from dataclasses import dataclass
import json
from typing import Optional
import requests
from flink.sqlgateway.result_parser import SqlGatewayResult, SqlGatewayResultParser
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
            url=f"{session.session_endpoint_url()}/statements",
            data=json.dumps(statement_request),
            headers={
                "Content-Type": "application/json",
            },
        )

        print(f"SQL gateway response: {json.dumps(response.json())}")

        if response.status_code == 200:
            operation_handle = response.json()["operationHandle"]
            return SqlGatewayOperation(session=session, operation_handle=operation_handle)
        else:
            raise Exception("SQL gateway error: ", response.status_code)

    def statement_endpoint_url(self) -> str:
        return f"{self.session.session_endpoint_url()}/operations/{self.operation_handle}"

    def get_status(self) -> str:
        response = requests.get(
            url=f"{self.statement_endpoint_url()}/status",
            headers={
                "Content-Type": "application/json",
            },
        )

        if response.status_code == 200:
            return response.json()["status"]
        else:
            raise Exception("SQL gateway error: ", response.status_code)

    def cancel(self) -> str:
        response = requests.post(
            url=f"{self.statement_endpoint_url()}/cancel",
            headers={
                "Content-Type": "application/json",
            },
        )

        if response.status_code == 200:
            return response.json()["status"]
        else:
            raise Exception("SQL gateway error: ", response.status_code)

    def close(self) -> str:
        response = requests.delete(
            url=f"{self.statement_endpoint_url()}/close",
            headers={
                "Content-Type": "application/json",
            },
        )

        if response.status_code == 200:
            return response.json()["status"]
        else:
            raise Exception("SQL gateway error: ", response.status_code)

    def get_result(self, next_page: Optional[str] = None) -> SqlGatewayResult:
        if next_page is None:
            result_page_url = f"{self.statement_endpoint_url()}/result/0"
        else:
            result_page_url = f"{self.session.config.gateway_url()}{next_page}"

        response = requests.get(
            url=result_page_url,
            headers={
                "Content-Type": "application/json",
            },
        )

        if response.status_code == 200:
            return SqlGatewayResultParser.parse_result(response.json())
        else:
            raise Exception("SQL gateway error: ", response.reason)
