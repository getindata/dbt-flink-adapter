from flink.sqlgateway.session import SqlGatewaySession
from flink.sqlgateway.statement import SqlGatewayStatement


class FlinkSqlGatewayClient:
    def __init__(self):
        pass

    def connect(self) -> SqlGatewaySession:
        print("Connected...")
        # TODO connect and get session_id
        session_handle = "someSessionHandle"

        return SqlGatewaySession(session_handle)

    def execute_statement(self, session: SqlGatewaySession, sql: str) -> SqlGatewayStatement:
        # TODO execute statement on the session
        return SqlGatewayStatement("someStatementId")
