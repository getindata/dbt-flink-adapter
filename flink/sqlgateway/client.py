from flink.sqlgateway.operation import SqlGatewayOperation
from flink.sqlgateway.session import SqlGatewaySession
from flink.sqlgateway.config import SqlGatewayConfig


class FlinkSqlGatewayClient:
    @staticmethod
    def create_session(host: str, port: int, session_name: str) -> SqlGatewaySession:
        config = SqlGatewayConfig(host, port, session_name)
        return SqlGatewaySession.create(config)

    @staticmethod
    def execute_statement(session: SqlGatewaySession, sql: str) -> SqlGatewayOperation:
        if session.session_handle is None:
            raise Exception(
                f"Session '{session.config.session_name}' is not created. Call create() method first"
            )

        return SqlGatewayOperation.execute_statement(session=session, sql=sql)
