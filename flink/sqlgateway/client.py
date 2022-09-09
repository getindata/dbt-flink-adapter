from flink.sqlgateway.session import SqlGatewaySession
from flink.sqlgateway.config import SqlGatewayConfig


class FlinkSqlGatewayClient:
    @staticmethod
    def create_session(host: str, port: int, session_name: str) -> SqlGatewaySession:
        config = SqlGatewayConfig(host, port, session_name)
        return SqlGatewaySession.create(config)
