from dataclasses import dataclass


@dataclass
class SqlGatewayConfig:
    host: str
    port: int
    session_name: str

    def __init__(self, host: str, port: int, session_name: str):
        self.host = host
        self.port = port
        self.session_name = session_name

    def gateway_url(self) -> str:
        return f"http://{self.host}:{self.port}"
