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
        protocol = "https" if self.port == 443 else "http"
        return f"{protocol}://{self.host}:{self.port}"
