from dataclasses import dataclass


@dataclass
class SqlGatewaySession:
    session_handle: str = None

    def __init__(self, session_handle: str):
        self.session_handle = session_handle
