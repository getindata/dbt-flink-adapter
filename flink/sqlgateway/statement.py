from dataclasses import dataclass


@dataclass
class SqlGatewayStatement:
    statement_id: str

    def __init__(self, statement_id: str):
        self.statement_id = statement_id
