from dataclasses import dataclass
from typing import Dict, List, Any


@dataclass
class SqlGatewayResult:
    rows: List[Dict[str, Any]]
    next_result_url: str

    def __init__(self, rows: List[Dict[str, Any]], next_result_url: str):
        self.rows = rows
        self.next_result_url = next_result_url


class SqlGatewayResultParser:
    @staticmethod
    def parse_result(data: Dict[str, Any]) -> SqlGatewayResult:
        columns = data["results"]["columns"]
        rows: List[Dict[str, Any]] = []
        next_result_url = data["nextResultUri"]

        for record in data["results"]["data"]:
            current_row: Dict[str, Any] = {}
            for column_index in range(0, len(columns)):
                column_name: str = columns[column_index]["name"]
                current_row[column_name] = record["fields"][column_index]
            rows.append(current_row)

        return SqlGatewayResult(
            rows=rows,
            next_result_url=next_result_url
        )
