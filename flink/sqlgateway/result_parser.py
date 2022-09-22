from dataclasses import dataclass
from typing import Dict, List, Any

from dbt.events import AdapterLogger

logger = AdapterLogger("Flink")

@dataclass
class SqlGatewayResult:
    rows: List[Dict[str, Any]]
    next_result_url: str
    column_names: List[str]

    def __init__(self, rows: List[Dict[str, Any]], column_names: List[str], next_result_url: str):
        self.rows = rows
        self.column_names = column_names
        self.next_result_url = next_result_url


class SqlGatewayResultParser:
    @staticmethod
    def parse_result(data: Dict[str, Any]) -> SqlGatewayResult:
        columns = data["results"]["columns"]
        rows: List[Dict[str, Any]] = []
        next_result_url = data["nextResultUri"]
        column_names: List[str] = list(map(lambda c: c["name"], columns))

        logger.info(f"SQL rows returned: {data['results']['data']}")
        for record in data["results"]["data"]:
            current_row: Dict[str, Any] = {}
            for column_index in range(0, len(columns)):
                column_name: str = columns[column_index]["name"]
                current_row[column_name] = record["fields"][column_index]
            rows.append(current_row)

        return SqlGatewayResult(
            rows=rows,
            column_names=column_names,
            next_result_url=next_result_url
        )
