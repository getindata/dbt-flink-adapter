from dataclasses import dataclass

import re
from typing import Dict


class QueryHints:
    fetch_max: int = None
    fetch_timeout_ms: int = None
    fetch_mode: str = None

    def __init__(self, hints: Dict[str, str]):
        if "fetch_max" in hints:
            self.fetch_max = int(hints["fetch_max"])
        if "fetch_timeout_ms" in hints:
            self.fetch_timeout_ms = int(hints["fetch_timeout_ms"])
        if "fetch_mode" in hints:
            self.fetch_mode = hints["fetch_mode"]


class QueryHintsParser:
    @staticmethod
    def parse(sql: str) -> QueryHints:
        hints_clauses = re.findall("\/\*\*(.+?)\*\/", sql)
        hints = {}
        for clause in hints_clauses:
            for hint in re.findall("([a-zA-Z0-9_]+?\(.+?\))", clause):
                groups = re.findall("([a-zA-Z0-9_]+?)\((.+?)\)", hint)
                hint_name = groups[0][0].strip()
                hint_value = QueryHintsParser._strip_quotes(groups[0][1])
                hints[hint_name] = hint_value
        return QueryHints(hints)

    @staticmethod
    def _strip_quotes(txt: str) -> str:
        return txt.strip().strip('"').strip("'")
