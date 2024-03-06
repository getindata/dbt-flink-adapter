import re
from enum import Enum
from typing import Dict, Optional


class QueryMode(Enum):
    BATCH = "batch"
    STREAMING = "streaming"


class QueryHints:
    fetch_max: Optional[int] = None
    fetch_timeout_ms: Optional[int] = None
    mode: Optional[QueryMode] = None
    test_query: bool = False
    execution_config: Optional[Dict[str, str]] = None
    drop_statement: Optional[str] = None

    def __init__(self, hints=None):
        if hints is None:
            hints = {}
        if "fetch_max" in hints:
            self.fetch_max = int(hints["fetch_max"])
        if "fetch_timeout_ms" in hints:
            self.fetch_timeout_ms = int(hints["fetch_timeout_ms"])
        if "mode" in hints:
            self.mode = QueryMode(hints["mode"].lower())
        if "test_query" in hints:
            self.test_query = bool(hints["test_query"])
        if "execution_config" in hints:
            self.execution_config = {}
            for cfg_item in hints["execution_config"].split(";"):
                key_val = cfg_item.split("=")
                if len(key_val) != 2:
                    raise RuntimeError(f"Improper format of execution config {key_val}")
                self.execution_config[key_val[0]] = key_val[1]
        if "drop_statement" in hints:
            self.drop_statement = hints["drop_statement"]
        if "upgrade_mode" in hints:
            self.upgrade_mode = hints["upgrade_mode"]


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
