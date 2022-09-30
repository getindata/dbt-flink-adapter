from typing import List

import agate
from dbt.adapters.base import BaseAdapter as adapter_cls, BaseRelation, Column as BaseColumn

from dbt.adapters.flink import FlinkConnectionManager
from dbt.adapters.flink.relation import FlinkRelation


class FlinkAdapter(adapter_cls):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = FlinkConnectionManager
    Relation = FlinkRelation

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "STRING"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "DECIMAL"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "BOOLEAN"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "TIMESTAMP"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "DATE"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "TIME"

    def create_schema(self, relation: BaseRelation):
        pass

    def drop_relation(self, relation: BaseRelation) -> None:
        pass

    def drop_schema(self, relation: BaseRelation):
        pass

    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        pass

    def get_columns_in_relation(self, relation: BaseRelation) -> List[BaseColumn]:
        return [] # TODO

    @classmethod
    def is_cancelable(cls) -> bool:
        return False # TODO

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        return [] # TODO

    def list_schemas(self, database: str) -> List[str]:
        return [] # TODO

    @classmethod
    def quote(cls, identifier: str) -> str:
        return identifier
        # return '"{}"'.format(identifier)

    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        pass

    def truncate_relation(self, relation: BaseRelation) -> None:
        pass

