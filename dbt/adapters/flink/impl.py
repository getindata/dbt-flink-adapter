from typing import List

import agate
from dbt.adapters.base import BaseAdapter as adapter_cls, BaseRelation, Column as BaseColumn

from dbt.adapters.flink import FlinkConnectionManager


class FlinkAdapter(adapter_cls):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = FlinkConnectionManager

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        pass

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        pass

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        pass

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        pass

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        pass

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        pass

    def create_schema(self, relation: BaseRelation):
        print("create_schema called", relation)
        pass

    def drop_relation(self, relation: BaseRelation) -> None:
        print("drop_relation called", relation)
        pass

    def drop_schema(self, relation: BaseRelation):
        print("drop_schema called", relation)
        pass

    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        pass

    def get_columns_in_relation(self, relation: BaseRelation) -> List[BaseColumn]:
        print("get_columns_in_relation called", relation)
        return [] # TODO

    @classmethod
    def is_cancelable(cls) -> bool:
        return False # TODO

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        return [] # TODO

    def list_schemas(self, database: str) -> List[str]:
        print("list_schemas called", database)
        return [] # TODO

    @classmethod
    def quote(cls, identifier: str) -> str:
        pass

    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        pass

    def truncate_relation(self, relation: BaseRelation) -> None:
        pass

    @classmethod
    def define_source_tables(cls, config) -> str:
        print(config)
        return "### create or replace source_table"
