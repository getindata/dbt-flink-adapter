from typing import List, Optional, Any, Tuple, Dict, Type

import agate
from dbt.adapters.base import (
    BaseAdapter,
    BaseRelation,
    Column as BaseColumn,
    available,
    PythonJobHelper,
)

from dbt.adapters.flink import FlinkConnectionManager
from dbt.adapters.flink.relation import FlinkRelation


class FlinkAdapter(BaseAdapter):
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
        return "CURRENT_DATE"

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
        return []  # TODO

    @classmethod
    def is_cancelable(cls) -> bool:
        return False  # TODO

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        catalog = schema_relation.path.database
        if not catalog:
            raise RuntimeError("database(flink catalog) should not be empty")

        schema = schema_relation.schema
        if not schema:
            raise RuntimeError("schema(flink database) should not be empty")

        conn: FlinkConnectionManager = self.connections
        tables, views = conn.show_relations(catalog, schema)

        relations = []
        if schema_relation.type is None or schema_relation.type == FlinkRelation.Table:
            for t in tables:
                table = self.Relation.create(database=catalog, schema=schema, id=t, type=FlinkRelation.Table)
                relations.append(table)
        if schema_relation.type is None or schema_relation.type == FlinkRelation.View:
            for v in views:
                view = self.Relation.create(database=catalog, schema=schema, id=v, type=FlinkRelation.View)
                relations.append(view)

        return relations

    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        conn: FlinkConnectionManager = self.connections
        tables, views = conn.show_relations(database, schema)
        if identifier in tables:
            rel = self.Relation.create(database=database, schema=schema, id=identifier, type=FlinkRelation.Table)
        elif identifier in views:
            rel = self.Relation.create(database=database, schema=schema, id=identifier, type=FlinkRelation.View)

        return rel

    def list_schemas(self, database: str) -> List[str]:
        conn: FlinkConnectionManager = self.connections
        return conn.show_catalogs()

    @classmethod
    def quote(cls, identifier: str) -> str:
        return identifier
        # return '"{}"'.format(identifier)

    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        pass

    def truncate_relation(self, relation: BaseRelation) -> None:
        pass

    @available.parse(lambda *a, **k: (None, None))
    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[FlinkConnectionManager, Any]:
        """Add a query to the current transaction. A thin wrapper around
        ConnectionManager.add_query.

        :param sql: The SQL query to add
        :param auto_begin: If set and there is no transaction in progress,
            begin a new one.
        :param bindings: An optional list of bindings for the query.
        :param abridge_sql_log: If set, limit the raw sql logged to 512
            characters
        """
        return self.connections.add_query(sql, auto_begin, bindings, abridge_sql_log)
