import time
from typing import List

from flink.sqlgateway.operation import SqlGatewayOperation
from flink.sqlgateway.result_parser import SqlGatewayResult
from flink.sqlgateway.session import SqlGatewaySession

STATUS_RUNNING = "RUNNING"
STATUS_OK = "OK"
STATUS_ERROR = "ERROR"
STATUS_FINISHED = "FINISHED"

sql_show_catalogs = "show catalogs"
sql_show_current_catalog = "show current catalog"
sql_show_databases = "show databases"
sql_show_current_database = "show current database"
sql_show_tables = "show tables"

pull_interval_s = 0.1


class ResultCollector:
    """extract data from SqlGatewayResult"""

    def receive(self, result: SqlGatewayResult) -> None:
        raise RuntimeError("not implement")

    def get_result(self):
        raise RuntimeError("not implement")


class ResultCollectorExtractRowDicValue(ResultCollector):
    """
    Row [ {key1:value1}, {key2:value2} ]
    Row [ {key3:value3}, {key4:value4} ]
        => [value1, value2, value3, value4]
    """

    def __init__(self):
        self.values = []

    def receive(self, result: SqlGatewayResult) -> None:
        for kv in result.rows:
            self.values.extend(list(kv.values()))

    def get_result(self):
        return self.values


class SchemaOperation:
    """
    this method is designed for tiny query, which returns fast and with small result
    it wait remote executing done ( finished or error) and return full result
    - typically for query base information like  catalog / database / relation
    - NOT for streaming sql, which will never end
    - NOT for large dataset query, which has a large result-set
    """

    @staticmethod
    def use_catalog_database(session: SqlGatewaySession, catalog: str = None, database: str = None) -> bool:
        if catalog:
            if database:
                sql = f"use {catalog}.{database}"
            else:
                sql = f"use catalog {catalog}"
        else:
            if database:
                sql = f"use {database}"
            else:
                raise RuntimeError(f"use but both catalog and database is empty")
        SchemaOperation.execute_sql_and_wait_end(session, sql)
        return True

    @staticmethod
    def show_current_catalogs(session: SqlGatewaySession) -> str:
        catalogs = SchemaOperation.show_xxx(session, sql_show_current_catalog)
        return catalogs[0]

    @staticmethod
    def show_catalogs(session: SqlGatewaySession) -> List[str]:
        return SchemaOperation.show_xxx(session, sql_show_catalogs)

    @staticmethod
    def show_current_databases(session: SqlGatewaySession) -> str:
        catalogs = SchemaOperation.show_xxx(session, sql_show_current_database)
        return catalogs[0]

    @staticmethod
    def show_databases(session: SqlGatewaySession, catalog: str) -> List[str]:
        need_switch_catalog = False
        current_catalog = ''

        if catalog:
            current_catalog = SchemaOperation.show_current_catalogs(session)
            if SchemaOperation.show_current_catalogs(session) != catalog:
                need_switch_catalog = True

        if need_switch_catalog:
            if SchemaOperation.use_catalog_database(session, catalog):
                raise RuntimeError(f"fail query database in {catalog}, please check catalog {catalog} exists")
        dbs = SchemaOperation.show_xxx(session, sql_show_databases)

        # switch back database if necessary
        if need_switch_catalog:
            SchemaOperation.use_catalog_database(session, current_catalog)

        return dbs

    @staticmethod
    def show_relations(session, catalog: str, database: str) -> (List[str], List[str]):
        """
        note: for hive catalog, show tables also returns views
        views = show views in catalog.database
        tables = show tables in catalog.database - views
        """
        views = SchemaOperation.show_views(session, catalog, database)

        if catalog is None:
            if database is None:
                sql = "show tables"
            else:
                sql = "show tables in {}".format(database)
        else:
            if database is None:
                raise RuntimeError("show tables with catalog but no database")
            sql = "show tables in {}.{}".format(catalog, database)

        tables_has_view = SchemaOperation.show_xxx(session, sql)
        real_tables = list(set(tables_has_view) - set(views))
        return real_tables, views

    @staticmethod
    def show_tables(session: SqlGatewaySession, catalog: str = None, database: str = None) -> List[str]:
        real_tables, _ = SchemaOperation.show_relations(session, catalog, database)
        return real_tables

    @staticmethod
    def show_views(session: SqlGatewaySession, catalog: str = None, database: str = None) -> List[str]:
        """
        NO such sql
        show views in|from catalog.database
        """
        sql = "show views"
        return SchemaOperation.show_xxx(session, sql)

    @staticmethod
    def show_xxx(session: SqlGatewaySession, sql: str) -> List[str]:
        collector = ResultCollectorExtractRowDicValue()
        return SchemaOperation.execute_sql_collect_all_result(session, sql, collector)

    @staticmethod
    def execute_sql_collect_all_result(session: SqlGatewaySession, sql: str, collector: ResultCollector):
        operation = SchemaOperation.execute_sql_and_wait_end(session, sql)

        try:
            result: SqlGatewayResult = operation.get_result(None)
            collector.receive(result)
            while not result.is_end_of_stream:
                result = operation.get_result(result.next_result_url)
                collector.receive(result)
            return collector.get_result()
        except Exception as e:
            raise e

    @staticmethod
    def execute_sql_and_wait_end(session: SqlGatewaySession, sql) -> SqlGatewayOperation:
        operation = SqlGatewayOperation.execute_statement(session, sql)
        try:
            status = operation.get_status()
            while status == STATUS_RUNNING:
                time.sleep(pull_interval_s)
                status = operation.get_status()

            if status == STATUS_ERROR:
                raise RuntimeError(f"ran {sql} return error")
            return operation
        except Exception as e:
            raise e
