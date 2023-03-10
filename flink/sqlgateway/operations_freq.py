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


class FrequentSyncOperation:
    """
    some frequent operations
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
        e, operation = FrequentSyncOperation.execute_sql_and_wait_end(session, sql)
        return e is None

    @staticmethod
    def show_current_catalogs(session: SqlGatewaySession) -> str:
        catalogs = FrequentSyncOperation.execute_sql_collect_all_result(session, sql_show_current_catalog)
        return catalogs[0]

    @staticmethod
    def show_catalogs(session: SqlGatewaySession) -> List[str]:
        return FrequentSyncOperation.execute_sql_collect_all_result(session, sql_show_catalogs)

    @staticmethod
    def show_current_databases(session: SqlGatewaySession) -> str:
        catalogs = FrequentSyncOperation.execute_sql_collect_all_result(session, sql_show_current_database)
        return catalogs[0]

    @staticmethod
    def show_databases(session: SqlGatewaySession, catalog: str) -> List[str]:
        dbs = []
        need_switch_catalog = False
        current_catalog = ''

        if catalog:
            current_catalog = FrequentSyncOperation.show_current_catalogs(session)
            if FrequentSyncOperation.show_current_catalogs(session) != catalog:
                need_switch_catalog = True

        if need_switch_catalog:
            if FrequentSyncOperation.use_catalog(catalog):
                raise RuntimeError(f"fail query database in {catalog}, please check catalog {catalog} exists")

        # know under catalog
        dbs = FrequentSyncOperation.execute_sql_collect_all_result(session, sql_show_databases)

        # switch back if necessary
        if need_switch_catalog:
            FrequentSyncOperation.use_catalog(current_catalog)

        return dbs

    @staticmethod
    def show_relations(session, catalog: str, database: str) -> (List[str], List[str]):
        """
        note: for hive catalog, show tables also returns views
        views = show views in catalog.database
        tables = show tables in catalog.database - views
        """
        views = FrequentSyncOperation.show_views(session)

        if catalog is None or database is None:
            raise RuntimeError("show table require at least catalog or database")
        if catalog is None:
            if database is None:
                sql = "show tables"
            else:
                sql = "show tables in {}".format(database)
        else:
            if database is None:
                raise RuntimeError("show tables with catalog but no database")
            sql = "show tables in {}.{}".format(catalog, database)

        tables_has_view = FrequentSyncOperation.execute_sql_collect_all_result(session, sql)
        real_tables = list(set(tables_has_view) - set(views))
        return real_tables, views

    @staticmethod
    def show_tables(session: SqlGatewaySession, catalog: str = None, database: str = None) -> List[str]:
        real_tables, _ = FrequentSyncOperation.show_relations(session, catalog, database)
        return real_tables

    @staticmethod
    def show_views(session: SqlGatewaySession, catalog: str = None, database: str = None) -> List[str]:
        """
        NO such sql
        show views in|from catalog.database
        """
        sql = "show views"
        return FrequentSyncOperation.execute_sql_collect_all_result(session, sql)

    @staticmethod
    def execute_sql_collect_all_result(session: SqlGatewaySession, sql_show_xxx) -> List[str]:
        """TODO
        this method is suit for small query, which returns fast and with small result
        it wait remote executing done ( finished or error) and return full result
        - typically for query base information like  catalog / database / relation
        - NOT for streaming sql, which will never end
        - NOT for large dataset query, which has a large result-set
        """
        e, operation = FrequentSyncOperation.execute_sql_and_wait_end(session, sql_show_xxx)
        if e is not None:
            raise e

        try:
            results = []
            result: SqlGatewayResult = operation.get_result(None)
            # print(result)
            for kv in result.rows:
                # {'catalog name': 'default_catalog'}
                results.extend(list(kv.values()))
            while not result.is_end_of_stream:
                result = operation.get_result(result.next_result_url)
                for kv in result.rows:
                    results.extend(list(kv.values()))
            # print(results)
            return results
        except Exception as e:
            raise e

    @staticmethod
    def execute_sql_and_wait_end(session: SqlGatewaySession, sql) -> (Exception, SqlGatewayOperation):
        operation = SqlGatewayOperation.execute_statement(session, sql)
        try:
            # wait till over
            status = operation.get_status()
            while status == STATUS_RUNNING:
                time.sleep(pull_interval_s)
                status = operation.get_status()

            if status == STATUS_ERROR:
                return RuntimeError(f"ran {sql} return error"), operation

            return None, operation
        except Exception as e:
            return e, operation
