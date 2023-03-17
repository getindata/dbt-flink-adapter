import unittest
from typing import List

from tests.component.mock_gw.gw_handle_plus import GwHandlePlus
from tests.component.mock_gw.mock_gw import default_config_sample

sql_create_table_t99 = """
create table t99(id int, name varchar(20), ts timestamp)
with(
'connector' = 'filesystem', 
'path' = 's3a://warehouse/flinkdb03.db/raw_orders3', 
'format' = 'orc'
)
""".strip()
sql_create_table_t98 = "create table t98(id int, name varchar(20), ts timestamp)"
sql_create_view_v99_as_t99 = "create view v99 as select * from t99"
sql_create_view_v98_as_t99 = "create view v98 as select * from t99"
sql_drop_table_t99 = "drop table t99"
sql_drop_view_v99 = "drop view v99"

sql_use_catalog_cat1 = "use catalog cat1"
sql_use_db_cat1_db1 = "use cat1_db1"
sql_use_catalog_db_cat2_db1 = "use cat2.cat2_db1"

sql_show_catalogs = "show catalogs"
sql_show_create_table = "show create table t99"
sql_show_current_catalog = "show current catalog"
sql_show_current_database = "show current database"
sql_show_databases = "show databases"
sql_show_tables = "show tables"
sql_show_views = "show views"


def _extract_all_data_sorted(payload):
    data = payload["results"]["data"]
    r = []
    for item in data:
        r.extend(item['fields'])
    return sorted(r)


def _execute_sql_check_result_twice(sql_s: List[str]):
    """
    execute some sql, and return result of last sql
    typically we mock results = some-payload, EOS
    """
    gw_handle = GwHandlePlus(default_config_sample)
    session_handle = gw_handle.session_create()["sessionHandle"]
    # first
    sql = sql_s[0]
    result = gw_handle.statement_create(session_handle, {"statement": sql})
    # middle
    if len(sql_s) > 2:
        for i in range(1, len(sql_s) - 1):
            gw_handle.statement_create(session_handle, {"statement": sql_s[i]})
    # last
    if len(sql_s) >= 2:
        result = gw_handle.statement_create(session_handle, {"statement": sql_s[-1]})
    operation_handle = result["operationHandle"]

    r0 = gw_handle.operation_result(session_handle, operation_handle, 0)
    eos = gw_handle.operation_result(session_handle, operation_handle, 1)
    print("=============== sql & result ===============")
    print(sql)
    print(r0)
    print(eos)
    print("===============")
    return r0


class TestGwHandleV1(unittest.TestCase):
    def test_show_tables(self):
        payload = _execute_sql_check_result_twice([sql_show_tables])
        self.assertEqual(sorted(["default_table"]), _extract_all_data_sorted(payload))

    def test_create_table(self):
        payload = _execute_sql_check_result_twice([
            sql_create_table_t99,
            sql_show_tables
        ])
        self.assertEqual(sorted(["default_table", "t99"]), _extract_all_data_sorted(payload))

    def test_create_table_view(self):
        payload = _execute_sql_check_result_twice([
            sql_create_table_t99,
            sql_create_view_v99_as_t99,
            sql_create_view_v98_as_t99,
            sql_show_views
        ])
        self.assertEqual(sorted(["v99", "v98"]), _extract_all_data_sorted(payload))

    def test_use_catalog(self):
        payload = _execute_sql_check_result_twice([
            sql_use_catalog_cat1,
            sql_show_current_catalog
        ])
        self.assertEqual(sorted(["cat1"]), _extract_all_data_sorted(payload))

    def test_use_db(self):
        payload = _execute_sql_check_result_twice([
            sql_use_catalog_cat1,
            sql_use_db_cat1_db1,
            sql_show_current_database
        ])
        self.assertEqual(sorted(["cat1_db1"]), _extract_all_data_sorted(payload))

    def test_use_catalog_db(self):
        payload = _execute_sql_check_result_twice([
            sql_use_catalog_db_cat2_db1,
            sql_show_current_database
        ])
        self.assertEqual(sorted(["cat2_db1"]), _extract_all_data_sorted(payload))

    def test_drop_table_view(self):
        payload = _execute_sql_check_result_twice([
            sql_create_table_t99,
            sql_create_table_t98,
            sql_drop_table_t99,
            sql_show_tables
        ])
        self.assertEqual(sorted(["default_table", "t98"]), _extract_all_data_sorted(payload))

    def test_drop_view(self):
        payload = _execute_sql_check_result_twice([
            sql_create_table_t99,
            sql_create_view_v98_as_t99,
            sql_create_view_v99_as_t99,
            sql_drop_view_v99,
            sql_show_views
        ])
        self.assertEqual(sorted(["v98"]), _extract_all_data_sorted(payload))
