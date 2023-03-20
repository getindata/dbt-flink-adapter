import unittest

from tests.component.mock_gw.gw_backend import GwBackend
from tests.component.mock_gw.mock_gw import many_catalog_config

NAME_CATALOG = "catalog"
NAME_DATABASE = "db"
NAME_TABLE = "tbl"


class TestGwBackend(unittest.TestCase):

    def test_set_kv(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("set 'k1'='v1'")
        self.assertEqual("OK", res)

    def test_show_catalogs(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("show catalogs  ")
        self.assertEqual(sorted(["default_catalog", "cat1", "cat2"]), sorted(res))

    def test_show_current_catalog(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("show current catalog")
        self.assertEqual(many_catalog_config["current_catalog"], res)

    def test_show_databases(self):
        backend = GwBackend(many_catalog_config)
        backend.execute_statement("use catalog cat1")
        res, _type = backend.execute_statement("show databases")
        self.assertEqual(sorted(["cat1_db1", "cat1_db2"]), sorted(res))

    def test_show_current_database(self):
        backend = GwBackend(many_catalog_config)
        backend.execute_statement("use catalog cat1")
        backend.execute_statement("use cat1_db1")
        res, _type = backend.execute_statement("show current database")
        self.assertEqual("cat1_db1", res)

    def test_show_current_database_2(self):
        backend = GwBackend(many_catalog_config)
        backend.execute_statement("use cat1.cat1_db1")
        res, _type = backend.execute_statement("show current database")
        self.assertEqual("cat1_db1", res)

    def test_show_tables(self):
        backend = GwBackend(many_catalog_config)
        backend.execute_statement("use cat1.cat1_db1")
        res, _type = backend.execute_statement("show tables")
        self.assertEqual(sorted(["t111", "t112", "v111", "v112"]), sorted(res))

    def test_show_views(self):
        backend = GwBackend(many_catalog_config)
        backend.execute_statement("use cat1.cat1_db1")
        res, _type = backend.execute_statement("show views")
        self.assertEqual(sorted(["v111", "v112"]), sorted(res))

    def test_use_cat(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("use catalog cat2")
        self.assertEqual("OK", res)
        res, _type = backend.execute_statement("show current catalog")
        self.assertEqual("cat2", res)

    def test_use_db(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("use cat1_db2")
        self.assertEqual("OK", res)
        res, _type = backend.execute_statement("show current database")
        self.assertEqual("cat1_db2", res)

    def test_use_cat_db(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("use cat2.flink04")
        self.assertEqual("OK", res)
        res, _type = backend.execute_statement("show current catalog")
        self.assertEqual("cat2", res)
        res, _type = backend.execute_statement("show current database")
        self.assertEqual("flink04", res)

    def test_db_create(self):
        backend = GwBackend(many_catalog_config)
        backend.execute_statement("use catalog cat1")
        res, _type = backend.execute_statement("create database xx_db")
        self.assertEqual("OK", res)
        res, _type = backend.execute_statement("show databases")
        self.assertEqual(sorted(["cat1_db1", "cat1_db2", "xx_db"]), sorted(res))

    def test_table_create(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("create table cat1.cat1_db1.t99(id int primary key)")
        self.assertEqual("OK", res)
        #
        res, _type = backend.execute_statement("show tables from cat1.cat1_db1")
        self.assertEqual(sorted(["t111", "t112", "v111", "v112", "t99"]), sorted(res))

    def test_table_create_with_property(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("create table cat1.cat1_db1.t99(id int primary key) with('a'='b')")
        self.assertEqual("OK", res)
        #
        res, _type = backend.execute_statement("show tables in cat1.cat1_db1")
        self.assertEqual(sorted(["t111", "t112", "v111", "v112", "t99"]), sorted(res))

    def test_table_view(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("create table cat1.cat1_db1.t99(id int primary key)")
        res, _type = backend.execute_statement("create view cat1.cat1_db1.v99 as select * from t99")
        self.assertEqual("OK", res)
        #
        res, _type = backend.execute_statement("show views from cat1.cat1_db1")
        self.assertEqual(sorted(["v111", "v112", "v99"]), sorted(res))

    def test_table_create_then_insert(self):
        backend = GwBackend(many_catalog_config)
        backend.execute_statement("create table t99(id int primary key)")
        res, _type = backend.execute_statement("insert into t99 values(1),(2)")
        #
        res, _type = backend.execute_statement("select id, id+1 from t99")
        self.assertEqual(2, len(res))
        self.assertEqual([1, 2], sorted([x[0] for x in res]))

    def test_table_create_has_catalog_and_db(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("create table cat1.cat1_db1.t99(id int primary key)")
        self.assertEqual("OK", res)
        #
        res, _type = backend.execute_statement("show tables from cat1.cat1_db1")
        self.assertEqual(sorted(["t111", "t112", "v111", "v112", "t99"]), sorted(res))

    def test_table_create_has_catalog_and_db_then_insert(self):
        backend = GwBackend(many_catalog_config)
        backend.execute_statement("create table cat1.cat1_db1.t99(id int primary key)")
        res, _type = backend.execute_statement("insert into cat1.cat1_db1.t99 values(1),(2)")
        res, _type = backend.execute_statement("select id, id+1 from cat1.cat1_db1.t99")
        self.assertEqual(2, len(res))
        self.assertEqual([1, 2], sorted([x[0] for x in res]))

    def test_table_drop(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("create table cat1.cat1_db1.t99(id int primary key)")
        res, _type = backend.execute_statement("drop table t99")
        self.assertEqual(2, len(res))

    def test_view_drop(self):
        backend = GwBackend(many_catalog_config)
        res, _type = backend.execute_statement("create view cat1.cat1_db1.v99 as select * from t1")
        res, _type = backend.execute_statement("show views from cat1.cat1_db1")
        self.assertEqual(sorted(["v111", "v112", "v99"]), sorted(res))
        res, _type = backend.execute_statement("drop view cat1.cat1_db1.v99  ")
        res, _type = backend.execute_statement("show views in cat1.cat1_db1")
        self.assertEqual(sorted(["v111", "v112"]), sorted(res))

    def test_table_select(self):
        backend = GwBackend(many_catalog_config)
        sql = """
        with e as (
            select 1 as id
            union
            select 2 as id
        ) 
        select id, id+1 from e
        """.strip()
        res, _type = backend.execute_statement(sql)
        self.assertEqual(2, len(res))
        self.assertEqual([1, 2], sorted([x[0] for x in res]))

    def test_table_insert_select(self):
        backend = GwBackend(many_catalog_config)
        # t99 = (1), (2)
        backend.execute_statement("create table cat1.cat1_db1.t99(id int primary key)")
        res, _type = backend.execute_statement("insert into cat1.cat1_db1.t99 values(1),(2)")
        # t98 <= t99
        backend.execute_statement("create table cat1.cat1_db1.t98(id int, id2 int)")
        sql = """
        with e as (select * from t99) 
        insert into t98
        select id, id+1 from e
        """.strip()
        res, _type = backend.execute_statement(sql)
        # check t98
        res, _type = backend.execute_statement("select id, id+1 from cat1.cat1_db1.t98")
        self.assertEqual(2, len(res))
        self.assertEqual([1, 2], sorted([x[0] for x in res]))
