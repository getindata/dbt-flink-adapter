import unittest

from flink.sqlgateway.client import FlinkSqlGatewayClient
from flink.sqlgateway.config import SqlGatewayConfig
from flink.sqlgateway.operations_freq import FrequentSyncOperation

from flink.sqlgateway.session import SqlGatewaySession

host = '127.0.0.1'
port = 8083
session_name = 'test'

# need setup a flink-sql-gateway for this test, otherwise these test won't pas
# need setup a flink-sql-gateway for this test, otherwise these test won't pas
# need setup a flink-sql-gateway for this test, otherwise these test won't pas

# catalog:database:[tables],[views]
# set up for test
# default_catalog:default_database:[]
# myhive:default:[]
# myhive:flinkdb01:["raw_orders1", "raw_orders2", "raw_orders2_view"]
# myhive:flinkdb04:["dim_user_info", "fct_user_behavior"]
# hive_catalog:flinkdb04:["dim_user_info", "fct_user_behavior"]

flink_default = {
    "session": FlinkSqlGatewayClient.create_session(host, port, session_name),
    "expect_catalogs": ["default_catalog"],
    "use_catalog": "default_catalog",
    "expect_databases": ["default_database"],
    "use_database": "default_database",
    "expect_relations": ([], []),
    "expect_tables": [],
    "expect_views": [],
}

myhive = {
    "session": SqlGatewaySession(SqlGatewayConfig(host, port, session_name), "bd6a42fe-0905-41d6-8d5f-064d72aaaa59"),
    "expect_catalogs": ["hive_catalog", "default_catalog", "myhive"],
    "use_catalog": "myhive",
    "expect_databases": ["default", "flinkdb01", "flinkdb04"],
    "use_database": "flinkdb01",
    "expect_relations": (["raw_orders1", "raw_orders2"], ["raw_orders2_view"]),
    "expect_tables": ["raw_orders1", "raw_orders2"],
    "expect_views": ["raw_orders2_view"],
}

# hive catalog for iceberg
# this catalog do not support view create
# these databases is visible in myhive catalog
hms_iceberg = {
    "session": SqlGatewaySession(SqlGatewayConfig(host, port, session_name), "bd6a42fe-0905-41d6-8d5f-064d72aaaa59"),
    "expect_catalogs": ["hive_catalog", "default_catalog", "myhive"],
    "use_catalog": "hive_catalog",
    "expect_databases": ["default", "flinkdb01", "flinkdb04"],
    "use_database": "flinkdb04",
    "expect_relations": (["dim_user_info", "fct_user_behavior"], []),
    "expect_tables": ["dim_user_info", "fct_user_behavior"],
    "expect_views": [],
}


# you should prepare some test data for testing
# you should prepare some test data for testing
# you should prepare some test data for testing
# tdata = flink_default
# tdata = myhive
tdata = hms_iceberg


class SqlGatewayResultParserTest(unittest.TestCase):

    def test_show_catalogs(self):
        catalogs = FrequentSyncOperation.show_catalogs(tdata["session"])
        self.assertEqual(sorted(tdata["expect_catalogs"]), sorted(catalogs))

    def test_use_catalog(self):
        ok = FrequentSyncOperation.use_catalog_database(tdata["session"], tdata["use_catalog"])
        self.assertTrue(ok)

    def test_show_current_catalogs(self):
        FrequentSyncOperation.use_catalog_database(tdata["session"], tdata["use_catalog"])
        catalog = FrequentSyncOperation.show_current_catalogs(tdata["session"])
        self.assertEqual(tdata["use_catalog"], catalog)

    def test_show_databases(self):
        FrequentSyncOperation.use_catalog_database(tdata["session"], tdata["use_catalog"])
        databases = FrequentSyncOperation.show_databases(tdata["session"], tdata["use_catalog"])
        self.assertEqual(sorted(tdata["expect_databases"]), sorted(databases))

    def test_show_tables(self):
        FrequentSyncOperation.use_catalog_database(tdata["session"], tdata["use_catalog"])
        tables = FrequentSyncOperation.show_tables(tdata["session"], tdata["use_catalog"], tdata["use_database"])
        self.assertEqual(sorted(tdata["expect_tables"]), sorted(tables))

    def test_show_views(self):
        FrequentSyncOperation.use_catalog_database(tdata["session"], tdata["use_catalog"], tdata["use_database"])
        tables = FrequentSyncOperation.show_views(tdata["session"], tdata["use_catalog"], tdata["use_database"])
        self.assertEqual(sorted(tdata["expect_views"]), sorted(tables))

    def test_show_relations(self):
        FrequentSyncOperation.use_catalog_database(tdata["session"], tdata["use_catalog"], tdata["use_database"])
        tables, views = FrequentSyncOperation.show_relations(tdata["session"], tdata["use_catalog"],
                                                             tdata["use_database"])
        self.assertEqual(sorted(tdata["expect_relations"][0]), sorted(tables))
        self.assertEqual(sorted(tdata["expect_relations"][1]), sorted(views))


if __name__ == '__main__':
    unittest.main()
