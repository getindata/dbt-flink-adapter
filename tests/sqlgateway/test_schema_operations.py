import unittest

from flink.sqlgateway.client import FlinkSqlGatewayClient
from flink.sqlgateway.config import SqlGatewayConfig
from flink.sqlgateway.schema_operations import SchemaOperation

from flink.sqlgateway.session import SqlGatewaySession

# need setup a flink-sql-gateway for this test, otherwise these test won't pass
# need setup a flink-sql-gateway for this test, otherwise these test won't pass
# need setup a flink-sql-gateway for this test, otherwise these test won't pass

# catalog:database:[tables],[views]
# set up for test
# default_catalog:default_database:[]
# myhive:default:[]
# myhive:flinkdb01:["raw_orders1", "raw_orders2", "raw_orders2_view"]
# myhive:flinkdb04:["dim_user_info", "fct_user_behavior"]
# hive_catalog:flinkdb04:["dim_user_info", "fct_user_behavior"]

host = '127.0.0.1'
port = 8083
session_name = 'test'
session_handler = "178f4171-ca41-4fe0-9a9b-efa1994edb46"

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

# CREATE CATALOG myhive WITH ('type' = 'hive', 'hive-conf-dir' = '/opt/flink/conf');
myhive = {
    "session": SqlGatewaySession(SqlGatewayConfig(host, port, session_name), session_handler),
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
# CREATE CATALOG hive_catalog WITH (
#   'type'='iceberg',
#   'catalog-type'='hive',
#   'uri'='thrift://hms.dormi.io:9083',
#   'clients'='5',
#   'property-version'='1',
#   'warehouse'='s3a://warehouse/'
# );
hms_iceberg = {
    "session": SqlGatewaySession(SqlGatewayConfig(host, port, session_name), session_handler),
    "expect_catalogs": ["hive_catalog", "default_catalog", "myhive"],
    "use_catalog": "hive_catalog",
    "expect_databases": ["default", "flinkdb01", "flinkdb04"],
    "use_database": "flinkdb04",
    "expect_relations": (["dim_user_info", "fct_user_behavior"], []),
    "expect_tables": ["dim_user_info", "fct_user_behavior"],
    "expect_views": [],
}


# you should prepare some test data for testing
tdata = flink_default
# tdata = myhive
# tdata = hms_iceberg


class SqlGatewayResultParserTest(unittest.TestCase):

    def test_show_catalogs(self):
        catalogs = SchemaOperation.show_catalogs(tdata["session"])
        self.assertEqual(sorted(tdata["expect_catalogs"]), sorted(catalogs))

    def test_use_catalog(self):
        ok = SchemaOperation.use_catalog_database(tdata["session"], tdata["use_catalog"])
        self.assertTrue(ok)

    def test_show_current_catalogs(self):
        SchemaOperation.use_catalog_database(tdata["session"], tdata["use_catalog"])
        catalog = SchemaOperation.show_current_catalogs(tdata["session"])
        self.assertEqual(tdata["use_catalog"], catalog)

    def test_show_databases(self):
        SchemaOperation.use_catalog_database(tdata["session"], tdata["use_catalog"])
        databases = SchemaOperation.show_databases(tdata["session"], tdata["use_catalog"])
        self.assertEqual(sorted(tdata["expect_databases"]), sorted(databases))

    def test_show_tables(self):
        SchemaOperation.use_catalog_database(tdata["session"], tdata["use_catalog"])
        tables = SchemaOperation.show_tables(tdata["session"], tdata["use_catalog"], tdata["use_database"])
        self.assertEqual(sorted(tdata["expect_tables"]), sorted(tables))

    def test_show_views(self):
        SchemaOperation.use_catalog_database(tdata["session"], tdata["use_catalog"], tdata["use_database"])
        tables = SchemaOperation.show_views(tdata["session"], tdata["use_catalog"], tdata["use_database"])
        self.assertEqual(sorted(tdata["expect_views"]), sorted(tables))

    def test_show_relations(self):
        SchemaOperation.use_catalog_database(tdata["session"], tdata["use_catalog"], tdata["use_database"])
        tables, views = SchemaOperation.show_relations(tdata["session"], tdata["use_catalog"],
                                                       tdata["use_database"])
        self.assertEqual(sorted(tdata["expect_relations"][0]), sorted(tables))
        self.assertEqual(sorted(tdata["expect_relations"][1]), sorted(views))


if __name__ == '__main__':
    unittest.main()
