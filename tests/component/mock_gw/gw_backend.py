import sqlite3
import re
from typing import Any

import sqlglot
from sqlglot import exp

NAME_CATALOG = "catalog"
NAME_DATABASE = "db"
NAME_TABLE = "tbl"

SQL_CREATE_CATALOG = """
create table catalog(
    _cat_name varchar(100) primary key
)
""".strip()

SQL_CREATE_DB = """
create table db(
    _cat_name varchar(100), 
    _db_name varchar(100),
    primary key (_cat_name, _db_name)
)
""".strip()

SQL_CREATE_TBL = """
create table tbl(
    _cat_name varchar(100), 
    _db_name varchar(100),
    _tbl_name varchar(100),
    _is_table tinyint,
    primary key (_cat_name, _db_name, _tbl_name, _is_table)
)
""".strip()

SQL_INSERT_CATALOG = """
insert into catalog 
values('{}') 
ON CONFLICT(_cat_name) DO NOTHING
""".strip()

SQL_INSERT_DB = """
insert into db 
values('{}', '{}') 
ON CONFLICT(_cat_name, _db_name) DO NOTHING
""".strip()

SQL_INSERT_TBL = """
insert into tbl 
values('{}', '{}', '{}', '{}') 
ON CONFLICT(_cat_name, _db_name, _tbl_name, _is_table) DO NOTHING
""".strip()

SQL_DROP_TBL = """
delete from tbl 
where _cat_name='{}' and _db_name='{}' and _tbl_name='{}' and _is_table={}
""".strip()

SQL_SHOW_TBL = """
select _tbl_name 
from tbl 
where _cat_name = '{}' 
and _db_name='{}'
""".strip()

SQL_SHOW_VIEW = """
select _tbl_name from tbl 
where _cat_name = '{}'
and _db_name='{}'
and _is_table=0
""".strip()

re_show_dbs = re.compile(r"^show\s+databases$")
re_show_cats = re.compile(r"^show\s+catalogs$")

# show tables [from|in [catalog.]database]
re_show_tbls = re.compile(r"^show\s+tables(\s+(from|in)\s+((\w)+\.)?(\w+))?$")
re_show_views = re.compile(r"^show\s+views(\s+(from|in)\s+((\w)+\.)?(\w+))?$")

re_show_curr_db = re.compile(r"^show\s+current\s+database$")
re_show_curr_cat = re.compile(r"^show\s+current\s+catalog$")

re_use_cat = re.compile(r"^use\s+catalog\s+(\w+)$")
re_use_db = re.compile(r"^use\s+(\w+)$")
re_use_cat_db = re.compile(r"^use\s+(\w+)\.(\w+)$")

re_create_db = re.compile(r"create\s+database\s+(\w+)")

read_dialect = "hive"
write_dialect = "sqlite3"


class GwBackend:
    """
    this backend mocks a hive catalog + flink
    """

    # config: dict
    # current_catalog: str
    # current_database: str
    # mode: str  # streaming or batch

    def __init__(self, config):
        self.conf = config
        con = sqlite3.connect(":memory:", check_same_thread=False)
        cur = con.cursor()

        self.con = con
        self.cur = cur
        self.curr_cat = config["current_catalog"]
        self.curr_db = config["current_database"]

        cur.execute(SQL_CREATE_CATALOG)
        cur.execute(SQL_CREATE_DB)
        cur.execute(SQL_CREATE_TBL)

        for schema in config["schemas"]:
            catalog = schema["catalog"]
            database = schema["database"]
            sql = SQL_INSERT_CATALOG.format(catalog)
            # print(sql)
            cur.execute(sql)

            sql = SQL_INSERT_DB.format(catalog, database)
            # print(sql)
            cur.execute(sql)

            for tbl in schema["tables"]:
                sql = SQL_INSERT_TBL.format(catalog, database, tbl, 1)
                # print(sql)
                cur.execute(sql)
            for view in schema["views"]:
                sql = SQL_INSERT_TBL.format(catalog, database, view, 0)
                # print(sql)
                cur.execute(sql)

        print("============ init check: backend metadata ==========")
        res = cur.execute("select * from catalog")
        print(res.fetchall())
        res = cur.execute("select * from db")
        print(res.fetchall())
        res = cur.execute("select * from tbl")
        print(res.fetchall())
        print("============ init check ==========")

    def execute_statement(self, sql: str):
        """
        return err, result
        sqlglot may be useful
        """
        # case set k=v, simple ignore
        if sql.startswith("set"):
            return "OK", "set"

        # case: use catalog xx_cat
        # case: use xx_db
        if sql.startswith("use"):
            if group := re.search(re_use_cat_db, sql):
                return self._use_cat_db(group[1], group[2]), "use_catalog_db"
            if group := re.search(re_use_cat, sql):
                return self._use_cat_db(group[1], None), "use_catalog"
            if group := re.search(re_use_db, sql):
                return self._use_cat_db(None, group[1]), "use_database"
            raise RuntimeError(f"unsupported sql {sql}")

        # for some sql contains /* xxx */ we will simply ignore this
        sql = str(sqlglot.parse_one(sql))
        sql = sql.strip().lower()

        # case: show catalogs
        # case: show databases
        # case: show current catalog
        # case: show current database
        if sql.startswith("show"):
            if re.search(re_show_cats, sql):
                return self._show_catalogs(), "show_catalogs"
            elif re.search(re_show_dbs, sql):
                return self._show_databases(), "show_databases"
            elif re.search(re_show_curr_cat, sql):
                return self._show_current_catalog(), "show_current_catalog"
            elif re.search(re_show_curr_db, sql):
                return self._show_current_database(), "show_current_database"
            elif match := re.search(re_show_tbls, sql):
                catalog = match[3][:-1] if match[3] else None
                database = match[5] if match[5] else None
                return self._show_tables(catalog, database), "show_tables"
            elif match := re.search(re_show_views, sql):
                catalog = match[3][:-1] if match[3] else None
                database = match[5] if match[5] else None
                return self._show_views(catalog, database), "show_views"
            else:
                raise RuntimeError(f"unsupported sql {sql}")

        # create database xx_db
        if match := re.search(re_create_db, sql):
            db_name = match.group(1)
            return self._db_create(db_name), "create_database"

        # execute by sqlite
        parsed_sql = sqlglot.parse_one(sql)
        if parsed_sql.key == "create":
            is_table = GwBackend._is_table_not_view_when_create_or_drop(sql)
            return self._table_create(sql, is_table), "create_table" if is_table else "create_view"
        elif parsed_sql.key == "select":
            return self._table_select(sql), "_type"
        elif parsed_sql.key == "insert":
            return self._table_insert(sql), "_type"
        elif parsed_sql.key == "drop":
            is_table = GwBackend._is_table_not_view_when_create_or_drop(sql)
            return self._table_drop(sql, is_table), "_type"
        else:
            raise "NOT support"

    def _execute_by_sqlite(self, sql) -> list[Any]:
        print(f"""
===================sqlite execute==================
{sql}
===================================================
        """)
        try:
            res = self.cur.execute(sql)
            result = res.fetchall()
        except Exception as e:
            raise e
        return result

    def _util_sqlite_return_not_empty(self, sql) -> bool:
        result = self._execute_by_sqlite(sql)
        return len(result) == 1

    def _show_catalogs(self):
        sql = "select * from catalog"
        result = self._execute_by_sqlite(sql)
        cats = [x[0] for x in result]
        return cats

    def _show_current_catalog(self):
        return self.curr_cat

    def _show_databases(self):
        sql = f"select _db_name from db where _cat_name = '{self.curr_cat}'"
        result = self._execute_by_sqlite(sql)
        dbs = [x[0] for x in result]
        return dbs

    def _show_current_database(self):
        return self.curr_db

    def _show_tables(self, catalog, database):
        cat = catalog if catalog else self.curr_cat
        db = database if database else self.curr_db
        sql = SQL_SHOW_TBL.format(cat, db)
        result = self._execute_by_sqlite(sql)
        dbs = [x[0] for x in result]
        return dbs

    def _show_views(self, catalog, database):
        cat = catalog if catalog else self.curr_cat
        db = database if database else self.curr_db
        sql = SQL_SHOW_VIEW.format(cat, db)
        result = self._execute_by_sqlite(sql)
        dbs = [x[0] for x in result]
        return dbs

    def _use_cat_db(self, xx_cat, xx_db):
        # todo make sure xx_cat, xx_db exists
        xx_cat = xx_cat if xx_cat else self.curr_cat
        xx_db = xx_db if xx_db else self.curr_db
        self.curr_cat = xx_cat
        self.curr_db = xx_db
        return "OK"

    def _db_create(self, xx_db):
        sql = f"select 1 from db where _cat_name='{self.curr_cat}' and _db_name='{xx_db}'"
        if self._util_sqlite_return_not_empty(sql):
            return RuntimeError(f"db {xx_db} exists in {self.curr_cat}"), None
        sql = f"insert into db values('{self.curr_cat}', '{xx_db}')"
        result = self._execute_by_sqlite(sql)
        return "OK"

    def _table_create(self, sql: str, is_table: bool = True):
        rewrite_sql, _ = GwBackend._sql_rewrite_remove_catalog_and_schema(sql)
        rewrite_sql, _ = GwBackend._sql_rewrite_drop_with_properties(rewrite_sql)
        result = self._execute_by_sqlite(rewrite_sql)
        # recode meta data
        parsed_sql = sqlglot.parse_one(sql)
        for table in parsed_sql.find_all(exp.Table):
            cat_name = table.catalog if table.catalog else self.curr_cat
            db_name = table.db if table.db else self.curr_db
            sql2 = SQL_INSERT_TBL.format(cat_name, db_name, table.name, 1 if is_table else 0)
            result = self._execute_by_sqlite(sql2)
            break
        return "OK"

    def _table_select(self, sql):
        rewrite_sql, _ = GwBackend._sql_rewrite_remove_catalog_and_schema(sql)
        result = self._execute_by_sqlite(rewrite_sql)
        return result

    def _table_insert(self, sql):
        rewrite_sql, _ = GwBackend._sql_rewrite_remove_catalog_and_schema(sql)
        result = self._execute_by_sqlite(rewrite_sql)
        return "OK"

    def _table_drop(self, sql, is_table: bool = True):
        rewrite_sql, _ = GwBackend._sql_rewrite_remove_catalog_and_schema(sql)
        result = self._execute_by_sqlite(rewrite_sql)
        # recode meta data
        parsed_sql = sqlglot.parse_one(sql)
        for table in parsed_sql.find_all(exp.Table):
            cat_name = table.catalog if table.catalog else self.curr_cat
            db_name = table.db if table.db else self.curr_db
            sql2 = SQL_DROP_TBL.format(cat_name, db_name, table.name, 1 if is_table else 0)
            result = self._execute_by_sqlite(sql2)
            break
        return "OK"

    @staticmethod
    def _sql_rewrite_remove_catalog_and_schema(sql):
        # TODO cannot do complex rewrite, simple str replace
        parsed_sql = sqlglot.parse_one(sql)
        rewrite_sql = sql
        replace_list = []
        for table in parsed_sql.find_all(exp.Table):
            print(table.name)
            if table.catalog and table.db:
                replace_list.append((f"{table.catalog}.{table.db}.{table.name}", table.name))
                rewrite_sql = rewrite_sql.replace(f"{table.catalog}.{table.db}.{table.name}", table.name)
            elif table.db:
                replace_list.append((f"{table.db}.{table.name}", table.name))
                rewrite_sql = rewrite_sql.replace(f"{table.db}.{table.name}", table.name)
        return rewrite_sql, replace_list

    @staticmethod
    def _sql_rewrite_drop_with_properties(sql):
        # create table t1(a int) with (some-properties) =>
        parsed_sql = sqlglot.parse_one(sql)
        with_props = None
        if parsed_sql.args.get('properties'):
            with_props = parsed_sql.args['properties']
            del parsed_sql.args['properties']
        return str(parsed_sql), with_props

    @staticmethod
    def _is_table_not_view_when_create_or_drop(sql: str):
        sql1 = sql.lower()
        re_create_table = re.compile(r"create[\s\n\r]+table")
        re_create_view = re.compile(r"create[\s\n\r]+view")
        re_drop_table = re.compile(r"drop[\s\n\r]+table")
        re_drop_view = re.compile(r"drop[\s\n\r]+view")

        if re.search(re_create_table, sql1) or re.search(re_drop_table, sql1):
            return True
        elif re.search(re_create_view, sql1) or re.search(re_drop_view, sql1):
            return False
        else:
            raise f"known create|drop statement:{sql}"
