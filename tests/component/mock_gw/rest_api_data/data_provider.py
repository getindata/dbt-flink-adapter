import json
from typing import Any, List

from tests.component.mock_gw.rest_api_data.create.create_table import create_table
from tests.component.mock_gw.rest_api_data.show.show_catalogs import show_catalogs
from tests.component.mock_gw.rest_api_data.show.show_create_table import show_create_table
from tests.component.mock_gw.rest_api_data.show.show_current_catalog import show_current_catalog
from tests.component.mock_gw.rest_api_data.show.show_current_database import show_current_database
from tests.component.mock_gw.rest_api_data.show.show_databases import show_databases
from tests.component.mock_gw.rest_api_data.show.show_tables import show_tables
from tests.component.mock_gw.rest_api_data.show.show_views import show_views
from tests.component.mock_gw.rest_api_data.drop.drop_table import drop_table
from tests.component.mock_gw.rest_api_data.use.use_catalog import use_catalog
from tests.component.mock_gw.rest_api_data.set_kv.set_kv import set_kv


class MockDataProvider:

    @staticmethod
    def provide(_type, session, operation, extra):
        if _type == "create_table" or _type == "create_view" or _type == "create_catalog" or _type == "create_database":
            return MockDataProvider._replace_list(create_table, session, operation)

        if _type == "use_catalog" or _type == "use_database" or _type == "use_catalog_db":
            return MockDataProvider._replace_list(use_catalog, session, operation)

        if _type == "set":
            return MockDataProvider._replace_list(set_kv, session, operation)

        if _type == "drop_table" or _type == "drop_view":
            return MockDataProvider._replace_list(drop_table, session, operation)

        if _type == "show_catalogs":
            r = MockDataProvider._replace_list(show_catalogs, session, operation)
            return MockDataProvider._add_data_node(r, kind="INSERT", fields=extra)

        if _type == "show_create_table":
            r = MockDataProvider._replace_list(show_create_table, session, operation)
            return MockDataProvider._add_data_node(r, kind="INSERT", fields=extra)

        if _type == "show_current_catalog":
            r = MockDataProvider._replace_list(show_current_catalog, session, operation)
            return MockDataProvider._add_data_node(r, kind="INSERT", fields=extra)

        if _type == "show_current_database":
            r = MockDataProvider._replace_list(show_current_database, session, operation)
            return MockDataProvider._add_data_node(r, kind="INSERT", fields=extra)

        if _type == "show_databases":
            r = MockDataProvider._replace_list(show_databases, session, operation)
            return MockDataProvider._add_data_node(r, kind="INSERT", fields=extra)

        if _type == "show_tables":
            r = MockDataProvider._replace_list(show_tables, session, operation)
            return MockDataProvider._add_data_node(r, kind="INSERT", fields=extra)

        if _type == "show_views":
            r = MockDataProvider._replace_list(show_views, session, operation)
            return MockDataProvider._add_data_node(r, kind="INSERT", fields=extra)

        raise RuntimeError(f"{_type} not support")

    @staticmethod
    def _replace_list(res_list, session, operation, pattern=None, chars=None):
        r = [x.replace("_session", session) for x in res_list]
        r = [x.replace("_operation", operation) for x in r]
        return [x.replace(pattern, chars) for x in r] if pattern else r

    @staticmethod
    def _add_data_node(res_list, kind, fields):
        # fields: str | List[Any]
        r = []
        _data = []
        if isinstance(fields, str):
            _data.append({"kind": kind, "fields": [fields]})
        else:
            for f in fields:
                _data.append({"kind": kind, "fields": [str(f)]})

        for i in res_list:
            j = json.loads(i)
            result_type = j["resultType"]
            results = j["results"]
            if result_type == 'PAYLOAD':
                results["data"] = _data
            elif result_type == "EOS":
                results["data"] = []
            else:
                raise RuntimeError("NOT support")
            r.append(json.dumps(j))
        return r
