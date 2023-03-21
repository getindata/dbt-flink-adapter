import re

import httpretty
import json

from tests.component.mock_gw.gw_handle_dummy import DummyGwHandle
from tests.component.mock_gw.gw_handle_plus import GwHandlePlus

mock_session_info = {
    "properties": {
        "state.checkpoints.num-retained": "5",
        "sql-gateway.worker.threads.max": "10",
        "jobmanager.execution.failover-strategy": "region",
        "other_infos": "you_can_get /session/:session_handle"
    }
}

many_catalog_config = {
    "host": "127.0.0.1",
    "port": "8083",
    "schemas": [
        # catalog | databases | tables | views pre-defined will be load as metadata, these table cannot be selected
        # only works in "show tables", for example: if you run
        #    "select * from cat1.cat1_db1.t111"  ===> turns not no such table
        #
        # tables you manual create can insert and select
        {"catalog": "default_catalog", "database": "default_database", "tables": ["default_table"], "views": []},
        #
        {"catalog": "cat1", "database": "cat1_db1", "tables": ["t111", "t112"], "views": ["v111", "v112"]},
        {"catalog": "cat1", "database": "cat1_db2", "tables": ["t121", "t122"], "views": ["v121", "v122"]},
        #
        {"catalog": "cat2", "database": "cat2_db1", "tables": ["t211", "t212", ], "views": []},
    ],
    "current_catalog": "default_catalog",
    "current_database": "default_database",
}

default_config = {
    "host": "127.0.0.1",
    "port": "8083",
    "schemas": [
        {"catalog": "default_catalog", "database": "default_database", "tables": [], "views": []},
    ],
    "current_catalog": "default_catalog",
    "current_database": "default_database",
}


class MockSqlGateway:
    """ simple route use httpretty """

    @staticmethod
    def use_default_config():
        return MockSqlGateway(default_config)

    @staticmethod
    def use_many_catalog_config():
        return MockSqlGateway(many_catalog_config)

    def __init__(self, config):
        print("============ gateway config ==========")
        print(json.dumps(
            config,
            indent=4,
            separators=(',', ': ')
        ))
        print("====================================")
        self.config = config
        # self.handle = DummyGwHandle(config)
        self.handle = GwHandlePlus(config)
        self._setup_router()

    def get_handle(self):
        return self.handle

    def get_v1_endpoint(self):
        return f"http://{self.config.get('host')}:{self.config.get('port')}/v1"

    def _setup_router(self):
        v1_endpoint = self.get_v1_endpoint()
        self.url_api_version = f"{v1_endpoint}/api_version"
        self.url_info = f"{v1_endpoint}/info"
        # operation
        self.url_op_status = re.compile(f"{v1_endpoint}/sessions/\\w*/operations/\\w*/status")
        self.url_op_cancel = re.compile(f"{v1_endpoint}/sessions/\\w*/operations/\\w*/cancel")
        self.url_op_close = re.compile(f"{v1_endpoint}/sessions/\\w*/operations/\\w*/close")
        self.url_op_result = re.compile(f"{v1_endpoint}/sessions/\\w*/operations/\\w*/result/\\w*")
        self.url_op_create = re.compile(f"{v1_endpoint}/sessions/\\w*/statements")
        # session
        self.url_s_create = f"{v1_endpoint}/sessions"
        self.url_s_get = re.compile(f"{v1_endpoint}/sessions/\\w*")
        self.url_s_delete = re.compile(f"{v1_endpoint}/sessions/\\w*")
        self.url_s_hb = re.compile(f"{v1_endpoint}/sessions/\\w*/heartbeat")

        # start router
        httpretty.enable()
        # DO NOT change register order, httpretty regex match = first match
        httpretty.register_uri('GET', self.url_api_version, body=json.dumps({"versions": ["V1"]}))
        httpretty.register_uri('GET', self.url_info,
                               body=json.dumps({"productName": "Apache Flink", "version": "1.16.1"}))

        # operation
        httpretty.register_uri('GET', self.url_op_status, body=self.operation_status)
        httpretty.register_uri('POST', self.url_op_cancel, body=self.operation_cancel)
        httpretty.register_uri('DELETE', self.url_op_close, body=self.operation_close)
        httpretty.register_uri('GET', self.url_op_result, body=self.operation_result)
        httpretty.register_uri('POST', self.url_op_create, body=self.statement_create)

        # session
        httpretty.register_uri('POST', self.url_s_create, body=self.session_create)
        httpretty.register_uri('GET', self.url_s_get, body=json.dumps(mock_session_info))
        httpretty.register_uri('DELETE', self.url_s_delete, body=self.session_delete)
        httpretty.register_uri('POST', self.url_s_hb, body=json.dumps({}))

    def session_create(self, request, uri, response_headers):
        body = self.handle.session_create()
        return [200, response_headers, json.dumps(body)]

    def session_delete(self, request, uri, response_headers):
        match = re.search("/sessions/(.*)", request.path)
        session_handle = match.group(1)
        body = self.handle.session_delete(session_handle)
        return [200, response_headers, json.dumps(body)]

    def statement_create(self, request, uri, response_headers):
        match = re.search("/sessions/(.*)/statements", request.path)
        session_handle = match.group(1)
        request_body = json.loads(request.body)
        body = self.handle.statement_create(session_handle, request_body)
        return [200, response_headers, json.dumps(body)]

    def operation_status(self, request, uri, response_headers):
        match = re.search("/sessions/(.*)/operations/(.*)/status", request.path)
        session_handle = match.group(1)
        operation_handle = match.group(2)
        body = self.handle.operation_status(session_handle, operation_handle)
        return [200, response_headers, json.dumps(body)]

    def operation_cancel(self, request, uri, response_headers):
        match = re.search("/sessions/(.*)/operations/(.*)/cancel", request.path)
        session_handle = match.group(1)
        operation_handle = match.group(2)
        body = self.handle.operation_cancel(session_handle, operation_handle)
        return [200, response_headers, json.dumps(body)]

    def operation_close(self, request, uri, response_headers):
        match = re.search("/sessions/(.*)/operations/(.*)/close", request.path)
        session_handle = match.group(1)
        operation_handle = match.group(2)
        body = self.handle.operation_close(session_handle, operation_handle)
        return [200, response_headers, json.dumps(body)]

    def operation_result(self, request, uri, response_headers):
        match = re.search("/sessions/(.*)/operations/(.*)/result/(.*)", request.path)
        session_handle = match.group(1)
        operation_handle = match.group(2)
        token = match.group(3)
        body = self.handle.operation_result(session_handle, operation_handle, token)
        return [200, response_headers, json.dumps(body)]

    def all_statements(self):
        return self.handle.all_statements()

    def clear_statements(self):
        return self.handle.clear_statements()

    @staticmethod
    def stop():
        httpretty.disable()
        httpretty.reset()
