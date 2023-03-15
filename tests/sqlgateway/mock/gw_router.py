import re

import httpretty
import json

from tests.sqlgateway.mock.gw_handler import DummyGwHandle

mock_session_info = {
    "properties": {
        "state.checkpoints.num-retained": "5",
        "sql-gateway.worker.threads.max": "10",
        "jobmanager.execution.failover-strategy": "region",
        "other_infos": "you_can_get /session/:session_handle"
    }
}


class GwRouter:
    """ simple route use httpretty """

    def __init__(self, config):
        self.config = config
        self.handle = DummyGwHandle(config)
        # url
        v1_endpoint = f"{config.get('host_port')}/v1"
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

    def start(self):
        httpretty.enable()
        # DO NOT change register order, httpretty regex match = first match
        httpretty.register_uri('GET', self.url_api_version, body=json.dumps({"versions": ["V1"]}))
        httpretty.register_uri('GET', self.url_info,
                               body=json.dumps({"productName": "Apache Flink", "version": "1.16.1"}))

        # operation
        httpretty.register_uri('GET', self.url_op_status, body=self.handle.operation_status)
        httpretty.register_uri('POST', self.url_op_cancel, body=self.handle.operation_cancel)
        httpretty.register_uri('DELETE', self.url_op_close, body=self.handle.operation_close)
        httpretty.register_uri('GET', self.url_op_result, body=self.handle.operation_result)
        httpretty.register_uri('POST', self.url_op_create, body=self.handle.statement_create)

        # session
        httpretty.register_uri('POST', self.url_s_create, body=self.handle.session_create)
        httpretty.register_uri('GET', self.url_s_get, body=json.dumps(mock_session_info))
        httpretty.register_uri('DELETE', self.url_s_delete, body=self.handle.session_delete)
        httpretty.register_uri('POST', self.url_s_hb, body=json.dumps({}))

    def all_statements(self):
        return self.handle.all_statements()

    def clear_statements(self):
        return self.handle.clear_statements()

    def stop(self):
        httpretty.disable()
        httpretty.reset()
