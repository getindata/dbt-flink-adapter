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
    config = None
    handle: DummyGwHandle

    url_api_version: str
    url_info: str
    url_op_status: str
    url_op_cancel: str
    url_op_close: str
    url_op_result: str
    url_op_create: str
    url_s_create: str
    url_s_get: str
    url_s_delete: str
    url_s_hb: str

    def __init__(self, config):
        GwRouter.config = config
        GwRouter.handle = DummyGwHandle(config)
        # url
        v1_endpoint = f"{config.get('host_port')}/v1"
        GwRouter.url_api_version = f"{v1_endpoint}/api_version"
        GwRouter.url_info = f"{v1_endpoint}/info"
        # operation
        GwRouter.url_op_status = re.compile(f"{v1_endpoint}/sessions/\\w*/operations/\\w*/status")
        GwRouter.url_op_cancel = re.compile(f"{v1_endpoint}/sessions/\\w*/operations/\\w*/cancel")
        GwRouter.url_op_close = re.compile(f"{v1_endpoint}/sessions/\\w*/operations/\\w*/close")
        GwRouter.url_op_result = re.compile(f"{v1_endpoint}/sessions/\\w*/operations/\\w*/result/\\w*")
        GwRouter.url_op_create = re.compile(f"{v1_endpoint}/sessions/\\w*/statements")
        # session
        GwRouter.url_s_create = f"{v1_endpoint}/sessions"
        GwRouter.url_s_get = re.compile(f"{v1_endpoint}/sessions/\\w*")
        GwRouter.url_s_delete = re.compile(f"{v1_endpoint}/sessions/\\w*")
        GwRouter.url_s_hb = re.compile(f"{v1_endpoint}/sessions/\\w*/heartbeat")

    @staticmethod
    def start():
        httpretty.enable()
        # DO NOT change register order, httpretty regex match = first match
        # DO NOT change register order, httpretty regex match = first match
        # DO NOT change register order, httpretty regex match = first match
        httpretty.register_uri('GET', GwRouter.url_api_version, body=json.dumps({"versions": ["V1"]}))
        httpretty.register_uri('GET', GwRouter.url_info,
                               body=json.dumps({"productName": "Apache Flink", "version": "1.16.1"}))

        # operation
        httpretty.register_uri('GET', GwRouter.url_op_status, body=GwRouter.handle.operation_status)
        httpretty.register_uri('POST', GwRouter.url_op_cancel, body=GwRouter.handle.operation_cancel)
        httpretty.register_uri('DELETE', GwRouter.url_op_close, body=GwRouter.handle.operation_close)
        httpretty.register_uri('GET', GwRouter.url_op_result, body=GwRouter.handle.operation_result)
        httpretty.register_uri('POST', GwRouter.url_op_create, body=GwRouter.handle.statement_create)

        # session
        httpretty.register_uri('POST', GwRouter.url_s_create, body=GwRouter.handle.session_create)
        httpretty.register_uri('GET', GwRouter.url_s_get, body=json.dumps(mock_session_info))
        httpretty.register_uri('DELETE', GwRouter.url_s_delete, body=GwRouter.handle.session_delete)
        httpretty.register_uri('POST', GwRouter.url_s_hb, body=json.dumps({}))

    @staticmethod
    def all_statements():
        return GwRouter.handle.all_statements()

    @staticmethod
    def clear_statements():
        return GwRouter.handle.clear_statements()

    @staticmethod
    def stop():
        httpretty.disable()
        httpretty.reset()
