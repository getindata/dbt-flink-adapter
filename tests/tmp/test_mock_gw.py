import json
import unittest

import requests

from tests.sqlgateway.mock.gw_router import GwRouter

test_config = {
    "host_port": "http://127.0.0.1:8083",
    "schemas": [
        {"catalog": "default_catalog", "database": "default_database", "tables": ["a"], "views": []},
        {"catalog": "default_catalog", "database": "default_database", "tables": [], "views": ["b"]},
        {"catalog": "my_hive", "database": "flink01", "tables": ["t01", "t02", "v01", "v02"], "views": ["v01", "v02"]},
        {"catalog": "hive_catalog", "database": "flink04", "tables": ["t11", "t12", ], "views": []},
        # {"catalog": "", "database": "", "tables": [], "views": []},
    ],
    "current_catalog": "my_hive",
    "current_database": "flink01",
}


class TestTmp(unittest.TestCase):
    def test1(self):
        router = GwRouter(test_config)
        v1_ep = f"{test_config.get('host_port')}/v1"
        router.start()

        print(f"======= get {v1_ep}/api_version")
        r = requests.get(f"{v1_ep}/api_version")
        # print(r.status_code)
        self.assertEqual(200, r.status_code)
        print(r.text)

        # session 测试 =========
        # session 测试 =========
        # session 测试 =========
        print(f"======= post {v1_ep}/sessions")
        r = requests.post(f"{v1_ep}/sessions", '{"sessionName" : "hehe"}')
        # print(r.status_code)
        print(r.text)
        self.assertEqual(200, r.status_code)
        session_handle = r.json()['sessionHandle']

        print(f"======= get {v1_ep}/sessions/{session_handle}")
        r = requests.get(f"{v1_ep}/sessions/{session_handle}")
        # print(r.status_code)
        self.assertEqual(200, r.status_code)
        print(r.text)

        print(f"======= post {v1_ep}/sessions/{session_handle}/statements")
        data = {"statement": "select 1 as id", "executionTimeout": 100}
        # r = requests.post(url=f"{v1_ep}/sessions/{session_handle}/statements", data=json.dumps(data))
        r = requests.post(
            url=f"{v1_ep}/sessions/{session_handle}/statements",
            data=json.dumps(data),
            headers={
                "Content-Type": "application/json",
            },
        )
        # print(r.status_code)
        print(r.text)
        operation_handle = r.json()['operationHandle']
        print(router.all_statements())

        router.stop()
