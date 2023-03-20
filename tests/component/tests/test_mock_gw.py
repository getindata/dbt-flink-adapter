import json
import unittest

import requests

from tests.component.mock_gw.mock_gw import MockSqlGateway


class TestTmp(unittest.TestCase):
    def test1(self):
        gw = MockSqlGateway.use_default_config()
        v1_ep = gw.get_v1_endpoint()

        # ========= test basic api =========

        # print(f"======= get {v1_ep}/api_version")
        r = requests.get(f"{v1_ep}/api_version")
        self.assertEqual(200, r.status_code)
        # print(r.text)

        # ========= test session api =========
        # print(f"======= post {v1_ep}/sessions")
        r = requests.post(f"{v1_ep}/sessions", '{"sessionName" : "hehe"}')
        # print(r.text)
        self.assertEqual(200, r.status_code)
        session_handle = r.json()['sessionHandle']

        # print(f"======= get {v1_ep}/sessions/{session_handle}")
        r = requests.get(f"{v1_ep}/sessions/{session_handle}")
        self.assertEqual(200, r.status_code)
        # print(r.text)

        # print(f"======= post {v1_ep}/sessions/{session_handle}/statements")
        data = {"statement": "select 1 as id", "executionTimeout": 100}
        # r = requests.post(url=f"{v1_ep}/sessions/{session_handle}/statements", data=json.dumps(data))
        r = requests.post(
            url=f"{v1_ep}/sessions/{session_handle}/statements",
            data=json.dumps(data),
            headers={
                "Content-Type": "application/json",
            },
        )
        # print(r.text)
        # operation_handle = r.json()['operationHandle']
        # print(gw.all_statements())
        self.assertEqual(200, r.status_code)

        gw.stop()
