from typing import List

import json


class DummyGwHandle:
    session_prefix = "session_"
    operation_prefix = "operation_"
    session_cnt = 1
    operation_cnt = 1
    session_handles = set()
    operation_handles = {}
    statements: List[str] = []

    @staticmethod
    def next_session_handle():
        return f"{DummyGwHandle.session_prefix}{DummyGwHandle.session_cnt}"

    @staticmethod
    def next_operation_handle():
        return f"{DummyGwHandle.operation_prefix}{DummyGwHandle.operation_cnt}"

    def __init__(self, config):
        pass

    @staticmethod
    def all_statements() -> List[str]:
        return DummyGwHandle.statements

    @staticmethod
    def clear_statements():
        DummyGwHandle.statements = []

    @staticmethod
    def start():
        pass

    @staticmethod
    def stop():
        pass

    @staticmethod
    def session_create(request, uri, response_headers):
        session_handle = DummyGwHandle.next_session_handle()
        DummyGwHandle.session_handles.add(session_handle)
        body = {"sessionHandle": session_handle}
        return [200, response_headers, json.dumps(body)]

    @staticmethod
    def session_delete(request, uri, response_headers):
        raise RuntimeError("not_support")

    @staticmethod
    def statement_create(request, uri, response_headers):
        request_body = json.loads(request.body)
        statement = request_body["statement"].strip()
        operation_handle = DummyGwHandle.next_operation_handle()
        DummyGwHandle.statements.append(statement)
        body = {"operationHandle": operation_handle}
        return [200, response_headers, json.dumps(body)]

    @staticmethod
    def operation_status(request, uri, response_headers):
        """always return FINISHED"""
        # raise RuntimeError("not_support")
        body = {"status": "FINISHED"}
        return [200, response_headers, json.dumps(body)]

    @staticmethod
    def operation_cancel(request, uri, response_headers):
        raise RuntimeError("not_support")

    @staticmethod
    def operation_close(request, uri, response_headers):
        raise RuntimeError("not_support")

    @staticmethod
    def operation_result(request, uri, response_headers):
        raise RuntimeError("not_support")
