import json
from typing import List


class DummyGwHandle:
    session_prefix = "session_"
    operation_prefix = "operation_"

    def __init__(self, config):
        self.config = config
        self.session_cnt = 0
        self.operation_cnt = 0
        self.session_handles = set()
        self.operation_handles = {}
        self.statements: List[str] = []

    def next_session_handle(self):
        self.session_cnt += 1
        return f"{DummyGwHandle.session_prefix}{self.session_cnt}"

    def next_operation_handle(self):
        self.operation_cnt += 1
        return f"{DummyGwHandle.operation_prefix}{self.operation_cnt}"

    def all_statements(self) -> List[str]:
        return self.statements

    def clear_statements(self):
        self.statements = []

    def start(self):
        pass

    def stop(self):
        pass

    def session_create(self, request, uri, response_headers):
        session_handle = self.next_session_handle()
        self.session_handles.add(session_handle)
        body = {"sessionHandle": session_handle}
        return [200, response_headers, json.dumps(body)]

    def session_delete(self, request, uri, response_headers):
        raise RuntimeError("not_support")

    def statement_create(self, request, uri, response_headers):
        request_body = json.loads(request.body)
        statement = request_body["statement"].strip()
        operation_handle = self.next_operation_handle()
        self.operation_handles[operation_handle] = "TODO: an object to keep context info"
        self.statements.append(statement)
        body = {"operationHandle": operation_handle}
        return [200, response_headers, json.dumps(body)]

    def operation_status(self, request, uri, response_headers):
        """always return FINISHED"""
        # raise RuntimeError("not_support")
        body = {"status": "FINISHED"}
        return [200, response_headers, json.dumps(body)]

    def operation_cancel(self, request, uri, response_headers):
        raise RuntimeError("not_support")

    def operation_close(self, request, uri, response_headers):
        raise RuntimeError("not_support")

    def operation_result(self, request, uri, response_headers):
        raise RuntimeError("not_support")
