from typing import List


class DummyGwHandle:
    """
    simply record all received sql, do nothing
    """
    session_prefix = "session_"
    operation_prefix = "operation_"

    def __init__(self, config):
        self.config = config
        self.session_cnt = 0
        self.operation_cnt = 0
        self.session_handles = set()
        self.operation_handles = {}
        self.statements: List[str] = []

    def get_backend(self):
        return None

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

    def session_create(self):
        session_handle = self.next_session_handle()
        self.session_handles.add(session_handle)
        return {"sessionHandle": session_handle}

    def session_delete(self, session_handle):
        raise RuntimeError("not_support")

    def statement_create(self, session_handle, request_body):
        statement = request_body["statement"].strip()
        operation_handle = self.next_operation_handle()
        self.operation_handles[operation_handle] = "TODO: an object to keep context info"
        self.statements.append(statement)
        return {"operationHandle": operation_handle}

    def operation_status(self, session_handle, operation_handle):
        """always return FINISHED"""
        return {"status": "FINISHED"}

    def operation_cancel(self, session_handle, operation_handle):
        raise RuntimeError("not_support")

    def operation_close(self, session_handle, operation_handle):
        raise RuntimeError("not_support")

    def operation_result(self, session_handle, operation_handle, token):
        raise RuntimeError("not_support")
