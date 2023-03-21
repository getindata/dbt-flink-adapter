import json
from typing import List

from tests.component.mock_gw.gw_backend import GwBackend
from tests.component.mock_gw.gw_handle_dummy import DummyGwHandle
from tests.component.mock_gw.rest_api_data.data_provider import MockDataProvider


class OperationInfo:
    def __init__(self, session_handle, operation_handle, execution_timeout, statement):
        self.session_handle = session_handle
        self.operation_handle = operation_handle
        self.execution_timeout = execution_timeout
        self.statement = statement
        #
        self.status = None
        self.result: List[str] = []
        self.result_index = 0
        self.error = None

    def next_result(self):
        if self.error:
            raise self.error
        if len(self.result) == 0:
            return None
        if self.result_index > len(self.result):
            self.result_index = len(self.result) - 1
        ret_val = self.result[self.result_index]
        self.result_index += 1
        return ret_val


class GwHandlePlus(DummyGwHandle):
    """
    for this handle
    every status -> "OK" / "FINISHED" as soon as possible (statements are eager handled)
    """

    def __init__(self, config):
        DummyGwHandle.__init__(self, config)
        self.backend = GwBackend(config)

    def get_backend(self):
        return self.backend

    def session_delete(self, session_handle):
        self.session_handles.remove(session_handle)
        # TODO: if this session has operation running, should not delete
        # delete session, also delete related operations
        keep = {}
        for op_handle, op_info in self.operation_handles.items():
            if op_info.session_handle != session_handle:
                keep[op_handle] = op_info
        self.operation_handles = keep
        #
        return {"status": "CLOSED"}

    def statement_create(self, session_handle, request_body):
        operation_handle = self.next_operation_handle()
        # save status
        statement = request_body["statement"].strip()
        execution_timeout = request_body.get("executionTimeout")
        op_info = OperationInfo(session_handle, operation_handle, execution_timeout, statement)
        self.operation_handles[operation_handle] = op_info

        # execute right now
        try:
            res, _type = self.backend.execute_statement(statement)
            GwHandlePlus.fill_mock_result(op_info, res, _type)
        except Exception as e:
            print(e)
            op_info.error = e
        #
        return {"operationHandle": operation_handle}

    def all_statements(self) -> List[str]:
        return [op.statement for op in list(self.operation_handles.values())]

    def clear_statements(self):
        self.operation_handles = {}

    def operation_status(self, session_handle, operation_handle):
        # get current status
        status = self.operation_handles[operation_handle].status
        return {"status": status}

    def operation_cancel(self, session_handle, operation_handle):
        return {"status": "OK"}

    def operation_close(self, session_handle, operation_handle):
        del self.operation_handles[operation_handle]
        return {"status": "OK"}

    def operation_result(self, session_handle, operation_handle, token):
        op_info: OperationInfo = self.operation_handles[operation_handle]
        str_result = op_info.next_result()
        return json.loads(str_result)

    @staticmethod
    def fill_mock_result(op_info, result, _type):
        op_info.result = MockDataProvider.provide(_type, op_info.session_handle, op_info.operation_handle, result)
