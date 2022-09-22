import json
from time import sleep
from typing import Sequence, Tuple, Optional, Any, List

from dbt.events import AdapterLogger

from flink.sqlgateway.operation import SqlGatewayOperation
from flink.sqlgateway.result_parser import SqlGatewayResult
from flink.sqlgateway.session import SqlGatewaySession
from flink.sqlgateway.client import FlinkSqlGatewayClient

logger = AdapterLogger("Flink")

class FlinkCursor:
    session: SqlGatewaySession
    last_operation: SqlGatewayOperation = None
    last_result: SqlGatewayResult = None

    def __init__(self, session):
        self.session = session

    def cancel(self) -> None:
        pass

    def close(self) -> None:
        pass

    def fetchall(self) -> Sequence[Tuple]:
        # TODO I think we need to wait until last_operation is finished before fetching the result otherwise first pages will not have any result
        result_list: List[Tuple] = []

        logger.info(f"Fetching results... {self.last_operation.statement_endpoint_url()}/result/0")
        last_result = self.last_operation.get_result(next_page=None)
        while last_result is not None and last_result.next_result_url is not None:
            logger.info(f"Fetchall returned: {len(last_result.rows)} rows")
            for record in last_result.rows:
                result_list.append(tuple(record.values()))
            last_result = self.last_operation.get_result(next_page=last_result.next_result_url)

        logger.info(f"Fetchall returned: {result_list}")
        return result_list

    def fetchone(self) -> Optional[Tuple]:
        return None

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> None:
        logger.info('Executing statement "{}"'.format(sql))
        operation_handle = FlinkSqlGatewayClient.execute_statement(self.session, sql)
        status = self.wait_till_finished(operation_handle)
        logger.info("Statement executed. Status {}, operation handle: {}"
                    .format(status, operation_handle.operation_handle))
        if status == 'ERROR':
            raise Exception('Statement execution failed')

        self.last_operation = operation_handle

    @property
    def description(self) -> Tuple[Tuple[str]]:
        # FIXME This method is called by DBT before fetchall - so we dont know the columns yet :(
        if self.last_result is not None:
            return self.last_result.column_names
        # This must return column names so this can read it dbt/adapters/sql/connections.py:113
        return (())

    @staticmethod
    def wait_till_finished(operation_handle: SqlGatewayOperation) -> str:
        status = operation_handle.get_status()
        while status == 'RUNNING':
            sleep(0.1)
            status = operation_handle.get_status()
        return status

    def get_status(self) -> str:
        if self.last_operation is not None:
            return self.last_operation.get_status()
        return "UNKNOWN"


class FlinkHandler:
    session: SqlGatewaySession

    def __init__(self, session: SqlGatewaySession):
        self.session = session

    def cursor(self) -> FlinkCursor:
        return FlinkCursor(self.session)
