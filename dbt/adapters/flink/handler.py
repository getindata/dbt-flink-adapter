from datetime import datetime
from time import sleep
from typing import Sequence, Tuple, Optional, Any, List

from dbt.events import AdapterLogger

from dbt.adapters.flink.query_hints_parser import QueryHints, QueryHintsParser
from flink.sqlgateway.client import FlinkSqlGatewayClient
from flink.sqlgateway.operation import SqlGatewayOperation
from flink.sqlgateway.result_parser import SqlGatewayResult
from flink.sqlgateway.session import SqlGatewaySession

logger = AdapterLogger("Flink")


class FlinkCursor:
    session: SqlGatewaySession
    last_operation: SqlGatewayOperation = None
    result_buffer: List[Tuple]
    buffered_results_counter: int = 0
    last_result: SqlGatewayResult = None
    last_query_hints: QueryHints = None
    last_query_start_time: float = None

    def __init__(self, session):
        logger.info("Creating new cursor for session {}".format(session))
        self.session = session
        self.result_buffer = []
        self.execute("SET 'execution.runtime-mode' = 'batch'")

    def cancel(self) -> None:
        pass

    def close(self) -> None:
        pass

    def fetchall(self) -> Sequence[Tuple]:
        if self.last_result is None:
            self._buffer_results()

        while not self.last_result.is_end_of_stream and not self._buffered_fetch_max() and not self._exceeded_timeout():
            sleep(0.1)
            self._buffer_results()

        result = self.result_buffer
        logger.info("Fetched results: {}".format(result))
        self._close()
        self._clean()
        return result

    def _close(self):
        if self.last_query_hints.fetch_mode == "streaming":
            self.last_operation.close()

    def _buffered_fetch_max(self):
        return self.last_query_hints.fetch_max is not None \
               and self.buffered_results_counter >= self.last_query_hints.fetch_max

    def _exceeded_timeout(self):
        return self.last_query_hints.fetch_timeout_ms is not None \
               and self._get_current_timestamp() > self.last_query_start_time + self.last_query_hints.fetch_timeout_ms / 1000

    def fetchone(self) -> Optional[Tuple]:
        if len(self.result_buffer) == 0:
            if self.last_result is None or not self.last_result.is_end_of_stream:
                self._buffer_results()
                if len(self.result_buffer) > 0:
                    return self.result_buffer.pop(0)
        self._clean()
        return None

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> None:
        logger.info('Executing statement "{}"'.format(sql))
        self.last_query_hints = QueryHintsParser.parse(sql)
        self._set_query_mode()
        operation_handle = FlinkSqlGatewayClient.execute_statement(self.session, sql)
        status = self._wait_till_finished(operation_handle)
        logger.info("Statement executed. Status {}, operation handle: {}"
                    .format(status, operation_handle.operation_handle))
        if status == 'ERROR':
            raise Exception('Statement execution failed')

        self.last_query_start_time = self._get_current_timestamp()

        self.last_operation = operation_handle

    @property
    def description(self) -> Tuple[Tuple[str]]:
        if self.last_result is None:
            self._buffer_results()
        result = []
        for column_name in self.last_result.column_names:
            result.append((column_name,))
        return tuple(result)

    def _buffer_results(self):
        next_page = self.last_result.next_result_url if self.last_result is not None else None
        result = self.last_operation.get_result(next_page=next_page)
        for record in result.rows:
            self.buffered_results_counter += 1
            self.result_buffer.append(tuple(record.values()))
            if self._buffered_fetch_max():
                break
        logger.info(f"Buffered: {len(result.rows)} rows")
        self.last_result = result

    @staticmethod
    def _wait_till_finished(operation_handle: SqlGatewayOperation) -> str:
        status = operation_handle.get_status()
        while status == 'RUNNING':
            sleep(0.1)
            status = operation_handle.get_status()
        return status

    def _set_query_mode(self):
        if self.last_query_hints.fetch_mode is not None:
            FlinkSqlGatewayClient.execute_statement(self.session, "SET 'execution.runtime-mode' = '{}'".format(
                self.last_query_hints.fetch_mode))

    def get_status(self) -> str:
        if self.last_operation is not None:
            return self.last_operation.get_status()
        return "UNKNOWN"

    def _clean(self):
        self.result_buffer = []
        self.last_result = None
        self.last_operation = None
        self.buffered_results_counter = 0

    @staticmethod
    def _get_current_timestamp() -> float:
        return datetime.timestamp(datetime.utcnow())


class FlinkHandler:
    session: SqlGatewaySession

    def __init__(self, session: SqlGatewaySession):
        self.session = session

    def cursor(self) -> FlinkCursor:
        return FlinkCursor(self.session)
