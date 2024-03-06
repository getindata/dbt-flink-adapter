from datetime import datetime
from time import sleep
from typing import Dict, Sequence, Tuple, Optional, Any, List

from dbt.events import AdapterLogger

from dbt.adapters.flink.constants import ExecutionConfig
from dbt.adapters.flink.query_hints_parser import (
    QueryHints,
    QueryHintsParser,
    QueryMode,
)

from flink.sqlgateway.client import FlinkSqlGatewayClient
from flink.sqlgateway.operation import SqlGatewayOperation
from flink.sqlgateway.result_parser import SqlGatewayResult
from flink.sqlgateway.session import SqlGatewaySession

logger = AdapterLogger("Flink")


class FlinkCursor:
    session: SqlGatewaySession
    last_operation: Optional[SqlGatewayOperation] = None
    result_buffer: List[Tuple]
    buffered_results_counter: int = 0
    last_result: Optional[SqlGatewayResult] = None
    last_query_hints: QueryHints = QueryHints()
    last_query_start_time: Optional[float] = None

    def __init__(self, session):
        logger.info("Creating new cursor for session {}".format(session))
        self.session = session
        self.result_buffer = []

    def cancel(self) -> None:
        pass

    def close(self) -> None:
        pass

    def fetchall(self) -> Sequence[Tuple]:
        if self.last_result is None:
            self._buffer_results()

        if self.last_result is None:
            raise Exception("No result after fetch")

        while (
            not self.last_result.is_end_of_stream
            and not self._buffered_fetch_max()
            and not self._exceeded_timeout()
        ):
            sleep(0.1)
            self._buffer_results()

        result = self.result_buffer
        logger.info("Fetched results from Flink: {}".format(result))
        if self.last_query_hints.test_query is True:
            result = self._handle_test_query(result)
        logger.info("Returned results from adapter: {}".format(result))
        self._close()
        self._clean()
        return result

    def _handle_test_query(self, result: List[Tuple]) -> List[Tuple]:
        if self.last_query_hints.mode == QueryMode.STREAMING:
            if len(result) > 0:
                return [result[-1]]
            else:
                return [(0, False, False)]
        return result

    def _close(self):
        if self.last_query_hints.mode == QueryMode.STREAMING:
            self.last_operation.close()

    def _buffered_fetch_max(self):
        return (
            self.last_query_hints.fetch_max is not None
            and self.buffered_results_counter >= self.last_query_hints.fetch_max
        )

    def _exceeded_timeout(self):
        return (
            self.last_query_hints.fetch_timeout_ms is not None
            and self._get_current_timestamp()
            > self.last_query_start_time + self.last_query_hints.fetch_timeout_ms / 1000
        )

    def fetchone(self) -> Optional[Tuple]:
        if len(self.result_buffer) == 0:
            if self.last_result is None or not self.last_result.is_end_of_stream:
                self._buffer_results()
                if len(self.result_buffer) > 0:
                    return self.result_buffer.pop(0)
        self._clean()
        return None

    def execute(self, sql: str, bindings: Optional[Sequence[Any]] = None) -> None:
        logger.debug('Preparing statement "{}"'.format(sql))
        if bindings is not None:
            sql = sql.format(*[self._convert_binding(binding) for binding in bindings])
        self.last_query_hints: QueryHints = QueryHintsParser.parse(sql)
        execution_config = self.last_query_hints.execution_config
        start_from_savepoint = False
        if execution_config:
            if not self.last_query_hints.test_query:
                with_savepoint = self.last_query_hints.upgrade_mode == "savepoint"
                savepoint_path = FlinkJobManager(self.session).stop_job(
                    execution_config, with_savepoint
                )
                if savepoint_path:
                    logger.debug("Savepoint path {}", savepoint_path)
                    execution_config[ExecutionConfig.SAVEPOINT_PATH] = savepoint_path
                    start_from_savepoint = True
            if not start_from_savepoint:
                logger.debug("Job starting without savepoint")
                execution_config.pop(ExecutionConfig.SAVEPOINT_PATH, None)

        if self.last_query_hints.drop_statement:
            logger.debug("Executing drop statement: {}", self.last_query_hints.drop_statement)
            FlinkCursor(self.session).execute(self.last_query_hints.drop_statement)

        self._set_query_mode()
        logger.info("Executing statement:\n{}\nExecution config:\n{}", sql, execution_config)
        operation_handle = FlinkSqlGatewayClient.execute_statement(
            self.session, sql, execution_config
        )
        status = self._wait_till_finished(operation_handle)
        logger.info(
            "Statement executed. Status {}, operation handle: {}",
            status,
            operation_handle.operation_handle,
        )
        if status == "ERROR":
            operation_handle.get_result()  # throw exception
        self.last_query_start_time = self._get_current_timestamp()
        self.last_operation = operation_handle

    def _convert_binding(self, binding):
        if isinstance(binding, str):
            return "'{}'".format(binding)
        if isinstance(binding, datetime):
            return "TIMESTAMP '{}'".format(binding)
        return binding

    @property
    def description(self) -> Tuple[Tuple[str], ...]:
        if self.last_result is None:
            self._buffer_results()
        result = []

        if self.last_result is None:
            raise Exception("No result after fetch")

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
        logger.debug(f"Buffered: {len(result.rows)} rows")
        self.last_result = result

    @staticmethod
    def _wait_till_finished(operation_handle: SqlGatewayOperation) -> str:
        status = operation_handle.get_status()
        while status == "RUNNING":
            sleep(0.1)
            status = operation_handle.get_status()
        return status

    def _set_query_mode(self):
        runtime_mode = "batch"
        if self.last_query_hints.mode is not None:
            runtime_mode = self.last_query_hints.mode.value
        logger.info("Setting 'execution.runtime-mode' to '{}'".format(runtime_mode))
        FlinkSqlGatewayClient.execute_statement(
            self.session, "SET 'execution.runtime-mode' = '{}'".format(runtime_mode)
        )

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


class FlinkJobManager:
    def __init__(self, session: SqlGatewaySession):
        self.session = session

    def stop_job(
        self, execution_config: Dict[str, str], with_savepoint: bool = True
    ) -> Optional[str]:
        if ExecutionConfig.JOB_NAME not in execution_config:
            return None
        job_name = execution_config[ExecutionConfig.JOB_NAME]
        logger.debug("Getting job by name {}", job_name)
        job_id = self._get_job_id(job_name)
        if job_id:
            state_path = execution_config.get(ExecutionConfig.STATE_PATH)
            logger.debug("Stopping job {}, state path {}", job_id, state_path)
            path = self._do_stop_job(job_id, with_savepoint, state_path)
            logger.info("Job stopped {}", job_id)
            return path
        return None

    def _do_stop_job(
        self, job_id: str, with_savepoint: bool, path: Optional[str] = None
    ) -> Optional[str]:
        cursor = FlinkCursor(self.session)
        hints = f"/** execution_config('{ExecutionConfig.STATE_PATH}={path}') */ " if path else ""
        savepoint_statement = " WITH SAVEPOINT" if with_savepoint else ""
        cursor.execute(f"{hints} STOP JOB '{job_id}'{savepoint_statement}")
        for result in cursor.fetchall():
            logger.debug("STOP JOB RESULT: {}", result)
            if with_savepoint:
                return result[0]
        return None

    def _get_job_id(self, job_name: str) -> Optional[str]:
        cursor = FlinkCursor(self.session)
        cursor.execute("SHOW JOBS")
        for job in cursor.fetchall():
            if job_name == job[1]:
                if job[2] not in ("FAILED", "FINISHED", "CANCELED"):
                    return job[0]
        return None
