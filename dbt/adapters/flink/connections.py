from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Any, Tuple

import dbt.exceptions  # noqa
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager  # type: ignore
from dbt.contracts.connection import Connection
from dbt.events import AdapterLogger

from dbt.adapters.flink.handler import FlinkHandler, FlinkCursor
from flink.sqlgateway.client import FlinkSqlGatewayClient
from flink.sqlgateway.session import SqlGatewaySession

logger = AdapterLogger("Flink")


@dataclass
class FlinkCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    host: str
    port: int
    session_name: str

    _ALIASES = {"session": "session_name"}

    @property
    def type(self):
        """Return name of adapter."""
        return "flink"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.host

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return "host", "port", "session_name"


class FlinkConnectionManager(SQLConnectionManager):
    TYPE = "flink"

    session: SqlGatewaySession

    @contextmanager
    def exception_handler(self, sql: str):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        try:
            yield
        except Exception as e:
            logger.error("Exception thrown during execution: {}".format(str(e)))
            raise dbt.exceptions.RuntimeException(str(e))

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials: FlinkCredentials = connection.credentials
        try:
            session: SqlGatewaySession = FlinkSqlGatewayClient.create_session(
                host=credentials.host,
                port=credentials.port,
                session_name=credentials.session_name,
            )

            connection.state = "open"
            connection.handle = FlinkHandler(session)

            logger.info(f"Session created: {session.session_handle}")
        except Exception as e:
            logger.error("Error during creating session {}".format(str(e)))
            raise e

        return connection

    @classmethod
    def get_response(cls, cursor: FlinkCursor):
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse ojbect
        that has items such as code, rows_affected,etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        return cursor.get_status()

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        # ## Example ##
        # tid = connection.handle.transaction_id()
        # sql = "select cancel_transaction({})".format(tid)
        # logger.debug("Cancelling query "{}" ({})".format(connection_name, pid))
        # _, cursor = self.add_query(sql, "master")
        # res = cursor.fetchone()
        # logger.debug("Canceled query "{}": {}".format(connection_name, res))
        pass

    # supress adding BEGIN and COMMIT as Flink does not handle transactions
    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass
