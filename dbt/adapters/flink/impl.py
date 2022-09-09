from dbt.adapters.base import BaseAdapter as adapter_cls

from dbt.adapters.flink import FlinkConnectionManager


class FlinkAdapter(adapter_cls):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = FlinkConnectionManager

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"


# may require more build out to make more user friendly to confer with team and community.
