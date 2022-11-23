from dbt.adapters.flink.connections import FlinkConnectionManager  # noqa
from dbt.adapters.flink.connections import FlinkCredentials
from dbt.adapters.flink.impl import FlinkAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import flink


Plugin = AdapterPlugin(
    adapter=FlinkAdapter, credentials=FlinkCredentials, include_path=flink.PACKAGE_PATH  # type: ignore
)
