import pytest

import os
import uuid

# import json

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target(request):
    # Isolate SQL Gateway sessions between test classes to avoid stale context reuse.
    class_suffix = getattr(request.node, "name", "class")
    session_name = f"test_session_{class_suffix}_{uuid.uuid4().hex[:8]}"
    return {
        "type": "flink",
        "threads": 1,
        "host": os.getenv("FLINK_SQL_GATEWAY_HOST", "127.0.0.1"),
        "port": int(os.getenv("FLINK_SQL_GATEWAY_PORT", "8083")),
        "session_name": os.getenv("SESSION_NAME", session_name),
        "database": os.getenv("DATABASE_NAME", "test_db"),
        "schema": os.getenv("SCHEMA_NAME", "test_db"),
    }
