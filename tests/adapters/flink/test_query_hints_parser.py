from dbt.adapters.flink.query_hints_parser import (
    QueryHintsParser,
    QueryMode,
    JobState,
    UpgradeMode,
)


class TestQueryHintsParser:
    def test_no_hints(self):
        sql = "select * from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.fetch_timeout_ms is None
        assert hints.fetch_max is None
        assert hints.mode is None
        assert hints.test_query is False

    def test_fetch_timeout(self):
        sql = "select /** fetch_timeout_ms(1000) */ from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.fetch_timeout_ms == 1000

    def test_fetch_max(self):
        sql = "select /** fetch_max(10) */ from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.fetch_max == 10

    def test_mode(self):
        sql = "select /** mode('streaming') */ from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.mode == QueryMode.STREAMING

    def test_test_query(self):
        sql = "select /** test_query('true') */ from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.test_query is True

    def test_execution_config(self):
        sql = "/** execution_config('key_a=value_a;key_b=value_b') */ select 1"
        hints = QueryHintsParser.parse(sql)
        assert hints.execution_config == {"key_a": "value_a", "key_b": "value_b"}

    def test_drop_statement(self):
        sql = "/** drop_statement('DROP TABLE IF EXISTS TABLE_A') */ CREATE TABLE TABLE_A (id STRING)"
        hints = QueryHintsParser.parse(sql)
        assert hints.drop_statement == "DROP TABLE IF EXISTS TABLE_A"

    def test_upgrade_mode(self):
        sql = "/** upgrade_mode('savepoint') */ CREATE TABLE TABLE_X (id String) AS SELECT * FROM Y"
        hints = QueryHintsParser.parse(sql)
        assert hints.upgrade_mode == UpgradeMode.SAVEPOINT

    def test_default_upgrade_mode(self):
        sql = "CREATE TABLE TABLE_X (id String) AS SELECT * FROM Y"
        hints = QueryHintsParser.parse(sql)
        assert hints.upgrade_mode == UpgradeMode.STATELESS

    def test_job_state(self):
        sql = "/** job_state('suspended') */ CREATE TABLE TABLE_X (id String) AS SELECT * FROM Y"
        hints = QueryHintsParser.parse(sql)
        assert hints.job_state == JobState.SUSPENDED

    def test_default_job_state(self):
        sql = "CREATE TABLE TABLE_X (id String) AS SELECT * FROM Y"
        hints = QueryHintsParser.parse(sql)
        assert hints.job_state == JobState.RUNNING

    def test_multiple_hints_in_single_comment(self):
        sql = "select /** fetch_max(10) fetch_timeout_ms(1000) */ from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.fetch_timeout_ms == 1000
        assert hints.fetch_max == 10
        assert hints.mode is None

    def test_multiple_hints_in_multiple_comment(self):
        sql = "select /** fetch_max(10) */ /** mode('streaming') */ from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.fetch_timeout_ms is None
        assert hints.fetch_max == 10
        assert hints.mode == QueryMode.STREAMING
