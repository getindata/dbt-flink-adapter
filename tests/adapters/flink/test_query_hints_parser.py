from dbt.adapters.flink.query_hints_parser import QueryHintsParser, QueryMode


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
        assert hints.execution_config == {'key_a': 'value_a', 'key_b': 'value_b'}
        
    def test_drop_statement(self):
        sql = "/** drop_statement('DROP TABLE IF EXISTS TABLE_A') */ CREATE TABLE TABLE_A (id STRING)"
        hints = QueryHintsParser.parse(sql)
        assert hints.drop_statement == "DROP TABLE IF EXISTS TABLE_A"

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
