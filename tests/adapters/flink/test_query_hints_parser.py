from dbt.adapters.flink.query_hints_parser import QueryHintsParser


class TestQueryHintsParser:
    def test_no_hints(self):
        sql = "select * from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.fetch_timeout_ms is None
        assert hints.fetch_max is None
        assert hints.fetch_mode is None

    def test_single_hint(self):
        sql = "select /** fetch_max(10) */ from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.fetch_timeout_ms is None
        assert hints.fetch_max == 10
        assert hints.fetch_mode is None

    def test_multiple_hints_in_single_comment(self):
        sql = "select /** fetch_max(10) fetch_timeout_ms(1000) */ from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.fetch_timeout_ms == 1000
        assert hints.fetch_max == 10
        assert hints.fetch_mode is None

    def test_multiple_hints_in_multiple_comment(self):
        sql = "select /** fetch_max(10) */ /** fetch_mode('streaming') */ from input"
        hints = QueryHintsParser.parse(sql)
        assert hints.fetch_timeout_ms is None
        assert hints.fetch_max == 10
        assert hints.fetch_mode == "streaming"
