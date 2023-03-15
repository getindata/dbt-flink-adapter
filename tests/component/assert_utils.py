from typing import List


def assert_sql_equals(expect: List[str], actual: List[str]):
    assert len(expect) == len(actual)
    for i in range(0, len(expect)):
        assert _sql_equivalent(expect[i], actual[i])


def _sql_equivalent(s1: str, s2: str) -> bool:
    return "".join(s1.strip().split()) == "".join(s2.strip().split())
