from typing import List


def assert_sql_equals(expect: List[str], actual: List[str]):
    len_expect = len(expect)
    len_actual = len(actual)
    len_equal = len_expect == len_actual
    err_msg = "" if len_equal else f"len(expect)={len(expect)}, len(actual)={len(actual)}\n"

    min_len = min(len_expect, len_actual)
    first_non_equal = False
    for i in range(0, min_len):
        sql_expect = expect[i]
        sql_actual = actual[i]
        if not _sql_equivalent(sql_expect, sql_actual):
            first_non_equal = True
            err_msg = f"""
{err_msg}
first {i} same sql, and first not equal sql index={i + 1} is
==============================
{expect[i]}
==============================
{actual[i]}
==============================\n"""

    if (not len_equal) and (not first_non_equal):
        next_expect = ""
        if len_expect > min_len:
            next_expect = expect[min_len]
        next_actual = ""
        if len_actual > min_len:
            next_actual = actual[min_len]

        err_msg = f"""
{err_msg}
first {min_len} same sql, and first not equal sql index={min_len + 1} is
==============================
{next_expect}
==============================
{next_actual}
==============================\n"""

    assert (not err_msg), err_msg


def assert_sql_equals_basic(expect: List[str], actual: List[str]):
    assert len(expect) == len(actual), f"len(expect)={len(expect)}, len(actual)={len(actual)}"
    for i in range(0, len(expect)):
        assert _sql_equivalent(expect[i], actual[i])


def _sql_equivalent(s1: str, s2: str) -> bool:
    return "".join(s1.strip().split()) == "".join(s2.strip().split())


# sql1 = ["""
#
#  a b c d
# e
# f
# select gg from hh
# g
#
# """]
# sql2 = ["""
#  a b c d e f select gg from hh g
# """]
# assert_sql_equals(sql1, sql2)
#
#
# sql1 = ["a", "b", "c"]
# sql2 = ["a", "b", "d"]
# assert_sql_equals(sql1, sql2)

# sql1 = ["a", "b", "c", "d"]
# sql2 = ["a", "b", "c"]
# assert_sql_equals(sql1, sql2)

# sql1 = ["a", "b", "c", ]
# sql2 = ["a", "b", "c", "d"]
# assert_sql_equals(sql1, sql2)

# sql1 = []
# sql2 = ["a", "b", "c", "d"]
# assert_sql_equals(sql1, sql2)

# sql1 = ["a", "b", "c", "d"]
# sql2 = []
# assert_sql_equals(sql1, sql2)

# sql1 = []
# sql2 = []
# assert_sql_equals(sql1, sql2)
