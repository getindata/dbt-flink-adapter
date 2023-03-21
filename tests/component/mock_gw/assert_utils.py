from typing import List

pattern_err_msg = """
len(expect)={}, len(actual)={}
first {} sql equal, first unequal sql is
==============================
{}
==============================
{}
==============================
"""


class AssertUtils:

    @staticmethod
    def assert_sql_equals(expect: List[str], actual: List[str], err_msg: str = None):
        equal, msg = AssertUtils._check_sql_equal_with_detail_msg(expect, actual)
        assert equal, err_msg if err_msg else msg

    @staticmethod
    def assert_sql_not_equals(expect: List[str], actual: List[str], err_msg: str = None):
        equal, msg = AssertUtils._check_sql_equal_with_detail_msg(expect, actual)
        assert (not equal), err_msg if err_msg else msg

    @staticmethod
    def _str_equal_ignore_space_tab_cr(s1: str, s2: str) -> bool:
        return "".join(s1.strip().split()) == "".join(s2.strip().split())

    @staticmethod
    def _check_sql_equal_with_detail_msg(expect: List[str], actual: List[str]):
        len_expect = len(expect)
        len_actual = len(actual)
        min_len = min(len_expect, len_actual)
        max_len = max(len_expect, len_actual)

        last_equal_index = -1
        for i in range(0, min_len):
            last_equal_index = i
            if not AssertUtils._str_equal_ignore_space_tab_cr(expect[i], actual[i]):
                last_equal_index = i - 1
                break

        if last_equal_index + 1 == max_len:
            return True, None

        next_expect = expect[last_equal_index + 1] if last_equal_index + 1 < len_expect else ""
        next_actual = actual[last_equal_index + 1] if last_equal_index + 1 < len_actual else ""
        err_msg = pattern_err_msg.format(len_expect, len_actual, last_equal_index + 1, next_expect, next_actual)
        return False, err_msg
