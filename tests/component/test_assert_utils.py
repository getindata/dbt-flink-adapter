import unittest

from tests.component.assert_utils import AssertUtils


class TestTmp(unittest.TestCase):

    def test1(self):
        sql1 = ["""
        \t \n \r
         a b c d
        e f
        select gg from hh(id int primary key),
        (name varchar)
        g

        """]
        sql2 = [""" a b c d e f select gg from hh(id int primary key)   ,(name varchar)g \t \n \r       """]
        AssertUtils.assert_sql_equals(sql1, sql2)
        AssertUtils.assert_sql_equals(sql2, sql1)

    def test7(self):
        sql1 = []
        sql2 = []
        AssertUtils.assert_sql_equals(sql1, sql2)
        AssertUtils.assert_sql_equals(sql2, sql1)

    def test8(self):
        sql1 = [""]
        sql2 = [""]
        AssertUtils.assert_sql_equals(sql1, sql2)
        AssertUtils.assert_sql_equals(sql2, sql1)

    def test2(self):
        sql1 = ["a", "b", "c"]
        sql2 = ["a", "b", "d"]
        AssertUtils.assert_sql_not_equals(sql1, sql2)
        AssertUtils.assert_sql_not_equals(sql2, sql1)

    def test3(self):
        sql1 = ["a", "b", "c", "d"]
        sql2 = ["a", "b", "c"]
        AssertUtils.assert_sql_not_equals(sql1, sql2)
        AssertUtils.assert_sql_not_equals(sql2, sql1)

    def test5(self):
        sql1 = []
        sql2 = ["a", "b", "c", "d"]
        AssertUtils.assert_sql_not_equals(sql1, sql2)
        AssertUtils.assert_sql_not_equals(sql2, sql1)
