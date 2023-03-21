from typing import Any, List


class Data:
    def __init__(self, kind, one_field):
        self.kind = kind
        self.fields = List[one_field]


class LogicType:
    def __init__(self, _type, nullable, length):
        self.type = _type
        self.nullable: nullable
        self.length: length

    @staticmethod
    def varchar():
        return LogicType("VARCHAR", True, 2147483647)


class Column:
    def __init__(self, name, logical_type, comment=None):
        self.name: str
        self.logicalType = LogicType
        self.comment: str

    @staticmethod
    def varchar(name: str):
        return Column(name, LogicType.varchar(), None)


class Results:
    columns: List[Column]
    data: List[Data]


class Payload:
    results: Results
    resultType: str
    nextResultUri: str


def data_gen(kind, fields):
    d = []
    for f in fields:
        d.append(Data(kind, f))
