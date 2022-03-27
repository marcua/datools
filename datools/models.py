from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Tuple


AggregateFunction = Enum(
    'AggregateFunction',
    'SUM COUNT AVERAGE')


Operator = Enum(
    'Operator',
    'EQUALS NOT_EQUALS GT LT GTEQ LTEQ')


OPERATOR_TO_SQL = {
    Operator.EQUALS: '=',
    Operator.NOT_EQUALS: '<>',
    Operator.GT: '>',
    Operator.LT: '<',
    Operator.GTEQ: '>=',
    Operator.LTEQ: '<=',
}


@dataclass
class Column:
    name: str

    def __hash__(self):
        return self.name.__hash__()

    def __eq__(self, other):
        return self.name.__eq__(other.name)


@dataclass
class Constant:
    value: Any


@dataclass
class Aggregate:
    function: AggregateFunction
    column: Column
    as_name: Column

    def to_sql(self):
        return (f'{self.function.name}({self.column.name}) '
                f'AS {self.as_name.name}')

    def __repr__(self):
        return f'Aggregate({self.to_sql()})'


@dataclass
class Predicate:
    left: Column
    operator: Operator
    right: Constant

    def to_sql(self):
        return (f'{self.left.name} '
                f'{OPERATOR_TO_SQL[self.operator]} {self.right.value}')

    def __repr__(self):
        return f'Predicate({self.to_sql()})'


@dataclass
class Table:
    name: str


@dataclass
class Explanation:
    predicates: Tuple[Predicate, ...]
    risk_ratio: float
