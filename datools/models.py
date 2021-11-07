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


@dataclass
class Predicate:
    left: Column
    operator: Operator
    right: Constant


@dataclass
class Table:
    name: str


@dataclass
class Explanation:
    predicates: Tuple[Predicate, ...]
    risk_ratio: float
