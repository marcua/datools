from dataclasses import dataclass
from enum import Enum


AggregateFunction = Enum(
    'AggregateFunction',
    'SUM COUNT AVERAGE')


Operator = Enum(
    'Operator',
    'EQUALS NOT_EQUALS')


@dataclass
class Column:
    name: str

    def __hash__(self):
        return self.name.__hash__()

    def __eq__(self, other):
        return self.name.__eq__(other.name)


@dataclass
class StringConstant:
    value: str


@dataclass
class Aggregate:
    function: AggregateFunction
    column: Column


@dataclass
class Predicate:
    left: Column
    operator: Operator
    right: StringConstant  # TODO(marcua): get fancier.


@dataclass
class Table:
    name: str
