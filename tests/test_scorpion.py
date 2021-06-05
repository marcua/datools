#!/usr/bin/env python

from datools.models import Aggregate
from datools.models import AggregateFunction
from datools.models import Column
from datools.models import Operator
from datools.models import Predicate
from datools.models import StringConstant
from datools.models import Table
from datools.scorpion.explanations import generate_explanations
from .fixtures import generate_testdb


def test_scorpion():
    engine = generate_testdb()
    assert(
        generate_explanations(
            engine,
            Table('table'),
            (Column('group'), ),
            (Aggregate(AggregateFunction.AVERAGE, Column('agg')), ),
            (Predicate(Column('filter'),
                       Operator.EQUALS,
                       StringConstant('one')), ),
            (Predicate(Column('filter'),
                       Operator.NOT_EQUALS,
                       StringConstant('one')), ))
        ) == (
            Predicate(
                Column('foo'),
                Operator.EQUALS,
                StringConstant('bar')),)
