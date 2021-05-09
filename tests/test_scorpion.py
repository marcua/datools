#!/usr/bin/env python

from datools.models import Aggregate
from datools.models import AggregateFunction
from datools.models import Column
from datools.models import Operator
from datools.models import Predicate
from datools.models import StringConstant
from datools.models import Table
from datools.scorpion.hypotheses import generate_hypotheses


def test_scorpion():
    assert(
        generate_hypotheses(
            None,
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
