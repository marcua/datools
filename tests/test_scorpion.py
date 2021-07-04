#!/usr/bin/env python

from datetime import datetime

from datools.models import Aggregate
from datools.models import AggregateFunction
from datools.models import Column
from datools.models import Operator
from datools.models import Predicate
from datools.models import Constant
from datools.models import Table
from datools.scorpion.explanations import generate_explanations
from .fixtures import generate_scorpion_testdb


def test_scorpion():
    engine = generate_scorpion_testdb()
    candidates = generate_explanations(
        engine,
        Table('sensor_readings'),
        (Column('created_at'), ),  # TODO(marcua): Round to hours.
        Aggregate(AggregateFunction.AVERAGE, Column('temperature')),
        (Predicate(Column('created_at'),
                   Operator.EQUALS,
                   Constant(datetime(2021, 5, 5, 11))), ),
        (Predicate(Column('created_at'),
                   Operator.NOT_EQUALS,
                   Constant(datetime(2021, 5, 5, 11))), ))
    assert(len(candidates) == 24)
