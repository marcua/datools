#!/usr/bin/env python

from pytest import approx
from sqlalchemy.engine import Engine

from datools.models import Column
from datools.models import Constant
from datools.models import Explanation
from datools.models import Operator
from datools.models import Predicate
from datools.explanations import diff
from .fixtures import generate_scorpion_testdb


def test_diff(db_engine: Engine):
    generate_scorpion_testdb(db_engine)
    candidates = diff(
        db_engine,
        'SELECT * FROM sensor_readings WHERE temperature > 50',
        'SELECT * FROM sensor_readings WHERE temperature <= 50',
        {Column('created_at'), Column('sensor_id'), Column('voltage'),
         Column('humidity')},
        {Column('voltage'), Column('humidity')},
        0.05,
        2.0,
        1)
    assert(candidates == [
        Explanation(
            (Predicate(
                Column('voltage'), Operator.EQUALS, Constant(approx(2.3))), ),
            risk_ratio=9.0),
        Explanation(
            (Predicate(Column('sensor_id'), Operator.EQUALS, Constant('3')), ),
            risk_ratio=5 + (1.0 / 3))])
