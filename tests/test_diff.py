#!/usr/bin/env python

from datools.models import Column
from datools.explanation.algorithms import diff
from .fixtures import generate_scorpion_testdb


def test_diff():
    engine = generate_scorpion_testdb()
    candidates = diff(
        engine,
        'SELECT * FROM sensor_readings WHERE temperature > 50',
        'SELECT * FROM sensor_readings WHERE temperature <= 50',
        (Column('created_at'), Column('sensor_id'), Column('voltage'),
         Column('humidity')),
        .05,
        2.0,
        1)
    assert(len(candidates) == 2)
