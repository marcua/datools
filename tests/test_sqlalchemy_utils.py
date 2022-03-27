#!/usr/bin/env python

from operator import itemgetter
from sqlalchemy.engine import Engine

from datools.models import Aggregate
from datools.models import AggregateFunction
from datools.models import Column
from datools.sqlalchemy_utils import grouping_sets_query
from .fixtures import generate_scorpion_testdb
from .utils import engine_based_datetime


def test_grouping_sets(db_engine: Engine):
    generate_scorpion_testdb(db_engine)
    query, set_index = grouping_sets_query(
        db_engine,
        'SELECT * FROM sensor_readings',
        (
            (Column('created_at'), Column('sensor_id')),
            (Column('created_at'),),
            (Column('sensor_id'),),
            (),
        ),
        (Aggregate(AggregateFunction.COUNT, Column('*'), Column('num_rows')), )
    )
    if db_engine.url.get_backend_name() == 'sqlite':
        assert('UNION ALL' in query)
        assert('GROUPING SETS' not in query)
    else:
        assert('UNION ALL' not in query)
        assert('GROUPING SETS' in query)
    result = db_engine.execute(query)
    sort_keys = ('grouping_id', 'created_at', 'sensor_id', 'num_rows')
    all_rows = [dict(row) for row in result]
    all_rows.sort(key=itemgetter(*sort_keys))

    def dt(datetime_string):
        return engine_based_datetime(db_engine, datetime_string)
    expected = [
        {'grouping_id': 0, 'created_at': dt('2021-05-05 11:00:00.000000'),
         'sensor_id': '1', 'num_rows': 1},
        {'grouping_id': 0, 'created_at': dt('2021-05-05 11:00:00.000000'),
         'sensor_id': '2', 'num_rows': 1},
        {'grouping_id': 0, 'created_at': dt('2021-05-05 11:00:00.000000'),
         'sensor_id': '3', 'num_rows': 1},
        {'grouping_id': 0, 'created_at': dt('2021-05-05 12:00:00.000000'),
         'sensor_id': '1', 'num_rows': 1},
        {'grouping_id': 0, 'created_at': dt('2021-05-05 12:00:00.000000'),
         'sensor_id': '2', 'num_rows': 1},
        {'grouping_id': 0, 'created_at': dt('2021-05-05 12:00:00.000000'),
         'sensor_id': '3', 'num_rows': 1},
        {'grouping_id': 0, 'created_at': dt('2021-05-05 13:00:00.000000'),
         'sensor_id': '1', 'num_rows': 1},
        {'grouping_id': 0, 'created_at': dt('2021-05-05 13:00:00.000000'),
         'sensor_id': '2', 'num_rows': 1},
        {'grouping_id': 0, 'created_at': dt('2021-05-05 13:00:00.000000'),
         'sensor_id': '3', 'num_rows': 1},
        {'grouping_id': 1, 'created_at': dt('2021-05-05 11:00:00.000000'),
         'sensor_id': None, 'num_rows': 3},
        {'grouping_id': 1, 'created_at': dt('2021-05-05 12:00:00.000000'),
         'sensor_id': None, 'num_rows': 3},
        {'grouping_id': 1, 'created_at': dt('2021-05-05 13:00:00.000000'),
         'sensor_id': None, 'num_rows': 3},
        {'grouping_id': 2, 'created_at': None,
         'sensor_id': '1', 'num_rows': 3},
        {'grouping_id': 2, 'created_at': None,
         'sensor_id': '2', 'num_rows': 3},
        {'grouping_id': 2, 'created_at': None,
         'sensor_id': '3', 'num_rows': 3},
        {'grouping_id': 3, 'created_at': None,
         'sensor_id': None, 'num_rows': 9}
    ]
    expected.sort(key=itemgetter(*sort_keys))
    assert(all_rows == expected)
