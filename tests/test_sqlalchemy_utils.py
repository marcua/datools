#!/usr/bin/env python

from sqlalchemy.engine import Engine
from textwrap import dedent

from datools.models import Column
from datools.sqlalchemy_utils import GROUP_COLUMNS_KEY
from datools.sqlalchemy_utils import GROUPING_ID_KEY
from datools.sqlalchemy_utils import GROUPING_SETS_KEY
from datools.sqlalchemy_utils import grouping_sets_query
from .fixtures import generate_scorpion_testdb


def test_grouping_sets(db_engine: Engine):
    generate_scorpion_testdb(db_engine)
    grouping_query = dedent(
        f'''
        SELECT
            {GROUPING_ID_KEY} AS grouping_id,
            {GROUP_COLUMNS_KEY},
            COUNT(*) AS num_rows
        FROM sensor_readings
        {GROUPING_SETS_KEY}
        ''')
    query, set_index = grouping_sets_query(
        db_engine, grouping_query, (
            (Column('created_at'), Column('sensor_id')),
            (Column('created_at'),),
            (Column('sensor_id'),),
            (),
        ))
    if db_engine.url.get_backend_name() == 'sqlite':
        assert('UNION ALL' in query)
        assert('GROUPING SETS' not in query)
    else:
        assert('UNION ALL' not in query)
        assert('GROUPING SETS' in query)
    result = db_engine.execute(query)
    all_rows = [dict(row) for row in result]
    assert(all_rows == [
    ])
