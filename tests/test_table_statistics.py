#!/usr/bin/env python

from datools.models import Column
from datools.models import Table
from datools.table_statistics import column_statistics
from datools.table_statistics import RangeValuedStatistics
from datools.table_statistics import SetValuedStatistics
from .fixtures import generate_synthetic_testdb


def test_table_statistics():
    """Tests `column_statistics` on synthetic data in 19 buckets with 9
    values each.
    """
    engine = generate_synthetic_testdb()
    statistics = column_statistics(engine, Table('synthetic_data'), {})
    assert {
        Column('id'):
            [SetValuedStatistics(171, list(range(1, 101))),
             RangeValuedStatistics([1, 58, 115])],
        Column('same_datetime'):
            [RangeValuedStatistics([
                '2021-05-05 11:00:00.000000'])],
        Column('unique_datetime'):
            [RangeValuedStatistics([
                '2021-05-01 01:00:00.000000',
                '2021-05-07 04:00:00.000000',
                '2021-05-13 07:00:00.000000'])],
        Column('bucket_unique_datetime'):
            [RangeValuedStatistics([
                '2021-05-05 01:00:00.000000',
                '2021-05-05 04:00:00.000000',
                '2021-05-05 07:00:00.000000'])],
        Column('same_string'):
            [SetValuedStatistics(1, ['hi'])],
        Column('unique_string'):
            [SetValuedStatistics(171,
                                 sorted([f'{bucket}-{row}'
                                         for bucket in range(1, 20)
                                         for row in range(1, 10)])[:100])],
        Column('bucket_unique_string'):
            [SetValuedStatistics(9, [str(x) for x in range(1, 10)])],
        Column('same_float'):
            [RangeValuedStatistics([1.1])],
        Column('unique_float'):
            [RangeValuedStatistics([1.01, 7.04, 13.07])],
        Column('bucket_unique_float'):
            [RangeValuedStatistics([1.0, 4.0, 7.0])],
        Column('same_int'):
            [SetValuedStatistics(1, [1]), RangeValuedStatistics([1])],
        Column('unique_int'):
            [SetValuedStatistics(171,
                                 sorted([bucket * 1000 + row
                                         for bucket in range(1, 20)
                                         for row in range(1, 10)])[:100]),
             RangeValuedStatistics([1001, 7004, 13007])],
        Column('bucket_unique_int'):
            [SetValuedStatistics(9, list(range(1, 10))),
             RangeValuedStatistics([1, 4, 7])],
    } == statistics
