import sqlalchemy

from collections import defaultdict
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Set

from datools.models import Column
from datools.models import Table


RANGE_VALUED_TYPES = {
    sqlalchemy.sql.sqltypes.BigInteger,
    sqlalchemy.sql.sqltypes.Date,
    sqlalchemy.sql.sqltypes.DateTime,
    sqlalchemy.sql.sqltypes.Float,
    sqlalchemy.sql.sqltypes.Integer,
    sqlalchemy.sql.sqltypes.Interval,
    sqlalchemy.sql.sqltypes.Numeric,
    sqlalchemy.sql.sqltypes.SmallInteger,
    sqlalchemy.sql.sqltypes.Time,
}
SET_VALUED_TYPES = {
    sqlalchemy.sql.sqltypes.Boolean,
    sqlalchemy.sql.sqltypes.Enum,
    sqlalchemy.sql.sqltypes.Integer,
    sqlalchemy.sql.sqltypes.SmallInteger,
    sqlalchemy.sql.sqltypes.String,
    sqlalchemy.sql.sqltypes.Text,
    sqlalchemy.sql.sqltypes.Unicode,
    sqlalchemy.sql.sqltypes.UnicodeText,
}


class ColumnStatistics:
    pass


@dataclass
class SetValuedStatistics(ColumnStatistics):
    distinct_values: int
    most_common_values: list

    def __repr__(self):
        return (
            f'SetValuedStatistics(distinct_values: {self.distinct_values}, '
            f'most_common_values: {self.most_common_values})')

    def __eq__(self, other):
        return (self.distinct_values == other.distinct_values
                and (set(self.most_common_values) ==
                     set(other.most_common_values)))


@dataclass
class RangeValuedStatistics(ColumnStatistics):
    bucket_minimums: List[Any]

    def __repr__(self):
        return (
            f'RangeValuedStatistics(bucket_minimums: '
            f'{[minimum for minimum in self.bucket_minimums]})')


def set_valued_statistics(
        engine: sqlalchemy.engine.Engine,
        query: str,
        columns: Set[Column],
        num_most_common_values: int = 100
) -> List[Tuple[Column, SetValuedStatistics]]:
    clauses: List[str] = []
    for column in columns:
        clauses.append(
            f'COUNT(DISTINCT {column.name})'
            f'AS {column.name}_distinct_values')
    results = engine.execute(
        f'WITH query AS ( '
        f'{query}'
        f')'
        f'SELECT {", ".join(clauses)} FROM query')
    count_statistics = list(results)[0]
    results.close()
    statistics: List[Tuple[Column, SetValuedStatistics]] = []
    for column in columns:
        results = engine.execute(
                f'WITH query AS ( '
                f'{query}'
                f')'
                f'SELECT {column.name}, COUNT(*) AS num_rows '
                f'FROM query '
                f'GROUP BY {column.name} '
                f'ORDER BY num_rows DESC, {column.name} '
                f'LIMIT {num_most_common_values}')
        values = [row[column.name] for row in results]
        results.close()
        statistics.append((
            column,
            SetValuedStatistics(
                count_statistics[f'{column.name}_distinct_values'],
                values)))
    return statistics


def range_valued_statistics(
        engine: sqlalchemy.engine.Engine,
        query: str,
        columns: Set[Column],
        num_buckets: int = 3
) -> List[Tuple[Column, RangeValuedStatistics]]:
    bucket_clauses: List[str] = []
    first_clauses: List[str] = []
    for column in columns:
        bucket_clauses.append(
            f'{column.name}, '
            f'NTILE({num_buckets}) OVER (ORDER BY {column.name} ASC) '
            f'AS {column.name}_bucket')
        first_clauses.append(
            f'first_value({column.name}) OVER '
            f'(PARTITION BY {column.name}_bucket '
            f' ORDER BY {column.name} ASC '
            f' RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) '
            f'AS {column.name}_bucket_value')

    statistics: List[Tuple[Column, RangeValuedStatistics]] = []
    if bucket_clauses:
        results = engine.execute(
            f'WITH query AS ( '
            f'{query}'
            f'), '
            f'buckets AS ( '
            f'SELECT {", ".join(bucket_clauses)} '
            f'FROM query '
            f')'
            f'SELECT {", ".join(first_clauses)} '
            f'FROM buckets ')
        # The output of the window function is the number of rows in the
        # input rather than the number of range buckets. Because we have a
        # single query across multiple columns, we can't `GROUP BY
        # column_name_bucket` and get the `MIN` NTILE value for each
        # bucket. Since sqlite doesn't support CUBE/ROLLUP/GROUPING SETS,
        # we do this grouping manually by creating a `set` of the
        # `first_value` for each bucket.
        column_values: Dict[Column, Set[Any]] = defaultdict(set)
        for row in results:
            for column in columns:
                value = row[f'{column.name}_bucket_value']
                # Some engines (e.g., SQLite) happily store strings in
                # numeric columns, so we have to be a bit defensive of
                # the values we get back.
                if column is not None and not value == '':
                    column_values[column].add(value)
        results.close()
        for column in columns:
            statistics.append((
                column,
                RangeValuedStatistics(sorted(column_values[column]))))
    return statistics


def column_statistics(
        engine: sqlalchemy.engine.Engine,
        table: Table,
        columns_to_ignore: Set[Column],
) -> Dict[Column, List[ColumnStatistics]]:
    metadata = sqlalchemy.MetaData()
    table_metadata = sqlalchemy.Table(
        table.name, metadata, autoload_with=engine)
    candidate_columns = [column for column in table_metadata.columns
                         if Column(column.name) not in columns_to_ignore]
    statistics: Dict[Column, List[ColumnStatistics]] = defaultdict(list)
    for column, set_statistic in set_valued_statistics(
            engine,
            f'SELECT * FROM {table.name}',
            {Column(column.name) for column in candidate_columns if
             type(column.type.as_generic()) in SET_VALUED_TYPES}):
        statistics[column].append(set_statistic)
    for column, range_statistic in range_valued_statistics(
            engine,
            f'SELECT * FROM {table.name}',
            {Column(column.name) for column in candidate_columns if
             type(column.type.as_generic()) in RANGE_VALUED_TYPES}):
        statistics[column].append(range_statistic)
    return statistics
