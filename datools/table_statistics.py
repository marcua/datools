import sqlalchemy

from collections import defaultdict
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Tuple
from typing import Set

from datools.models import Column
from datools.models import Table


MAX_SET_VALUES_TO_INCLUDE = 100
RANGE_BUCKETS = 3
RANGE_VALUED_TYPES = {
    sqlalchemy.sql.sqltypes.BIGINT,
    sqlalchemy.sql.sqltypes.DATE,
    sqlalchemy.sql.sqltypes.DATETIME,
    sqlalchemy.sql.sqltypes.DECIMAL,
    sqlalchemy.sql.sqltypes.FLOAT,
    sqlalchemy.sql.sqltypes.INTEGER,
    sqlalchemy.sql.sqltypes.NUMERIC,
    sqlalchemy.sql.sqltypes.REAL,
    sqlalchemy.sql.sqltypes.SMALLINT,
    sqlalchemy.sql.sqltypes.TIME,
    sqlalchemy.sql.sqltypes.TIMESTAMP,
}
SET_VALUED_TYPES = {
    sqlalchemy.sql.sqltypes.BOOLEAN,
    sqlalchemy.sql.sqltypes.CHAR,
    sqlalchemy.sql.sqltypes.INTEGER,
    sqlalchemy.sql.sqltypes.NCHAR,
    sqlalchemy.sql.sqltypes.NVARCHAR,
    sqlalchemy.sql.sqltypes.SMALLINT,
    sqlalchemy.sql.sqltypes.TEXT,
    sqlalchemy.sql.sqltypes.VARCHAR,
}


class ColumnStatistics:
    pass


@dataclass
class SetValuedStatistics(ColumnStatistics):
    distinct_values: int
    popular_values: list

    def __repr__(self):
        return (
            f'SetValuedStatistics(distinct_values: {self.distinct_values}, '
            f'popular_values: {self.popular_values})')

    def __eq__(self, other):
        return (self.distinct_values == other.distinct_values
                and set(self.popular_values) == set(other.popular_values))


@dataclass
class RangeValuedStatistics(ColumnStatistics):
    bucket_minimums: List[Any]

    def __repr__(self):
        return (
            f'RangeValuedStatistics(bucket_minimums: '
            f'{[minimum for minimum in self.bucket_minimums]})')


def _set_valued_statistics(
        conn: sqlalchemy.engine.Connection,
        table: Table,
        columns: List[Column]
) -> Generator[Tuple[Column, SetValuedStatistics], None, None]:
    clauses: list[str] = []
    for column in columns:
        clauses.append(
            f'COUNT(DISTINCT {column.name})'
            f'AS {column.name}_distinct_values')
    results = list(conn.execute(
        f'SELECT {", ".join(clauses)} FROM {table.name}'))[0]
    for column in columns:
        values = [
            row[column.name] for row in
            conn.execute(
                f'SELECT {column.name}, COUNT(*) AS num_rows '
                f'FROM {table.name} '
                f'GROUP BY {column.name} '
                f'ORDER BY num_rows DESC, {column.name} '
                f'LIMIT {MAX_SET_VALUES_TO_INCLUDE}')]
        yield (
            column,
            SetValuedStatistics(
                results[f'{column.name}_distinct_values'],
                values))


def _range_valued_statistics(
        conn: sqlalchemy.engine.Connection,
        table: Table,
        columns: List[Column]
) -> Generator[Tuple[Column, SetValuedStatistics], None, None]:
    bucket_clauses: list[str] = []
    first_clauses: list[str] = []
    for column in columns:
        bucket_clauses.append(
            f'{column.name}, '
            f'NTILE({RANGE_BUCKETS}) OVER (ORDER BY {column.name} ASC) '
            f'AS {column.name}_bucket')
        first_clauses.append(
            f'first_value({column.name}) OVER '
            f'(PARTITION BY {column.name}_bucket '
            f' ORDER BY {column.name} ASC '
            f' RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) '
            f'AS {column.name}_bucket_value')
    results = conn.execute(
        f'WITH buckets AS ( '
        f'SELECT {", ".join(bucket_clauses)} '
        f'FROM {table.name} '
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
    column_values: Dict[Column, List[Any]] = defaultdict(set)
    for row in results:
        for column in columns:
            column_values[column].add(row[f'{column.name}_bucket_value'])
    for column in columns:
        yield (
            column,
            RangeValuedStatistics(sorted(column_values[column])))


def column_statistics(
        engine: sqlalchemy.engine.Engine,
        table: Table,
        columns_to_ignore: Set[Column]
) -> Dict[Column, List[ColumnStatistics]]:
    metadata = sqlalchemy.MetaData()
    table_metadata = sqlalchemy.Table(
        table.name, metadata, autoload_with=engine)
    candidate_columns = [column for column in table_metadata.columns
                         if Column(column.name) not in columns_to_ignore]
    statistics: Dict[Column, List[ColumnStatistics]] = defaultdict(list)
    conn = engine.connect()
    for column, statistic in _set_valued_statistics(
            conn, table,
            [Column(column.name) for column in candidate_columns if
             type(column.type) in SET_VALUED_TYPES]):
        statistics[column].append(statistic)
    for column, statistic in _range_valued_statistics(
            conn, table,
            [Column(column.name) for column in candidate_columns if
             type(column.type) in RANGE_VALUED_TYPES]):
        statistics[column].append(statistic)
    return statistics
