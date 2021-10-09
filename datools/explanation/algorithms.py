import sqlalchemy

from math import floor
from textwrap import dedent
from textwrap import indent
from typing import Generator
from typing import Set
from typing import Tuple

from datools.errors import DatoolsError
from datools.models import Aggregate
from datools.models import Column
from datools.models import Constant
from datools.models import Operator
from datools.models import Predicate
from datools.models import Table
from datools.sqlalchemy_utils import GROUP_COLUMNS_KEY
from datools.sqlalchemy_utils import GROUPING_ID_KEY
from datools.sqlalchemy_utils import GROUPING_SETS_KEY
from datools.sqlalchemy_utils import INDENT
from datools.sqlalchemy_utils import grouping_sets_query
from datools.sqlalchemy_utils import query_columns
from datools.sqlalchemy_utils import query_rows
from datools.table_statistics import column_statistics
from datools.table_statistics import RangeValuedStatistics
from datools.table_statistics import SetValuedStatistics


def _single_column_candidate_predicates(
        engine: sqlalchemy.engine.Engine,
        table: Table,
        group_bys: Tuple[Column, ...],
        aggregate: Aggregate,
) -> Generator[Tuple[Predicate, ...], None, None]:
    """Based on column statistics for range- and set-valued columns,
    returns a list of predicates that either filter on ranges (i.e.,
    lower <= column AND column < higher) or equality (i.e., column =
    constant). These candidates can be tested as outlier explanations.

    :returns: A tuple representing a conjunction of
              predicates (e.g., predicate1 AND predicate2).
    """
    columns_to_ignore: Set[Column] = {group_by for group_by in group_bys}
    columns_to_ignore.add(aggregate.column)
    statistics = column_statistics(engine, table, columns_to_ignore)
    for column, statistics_list in statistics.items():
        for statistic in statistics_list:
            if isinstance(statistic, SetValuedStatistics):
                yield from (Predicate(
                    column,
                    Operator.EQUALS,
                    Constant(value)) for value in statistic.popular_values)
            elif isinstance(statistic, RangeValuedStatistics):
                if len(statistic.bucket_minimums) == 1:
                    continue
                pairs = zip(statistic.bucket_minimums,
                            statistic.bucket_minimums[1:] + [None])
                for first, second in pairs:
                    first_predicate = Predicate(
                        column, Operator.GTEQ, Constant(first))
                    if second is None:
                        yield (first_predicate, )
                    yield (
                        first_predicate,
                        Predicate(column, Operator.LT, Constant(second)))


def generate_explanations(
        engine: sqlalchemy.engine.Engine,
        table: Table,
        group_bys: Tuple[Column, ...],
        aggregate: Aggregate,
        outlier_predicates: Tuple[Predicate, ...],
        holdout_predicates: Tuple[Predicate, ...]
) -> Tuple[Tuple[Predicate, ...]]:
    """Generates explanation predicates based on Scorpion (Wu & Madden,
    VLDB 2013) that cause an aggregate to be different in an outlier
    set than a holdout set.

    :param engine: A SQLite engine.
    :param table: The name of a table.
    :param group_bys: A list of columns to group on.
    :param aggregate: An aggregate on which we identify an outlier.
    :param outlier_predicates: Filters on the result that
        exhibit unexpected conditions.
    :param holdout_predicates: Filters on the result that
        exhibit normal conditions.
    """
    candidates = _single_column_candidate_predicates(
        engine, table, group_bys, aggregate)
    return tuple(candidates)


def _explanation_counts_query(
        engine: sqlalchemy.engine.Engine,
        relation: str,
        on_columns: Tuple[str, ...],
        min_support_rows: int = None
) -> str:
    explanation_query = dedent(
        f'''
        SELECT
            {GROUPING_ID_KEY} AS grouping_id,
            {GROUP_COLUMNS_KEY},
            COUNT(*) AS explanation_size
        FROM query
        GROUP BY {GROUPING_SETS_KEY}
        ''')
    if min_support_rows is not None:
        explanation_query += f'HAVING explanation_size > {min_support_rows}\n'

    group_explanations_query = grouping_sets_query(
        engine,
        explanation_query,
        tuple((column, ) for column in on_columns))
    return dedent(
        f'''
        WITH query AS (
            {relation}
        )
        {group_explanations_query}
        ''')


def _diff_query(
        test_explanations_query: str,
        control_explanations_query: str,
        num_test_rows: float,
        num_control_rows: float,
        on_column_names: Tuple[str, ...],
        min_risk_ratio: float
) -> str:
    diff_query = dedent(
        f'''
        WITH
        test AS (
            {indent(test_explanations_query, 3 * INDENT)}
        ),
        control AS (
            {indent(control_explanations_query, 3 * INDENT)}
        )
        SELECT
            test.grouping_id,
            {', '.join(f'test.{column}' for column in on_column_names)},
            (test.explanation_size / (test.explanation_size + NULLIF(control.explanation_size, 0))) /
            (({num_test_rows} - test.explanation_size) / (({num_test_rows} - test.explanation_size) + ({num_control_rows} - NULLIF(control.explanation_size, 0)))) AS risk_ratio
        FROM test
        LEFT JOIN control ON
            test.grouping_id = control.grouping_id
            AND {' AND '.join(f'test.{column} = control.{column}'
                 for column in on_column_names)}
        ORDER BY risk_ratio DESC
        ''')
    # TODO(marcua): Figure out why all risk ratios are NULL (print results of the diff query, and also the results of the test and control queries)
    # TODO(marcua):         -- WHERE risk_ratio > {min_risk_ratio}
    # TODO(marcua): Clean up readability of ANDs
    return diff_query


def diff(
        engine: sqlalchemy.engine.Engine,
        test_relation: str,
        control_relation: str,
        on_columns: Tuple[Column, ...],
        min_support: float,
        min_risk_ratio: float,
        max_order: int
) -> Tuple[Tuple[Predicate, ...]]:
    if max_order != 1:
        raise DatoolsError('Only one-column predicates are supported for now')

    # Get all column names from test_relation and control_relation,
    # ensure they are the same.
    # TODO(marcua): compare types.
    test_column_names = query_columns(engine, test_relation)
    control_column_names = query_columns(engine, control_relation)
    if test_column_names != control_column_names:
        raise DatoolsError(
            'test_relation and control_relation have different schemas')

    # Ensure on_columns are a subset of the test/control columns.
    on_column_names = tuple(column.name for column in on_columns)
    if set(on_column_names) - set(test_column_names):
        raise DatoolsError('on_columns is not a subset of test_relation')

    # Get size of test_relation, control_relation.
    num_test_rows = 1.0 * query_rows(engine, test_relation)
    num_control_rows = 1.0 * query_rows(engine, control_relation)
    min_support_rows = floor(num_test_rows * min_support)

    # GROUP BY all test_relation columns, remove ones with a size less
    # than min_support_rows.
    test_explanations_query = _explanation_counts_query(
        engine, test_relation, on_column_names, min_support_rows)
    control_explanations_query = _explanation_counts_query(
        engine, control_relation, on_column_names, min_support_rows=None)
    diff_query = _diff_query(
        test_explanations_query, control_explanations_query,
        num_test_rows, num_control_rows,
        on_column_names, min_risk_ratio)
    for row in engine.execute(diff_query):
        print(row)
    # print(diff_query)
