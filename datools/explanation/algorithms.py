import sqlalchemy

from math import floor
from textwrap import dedent
from textwrap import indent
from typing import Dict
from typing import Generator
from typing import List
from typing import Set
from typing import Tuple

from datools.errors import DatoolsError
from datools.models import Aggregate
from datools.models import Column
from datools.models import Constant
from datools.models import Explanation
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


def _explanation_counts_query(
        engine: sqlalchemy.engine.Engine,
        relation: str,
        on_columns: Tuple[Column, ...],
        min_support_rows: int = None
) -> Tuple[str, Dict[int, Tuple[str, ...]]]:
    explanation_query = dedent(
        f'''
        SELECT
            {GROUPING_ID_KEY} AS grouping_id,
            {GROUP_COLUMNS_KEY},
            1.0 * COUNT(*) AS explanation_size
        FROM query
        GROUP BY {GROUPING_SETS_KEY}
        ''')
    if min_support_rows is not None:
        explanation_query += f'HAVING explanation_size > {min_support_rows}\n'

    group_explanations_query, grouping_set_index = grouping_sets_query(
        engine,
        explanation_query,
        tuple((column, ) for column in on_columns))
    return dedent(
        f'''
        WITH query AS (
            {relation}
        )
        {group_explanations_query}
        '''), grouping_set_index


def _diff_query(
        test_explanations_query: str,
        control_explanations_query: str,
        num_test_rows: float,
        num_control_rows: float,
        on_columns: Tuple[Column, ...],
        min_risk_ratio: float
) -> str:
    join_conditions = (
        ['test.grouping_id = control.grouping_id']
        + [f'((test.{column.name} = control.{column.name}) '
           f' OR ((test.{column.name} IS NULL) '
           f'     AND (control.{column.name} IS NULL)))'
           for column in on_columns])
    join_statement = ' AND '.join(join_conditions)

    # TODO(marcua): Consult with someone better at statistics on how
    # to avoid division by 0 in the risk ratio when a group encompases
    # the entire relation. For now, make the relation size one larger
    # than it actually is.
    adjusted_test_rows = num_test_rows + 1
    adjusted_control_rows = num_control_rows + 1

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
            {', '.join(f'test.{column.name}' for column in on_columns)},
            test.explanation_size AS test_explanation_size,
            control.explanation_size AS control_explanation_size,
            (test.explanation_size / (test.explanation_size
                                      + COALESCE(control.explanation_size, 0)))
            /
            (({adjusted_test_rows} - test.explanation_size)
             / (({adjusted_test_rows} - test.explanation_size)
                + ({adjusted_control_rows}
                   - COALESCE(control.explanation_size, 0)))
            ) AS risk_ratio
        FROM test
        LEFT JOIN control ON {join_statement}
        WHERE risk_ratio > {min_risk_ratio}
        ORDER BY risk_ratio DESC
        ''')
    return diff_query


def diff(
        engine: sqlalchemy.engine.Engine,
        test_relation: str,
        control_relation: str,
        on_columns: Tuple[Column, ...],
        min_support: float,
        min_risk_ratio: float,
        max_order: int
) -> List[Explanation]:
    """
    Generates candidate explanations for why records are more likely to appear
    in `test_relation` than `control_relation`. For example, if you provided
    a test set of customers who are more likely to churn and a control set of
    customers who are less likely to churn, the algorithm might list predicates
    related to business category or degree of service.

    This is an implementatin of "DIFF: A Relational Interface
    for Large-Scale Data Explanation"
    by Firas Abuzaid, Peter Kraft, Sahaana Suri, Edward Gan,
    Eric Xu, Atul Shenoy, Asvin Ananthanarayan, John Sheu,
    Erik Meijer, Xi Wu, Jeff Naughton, Peter Bailis, and Matei Zaharia.
    http://www.bailis.org/papers/diff-vldb2019.pdf

    Figure 1 of that paper contains a declarative description of the
    implemented algorithm.

    :param engine: A SQLAlchemy engine.
    :param test_relation: A SQL query resulting in rows whose presence
                          you would like to explain.
    :param control_relation: A SQL query (with the same schema
                             as `test_relation`) that contains a control
                             group of rows that are different from/a
                             baseline for `test_relation`.
    :param on_columns: A list of columns that might contain an explanation
                       for the difference between test and control.
    :param min_support: A value in the range [0, 1] with the minimum fraction
                        of the test set that an explanation should contain.
    :param min_risk_ratio: A positive floating point value with the minimum
                           risk ratio of an explanation. A high risk ratio
                           means an explanation is more likely to describe a
                           row in `test_relation` than `control_relation`.
    :param max_order: The largest number of columns on which to consider
                      an explanation. For example, a 2-column explanation could
                      be that (category='dog walker' AND signup_day='sunday').
                      The current implementation only supports 1-column
                      explanations.
    """
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
    test_explanations_query, grouping_set_index = _explanation_counts_query(
        engine, test_relation, on_columns, min_support_rows)
    control_explanations_query, _ = _explanation_counts_query(
        engine, control_relation, on_columns, min_support_rows=None)
    diff_query = _diff_query(
        test_explanations_query, control_explanations_query,
        num_test_rows, num_control_rows,
        on_columns, min_risk_ratio)

    result = engine.execute(diff_query)
    explanations = []
    for row in result:
        row = dict(row)
        predicates = tuple(
            Predicate(column, Operator.EQUALS, row[column.name])
            for column in grouping_set_index[row['grouping_id']])
        explanations.append(Explanation(predicates, row['risk_ratio']))
    result.close()
    return explanations
