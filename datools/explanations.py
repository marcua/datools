import sqlalchemy

from collections import defaultdict
from math import floor
from textwrap import dedent
from textwrap import indent
from typing import Dict
from typing import List
from typing import Set
from typing import Tuple

from datools.errors import DatoolsError
from datools.models import Aggregate
from datools.models import AggregateFunction
from datools.models import Column
from datools.models import Constant
from datools.models import Explanation
from datools.models import Operator
from datools.models import Predicate
from datools.sqlalchemy_utils import INDENT
from datools.sqlalchemy_utils import grouping_sets_query
from datools.sqlalchemy_utils import query_columns
from datools.sqlalchemy_utils import query_rows
from datools.table_statistics import range_valued_statistics
from datools.table_statistics import RangeValuedStatistics


NUM_RANGE_BUCKETS = 15


def _rewrite_query_with_ranges_as_buckets(
        query: str,
        range_statistics: List[Tuple[Column, RangeValuedStatistics]]
) -> Tuple[str, Dict[Column, List[Tuple[Predicate, ...]]]]:
    # For each of the columns we've got range predicates on, create a
    # proxy column for the bucketed range values.
    bucket_predicates: Dict[Column, List[Tuple[Predicate, ...]]] = defaultdict(
        list)
    for column, statistic in range_statistics:
        # If the bucket minimums in the statistics are (a, b, c),
        # we want to generate the following ranges:
        # * < b [we leave out a in case the control relation
        #        has smaller values than the test relation]
        # * >= b, < c [values in the middle bucket]
        # * >= c [the largest bucket has a minimum of c]
        pairs = zip([None] + statistic.bucket_minimums[1:],
                    statistic.bucket_minimums[1:] + [None])
        for first, second in pairs:
            bucket_column = Column(f'{column.name}__bucket')
            first_predicate = Predicate(
                column, Operator.GTEQ, Constant(first))
            second_predicate = Predicate(column, Operator.LT, Constant(second))
            if first is None and second is not None:
                bucket_predicates[bucket_column].append((second_predicate, ))
            elif first is not None and second is None:
                bucket_predicates[bucket_column].append((first_predicate, ))
            # Don't allow a 1-bucket degenerate case (`first` and `second`
            # are empty).
            elif first is not None and second is not None:
                bucket_predicates[bucket_column].append(
                    (first_predicate, second_predicate))

    # Create a CASE statement for each bucket column that converts
    # each range within that column into a single bucketed value.
    cases = []
    for column, column_predicates in bucket_predicates.items():
        whens = []
        for index, predicate_group in enumerate(column_predicates):
            clause = ' AND '.join(predicate.to_sql()
                                  for predicate in predicate_group)
            whens.append(f'WHEN {clause} THEN {index}')
        when_lines = indent(f'\n'.join(whens), 4 * INDENT)
        cases.append(f'CASE\n{when_lines}\nEND AS {column.name}')

    # Generate SQL.
    case_lines = ',\n'.join(cases)
    return dedent(
        f'''
        WITH original_query AS (
            {query}
        )
        SELECT
            original_query.*{',' if case_lines else ''}
            {case_lines}
        FROM original_query
        '''), bucket_predicates


def _explanation_counts_query(
        engine: sqlalchemy.engine.Engine,
        relation: str,
        on_columns: Set[Column],
        min_support_rows: int = None
) -> Tuple[str, Dict[int, Tuple[Column, ...]]]:
    group_explanations_query, grouping_set_index = grouping_sets_query(
        engine,
        relation,
        tuple((column, ) for column in on_columns),
        (Aggregate(
            AggregateFunction.COUNT,
            Column('*'),
            Column('explanation_size')), )
    )
    if min_support_rows is not None:
        group_explanations_query += (
            f'HAVING (1.0 * COUNT(*)) > {min_support_rows}\n')
    return group_explanations_query, grouping_set_index


def _diff_query(
        test_explanations_query: str,
        control_explanations_query: str,
        num_test_rows: float,
        num_control_rows: float,
        on_columns: Set[Column],
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
        ),
        comparison AS (
            SELECT
                test.grouping_id,
                {', '.join(f'test.{column.name}' for column in on_columns)},
                test.explanation_size AS test_explanation_size,
                control.explanation_size AS control_explanation_size,
                (1.0 * test.explanation_size
                 / (test.explanation_size
                   + COALESCE(control.explanation_size, 0)))
                /
                (1.0 * ({adjusted_test_rows} - test.explanation_size)
                 / (({adjusted_test_rows} - test.explanation_size)
                    + ({adjusted_control_rows}
                       - COALESCE(control.explanation_size, 0)))
                ) AS risk_ratio
            FROM test
            LEFT JOIN control ON {join_statement}
        )
        SELECT *
        FROM comparison
        WHERE risk_ratio > {min_risk_ratio}
        ORDER BY risk_ratio DESC
        ''')
    return diff_query


def diff(
        engine: sqlalchemy.engine.Engine,
        test_relation: str,
        control_relation: str,
        on_column_values: Set[Column],
        on_column_ranges: Set[Column],
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
    :param on_column_values: A set of columns that might contain a specific
                       value that explains the difference between
                       test and control. Example: the column `sensor_id`
                       might contain a specific sensor's ID that is the
    :param on_column_ranges: A set of columns that might contain a range of
                       values that explains the difference between
                       test and control. Example: the column `timestamp`
                       might contain a date/time range that is the
                       source of outlier readings.
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
    on_column_names = ({column.name for column in on_column_values} |
                       {column.name for column in on_column_ranges})
    if on_column_names - set(test_column_names):
        raise DatoolsError('on_columns is not a subset of test_relation')

    # Get size of test_relation, control_relation.
    num_test_rows = 1.0 * query_rows(engine, test_relation)
    num_control_rows = 1.0 * query_rows(engine, control_relation)
    min_support_rows = floor(num_test_rows * min_support)

    # Transform ranges in on_column_ranges into bucket IDs.
    range_statistics = range_valued_statistics(
        engine, test_relation, on_column_ranges,
        num_buckets=NUM_RANGE_BUCKETS)
    rewritten_test_relation, test_bucket_predicates = (
        _rewrite_query_with_ranges_as_buckets(
            test_relation, range_statistics))
    rewritten_control_relation, control_bucket_predicates = (
        _rewrite_query_with_ranges_as_buckets(
            control_relation, range_statistics))

    # GROUP BY all test_relation columns, remove ones with a size less
    # than min_support_rows.
    on_columns = (on_column_values |
                  {column for column in test_bucket_predicates.keys()})
    test_explanations_query, grouping_set_index = _explanation_counts_query(
        engine, rewritten_test_relation, on_columns, min_support_rows)
    control_explanations_query, _ = _explanation_counts_query(
        engine, rewritten_control_relation, on_columns, min_support_rows=None)
    diff_query = _diff_query(
        test_explanations_query, control_explanations_query,
        num_test_rows, num_control_rows,
        on_columns, min_risk_ratio)

    result = engine.execute(diff_query)
    explanations = []
    for row in result:
        predicates = []
        for column in grouping_set_index[row.grouping_id]:
            if column in on_column_values:
                predicates.append(Predicate(
                    column, Operator.EQUALS, Constant(row[column.name])))
            else:
                # Turn the proxy range bucket column back into a predicate on
                # the range-valued column.
                predicates += test_bucket_predicates[column][row[column.name]]
        # Some databases (e.g., PostgreSQL) cast `risk_ratio` as
        # Decimal, so we cast to float.
        explanations.append(
            Explanation(tuple(predicates), float(row.risk_ratio)))
    result.close()
    return explanations
