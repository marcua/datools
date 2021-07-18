import sqlalchemy

from typing import Generator
from typing import Set
from typing import Tuple

from datools.models import Aggregate
from datools.models import Column
from datools.models import Constant
from datools.models import Operator
from datools.models import Predicate
from datools.models import Table
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


def diff(
        engine: sqlalchemy.engine.Engine,
        test_relation: str,
        control_relation: str,
        on_columns: Tuple[Column, ...],
        min_support: float,
        min_risk_ratio: float,
        max_order: int
) -> Tuple[Tuple[Predicate, ...]]:
    results = engine.execute(test_relation)
    print(results.cursor.description)
    first = results.first()
    print(first)
    print(results._metadata._keymap)
    print(list(
        results._metadata._metadata_for_keys(keys=['voltage', 'created_at'])))
    print(
        results._metadata._reduce(keys=['voltage', 'created_at']))
    print(first._mapping.keys())

    # Get all column names from test_relation and control_relation,
    # ensure they are the same.
    # TODO(marcua): figure out why types are None, and match them if you can.

    # Ensure on_columns are a subset of the test/control columns.

    # Get size of test_relation, control_relation.

    # GROUP BY all test_relation columns, remove ones HAVING COUNT /
    # test_size < min_support.

    # Generate all size-max_order GROUPING SETS along with COUNTs of
    # test_relation & control_relation and JOIN the two, computing the
    # risk_ratio. Filter min_support, min_risk_ratio, and sort by
    # risk_ratio.
