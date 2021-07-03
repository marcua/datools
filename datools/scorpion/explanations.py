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

    :param connection: A SQLite connection.
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
