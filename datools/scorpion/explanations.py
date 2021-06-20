import sqlalchemy

from typing import Generator
from typing import Set
from typing import Tuple

from datools.models import Aggregate
from datools.models import Column
from datools.models import Operator
from datools.models import Predicate
from datools.models import StringConstant
from datools.models import Table
from datools.table_statistics import column_statistics


def _single_column_candidate_predicates(
        engine: sqlalchemy.engine.Engine,
        table: Table,
        group_bys: Tuple[Column, ...],
        aggregate: Aggregate,
) -> Generator[Predicate, None, None]:
    columns_to_ignore: Set[Column] = {group_by for group_by in group_bys}
    columns_to_ignore.add(aggregate.column)
    statistics = column_statistics(engine, table, columns_to_ignore)
    print(statistics)
    # For each column and statistic type,
    # * Decide whether to permit that column/statistic type (e.g., prune
    #   high cardinality)
    # * Generate column names and predicates (SingleColumnPredicate(Column,
    #   Predicate)).


def generate_explanations(
        engine: sqlalchemy.engine.Engine,
        table: Table,
        group_bys: Tuple[Column, ...],
        aggregate: Aggregate,
        outlier_predicates: Tuple[Predicate, ...],
        holdout_predicates: Tuple[Predicate, ...]
) -> Tuple[Predicate, ...]:
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
    _single_column_candidate_predicates(
        engine, table, group_bys, aggregate)
    return (
        Predicate(
            Column('foo'),
            Operator.EQUALS,
            StringConstant('bar')),)
