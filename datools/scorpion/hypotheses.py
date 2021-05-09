from typing import Tuple

from datools.models import Aggregate
from datools.models import Column
from datools.models import Operator
from datools.models import Predicate
from datools.models import StringConstant
from datools.models import Table


def generate_hypotheses(
        connection,
        table: Table,
        group_bys: Tuple[Column, ...],
        aggregate: Tuple[Aggregate, ...],
        outlier_predicates: Tuple[Predicate, ...],
        holdout_predicates: Tuple[Predicate, ...]
) -> Tuple[Predicate, ...]:
    """Generates hypothesis predicates based on Scorpion (Wu & Madden,
    VLDB 2013) that cause an aggregate to be different in an outlier
    set than a holdout set.

    :param connection: A SQLite connection.
    :param relation: The name of a table.
    :param group_bys: A list of columns to group on.
    :param aggregate: An aggregate on which we identify an outlier.
    :param outlier_predicates: Filters on the result that
        exhibit unexpected conditions.
    :param holdout_predicates: Filters on the result that
        exhibit normal conditions.
    """
    return (
        Predicate(
            Column('foo'),
            Operator.EQUALS,
            StringConstant('bar')),)
