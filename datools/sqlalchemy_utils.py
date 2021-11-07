import sqlalchemy
from tabulate import tabulate

from typing import Dict
from typing import Tuple

from datools.models import Column


GROUP_COLUMNS_KEY = '%%%GROUP_COLUMNS%%%'
GROUPING_SETS_KEY = '%%%GROUPING_SETS%%%'
GROUPING_ID_KEY = '%%%GROUPING_ID%%%'
INDENT = '    '


def query_columns(engine: sqlalchemy.engine.Engine, query: str) -> Tuple[str]:
    results = engine.execute(query)
    columns = tuple(column[0] for column in results.cursor.description)
    results.close()
    return columns


def query_results_pretty_print(
        engine: sqlalchemy.engine.Engine, query: str, label: str = None
) -> None:
    if label:
        print(f'*** {label} ***')
    result = engine.execute(query)
    all_rows = (dict(row) for row in result)
    print(tabulate(all_rows, headers='keys', tablefmt='psql'))
    result.close()


def query_rows(engine: sqlalchemy.engine.Engine, query: str) -> int:
    count_query = (
        f'WITH query AS ({query}) '
        f'SELECT COUNT(*) AS num_rows FROM query')
    results = engine.execute(count_query)
    rows = results.first().num_rows
    results.close()
    return rows


def grouping_sets_query(
        engine: sqlalchemy.engine.Engine,
        query: str,
        sets: Tuple[Tuple[Column, ...]],
        group_columns_key: str = GROUP_COLUMNS_KEY,
        grouping_sets_key: str = GROUPING_SETS_KEY,
        grouping_id_key: str = GROUPING_ID_KEY
) -> Tuple[str, Dict[int, Tuple[str, ...]]]:
    """Takes a `query` like

    SELECT
        {grouping_id_key} AS grouping_id,
        {group_columns_key},
        AGGREGATE1(args), AGGREGATE2(args)
    ...
    GROUP BY {grouping_sets_key}
    ...

    and returns a tuple with
      * a grouping sets query for the grouping sets in `sets`, and
      * a dictionary mapping set IDs to grouping set columns.

    If the database that `engine` is connected to natively supports
    grouping sets, utilize the standard SQL syntax for them. If it doesn't,
    implement the query by capturing the UNION ALL output of multiple
    GROUP BY subqueries.

    The `grouping_id` will be set to a unique value for each grouping set,
    and will be deterministic on the iteration order of `sets`.
    """
    if engine.url.get_backend_name() == 'postgresql':
        # TODO(marcua): Implement grouping sets using SQL syntax in
        # databases that support it.
        pass
    column_indices = {}
    for grouping_set in sets:
        for column in grouping_set:
            index = column_indices.get(column)
            if index is None:
                column_indices[column] = len(column_indices)

    queries = []
    set_index: Dict[int, Tuple[str, ...]] = {}
    for set_id, grouping_set in enumerate(sets):
        set_index[set_id] = grouping_set
        group_columns = [f'NULL AS {column.name}'
                         for column in column_indices.keys()]
        for column in grouping_set:
            group_columns[column_indices[column]] = column.name
        grouping_set_names = tuple(column.name for column in grouping_set)
        queries.append(
            query
            .replace(grouping_id_key, str(set_id))
            .replace(group_columns_key, ', '.join(group_columns))
            .replace(grouping_sets_key, ', '.join(grouping_set_names)))
    return '\nUNION ALL\n'.join(queries), set_index
