import sqlalchemy

from textwrap import indent
from typing import Tuple


GROUP_COLUMNS_KEY = '%%%GROUP_COLUMNS%%%'
GROUPING_SETS_KEY = '%%%GROUPING_SETS%%%'
GROUPING_ID_KEY = '%%%GROUPING_ID%%%'
INDENT = '    '


def query_columns(engine: sqlalchemy.engine.Engine, query: str) -> Tuple[str]:
    results = engine.execute(query)
    columns = tuple(column[0] for column in results.cursor.description)
    results.close()
    return columns


def query_rows(engine: sqlalchemy.engine.Engine, query: str) -> int:
    count_query = (
        f'WITH query AS ({query}) '
        f'SELECT COUNT(*) AS num_rows FROM query')
    results = engine.execute(count_query)
    rows = results.first()._mapping['num_rows']
    results.close()
    return rows


def grouping_sets_query(
        engine: sqlalchemy.engine.Engine,
        query: str,
        sets: Tuple[Tuple[str, ...]],
        group_columns_key: str = GROUP_COLUMNS_KEY,
        grouping_sets_key: str = GROUPING_SETS_KEY,
        grouping_id_key: str = GROUPING_ID_KEY
) -> str:
    """Takes a `query` like

    SELECT
        {grouping_id_key} AS grouping_id,
        {group_columns_key},
        AGGREGATE1(args), AGGREGATE2(args)
    ...
    GROUP BY {grouping_sets_key}
    ...

    and returns a grouping sets query for the grouping sets in `sets`.

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
    for set_id, grouping_set in enumerate(sets):
        group_columns = [f'NULL AS {column}'
                         for column in column_indices.keys()]
        for column in grouping_set:
            group_columns[column_indices[column]] = column
        queries.append(query
                       .replace(grouping_id_key, str(set_id))
                       .replace(group_columns_key, ', '.join(group_columns))
                       .replace(grouping_sets_key, ', '.join(grouping_set)))
    return '\nUNION ALL\n'.join(queries)
