import sqlalchemy
from tabulate import tabulate

from typing import Dict
from typing import Tuple

from datools.models import Column


GROUP_COLUMNS_KEY = '%%%GROUP_COLUMNS%%%'
GROUPING_SETS_KEY = '%%%GROUPING_SETS%%%'
GROUPING_ID_KEY = '%%%GROUPING_ID%%%'
INDENT = '    '


def query_columns(
        engine: sqlalchemy.engine.Engine, query: str
) -> Tuple[str, ...]:
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


def _native_grouping_sets_query(
        engine: sqlalchemy.engine.Engine,
        query: str,
        sets: Tuple[Tuple[Column, ...], ...],
        group_columns_key: str = GROUP_COLUMNS_KEY,
        grouping_sets_key: str = GROUPING_SETS_KEY,
        grouping_id_key: str = GROUPING_ID_KEY
) -> Tuple[str, Dict[int, Tuple[Column, ...]]]:
    """
    For databases like DuckDB, Postgres, and Snowflake that natively
    support GROUPING SETs, this function generates the SQL for a
    GROUPING SETs query and assigns each set a group ID based on the
    masking logic described in the Postgres documentation (whose
    behavior seems to be shared with other databases):
    https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-GROUPING-TABLE
    """
    column_indices: Dict[Column, int] = {}
    for grouping_set in sets:
        for column in grouping_set:
            index = column_indices.get(column)
            if index is None:
                column_indices[column] = len(column_indices)

    queries = []
    set_index: Dict[int, Tuple[Column, ...]] = {}
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


def _synthetic_grouping_sets_query(
        engine: sqlalchemy.engine.Engine,
        query: str,
        sets: Tuple[Tuple[Column, ...], ...],
        group_columns_key: str = GROUP_COLUMNS_KEY,
        grouping_sets_key: str = GROUPING_SETS_KEY,
        grouping_id_key: str = GROUPING_ID_KEY
) -> Tuple[str, Dict[int, Tuple[Column, ...]]]:
    """
    For databases like SQLite and Redshift, which don't natively
    support GROUPING SETs, this function generates a synthetic `UNION
    ALL`-based version that's probably slower but equivalent in
    output.

    """
    column_indices: Dict[Column, int] = {}
    for grouping_set in sets:
        for column in grouping_set:
            index = column_indices.get(column)
            if index is None:
                column_indices[column] = len(column_indices)

    queries = []
    set_index: Dict[int, Tuple[Column, ...]] = {}
    for set_id, grouping_set in enumerate(sets):
        set_index[set_id] = grouping_set
        group_columns = [f'NULL AS {column.name}'
                         for column in column_indices.keys()]
        for column in grouping_set:
            group_columns[column_indices[column]] = column.name
        grouping_set_names = tuple(column.name for column in grouping_set)
        group_by_columns = ', '.join(grouping_set_names)
        group_by = (
            f'{"GROUP BY" if len(group_by_columns) else ""} {group_by_columns}')
        queries.append(
            query
            .replace(grouping_id_key, str(set_id))
            .replace(group_columns_key, ', '.join(group_columns))
            .replace(grouping_sets_key, group_by))
    return '\nUNION ALL\n'.join(queries), set_index


def grouping_sets_query(
        engine: sqlalchemy.engine.Engine,
        query: str,
        sets: Tuple[Tuple[Column, ...], ...],
        group_columns_key: str = GROUP_COLUMNS_KEY,
        grouping_sets_key: str = GROUPING_SETS_KEY,
        grouping_id_key: str = GROUPING_ID_KEY
) -> Tuple[str, Dict[int, Tuple[Column, ...]]]:
    """Takes a `query` like

    SELECT
        {grouping_id_key} AS grouping_id,
        {group_columns_key},
        AGGREGATE1(args), AGGREGATE2(args)
    ...
    {grouping_sets_key}
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
    if engine.url.get_backend_name() == 'sqlite':
        return _synthetic_grouping_sets_query(
            engine, query, sets, group_columns_key,
            grouping_sets_key, grouping_id_key)
    else:
        return _native_grouping_sets_query(
            engine, query, sets, group_columns_key,
            grouping_sets_key, grouping_id_key)
