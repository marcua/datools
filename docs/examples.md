# Examples

## `diff`
We'll add more examples, but the best places to look for now are:
* [The blog post that introduces data diffing](https://blog.marcua.net/2022/02/20/data-diffs-algorithms-for-explaining-what-changed-in-a-dataset.html), and
* [A Jupyter Notebook showing an end-to-end example](https://github.com/marcua/datools/blob/main/examples/diff/intel-sensor.ipynb).

## `grouping_sets_query`

[Grouping
sets](https://www.geeksforgeeks.org/postgresql-grouping-sets/) are a
neat feature of some databases that allow you to GROUP BY multiple
combinations of columns in a single pass over your data. Some
databases, like PostgreSQL and DuckDB, support them natively, whereas
others, like SQLite, don't. `datools.sqlalchemy.grouping_sets_query`
will generate a GROUPING SETs query if your database allows it or
create a synthetic equivalent using a UNION ALL of several GROUP BY
queries.

This is concept best explained by example, and we'll use the [test
suite](https://github.com/marcua/datools/blob/14752f0e841a89a9c991bc9893e58d3b708cac7d/tests/test_sqlalchemy_utils.py#L15)
for our example. Say you have an underlying relation like `SELECT *
FROM sensor readings`, and you want to `COUNT(*)` across multiple
combinations of `created_at` and `sensor_id`. In datools, you'd write:

```python
from datools.sqlalchemy_utils import grouping_sets_query
query, set_index = grouping_sets_query(
    db_engine,
    'SELECT * FROM sensor_readings',
    (
        (Column('created_at'), Column('sensor_id')),
        (Column('created_at'),),
        (Column('sensor_id'),),
        (),
    ),
    (Aggregate(AggregateFunction.COUNT, Column('*'), Column('num_rows')), )
)

print('Query:', query)
print('Set index:', set_index)
```


On PostgreSQL (which supports GROUPING SETS), this would result in:
```sql
Query:
WITH query AS (SELECT * FROM sensor_readings)
SELECT
    GROUPING(created_at, sensor_id) AS grouping_id,
    created_at, sensor_id,
    COUNT(*) AS num_rows
FROM query
GROUP BY GROUPING SETS ((created_at, sensor_id), (created_at), (sensor_id), ())
```

```python
Set index: {7: (Column(name='created_at'), Column(name='sensor_id')), 11: (Column(name='created_at'),), 13: (Column(name='sensor_id'),), 14: ()}
```




On SQLite (which doesn't support GROUPING SETS), this would result in:
```sql
Query:
WITH query AS (SELECT * FROM sensor_readings)

SELECT
    0 AS grouping_id,
    created_at, sensor_id,
    COUNT(*) AS num_rows
FROM query
GROUP BY created_at, sensor_id

UNION ALL

SELECT
    1 AS grouping_id,
    created_at, NULL AS sensor_id,
    COUNT(*) AS num_rows
FROM query
GROUP BY created_at

UNION ALL

SELECT
    2 AS grouping_id,
    NULL AS created_at, sensor_id,
    COUNT(*) AS num_rows
FROM query
GROUP BY sensor_id

UNION ALL

SELECT
    3 AS grouping_id,
    NULL AS created_at, NULL AS sensor_id,
    COUNT(*) AS num_rows
FROM query
```

```python
Set index: {0: (Column(name='created_at'), Column(name='sensor_id')), 1: (Column(name='created_at'),), 2: (Column(name='sensor_id'),), 3: ()}
```
