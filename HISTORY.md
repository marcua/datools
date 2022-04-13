# History

## 0.1.5 (2022-04-13)
* Support for PostgreSQL! The test suite now runs against PostgreSQL, and `datools.explanations.diff` now allows you to ask "why" about data stored in Postgres. Get excited!
* `datools.sqlalchemy_utils.grouping_sets_query` will now generate a GROUPING SETs query for databases that support grouping sets (e.g., Postgres, DuckDB) or the equivalent UNION ALL version for databases without grouping sets support (e.g., SQLite). For more, check out the [example in the docs](https://datools.readthedocs.io/en/latest/examples.html#grouping-sets-query).

## 0.1.4 (2022-02-27)
* Python 3.10 support.
* Updated test suite to run tests against multiple databases, in particular expanding from SQLite only to DuckDB and SQLite.
* As a result of the last bullet, ensured code runs against DuckDB in addition to SQLite.
* First stab at documentation (https://datools.readthedocs.io/en/latest/).


## 0.1.3 (2021-12-31)
* Introduced mypy to linting and CI to ensure code that makes it to `main` has proper types.
* Created first working example of [DIFF working on a real-world dataset as a Jupyter notebook](https://github.com/marcua/datools/blob/main/examples/diff/intel-sensor.ipynb). This example partially replicates the Scorpion paper when only moteid/sensorids are considered.
* Separated the `on_columns` argument of `diff` into `on_column_values` (columns for which you want to generate equality predicates as explanations) and and `on_column_ranges` (columns for which you want to generate range predicates as explanations after bucketing the ranges into 15 equi-sized buckets).

## 0.1.2 (2021-11-07)
* First release of DIFF algorithm implementation.

## 0.1.0 (2021-05-09)
* First release on PyPI.
