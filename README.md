# `datools` 
[![Documentation status](https://readthedocs.org/projects/datools/badge/?version=latest)](https://datools.readthedocs.io/en/latest/?version=latest) [![PyPi link](https://img.shields.io/pypi/v/datools.svg)](https://pypi.python.org/pypi/datools) [![Build status](https://github.com/marcua/datools/actions/workflows/python-tests.yml/badge.svg)](https://github.com/marcua/datools/actions/workflows/python-tests.yml) [![Apache 2.0 License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/marcua/datools/blob/main/LICENSE)

## Introduction
`datools` is a collection of Python-based tools for working with data in relational databases. While it contains several utilities for smoothing the rough edges of SQL, its most baked component is `datools.diff`, an algorithm that's best explained in a [blog post](https://blog.marcua.net/2022/02/20/data-diffs-algorithms-for-explaining-what-changed-in-a-dataset.html) and [Jupyter Notebook](https://github.com/marcua/datools/blob/main/examples/diff/intel-sensor.ipynb).

To learn more, [read the docs](https://datools.readthedocs.io/en/latest/index.html) or [reach out](https://twitter.com/marcua/).

## Database support

While `datools` generates SQL for its operations, different databases
have their nuances. `datools` may run on your database today, but in
an attempt to give you some certainty as to databases we know it has
successfully run on, we run all tests in the test suite against the
following databases:

| Database      | Evaluated by test suite |
| ----------- | ----------- |
| SQLite      | Since v0.1.2 |
| DuckDB   | Since v0.1.4 |
| PostgreSQL   | Since v0.1.5 |
| Redshift, Snowflake   | *You provide an instance, I'll make the tests pass* |
