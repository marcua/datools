=======
History
=======

0.1.3 (2021-12-31)
------------------

* Introduced mypy to linting and CI to ensure code that makes it to `main` has proper types.
* Created first working example of [DIFF working on a real-world dataset as a Jupyter notebook](https://github.com/marcua/datools/blob/main/examples/diff/intel-sensor.ipynb). This example partially replicates the Scorpion paper when only moteid/sensorids are considered.
* Separated the `on_columns` argument of `diff` into `on_column_values` (columns for which you want to generate equality predicates as explanations) and and `on_column_ranges` (columns for which you want to generate range predicates as explanations after bucketing the ranges into 15 equi-sized buckets).

0.1.2 (2021-11-07)
------------------

* First release of DIFF algorithm implementation.

0.1.0 (2021-05-09)
------------------

* First release on PyPI.
