import pytest

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


DB_TYPE_TO_ENGINE_STRING = {
    'sqlite': 'sqlite://',
    'duckdb': 'duckdb:///:memory:'
}


def pytest_addoption(parser):
    parser.addoption('--db-type',
                     required=True,
                     choices=DB_TYPE_TO_ENGINE_STRING.keys())


@pytest.fixture(scope='function')
def db_engine(pytestconfig) -> Engine:
    engine_string = DB_TYPE_TO_ENGINE_STRING[pytestconfig.getoption('db_type')]
    return create_engine(engine_string)
