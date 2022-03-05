import pytest

from sqlalchemy.engine import Engine

from .db_engine_creators import DuckDbEngineCreator
from .db_engine_creators import PostgresqlEngineCreator
from .db_engine_creators import SqliteEngineCreator


DB_TYPE_TO_ENGINE_STRING = {
    'duckdb': DuckDbEngineCreator,
    'sqlite': SqliteEngineCreator,
    'postgresql': PostgresqlEngineCreator
}


def pytest_addoption(parser):
    parser.addoption('--db-type',
                     required=True,
                     choices=DB_TYPE_TO_ENGINE_STRING.keys())


@pytest.fixture(scope='function')
def db_engine(pytestconfig) -> Engine:
    creator = DB_TYPE_TO_ENGINE_STRING[pytestconfig.getoption('db_type')]()
    yield creator.get_engine()
    creator.teardown_engine()
