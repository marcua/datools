import pytest

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

@pytest.fixture(scope='function')
def db_engine() -> Engine:
    return create_engine('sqlite://')
