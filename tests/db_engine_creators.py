from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from testing.postgresql import PostgresqlFactory


class DbEngineCreator:

    def get_engine(self) -> Engine:
        raise NotImplementedError()

    def teardown_engine(self):
        pass


class SqliteEngineCreator(DbEngineCreator):

    def get_engine(self) -> Engine:
        return create_engine('sqlite://')


class DuckDbEngineCreator(DbEngineCreator):

    def get_engine(self) -> Engine:
        return create_engine('duckdb:///:memory:')


class PostgresqlEngineCreator(DbEngineCreator):

    # Lazy-loaded postgres factory: we only create it on the first
    # instantiation of a PostgresqlEngineCreator.
    POSTGRESQL_FACTORY = None

    def __init__(self):
        if PostgresqlEngineCreator.POSTGRESQL_FACTORY is None:
            PostgresqlEngineCreator.POSTGRESQL_FACTORY = PostgresqlFactory(
                cache_initialized_db=True)
        self.postgresql = PostgresqlEngineCreator.POSTGRESQL_FACTORY()

    def get_engine(self) -> Engine:
        return create_engine(self.postgresql.url())

    def teardown_engine(self):
        self.postgresql.stop()
