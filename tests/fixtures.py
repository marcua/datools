from datetime import datetime

from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Sequence
from sqlalchemy.engine import Engine
from sqlalchemy.schema import Column
from sqlalchemy.schema import Table


def _primary_key_column(
        engine: Engine, column_name: str, table_name: str
) -> Column:
    if engine.url.get_backend_name() == 'duckdb':
        # DuckDB needs a little help with primary keys:
        # https://github.com/Mause/duckdb_engine#auto-incrementing-id-columns
        sensor_id_seq = Sequence(f'{table_name}_id_seq')
        id_column = Column(column_name,
                           Integer,
                           sensor_id_seq,
                           server_default=sensor_id_seq.next_value(),
                           primary_key=True)
    else:
        id_column = Column(column_name, Integer, primary_key=True)

    return id_column


def generate_scorpion_testdb(engine: Engine):
    """Create a test DB from Table 1 of the Scorpion paper (Wu & Madden,
    VLDB 2013).

    Rather than make sensor_id a foreign key, we make it a character
    type to test a wider variety of column types.
    """
    metadata = MetaData(engine)
    sensor_readings = Table(
        'sensor_readings',
        metadata,
        _primary_key_column(engine, 'id', 'sensor_readings'),
        Column('created_at', DateTime, nullable=False),
        Column('sensor_id', String, nullable=False),
        Column('voltage', Float, nullable=False),
        Column('humidity', Float, nullable=False),
        Column('temperature', Float, nullable=False),
    )
    metadata.create_all(engine)
    values = [
        (datetime(2021, 5, 5, 11), '1', 2.64, 0.4, 34),
        (datetime(2021, 5, 5, 11), '2', 2.65, 0.5, 35),
        (datetime(2021, 5, 5, 11), '3', 2.63, 0.4, 35),
        (datetime(2021, 5, 5, 12), '1', 2.7, 0.3, 35),
        (datetime(2021, 5, 5, 12), '2', 2.7, 0.5, 35),
        (datetime(2021, 5, 5, 12), '3', 2.3, 0.4, 100),
        (datetime(2021, 5, 5, 13), '1', 2.7, 0.3, 35),
        (datetime(2021, 5, 5, 13), '2', 2.7, 0.5, 35),
        (datetime(2021, 5, 5, 13), '3', 2.3, 0.5, 80),
    ]
    with engine.connect() as conn:
        conn.execute(sensor_readings.insert(), [
            dict(zip(
                ('created_at', 'sensor_id',
                 'voltage', 'humidity', 'temperature'),
                value))
            for value in values
        ])


def generate_synthetic_testdb(engine: Engine):
    """Create a synthetic database with several data types and
    distributions.
    For various data types (datetime, integer, string, float), we
    generate distributions that are:
    * Unique overall (every value is different)
    * The same overall (every value is the same)
    * Unique within a bucket (but repeats across buckets).
    """
    metadata = MetaData(engine)
    synthetic_data = Table(
        'synthetic_data',
        metadata,
        _primary_key_column(engine, 'id', 'synthetic_data'),
        Column('same_datetime', DateTime, nullable=False),
        Column('unique_datetime', DateTime, nullable=False),
        Column('bucket_unique_datetime', DateTime, nullable=False),
        Column('same_string', String, nullable=False),
        Column('unique_string', String, nullable=False),
        Column('bucket_unique_string', String, nullable=False),
        Column('same_float', Float, nullable=False),
        Column('unique_float', Float, nullable=False),
        Column('bucket_unique_float', Float, nullable=False),
        Column('same_int', Integer, nullable=False),
        Column('unique_int', Integer, nullable=False),
        Column('bucket_unique_int', Integer, nullable=False),
    )
    metadata.create_all(engine)
    values = []
    for bucket in range(1, 20):
        for row in range(1, 10):
            values.append((
                datetime(2021, 5, 5, 11),
                datetime(2021, 5, bucket, row),
                datetime(2021, 5, 5, row),
                'hi',
                f'{bucket}-{row}',
                f'{row}',
                1.1,
                bucket * 1.0 + (row / 100.0),
                row * 1.0,
                1,
                bucket * 1000 + row,
                row
            ))
    with engine.connect() as conn:
        conn.execute(synthetic_data.insert(), [
            dict(zip(
                ('same_datetime', 'unique_datetime', 'bucket_unique_datetime',
                 'same_string', 'unique_string', 'bucket_unique_string',
                 'same_float', 'unique_float', 'bucket_unique_float',
                 'same_int', 'unique_int', 'bucket_unique_int'), value))
            for value in values
        ])
