from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy.engine import Engine
from sqlalchemy.schema import Column
from sqlalchemy.schema import Table


def generate_testdb() -> Engine:
    """Create a test DB from Table 1 of the Scorpion paper.

    Rather than make sensor_id a foreign key, we make it a character
    type to test a wider variety of column types.
    """
    engine = create_engine('sqlite://')
    conn = engine.connect()
    metadata = MetaData()
    sensor_readings = Table(
        'sensor_readings', metadata,
        Column('id', Integer, primary_key=True),
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
    conn.execute(sensor_readings.insert(), [
        {'created_at': created_at,
         'sensor_id': sensor_id,
         'voltage': voltage,
         'humidity': humidity,
         'temperature': temperature}
        for (created_at, sensor_id, voltage, humidity, temperature) in values
    ])
    return engine
    
