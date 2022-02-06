from datetime import datetime
from typing import Union

from sqlalchemy.engine import Engine


def engine_based_datetime(engine: Engine, string: str) -> Union[str, datetime]:
    """
    If the engine is sqlite (which represents datetimes as strings), we
    return a string representation of a datetime. If it's anything else,
    we return a Pythonic version.
    """
    if engine.url.get_backend_name() == 'sqlite':
        return string

    return datetime.strptime(string, '%Y-%m-%d %H:%M:%S.%f')
