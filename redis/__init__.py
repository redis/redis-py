from redis.client import Redis, StrictRedis
from redis.connection import (
    BlockingConnectionPool,
    ConnectionPool,
    Connection,
    SSLConnection,
    UnixDomainSocketConnection
)
from redis.utils import from_url
from redis.exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ChildDeadlockedError,
    ConnectionError,
    DataError,
    InvalidResponse,
    PubSubError,
    ReadOnlyError,
    RedisError,
    ResponseError,
    TimeoutError,
    WatchError
)
import pkg_resources


def int_or_str(value):
    try:
        return int(value)
    except ValueError:
        return value


try:
    __version__ = pkg_resources.get_distribution('redis').version
except pkg_resources.DistributionNotFound:
    __version__ = "99.99.99"

VERSION = tuple(map(int_or_str, __version__.split('.')))

__all__ = [
    'AuthenticationError',
    'AuthenticationWrongNumberOfArgsError',
    'BlockingConnectionPool',
    'BusyLoadingError',
    'ChildDeadlockedError',
    'Connection',
    'ConnectionError',
    'ConnectionPool',
    'DataError',
    'from_url',
    'InvalidResponse',
    'PubSubError',
    'ReadOnlyError',
    'Redis',
    'RedisError',
    'ResponseError',
    'SSLConnection',
    'StrictRedis',
    'TimeoutError',
    'UnixDomainSocketConnection',
    'WatchError',
]
