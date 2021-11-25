from redis.client import Redis, StrictRedis
from redis.cluster import RedisCluster
from redis.connection import (
    BlockingConnectionPool,
    ConnectionPool,
    Connection,
    SSLConnection,
    UnixDomainSocketConnection
)
from redis.sentinel import (
    Sentinel,
    SentinelConnectionPool,
    SentinelManagedConnection,
    SentinelManagedSSLConnection,
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


def int_or_str(value):
    try:
        return int(value)
    except ValueError:
        return value


__version__ = "4.1.0rc1"


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
    'RedisCluster',
    'RedisError',
    'ResponseError',
    'Sentinel',
    'SentinelConnectionPool',
    'SentinelManagedConnection',
    'SentinelManagedSSLConnection',
    'SSLConnection',
    'StrictRedis',
    'TimeoutError',
    'UnixDomainSocketConnection',
    'WatchError',
]
