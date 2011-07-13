from redis.client import Redis
from redis.connection import (
    ConnectionPool,
    Connection,
    UnixDomainSocketConnection
    )
from redis.exceptions import (
    AuthenticationError,
    ConnectionError,
    DataError,
    InvalidResponse,
    PubSubError,
    RedisError,
    ResponseError,
    WatchError,
    )


__version__ = '2.4.6'
VERSION = tuple(map(int, __version__.split('.')))

__all__ = [
    'Redis', 'ConnectionPool', 'Connection', 'UnixDomainSocketConnection',
    'RedisError', 'ConnectionError', 'ResponseError', 'AuthenticationError',
    'InvalidResponse', 'DataError', 'PubSubError', 'WatchError',
    ]
