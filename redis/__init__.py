import sys

from redis.client import Redis, StrictRedis
from redis.cluster import RedisCluster
from redis.connection import (
    BlockingConnectionPool,
    Connection,
    ConnectionPool,
    SSLConnection,
    UnixDomainSocketConnection,
)
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
    WatchError,
)
from redis.sentinel import (
    Sentinel,
    SentinelConnectionPool,
    SentinelManagedConnection,
    SentinelManagedSSLConnection,
)
from redis.utils import from_url

if sys.version_info >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata


def int_or_str(value):
    try:
        return int(value)
    except ValueError:
        return value


try:
    __version__ = metadata.version("redis")
except metadata.PackageNotFoundError:
    __version__ = "99.99.99"


VERSION = tuple(map(int_or_str, __version__.split(".")))

__all__ = [
    "AuthenticationError",
    "AuthenticationWrongNumberOfArgsError",
    "BlockingConnectionPool",
    "BusyLoadingError",
    "ChildDeadlockedError",
    "Connection",
    "ConnectionError",
    "ConnectionPool",
    "DataError",
    "from_url",
    "InvalidResponse",
    "PubSubError",
    "ReadOnlyError",
    "Redis",
    "RedisCluster",
    "RedisError",
    "ResponseError",
    "Sentinel",
    "SentinelConnectionPool",
    "SentinelManagedConnection",
    "SentinelManagedSSLConnection",
    "SSLConnection",
    "StrictRedis",
    "TimeoutError",
    "UnixDomainSocketConnection",
    "WatchError",
]
