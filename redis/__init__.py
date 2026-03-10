from redis import asyncio  # noqa
from redis.backoff import default_backoff
from redis.client import Redis, StrictRedis
from redis.driver_info import DriverInfo
from redis.cluster import RedisCluster
from redis.connection import (
    BlockingConnectionPool,
    Connection,
    ConnectionPool,
    SSLConnection,
    UnixDomainSocketConnection,
)
from redis.credentials import CredentialProvider, UsernamePasswordCredentialProvider
from redis.keyspace_notifications import (
    ClusterKeyspaceNotifications,
    EventType,
    KeyeventChannel,
    KeyNotification,
    KeyspaceChannel,
    NotifyKeyspaceEvents,
    is_keyevent_channel,
    is_keyspace_channel,
    is_keyspace_notification_channel,
)
from redis.exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ChildDeadlockedError,
    ConnectionError,
    CrossSlotTransactionError,
    DataError,
    InvalidPipelineStack,
    InvalidResponse,
    MaxConnectionsError,
    OutOfMemoryError,
    PubSubError,
    ReadOnlyError,
    RedisClusterException,
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


def int_or_str(value):
    try:
        return int(value)
    except ValueError:
        return value


__version__ = "7.1.1"

VERSION = tuple(map(int_or_str, __version__.split(".")))


__all__ = [
    "AuthenticationError",
    "AuthenticationWrongNumberOfArgsError",
    "BlockingConnectionPool",
    "BusyLoadingError",
    "ChildDeadlockedError",
    "ClusterKeyspaceNotifications",
    "Connection",
    "ConnectionError",
    "ConnectionPool",
    "CredentialProvider",
    "CrossSlotTransactionError",
    "DataError",
    "DriverInfo",
    "EventType",
    "from_url",
    "default_backoff",
    "InvalidPipelineStack",
    "InvalidResponse",
    "is_keyevent_channel",
    "is_keyspace_channel",
    "is_keyspace_notification_channel",
    "KeyeventChannel",
    "KeyNotification",
    "KeyspaceChannel",
    "NotifyKeyspaceEvents",
    "MaxConnectionsError",
    "OutOfMemoryError",
    "PubSubError",
    "ReadOnlyError",
    "Redis",
    "RedisCluster",
    "RedisClusterException",
    "RedisError",
    "ResponseError",
    "Sentinel",
    "SentinelConnectionPool",
    "SentinelManagedConnection",
    "SentinelManagedSSLConnection",
    "SSLConnection",
    "UsernamePasswordCredentialProvider",
    "StrictRedis",
    "TimeoutError",
    "UnixDomainSocketConnection",
    "WatchError",
]
