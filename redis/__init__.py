# legacy imports
from redis.client import Redis, ConnectionPool
from redis.exceptions import (
    AuthenticationError,
    ConnectionError,
    DataError,
    InvalidResponse,
    RedisError,
    ResponseError,
    )


__version__ = '2.2.3'

__all__ = [
    'Redis', 'ConnectionPool',
    'RedisError', 'ConnectionError', 'ResponseError', 'AuthenticationError',
    'InvalidResponse', 'DataError',
    ]
