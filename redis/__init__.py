# legacy imports
from redis.client import Redis, ConnectionPool
from redis.exceptions import RedisError, ConnectionError, AuthenticationError
from redis.exceptions import ResponseError, InvalidResponse, InvalidData

__version__ = '2.0.0'

__all__ = [
    'Redis', 'ConnectionPool',
    'RedisError', 'ConnectionError', 'ResponseError', 'AuthenticationError'
    'InvalidResponse', 'InvalidData',
    ]
