# legacy imports
from redis.old_client import Redis, RedisError, ConnectionError
from redis.old_client import ResponseError, InvalidResponse, InvalidData

__all__ = [
    'Redis', 'RedisError', 'ConnectionError', # legacy
    'ResponseError', 'InvalidResponse', 'InvalidData' # legacy
    ]
