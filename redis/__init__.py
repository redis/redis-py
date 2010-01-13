# legacy imports
from redis.old_client import Redis
from redis.exceptions import RedisError, ConnectionError
from redis.exceptions import ResponseError, InvalidResponse, InvalidData

__all__ = [
    'Redis', # legacy
    'RedisError', 'ConnectionError', 'ResponseError', # exceptions
    'InvalidResponse', 'InvalidData', # exceptions
    ]
