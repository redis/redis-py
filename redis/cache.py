from typing import Callable, TypeVar, Any, NoReturn, List, Union
from typing import Optional
from enum import Enum

from cachetools import TTLCache, Cache, LRUCache
from cachetools.keys import hashkey

from redis.typing import ResponseT

T = TypeVar('T')


class EvictionPolicy(Enum):
    LRU = "lru"
    LFU = "lfu"
    RANDOM = "random"


class CacheConfiguration:
    DEFAULT_EVICTION_POLICY = EvictionPolicy.LRU

    DEFAULT_ALLOW_LIST = [
        "BITCOUNT",
        "BITFIELD_RO",
        "BITPOS",
        "EXISTS",
        "GEODIST",
        "GEOHASH",
        "GEOPOS",
        "GEORADIUSBYMEMBER_RO",
        "GEORADIUS_RO",
        "GEOSEARCH",
        "GET",
        "GETBIT",
        "GETRANGE",
        "HEXISTS",
        "HGET",
        "HGETALL",
        "HKEYS",
        "HLEN",
        "HMGET",
        "HSTRLEN",
        "HVALS",
        "JSON.ARRINDEX",
        "JSON.ARRLEN",
        "JSON.GET",
        "JSON.MGET",
        "JSON.OBJKEYS",
        "JSON.OBJLEN",
        "JSON.RESP",
        "JSON.STRLEN",
        "JSON.TYPE",
        "LCS",
        "LINDEX",
        "LLEN",
        "LPOS",
        "LRANGE",
        "MGET",
        "SCARD",
        "SDIFF",
        "SINTER",
        "SINTERCARD",
        "SISMEMBER",
        "SMEMBERS",
        "SMISMEMBER",
        "SORT_RO",
        "STRLEN",
        "SUBSTR",
        "SUNION",
        "TS.GET",
        "TS.INFO",
        "TS.RANGE",
        "TS.REVRANGE",
        "TYPE",
        "XLEN",
        "XPENDING",
        "XRANGE",
        "XREAD",
        "XREVRANGE",
        "ZCARD",
        "ZCOUNT",
        "ZDIFF",
        "ZINTER",
        "ZINTERCARD",
        "ZLEXCOUNT",
        "ZMSCORE",
        "ZRANGE",
        "ZRANGEBYLEX",
        "ZRANGEBYSCORE",
        "ZRANK",
        "ZREVRANGE",
        "ZREVRANGEBYLEX",
        "ZREVRANGEBYSCORE",
        "ZREVRANK",
        "ZSCORE",
        "ZUNION",
    ]

    def __init__(self, **kwargs):
        self._max_size = kwargs.get("cache_size", 10000)
        self._ttl = kwargs.get("cache_ttl", 0)
        self._eviction_policy = kwargs.get("eviction_policy", self.DEFAULT_EVICTION_POLICY)

    def get_ttl(self) -> int:
        return self._ttl

    def get_eviction_policy(self) -> EvictionPolicy:
        return self._eviction_policy

    def is_exceeds_max_size(self, count: int) -> bool:
        return count > self._max_size

    def is_allowed_to_cache(self, command: str) -> bool:
        return command in self.DEFAULT_ALLOW_LIST