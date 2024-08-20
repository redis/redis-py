from abc import ABC, abstractmethod
from typing import TypeVar
from enum import Enum

from cachetools import LRUCache, LFUCache, RRCache, Cache, TTLCache

T = TypeVar("T")


class EvictionPolicy(Enum):
    LRU = "LRU"
    LFU = "LFU"
    RANDOM = "RANDOM"
    TTL = "TTL"


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
        self._max_size = kwargs.get("cache_size", None)
        self._ttl = kwargs.get("cache_ttl", None)
        self._eviction_policy = kwargs.get("cache_eviction", None)
        if self._max_size is None:
            self._max_size = 10000
        if self._ttl is None:
            self._ttl = 0
        if self._eviction_policy is None:
            self._eviction_policy = EvictionPolicy.LRU

        if self._eviction_policy not in EvictionPolicy:
            raise ValueError(f"Invalid eviction_policy {self._eviction_policy}")

    def get_ttl(self) -> int:
        return self._ttl

    def get_max_size(self) -> int:
        return self._max_size

    def get_eviction_policy(self) -> EvictionPolicy:
        return self._eviction_policy

    def is_exceeds_max_size(self, count: int) -> bool:
        return count > self._max_size

    def is_allowed_to_cache(self, command: str) -> bool:
        return command in self.DEFAULT_ALLOW_LIST


class CacheClass(Enum):
    LRU = LRUCache
    LFU = LFUCache
    RANDOM = RRCache
    TTL = TTLCache


class CacheFactoryInterface(ABC):
    @abstractmethod
    def get_cache(self) -> Cache:
        pass


class CacheFactory(CacheFactoryInterface):
    def __init__(self, conf: CacheConfiguration):
        self._conf = conf

    def get_cache(self) -> Cache:
        eviction_policy = self._conf.get_eviction_policy()
        cache_class = self._get_cache_class(eviction_policy).value

        if eviction_policy == EvictionPolicy.TTL:
            return cache_class(self._conf.get_max_size(), self._conf.get_ttl())

        return cache_class(self._conf.get_max_size())

    def _get_cache_class(self, eviction_policy: EvictionPolicy) -> CacheClass:
        return CacheClass[eviction_policy.value]
