from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Hashable

from cachetools import Cache, LFUCache, LRUCache, RRCache, TTLCache


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


class EvictionPolicyCacheClass(Enum):
    LRU = LRUCache
    LFU = LFUCache
    RANDOM = RRCache
    TTL = TTLCache


class CacheClassEvictionPolicy(Enum):
    LRUCache = EvictionPolicy.LRU
    LFUCache = EvictionPolicy.LFU
    RRCache = EvictionPolicy.RANDOM
    TTLCache = EvictionPolicy.TTL


class CacheInterface(ABC):

    @property
    @abstractmethod
    def currsize(self) -> float:
        pass

    @property
    @abstractmethod
    def maxsize(self) -> float:
        pass

    @property
    @abstractmethod
    def eviction_policy(self) -> EvictionPolicy:
        pass

    @abstractmethod
    def get(self, key: Hashable, default: Any = None):
        pass

    @abstractmethod
    def set(self, key: Hashable, value: Any):
        pass

    @abstractmethod
    def exists(self, key: Hashable) -> bool:
        pass

    @abstractmethod
    def remove(self, key: Hashable):
        pass

    @abstractmethod
    def clear(self):
        pass


class CacheFactoryInterface(ABC):
    @abstractmethod
    def get_cache(self) -> CacheInterface:
        pass


class CacheToolsFactory(CacheFactoryInterface):
    def __init__(self, conf: CacheConfiguration):
        self._conf = conf

    def get_cache(self) -> CacheInterface:
        eviction_policy = self._conf.get_eviction_policy()
        cache_class = self._get_cache_class(eviction_policy).value

        if eviction_policy == EvictionPolicy.TTL:
            cache_inst = cache_class(self._conf.get_max_size(), self._conf.get_ttl())
        else:
            cache_inst = cache_class(self._conf.get_max_size())

        return CacheToolsAdapter(cache_inst)

    def _get_cache_class(
        self, eviction_policy: EvictionPolicy
    ) -> EvictionPolicyCacheClass:
        return EvictionPolicyCacheClass[eviction_policy.value]


class CacheToolsAdapter(CacheInterface):
    def __init__(self, cache: Cache):
        self._cache = cache

    def get(self, key: Hashable, default: Any = None):
        return self._cache.get(key, default)

    def set(self, key: Hashable, value: Any):
        self._cache[key] = value

    def exists(self, key: Hashable) -> bool:
        return key in self._cache

    def remove(self, key: Hashable):
        self._cache.pop(key)

    def clear(self):
        self._cache.clear()

    @property
    def currsize(self) -> float:
        return self._cache.currsize

    @property
    def maxsize(self) -> float:
        return self._cache.maxsize

    @property
    def eviction_policy(self) -> EvictionPolicy:
        return CacheClassEvictionPolicy[self._cache.__class__.__name__].value
