from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Union

from redis.commands.metadata import MetadataResolver, StaticMetadataResolver
from redis.observability.attributes import CSCReason


class CacheEntryStatus(Enum):
    VALID = "VALID"
    IN_PROGRESS = "IN_PROGRESS"


class EvictionPolicyType(Enum):
    time_based = "time_based"
    frequency_based = "frequency_based"


@dataclass(frozen=True)
class CacheKey:
    """
    Represents a unique key for a cache entry.

    Attributes:
        command (str): The Redis command being cached.
        redis_keys (tuple): The Redis keys involved in the command.
        redis_args (tuple): Additional arguments for the Redis command.
            This field is included in the cache key to ensure uniqueness
            when commands have the same keys but different arguments.
            Changing this field will affect cache key uniqueness.
    """

    command: str
    redis_keys: tuple
    redis_args: tuple = ()  # Additional arguments for the Redis command; affects cache key uniqueness.


class CacheEntry:
    def __init__(
        self,
        cache_key: CacheKey,
        cache_value: bytes,
        status: CacheEntryStatus,
        connection_ref,
    ):
        self.cache_key = cache_key
        self.cache_value = cache_value
        self.status = status
        self.connection_ref = connection_ref

    def __hash__(self):
        return hash(
            (self.cache_key, self.cache_value, self.status, self.connection_ref)
        )

    def __eq__(self, other):
        return hash(self) == hash(other)


class EvictionPolicyInterface(ABC):
    @property
    @abstractmethod
    def cache(self):
        pass

    @cache.setter
    @abstractmethod
    def cache(self, value):
        pass

    @property
    @abstractmethod
    def type(self) -> EvictionPolicyType:
        pass

    @abstractmethod
    def evict_next(self) -> CacheKey:
        pass

    @abstractmethod
    def evict_many(self, count: int) -> List[CacheKey]:
        pass

    @abstractmethod
    def touch(self, cache_key: CacheKey) -> None:
        pass


class CacheConfigurationInterface(ABC):
    @abstractmethod
    def get_cache_class(self):
        pass

    @abstractmethod
    def get_max_size(self) -> int:
        pass

    @abstractmethod
    def get_eviction_policy(self):
        pass

    @abstractmethod
    def is_exceeds_max_size(self, count: int) -> bool:
        pass

    @abstractmethod
    def is_allowed_to_cache(self, command: str) -> bool:
        pass


class CacheInterface(ABC):
    @property
    @abstractmethod
    def collection(self) -> OrderedDict:
        pass

    @property
    @abstractmethod
    def config(self) -> CacheConfigurationInterface:
        pass

    @property
    @abstractmethod
    def eviction_policy(self) -> EvictionPolicyInterface:
        pass

    @property
    @abstractmethod
    def size(self) -> int:
        pass

    @abstractmethod
    def get(self, key: CacheKey) -> Union[CacheEntry, None]:
        pass

    @abstractmethod
    def set(self, entry: CacheEntry) -> bool:
        pass

    @abstractmethod
    def delete_by_cache_keys(self, cache_keys: List[CacheKey]) -> List[bool]:
        pass

    @abstractmethod
    def delete_by_redis_keys(self, redis_keys: List[bytes]) -> List[bool]:
        pass

    @abstractmethod
    def flush(self) -> int:
        pass

    @abstractmethod
    def is_cachable(self, key: CacheKey) -> bool:
        pass


class DefaultCache(CacheInterface):
    def __init__(
        self,
        cache_config: CacheConfigurationInterface,
    ) -> None:
        self._cache = OrderedDict()
        self._cache_config = cache_config
        self._eviction_policy = self._cache_config.get_eviction_policy().value()
        self._eviction_policy.cache = self

    @property
    def collection(self) -> OrderedDict:
        return self._cache

    @property
    def config(self) -> CacheConfigurationInterface:
        return self._cache_config

    @property
    def eviction_policy(self) -> EvictionPolicyInterface:
        return self._eviction_policy

    @property
    def size(self) -> int:
        return len(self._cache)

    def set(self, entry: CacheEntry) -> bool:
        if not self.is_cachable(entry.cache_key):
            return False

        self._cache[entry.cache_key] = entry
        self._eviction_policy.touch(entry.cache_key)

        return True

    def get(self, key: CacheKey) -> Union[CacheEntry, None]:
        entry = self._cache.get(key, None)

        if entry is None:
            return None

        self._eviction_policy.touch(key)
        return entry

    def delete_by_cache_keys(self, cache_keys: List[CacheKey]) -> List[bool]:
        response = []

        for key in cache_keys:
            if self.get(key) is not None:
                self._cache.pop(key)
                response.append(True)
            else:
                response.append(False)

        return response

    def delete_by_redis_keys(
        self, redis_keys: Union[List[bytes], List[str]]
    ) -> List[bool]:
        response = []
        keys_to_delete = []

        for redis_key in redis_keys:
            # Prepare both versions for lookup
            candidates = [redis_key]
            if isinstance(redis_key, str):
                candidates.append(redis_key.encode("utf-8"))
            elif isinstance(redis_key, bytes):
                try:
                    candidates.append(redis_key.decode("utf-8"))
                except UnicodeDecodeError:
                    pass  # Non-UTF-8 bytes, skip str version

            for cache_key in self._cache:
                if any(candidate in cache_key.redis_keys for candidate in candidates):
                    keys_to_delete.append(cache_key)
                    response.append(True)

        for key in keys_to_delete:
            self._cache.pop(key)

        return response

    def flush(self) -> int:
        elem_count = len(self._cache)
        self._cache.clear()
        return elem_count

    def is_cachable(self, key: CacheKey) -> bool:
        return self._cache_config.is_allowed_to_cache(key.command)


class CacheProxy(CacheInterface):
    """
    Proxy object that wraps cache implementations to enable additional logic on top
    """

    def __init__(self, cache: CacheInterface):
        self._cache = cache

    @property
    def collection(self) -> OrderedDict:
        return self._cache.collection

    @property
    def config(self) -> CacheConfigurationInterface:
        return self._cache.config

    @property
    def eviction_policy(self) -> EvictionPolicyInterface:
        return self._cache.eviction_policy

    @property
    def size(self) -> int:
        return self._cache.size

    def get(self, key: CacheKey) -> Union[CacheEntry, None]:
        return self._cache.get(key)

    def set(self, entry: CacheEntry) -> bool:
        is_set = self._cache.set(entry)

        if self.config.is_exceeds_max_size(self.size):
            # Lazy import to avoid circular dependency
            from redis.observability.recorder import record_csc_eviction

            record_csc_eviction(
                count=1,
                reason=CSCReason.FULL,
            )
            self.eviction_policy.evict_next()

        return is_set

    def delete_by_cache_keys(self, cache_keys: List[CacheKey]) -> List[bool]:
        return self._cache.delete_by_cache_keys(cache_keys)

    def delete_by_redis_keys(self, redis_keys: List[bytes]) -> List[bool]:
        return self._cache.delete_by_redis_keys(redis_keys)

    def flush(self) -> int:
        return self._cache.flush()

    def is_cachable(self, key: CacheKey) -> bool:
        return self._cache.is_cachable(key)


class LRUPolicy(EvictionPolicyInterface):
    def __init__(self):
        self.cache = None

    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, cache: CacheInterface):
        self._cache = cache

    @property
    def type(self) -> EvictionPolicyType:
        return EvictionPolicyType.time_based

    def evict_next(self) -> CacheKey:
        self._assert_cache()
        popped_entry = self._cache.collection.popitem(last=False)
        return popped_entry[0]

    def evict_many(self, count: int) -> List[CacheKey]:
        self._assert_cache()
        if count > len(self._cache.collection):
            raise ValueError("Evictions count is above cache size")

        popped_keys = []

        for _ in range(count):
            popped_entry = self._cache.collection.popitem(last=False)
            popped_keys.append(popped_entry[0])

        return popped_keys

    def touch(self, cache_key: CacheKey) -> None:
        self._assert_cache()

        if self._cache.collection.get(cache_key) is None:
            raise ValueError("Given entry does not belong to the cache")

        self._cache.collection.move_to_end(cache_key)

    def _assert_cache(self):
        if self.cache is None or not isinstance(self.cache, CacheInterface):
            raise ValueError("Eviction policy should be associated with valid cache.")


class EvictionPolicy(Enum):
    LRU = LRUPolicy


class CacheConfig(CacheConfigurationInterface):
    DEFAULT_CACHE_CLASS = DefaultCache
    DEFAULT_EVICTION_POLICY = EvictionPolicy.LRU
    DEFAULT_MAX_SIZE = 10000

    # DEPRECATED - no longer consulted, and it will be removed in a future release.
    #
    # Command eligibility is now decided from command metadata by the metadata resolver this
    # config holds, so this list no longer describes what gets cached. It is kept as a public
    # attribute only so an external caller reading it keeps working; editing it changes
    # nothing. The effective set it is replaced by differs from it by ``+FT.SUGGET``,
    # ``+FT.SUGLEN`` and ``-XPENDING``, ``-TS.INFO``, ``-XREAD`` - the three removals being
    # commands the server itself reports as not cacheable.
    #
    # To change eligibility, edit ``redis.commands.metadata._STATIC_COMMAND_METADATA`` or pass
    # a ``metadata_resolver`` to the client. Nothing here.
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

    def __init__(
        self,
        max_size: int = DEFAULT_MAX_SIZE,
        cache_class: Any = DEFAULT_CACHE_CLASS,
        eviction_policy: EvictionPolicy = DEFAULT_EVICTION_POLICY,
    ):
        self._cache_class = cache_class
        self._max_size = max_size
        self._eviction_policy = eviction_policy
        # Defaulted here rather than taken as a constructor argument: eligibility is
        # configured at client level, through the ``metadata_resolver`` of the client or the
        # pool, which injects it below. Defaulting it means a config built standalone - in a
        # test, or by a user who configures nothing else - is fully functional, and decides
        # eligibility from the command metadata this library ships.
        self._metadata_resolver: MetadataResolver = StaticMetadataResolver()

    def set_metadata_resolver(self, metadata_resolver: MetadataResolver) -> None:
        """
        Set the metadata resolver that decides which commands may be cached.

        Called by the connection pool with the client-level resolver, so that one object
        serves both cluster routing and cache eligibility. Deliberately not part of
        :class:`CacheConfigurationInterface`: that ABC is public and implemented by third
        parties, so a custom configuration keeps whatever eligibility logic it has.

        Which object the pool calls this on depends on how the cache was supplied, and the
        difference is observable to a caller who reuses one configuration:

        - Given ``cache_config=``, the pool copies the configuration first, so the caller's
          object keeps the resolver it had and two clients sharing it stay independent. A
          later call to this method on the caller's object does not reach a pool already
          built from it.
        - Given ``cache=`` or ``cache_factory=``, the caller supplied a whole cache that
          reads its configuration on every lookup and cannot be handed a different one, so
          the pool calls this on the configuration inside it. One configuration reused that
          way therefore ends up with whichever resolver was injected last.

        Args:
            metadata_resolver: The resolver to decide eligibility through.
        """
        self._metadata_resolver = metadata_resolver

    def get_cache_class(self):
        return self._cache_class

    def get_max_size(self) -> int:
        return self._max_size

    def get_eviction_policy(self) -> EvictionPolicy:
        return self._eviction_policy

    def is_exceeds_max_size(self, count: int) -> bool:
        return count > self._max_size

    def is_allowed_to_cache(self, command: str) -> bool:
        # Fails closed on everything the resolver cannot decide: an unknown command, a name
        # the record tables cannot be keyed by, and a record built from incomplete metadata
        # all resolve to False. The verdict is memoized per command name, so this is a dict
        # hit on the command execution path.
        return self._metadata_resolver.is_cacheable(command)


class CacheFactoryInterface(ABC):
    @abstractmethod
    def get_cache(self) -> CacheInterface:
        pass


class CacheFactory(CacheFactoryInterface):
    def __init__(self, cache_config: Optional[CacheConfig] = None):
        self._config = cache_config

        if self._config is None:
            self._config = CacheConfig()

    def get_cache(self) -> CacheInterface:
        cache_class = self._config.get_cache_class()
        return CacheProxy(cache_class(cache_config=self._config))
