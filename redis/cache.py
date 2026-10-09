import logging
import queue
import threading
import warnings
import weakref
from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from redis.commands.metadata import MetadataResolver, StaticMetadataResolver
from redis.exceptions import ConnectionError, TimeoutError
from redis.observability.attributes import CSCReason, CSCRefreshResult
from redis.observability.recorder import record_csc_refresh
from redis.utils import SENTINEL

logger = logging.getLogger(__name__)


class CacheEntryStatus(Enum):
    VALID = "VALID"
    IN_PROGRESS = "IN_PROGRESS"


class EvictionPolicyType(Enum):
    time_based = "time_based"
    frequency_based = "frequency_based"


class TrackingMode(Enum):
    """
    How a cache-managed connection enables server-side tracking.

    The mode is a connection-setup property: the server refuses to switch a live connection
    between ``OPTIN`` and ``OPTOUT``, so a configuration change applies to new connections
    only.

    ``PLAIN`` is the default, and was the only mode before 8.2, which added ``OPTIN`` and
    ``OPTOUT``.

    The two other modes reduce what the server has to remember, from the two ends,
    by pairing one ``CLIENT CACHING YES|NO`` immediately in front of a single read.
    """

    PLAIN = "plain"
    """``CLIENT TRACKING ON`` - every trackable read is tracked."""

    OPTIN = "optin"
    """``CLIENT TRACKING ON OPTIN`` - tracked only after ``CLIENT CACHING YES``."""

    OPTOUT = "optout"
    """``CLIENT TRACKING ON OPTOUT`` - tracked unless ``CLIENT CACHING NO``."""


class InvalidationPolicy(Enum):
    """
    What the cache does with an entry the server reports as invalidated.

    Either way the entry is removed the moment the invalidation is processed, so a
    value the server has replaced is never served again. The policies differ only in
    what happens next.
    """

    EVICT = "evict"
    """Remove the entry; the next read of it fetches it again. The default."""

    REFRESH = "refresh"
    """
    Remove the entry and queue it to be re-read in the background, on one thread per
    connection pool; ``refresh_max_inflight`` sizes the queue.

    Costs one extra server read per refreshed entry, whether or not the application
    reads it again.
    """


CachePredicate = Callable[[str, tuple], bool]
"""Decides whether the application wants an eligible reply cached.

Receives the command name as the command method spells it (``"GET"``, ``"FT.SEARCH"``) and the
key tuple exactly as the invocation supplied it in ``keys=``, so its elements are ``str`` or
``bytes`` depending on what the caller passed. Consulted under ``optin`` and ``optout`` only,
and only for a command that already passed eligibility and carried keys.
"""


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
    def evict_many(self, count: int) -> list[CacheKey]:
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

    # The three tracking-mode decisions are concrete, not abstract: this ABC is public and
    # implemented by third parties, so a configuration written against the previous version of
    # it must keep working. The defaults reproduce the existing behaviour exactly - plain tracking,
    # every eligible reply stored, no ``CLIENT CACHING NO`` ever paired. Same reasoning as
    # ``CacheConfig.set_metadata_resolver``, which was deliberately kept off this ABC.

    def get_tracking_mode(self) -> TrackingMode:
        return TrackingMode.PLAIN

    def should_cache(self, command: str, keys: tuple) -> bool:
        return True

    def is_trackable_read(self, command: str) -> bool:
        return False

    # Concrete for the same reason as the tracking-mode decisions above: the defaults
    # reproduce the existing behaviour for a third-party configuration, removing an
    # invalidated entry and re-reading nothing.

    def get_invalidation_policy(self) -> InvalidationPolicy:
        return InvalidationPolicy.EVICT

    def get_refresh_max_inflight(self) -> int | None:
        """
        The refresh queue size, read only under ``InvalidationPolicy.REFRESH``.

        The default is a usable bound, so a configuration that overrides only
        ``get_invalidation_policy`` still refreshes. ``CacheConfig`` returns None under
        ``InvalidationPolicy.EVICT``, where nothing reads it.
        """
        return CacheConfig.DEFAULT_REFRESH_MAX_INFLIGHT


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
    def get(self, key: CacheKey) -> CacheEntry | None:
        pass

    @abstractmethod
    def set(self, entry: CacheEntry) -> bool:
        pass

    @abstractmethod
    def delete_by_cache_keys(self, cache_keys: list[CacheKey]) -> list[bool]:
        pass

    @abstractmethod
    def delete_by_redis_keys(self, redis_keys: list[bytes]) -> list[bool]:
        pass

    @abstractmethod
    def flush(self) -> int:
        pass

    @abstractmethod
    def is_cachable(self, key: CacheKey) -> bool:
        pass

    def take_entries_by_redis_keys(
        self, redis_keys: list[bytes] | list[str]
    ) -> list[CacheEntry]:
        """
        Remove every entry whose invocation named one of ``redis_keys``, and return them.

        Entries of every status are removed and returned, so the caller can tell a stored
        reply from a fetch still in flight. Concrete, because this ABC is public and
        implemented by third parties: this default scans ``collection``, which is correct
        for any implementation and costs O(entries). :class:`DefaultCache` answers it
        from its reverse index instead.

        Args:
            redis_keys: The Redis keys the server reported, in either spelling.

        Returns:
            list[CacheEntry]: The removed entries.
        """
        wanted = set()
        for redis_key in redis_keys:
            wanted.update(_redis_key_spellings(redis_key))

        entries = [
            entry
            for cache_key, entry in list(self.collection.items())
            if any(key in wanted for key in cache_key.redis_keys)
        ]
        # Only what this call removed: an entry another caller removed since the snapshot
        # is that caller's to return.
        removed = self.delete_by_cache_keys([entry.cache_key for entry in entries])
        return [entry for entry, was_removed in zip(entries, removed) if was_removed]


def _redis_key_spellings(redis_key: bytes | str) -> list[bytes | str]:
    """
    The spellings a Redis key may be indexed under.

    An entry is indexed under its keys exactly as the invocation supplied them, while the
    server names them in its own encoding, so a lookup has to try both.
    """
    candidates = [redis_key]
    if isinstance(redis_key, str):
        candidates.append(redis_key.encode("utf-8"))
    elif isinstance(redis_key, bytes):
        try:
            candidates.append(redis_key.decode("utf-8"))
        except UnicodeDecodeError:
            pass  # Non-UTF-8 bytes, skip str version
    return candidates


class _IndexedCacheEntries(OrderedDict):
    """
    The cache's entry map, carrying a reverse index from Redis key to the entries holding it.

    Invalidation is the hot path: the server names a key and the cache has to find every entry
    whose invocation touched it. Doing that by scanning the whole map costs O(entries) per
    message, under the lock, for every message - and opt-in only reduces how many messages
    arrive, not what each one costs.

    The index lives on the mapping rather than in :class:`DefaultCache` because entries do not
    only leave through that class's methods: an eviction policy pops straight off
    ``cache.collection`` (see :meth:`LRUPolicy.evict_next`), so an index maintained one level
    up would go stale on every eviction, and a stale index is the one thing it must never be -
    a missed entry is a reply that is never invalidated. Overriding the mutating methods here
    means every insertion and removal path keeps it exact, whoever calls it.

    Most Redis keys have exactly one holder, and a ``set`` costs over 200 bytes, so a lone
    holder is stored bare and promoted to a ``set`` only when a second one arrives - and
    demoted back when it drops to one again. That trades an ``isinstance`` check on each
    index update for most of the index's memory.

    Every mutation holds ``_lock`` across both the map update and its index update. The map is
    shared by a whole pool, while each :class:`~redis.connection.CacheProxyConnection` guards it
    with a lock of its own, so two connections can mutate it at once - and an index update is a
    read-modify-write that would otherwise lose a holder, leaving an entry no invalidation can
    find. The lock is a leaf: nothing is acquired under it, so it cannot join a lock cycle with
    the connection and pool locks. Plain reads of the map - the cache-hit path - do not take it.
    """

    def __init__(self, *args, **kwargs) -> None:
        # Assigned before delegating: ``OrderedDict.__init__`` may populate, which routes
        # through the ``__setitem__`` below. The lock is re-entrant in case an ``OrderedDict``
        # implementation (PyPy's, say) routes one overridden method through another.
        self._lock = threading.RLock()
        self._by_redis_key: dict[Any, CacheKey | set[CacheKey]] = {}
        super().__init__(*args, **kwargs)

    def __setitem__(self, key: CacheKey, value: "CacheEntry") -> None:
        with self._lock:
            index = self._by_redis_key
            for redis_key in key.redis_keys:
                holders = index.get(redis_key)
                if holders is None:
                    index[redis_key] = key
                elif isinstance(holders, set):
                    holders.add(key)
                elif holders != key:
                    index[redis_key] = {holders, key}
            super().__setitem__(key, value)

    def __delitem__(self, key: CacheKey) -> None:
        with self._lock:
            super().__delitem__(key)
            self._unindex(key)

    def pop(self, key: CacheKey, *args):
        with self._lock:
            value = super().pop(key, *args)
            self._unindex(key)
            return value

    def popitem(self, last: bool = True):
        with self._lock:
            key, value = super().popitem(last=last)
            self._unindex(key)
            return key, value

    def clear(self) -> None:
        with self._lock:
            super().clear()
            self._by_redis_key.clear()

    def holders_of(self, redis_key) -> frozenset:
        """
        The cache keys of every entry whose invocation named ``redis_key``.

        Returned as a snapshot, because the caller deletes what it finds.
        """
        with self._lock:
            holders = self._by_redis_key.get(redis_key)
            if holders is None:
                return frozenset()
            if isinstance(holders, set):
                return frozenset(holders)
            return frozenset((holders,))

    def _unindex(self, key: CacheKey) -> None:
        # Called with ``_lock`` held.
        index = self._by_redis_key
        for redis_key in key.redis_keys:
            holders = index.get(redis_key)
            if holders is None:
                continue
            if isinstance(holders, set):
                holders.discard(key)
                if len(holders) == 1:
                    index[redis_key] = next(iter(holders))
                elif not holders:
                    del index[redis_key]
            elif holders == key:
                del index[redis_key]


class DefaultCache(CacheInterface):
    def __init__(
        self,
        cache_config: CacheConfigurationInterface,
    ) -> None:
        self._cache = _IndexedCacheEntries()
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

    def get(self, key: CacheKey) -> CacheEntry | None:
        entry = self._cache.get(key, None)

        if entry is None:
            return None

        self._eviction_policy.touch(key)
        return entry

    def delete_by_cache_keys(self, cache_keys: list[CacheKey]) -> list[bool]:
        response = []

        for key in cache_keys:
            if self.get(key) is not None:
                self._cache.pop(key)
                response.append(True)
            else:
                response.append(False)

        return response

    def delete_by_redis_keys(self, redis_keys: list[bytes] | list[str]) -> list[bool]:
        response = []
        keys_to_delete = []

        for redis_key in redis_keys:
            holders = self._holders_of(redis_key)

            # An invalidation message never carries more than one key, so an entry holding
            # several keys (MGET) cannot be collected twice by one call. A duplicate pop for
            # a multi-key batch is not a reachable case - do not "fix" it.
            for cache_key in holders:
                keys_to_delete.append(cache_key)
                response.append(True)

        for key in keys_to_delete:
            self._cache.pop(key)

        return response

    def take_entries_by_redis_keys(
        self, redis_keys: list[bytes] | list[str]
    ) -> list[CacheEntry]:
        entries = []

        for redis_key in redis_keys:
            # As in ``delete_by_redis_keys``, an invalidation names one key, so no entry is
            # collected twice. An entry another connection popped since the lookup is gone
            # already and has nothing left to return.
            for cache_key in self._holders_of(redis_key):
                entry = self._cache.pop(cache_key, None)
                if entry is not None:
                    entries.append(entry)

        return entries

    # Quoted: in this class body ``set`` is the ``set`` method, not the builtin.
    def _holders_of(self, redis_key: bytes | str) -> "set[CacheKey]":
        # The reverse index answers this without walking the map. Both spellings are looked
        # up, and the two results are unioned, so an entry indexed under both spellings of
        # this one key is collected once.
        holders: set[CacheKey] = set()
        for candidate in _redis_key_spellings(redis_key):
            holders |= self._cache.holders_of(candidate)
        return holders

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

    def get(self, key: CacheKey) -> CacheEntry | None:
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

    def delete_by_cache_keys(self, cache_keys: list[CacheKey]) -> list[bool]:
        return self._cache.delete_by_cache_keys(cache_keys)

    def delete_by_redis_keys(self, redis_keys: list[bytes]) -> list[bool]:
        return self._cache.delete_by_redis_keys(redis_keys)

    def take_entries_by_redis_keys(
        self, redis_keys: list[bytes] | list[str]
    ) -> list[CacheEntry]:
        return self._cache.take_entries_by_redis_keys(redis_keys)

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

    def evict_many(self, count: int) -> list[CacheKey]:
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
    """
    Configuration of a client-side cache.

    Args:
        max_size: The most entries the cache holds before it evicts.
        cache_class: The cache implementation built from this configuration.
        eviction_policy: How an entry is chosen for eviction when the cache is full.
        tracking_mode: How cache-managed connections enable server-side tracking.
        cache_predicate: Whether the application wants an eligible reply stored;
            consulted under ``optin`` and ``optout`` only.
        invalidation_policy: What happens to an entry the server invalidates. Under
            ``InvalidationPolicy.REFRESH`` it is re-read in the background.
        refresh_max_inflight: Under ``InvalidationPolicy.REFRESH``, the size of each
            connection pool's refresh queue: the most keys waiting for a refresh, counting
            the one being refreshed. Refreshes run one at a time; an invalidation
            beyond it only removes the entry. A positive integer, defaulting to
            ``DEFAULT_REFRESH_MAX_INFLIGHT`` when omitted. Under
            ``InvalidationPolicy.EVICT`` it is not used, so it is ``None``, and passing
            it warns.
    """

    DEFAULT_CACHE_CLASS = DefaultCache
    DEFAULT_EVICTION_POLICY = EvictionPolicy.LRU
    DEFAULT_MAX_SIZE = 10000
    DEFAULT_REFRESH_MAX_INFLIGHT = 16

    # DEPRECATED - no longer consulted, and it will be removed in a future release.
    #
    # Command eligibility is now decided from command metadata by the metadata resolver this
    # config holds, so this list no longer describes what gets cached. It is kept as a public
    # attribute only so an external caller reading it keeps working; editing it changes
    # nothing. The effective set it is replaced by differs from it by ``+FT.SUGGET``,
    # ``+FT.SUGLEN``, ``+DIGEST``, ``+EXPIRETIME``, ``+PEXPIRETIME``, ``+HEXPIRETIME``,
    # ``+HPEXPIRETIME``, ``+SDIFFCARD``, ``+SUNIONCARD`` and ``-XPENDING``, ``-TS.INFO``,
    # ``-XREAD`` - the three removals being commands the server itself reports as not cacheable.
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
        tracking_mode: TrackingMode = TrackingMode.PLAIN,
        cache_predicate: CachePredicate | None = None,
        invalidation_policy: InvalidationPolicy = InvalidationPolicy.EVICT,
        refresh_max_inflight: int | object = SENTINEL,
    ):
        # A bare string here - ``tracking_mode="optin"`` - would compare equal to no
        # ``TrackingMode`` member and so silently behave as plain mode, and the failure mode
        # of a mis-configured cache is a wrongly-cached reply. Refused instead.
        if not isinstance(tracking_mode, TrackingMode):
            raise TypeError(
                "tracking_mode must be a redis.cache.TrackingMode member, got "
                f"{tracking_mode!r}"
            )

        if cache_predicate is not None and tracking_mode is TrackingMode.PLAIN:
            warnings.warn(
                "cache_predicate is only consulted in optin and optout modes and is "
                "ignored with tracking_mode=plain.",
                UserWarning,
                stacklevel=2,
            )

        if cache_predicate is None and tracking_mode is TrackingMode.OPTIN:
            warnings.warn(
                "tracking_mode=optin with no cache_predicate stores nothing; every read is "
                "sent alone and left untracked. Configure cache_predicate to select what to "
                "cache.",
                UserWarning,
                stacklevel=2,
            )

        # Refused for the same reason as a bare-string ``tracking_mode``: ``"refresh"``
        # would compare equal to no member and silently behave as evict.
        if not isinstance(invalidation_policy, InvalidationPolicy):
            raise TypeError(
                "invalidation_policy must be a redis.cache.InvalidationPolicy member, "
                f"got {invalidation_policy!r}"
            )

        if invalidation_policy is InvalidationPolicy.EVICT:
            if refresh_max_inflight is not SENTINEL:
                warnings.warn(
                    "refresh_max_inflight is only consulted with "
                    "invalidation_policy=refresh and is ignored with "
                    "invalidation_policy=evict.",
                    UserWarning,
                    stacklevel=2,
                )
            refresh_max_inflight = None
        elif refresh_max_inflight is SENTINEL:
            refresh_max_inflight = CacheConfig.DEFAULT_REFRESH_MAX_INFLIGHT
        # ``bool`` is an ``int`` subclass, and ``True`` as a bound is a typo, not a
        # limit of 1.
        elif (
            not isinstance(refresh_max_inflight, int)
            or isinstance(refresh_max_inflight, bool)
            or refresh_max_inflight < 1
        ):
            raise ValueError(
                "refresh_max_inflight must be a positive integer, got "
                f"{refresh_max_inflight!r}"
            )

        self._cache_class = cache_class
        self._max_size = max_size
        self._eviction_policy = eviction_policy
        self._tracking_mode = tracking_mode
        self._cache_predicate = cache_predicate
        self._invalidation_policy = invalidation_policy
        self._refresh_max_inflight = refresh_max_inflight
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

    def get_tracking_mode(self) -> TrackingMode:
        return self._tracking_mode

    def get_cache_predicate(self) -> CachePredicate | None:
        return self._cache_predicate

    def get_invalidation_policy(self) -> InvalidationPolicy:
        return self._invalidation_policy

    def get_refresh_max_inflight(self) -> int | None:
        return self._refresh_max_inflight

    def is_allowed_to_cache(self, command: str) -> bool:
        # Fails closed on everything the resolver cannot decide: an unknown command, a name
        # the record tables cannot be keyed by, and a record built from incomplete metadata
        # all resolve to False. The verdict is memoized per command name, so this is a dict
        # hit on the command execution path.
        return self._metadata_resolver.is_cacheable(command)

    def should_cache(self, command: str, keys: tuple) -> bool:
        """
        Intent: does the application want this eligible reply stored?

        The second of the three decisions a cached read passes, and the only one the
        application configures directly.
        Eligibility - whether the reply is safe to cache at all - is answered before this
        by :meth:`is_allowed_to_cache` from the command metadata, and is never widened here.

        Asked only after eligibility said yes and the invocation supplied keys, so the
        predicate never sees a command it could not affect and never sees an empty key tuple.
        One function call per eligible read that reaches the cache layer.

        Args:
            command: The command name as the command method spells it.
            keys: The Redis keys of this invocation.

        Returns:
            bool: True when the reply may be stored.
        """
        if self._tracking_mode is TrackingMode.PLAIN:
            return True

        if self._cache_predicate is None:
            return self._tracking_mode is TrackingMode.OPTOUT

        return bool(self._cache_predicate(command, keys))

    def is_trackable_read(self, command: str) -> bool:
        """
        Whether the server would remember this command's keys - the readonly flag alone.

        Never affects what may be stored. It decides only whether a ``CLIENT CACHING NO`` in
        front of a read is worth sending under ``optout`` tracking, so it fails closed in the
        cheap direction: skipping the ``NO`` wastes invalidation-table entries, where storing
        an untracked reply is stale forever.

        Args:
            command: The command name as the command method spells it.

        Returns:
            bool: True only when the command carries the ``readonly`` command flag.
        """
        return self._metadata_resolver.is_trackable_read(command)


class CacheFactoryInterface(ABC):
    @abstractmethod
    def get_cache(self) -> CacheInterface:
        pass


class CacheFactory(CacheFactoryInterface):
    def __init__(self, cache_config: CacheConfig | None = None):
        self._config = cache_config

        if self._config is None:
            self._config = CacheConfig()

    def get_cache(self) -> CacheInterface:
        cache_class = self._config.get_cache_class()
        return CacheProxy(cache_class(cache_config=self._config))


class _CacheRefresher:
    """
    Re-reads invalidated entries in the background, for one connection pool.

    The invalidation callback hands over the keys of the entries it just removed, and a
    single worker thread re-runs each one through the pool's normal caching miss path. That
    path stakes the placeholder, re-registers tracking on the fetching connection and stores
    the reply exactly as a user read would, so a refreshed entry cannot be told from one a
    read stored, and an invalidation that lands mid-refresh drops the result the same way it
    drops any read's.

    ``submit`` is called from inside the invalidation callback, which can hold the pool lock
    and a connection's cache lock, so it does no I/O and takes no lock but ``_lock``. The
    one wait it can make is for a newly started worker thread to come up. ``_lock`` is a
    leaf: nothing is acquired under it, and it is never held across a round trip.

    ``max_inflight`` is the size of the queue: the number of keys accepted and not yet
    completed, the one being refreshed included. One worker drains it, so it bounds the
    backlog, not concurrency. A key that finds the queue full is rejected; the entry is
    already removed, so the next read fetches it, as without refresh. A key already
    pending is not queued twice.

    The thread starts on the first accepted key and exits once the queue has stayed empty
    for ``idle_timeout`` seconds, so a pool that is not refreshing holds no thread. It
    refers to the pool weakly and never keeps it alive.
    """

    IDLE_TIMEOUT = 30.0
    _STOP = object()

    def __init__(
        self, pool, max_inflight: int, idle_timeout: float = IDLE_TIMEOUT
    ) -> None:
        self._pool_ref = weakref.ref(pool)
        self._max_inflight = max_inflight
        self._idle_timeout = idle_timeout
        self._queue: queue.SimpleQueue = queue.SimpleQueue()
        self._pending: set[CacheKey] = set()
        # Bumped by every cancel. A queued job of an older generation is dropped unrun, and
        # a job that completes after a cancel does not touch ``_pending``, which by then
        # belongs to newer work.
        self._generation = 0
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def submit(self, cache_keys: Iterable[CacheKey]) -> list[CacheKey]:
        """
        Accept keys for refresh, as far as the queue has room.

        Args:
            cache_keys: The keys of entries the cache has already removed.

        Returns:
            list[CacheKey]: The keys rejected, because the queue was full or because no
            worker could be started. Their entries stay removed.
        """
        rejected = []
        start_error = None

        with self._lock:
            generation = self._generation
            accepted = []
            for cache_key in cache_keys:
                if cache_key in self._pending:
                    continue
                if len(self._pending) >= self._max_inflight:
                    rejected.append(cache_key)
                    continue
                self._pending.add(cache_key)
                accepted.append(cache_key)

            # Under ``_lock``, together with the puts: the worker decides to exit under the
            # same lock, so a key can never be queued after it checked and before it left.
            # A registered thread that is not alive is one inherited across a fork, which
            # never runs in this process.
            if accepted and (self._thread is None or not self._thread.is_alive()):
                worker = threading.Thread(
                    target=self._run, name="redis-csc-refresher", daemon=True
                )
                try:
                    worker.start()
                except RuntimeError as e:
                    # No thread can be started - a thread limit, or interpreter shutdown.
                    # Nothing would run what this call accepted, so it is handed back.
                    # Raising instead would escape into the invalidation callback.
                    start_error = e
                    self._pending.difference_update(accepted)
                    rejected.extend(accepted)
                    accepted = []
                else:
                    self._thread = worker

            for cache_key in accepted:
                self._queue.put((generation, cache_key))

        if start_error is not None:
            logger.debug("Client-side cache refresher could not start: %r", start_error)

        if rejected:
            record_csc_refresh(result=CSCRefreshResult.REJECTED, count=len(rejected))

        return rejected

    def cancel_pending(self) -> None:
        """
        Drop every accepted key that has not run yet.

        Called wherever the cache is flushed: an emptied cache must not be refilled by work
        queued before it was emptied. A job still waiting for a connection checks again
        before it sends. A refresh already sent is not stopped: its placeholder usually went
        with the flush, so its reply is not stored, and when it did not - the flush landed
        just before the placeholder was staked, or was a reconnect, which keeps it - the
        reply was read after the flush on a tracking connection, so what it stores is
        current.
        """
        with self._lock:
            self._generation += 1
            self._pending.clear()

    def stop(self) -> None:
        """
        Cancel all queued work and let the worker exit.

        The refresher stays usable: a pool is reusable after ``close()``, and the next
        accepted key starts a new worker.
        """
        self.cancel_pending()
        with self._lock:
            # Only a running worker needs waking; one started later finds the queue empty.
            if self._thread is not None:
                self._queue.put(self._STOP)

    def _is_current(self, generation: int) -> bool:
        with self._lock:
            return generation == self._generation

    def _run(self) -> None:
        try:
            self._work()
        finally:
            # Whatever ended the loop - an exception the loop does not catch, or a pool
            # that is gone - the next accepted key must find no worker registered and
            # start one, rather than queue behind a thread that no longer runs.
            with self._lock:
                if self._thread is threading.current_thread():
                    self._thread = None

    def _work(self) -> None:
        while True:
            try:
                job = self._queue.get(timeout=self._idle_timeout)
            except queue.Empty:
                job = self._STOP

            if job is self._STOP:
                with self._lock:
                    # Whatever is still queued was accepted after the stop, or is an older
                    # generation that the loop drops without a round trip.
                    if self._queue.empty():
                        self._thread = None
                        return
                continue

            generation, cache_key = job
            if not self._is_current(generation):
                continue

            pool = self._pool_ref()
            if pool is None:
                return

            try:
                self._refresh(pool, cache_key, generation)
            except Exception as e:
                # Refresh is best effort, and the worker must outlive any one failure.
                logger.debug("Client-side cache refresh failed: %r", e)
            finally:
                with self._lock:
                    if generation == self._generation:
                        self._pending.discard(cache_key)
                # Not held across the wait for the next job, so the pool can be collected.
                del pool

    def _refresh(self, pool, cache_key: CacheKey, generation: int) -> None:
        # A user read has already re-fetched the key, or is fetching it now. Refreshing too
        # would race it for a reply that is no newer. ``in`` rather than ``get``, so the
        # check does not count as a use for the eviction policy.
        if cache_key in pool.cache.collection:
            return

        try:
            # Only a connection the pool can hand out at once. With every connection in use
            # and the pool at its limit, the refresh is skipped rather than make an
            # application command fail or wait for the sake of a background read.
            conn = pool._get_connection(if_available=True)
        except Exception as e:
            # A server that cannot be reached: the entry stays removed and the next read
            # fetches it.
            logger.debug("Client-side cache refresh could not get a connection: %r", e)
            record_csc_refresh(result=CSCRefreshResult.FAILURE)
            return

        if conn is None:
            # Skipped for lack of capacity, like a key that found the queue full: the entry
            # stays removed and the next read fetches it.
            record_csc_refresh(result=CSCRefreshResult.REJECTED)
            return

        try:
            # Getting a connection can wait for a handshake, and a flush in that time must
            # still refresh nothing.
            if not self._is_current(generation):
                return
            # The cache key holds the whole command, so this replays the original read.
            # Read with default decoding: the decode mode is not part of the cache key, and
            # no cacheable command needs ``NEVER_DECODE``.
            conn.send_command(*cache_key.redis_args, keys=cache_key.redis_keys)
            conn.read_response()
        except (ConnectionError, TimeoutError, OSError) as e:
            # The socket is in an unknown state, so it is closed, as the client's own error
            # path does. Closing a caching connection flushes the cache.
            logger.debug("Client-side cache refresh failed: %r", e)
            record_csc_refresh(result=CSCRefreshResult.FAILURE)
            conn.disconnect()
        except Exception as e:
            # An error reply - WRONGTYPE, NOPERM, MOVED - or a failing cache predicate. The
            # placeholder is already dropped, so the entry stays removed.
            logger.debug("Client-side cache refresh failed: %r", e)
            record_csc_refresh(result=CSCRefreshResult.FAILURE)
        else:
            # Answered, whether or not the reply was stored: a nil reply, or one the cache
            # predicate does not select, is a completed refresh that leaves nothing behind.
            record_csc_refresh(result=CSCRefreshResult.SUCCESS)
        finally:
            pool.release(conn)
