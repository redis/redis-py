import gc
import sys
import threading
import time
import uuid
import warnings
import weakref
from collections import OrderedDict
from unittest.mock import MagicMock, patch

import pytest
import redis

from redis.cache import (
    CacheConfig,
    CacheConfigurationInterface,
    CacheEntry,
    CacheEntryStatus,
    CacheInterface,
    CacheKey,
    CacheProxy,
    DefaultCache,
    EvictionPolicy,
    EvictionPolicyType,
    InvalidationPolicy,
    LRUPolicy,
    TrackingMode,
    _CacheRefresher,
)
from redis.commands.metadata import (
    CommandMetadata,
    DynamicMetadataResolver,
    RequestPolicy,
    ResponsePolicy,
    StaticMetadataResolver,
)
from redis.connection import CacheProxyConnection
from redis.event import (
    EventDispatcher,
)
from redis.exceptions import (
    AskError,
    ConnectionError,
    MovedError,
    RedisError,
    ResponseError,
    TimeoutError,
)
from redis.observability.attributes import CSCReason, CSCRefreshResult
from redis.utils import str_if_bytes
from tests.conftest import _get_client, skip_if_resp_version, skip_if_server_version_lt
from tests.helpers import wait_for_condition

# A record for a command a resolver must report as ineligible, used to prove that eligibility
# comes from the resolver the config holds rather than from the config itself.
WRITE_KEYED = CommandMetadata(
    request_policy=RequestPolicy.DEFAULT_KEYED,
    response_policy=ResponsePolicy.DEFAULT_KEYED,
    is_readonly=False,
    has_key_argument=True,
    has_complete_metadata=True,
)


def wait_for_invalidated_value(client, key, expected, timeout=3.0, interval=0.05):
    """
    Reads ``key`` until the client stops serving the value it cached before invalidation.

    Server-assisted invalidation arrives on its own push message, so a read taken straight
    after another client's write is a race. A fixed sleep answers that race badly in both
    directions - too short and it flakes under CI load, long enough and every run pays for
    it - and the delay differs by topology, being longest across cluster nodes. Polls to
    the deadline and returns the last value read, so the caller's own assertion is what
    reports the failure if the invalidation never lands.
    """
    deadline = time.monotonic() + timeout
    value = client.get(key)
    while value not in expected and time.monotonic() < deadline:
        time.sleep(interval)
        value = client.get(key)

    return value


def keys_prefixed(prefix: str):
    """
    Builds a cache predicate that selects the invocations whose first key has ``prefix``.

    The predicate receives the keys exactly as the invocation supplied them - a command method
    passes ``keys=[name]``, so the element is whatever the caller typed - which is why this
    normalizes before comparing.
    """

    def predicate(command, keys):
        return str_if_bytes(keys[0]).startswith(prefix)

    return predicate


def keys_excluding(prefix: str):
    """The inverse of :func:`keys_prefixed`: everything but the keys carrying ``prefix``."""

    def predicate(command, keys):
        return not str_if_bytes(keys[0]).startswith(prefix)

    return predicate


def tracking_flags(client) -> set:
    """
    Reads the tracking flags the server reports for the client's connection.

    Tolerant of both RESP3 reply shapes, because the suite runs the whole
    ``legacy_responses`` axis and the two spell the map's keys differently.
    """
    info = client.client_trackinginfo()

    for key in (b"flags", "flags"):
        if key in info:
            return {str_if_bytes(flag) for flag in info[key]}

    raise AssertionError(f"No flags in the CLIENT TRACKINGINFO reply: {info!r}")


def tracked_key_count(client) -> int:
    """
    Reads how many keys the server's invalidation table holds, across all clients.

    The only server-side view of what was tracked: ``CLIENT TRACKINGINFO`` reports the mode but
    not the keys. Server-wide, so callers compare a before/after delta around reads of keys no
    other test uses, from a client that is not itself tracking.
    """
    return int(client.info("stats")["tracking_total_keys"])


@pytest.fixture()
def r(request):
    cache = request.param.get("cache")
    cache_config = request.param.get("cache_config")
    kwargs = request.param.get("kwargs", {})
    protocol = request.param.get("protocol", 3)
    ssl = request.param.get("ssl", False)
    single_connection_client = request.param.get("single_connection_client", False)
    decode_responses = request.param.get("decode_responses", False)
    with _get_client(
        redis.Redis,
        request,
        protocol=protocol,
        ssl=ssl,
        single_connection_client=single_connection_client,
        cache=cache,
        cache_config=cache_config,
        decode_responses=decode_responses,
        **kwargs,
    ) as client:
        yield client


@pytest.mark.onlynoncluster
@skip_if_resp_version(2)
@skip_if_server_version_lt("7.4.0")
class TestCache:
    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(CacheConfig(max_size=5)),
                "single_connection_client": True,
            },
            {
                "cache": DefaultCache(CacheConfig(max_size=5)),
                "single_connection_client": False,
            },
            {
                "cache": DefaultCache(CacheConfig(max_size=5)),
                "single_connection_client": False,
                "decode_responses": True,
            },
        ],
        ids=["single", "pool", "decoded"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_given_cache(self, r, r2):
        cache = r.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") in [b"bar", "bar"]
        # get key from local cache
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")

        # Add a small delay to allow invalidation to be processed
        time.sleep(0.1)

        # Retrieves a new value from server and cache it
        assert r.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"barbar",
            "barbar",
        ]

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(CacheConfig(max_size=5)),
                "single_connection_client": True,
            },
            {
                "cache": DefaultCache(CacheConfig(max_size=5)),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_zrevrange_cache_key_uses_whole_key(self, r, r2):
        # Regression: zrevrange stored options["keys"] as a bare string, so the
        # cache key was built from the key's individual characters
        # (("m", "y", "z", ...)) instead of the whole key. Verify the result is
        # cached under redis_keys=("myzset",) and invalidated when the set
        # changes from another client.
        cache = r.get_cache()
        r.delete("myzset")
        r.zadd("myzset", {"a": 1, "b": 2})
        # populate the local cache
        assert r.zrevrange("myzset", 0, -1) == [b"b", b"a"]
        # the entry must be stored under the whole key, not per-character
        cache_key = CacheKey(
            command="ZREVRANGE",
            redis_keys=("myzset",),
            redis_args=("ZREVRANGE", "myzset", 0, -1),
        )
        assert cache.get(cache_key) is not None
        # change the sorted set from a second client (causes invalidation)
        r2.zadd("myzset", {"c": 3})
        # Add a small delay to allow invalidation to be processed
        time.sleep(0.1)
        # a fresh value is fetched and re-cached
        assert r.zrevrange("myzset", 0, -1) == [b"c", b"b", b"a"]

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(CacheConfig(max_size=5)),
                "single_connection_client": True,
            },
            {
                "cache": DefaultCache(CacheConfig(max_size=5)),
                "single_connection_client": False,
            },
            {
                "cache": DefaultCache(CacheConfig(max_size=5)),
                "single_connection_client": False,
                "decode_responses": True,
            },
        ],
        ids=["single", "pool", "decoded"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_hash_get_from_given_cache(self, r, r2):
        cache = r.get_cache()
        hash_key = "hash_foo_key"
        field_1 = "bar"
        field_2 = "bar2"

        # add hash key to redis
        r.hset(hash_key, field_1, "baz")
        r.hset(hash_key, field_2, "baz2")
        # get keys from redis and save them in local cache
        assert r.hget(hash_key, field_1) in [b"baz", "baz"]
        assert r.hget(hash_key, field_2) in [b"baz2", "baz2"]
        # get key from local cache
        assert cache.get(
            CacheKey(
                command="HGET",
                redis_keys=(hash_key,),
                redis_args=("HGET", hash_key, field_1),
            )
        ).cache_value in [
            b"baz",
            "baz",
        ]
        assert cache.get(
            CacheKey(
                command="HGET",
                redis_keys=(hash_key,),
                redis_args=("HGET", hash_key, field_2),
            )
        ).cache_value in [
            b"baz2",
            "baz2",
        ]
        # change key in redis (cause invalidation)
        r2.hset(hash_key, field_1, "barbar")

        # Add a small delay to allow invalidation to be processed
        time.sleep(0.1)

        # Retrieves a new value from server and cache it
        assert r.hget(hash_key, field_1) in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(
            CacheKey(
                command="HGET",
                redis_keys=(hash_key,),
                redis_args=("HGET", hash_key, field_1),
            )
        ).cache_value in [
            b"barbar",
            "barbar",
        ]
        # The other field is also reset, because the invalidation message contains only the hash key.
        assert (
            cache.get(
                CacheKey(
                    command="HGET",
                    redis_keys=(hash_key,),
                    redis_args=("HGET", hash_key, field_2),
                )
            )
            is None
        )
        assert r.hget(hash_key, field_2) in [b"baz2", "baz2"]

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": True,
            },
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": False,
            },
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": False,
                "decode_responses": True,
            },
        ],
        ids=["single", "pool", "decoded"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_default_cache(self, r, r2):
        cache = r.get_cache()
        assert isinstance(cache.eviction_policy, LRUPolicy)
        assert cache.config.get_max_size() == 128

        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") in [b"bar", "bar"]
        # get key from local cache
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")

        # Add a small delay to allow invalidation to be processed
        time.sleep(0.1)

        # Retrieves a new value from server and cache it
        assert r.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"barbar",
            "barbar",
        ]

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": True,
            },
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_cache_clears_on_disconnect(self, r, cache):
        cache = r.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            ).cache_value
            == b"bar"
        )
        # Force disconnection
        r.connection_pool.get_connection().disconnect()
        # Make sure cache is empty
        assert cache.size == 0

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=3),
                "single_connection_client": True,
            },
            {
                "cache_config": CacheConfig(max_size=3),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_cache_lru_eviction(self, r, cache):
        cache = r.get_cache()
        # add 3 keys to redis
        r.set("foo", "bar")
        r.set("foo2", "bar2")
        r.set("foo3", "bar3")
        # get 3 keys from redis and save in local cache
        assert r.get("foo") == b"bar"
        assert r.get("foo2") == b"bar2"
        assert r.get("foo3") == b"bar3"
        # get the 3 keys from local cache
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            ).cache_value
            == b"bar"
        )
        assert (
            cache.get(
                CacheKey(
                    command="GET", redis_keys=("foo2",), redis_args=("GET", "foo2")
                )
            ).cache_value
            == b"bar2"
        )
        assert (
            cache.get(
                CacheKey(
                    command="GET", redis_keys=("foo3",), redis_args=("GET", "foo3")
                )
            ).cache_value
            == b"bar3"
        )
        # add 1 more key to redis (exceed the max size)
        r.set("foo4", "bar4")
        assert r.get("foo4") == b"bar4"
        # the first key is not in the local cache anymore
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            )
            is None
        )
        assert cache.size == 3

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": True,
            },
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_cache_ignore_not_allowed_command(self, r):
        cache = r.get_cache()
        # add fields to hash
        assert r.hset("foo", "bar", "baz")
        # get random field
        assert r.hrandfield("foo") == b"bar"
        assert (
            cache.get(
                CacheKey(
                    command="HRANDFIELD",
                    redis_keys=("foo",),
                    redis_args=("HRANDFIELD", "foo"),
                )
            )
            is None
        )

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": True,
            },
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_eligible_command_without_keys_does_not_raise(self, r):
        """
        Regression: an eligible command whose method does not pass ``keys=`` used to raise
        ``ValueError: Cannot create cache key.`` on a CSC connection.

        ZRANK is eligible by metadata and passes no key list, so it is the two-step
        eligibility end to end: the command executes normally and nothing is cached.
        """
        cache = r.get_cache()
        assert r.zadd("foo", {"a": 1, "b": 2}) == 2

        assert r.zrank("foo", "b") == 1
        assert cache.size == 0

        # And the reply is still the server's on a second call, rather than a cached one.
        assert r.zrank("foo", "a") == 0
        assert cache.size == 0

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": False,
            },
        ],
        ids=["pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_cache_skips_a_command_the_metadata_excludes(self, r):
        """
        XPENDING is on the legacy ``DEFAULT_ALLOW_LIST`` and does pass ``keys=``, but the
        server tips it ``nondeterministic_output``, so metadata-driven eligibility excludes
        it. The command must still work.
        """
        cache = r.get_cache()
        r.xadd("foo", {"a": 1})
        r.xgroup_create("foo", "group", 0)

        assert r.xpending("foo", "group")["pending"] == 0
        assert cache.size == 0

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "kwargs": {
                    "metadata_resolver": DynamicMetadataResolver(
                        {"core": {"get": WRITE_KEYED}}
                    )
                },
            },
        ],
        ids=["pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_client_level_metadata_resolver_decides_eligibility(self, r):
        """
        The resolver the client was built with is what CSC reads: this one carries a single
        record saying GET is a write, so the reply of the one command CSC always caches is not
        cached - and the command still returns the right value.
        """
        cache = r.get_cache()
        r.set("foo", "bar")

        assert r.get("foo") == b"bar"
        assert cache.size == 0

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "kwargs": {
                    "metadata_resolver": StaticMetadataResolver(),
                },
            },
        ],
        ids=["pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_the_pool_resolves_eligibility_through_the_given_resolver(self, r):
        """The injected resolver is the object the decision point reads, by identity."""
        pool = r.connection_pool
        resolver = pool.metadata_resolver

        assert isinstance(resolver, StaticMetadataResolver)
        assert pool.cache.config._metadata_resolver is resolver

        r.set("foo", "bar")
        assert r.get("foo") == b"bar"
        assert (
            r.get_cache()
            .get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            )
            .cache_value
            == b"bar"
        )

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": True,
            },
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_cache_invalidate_all_related_responses(self, r):
        cache = r.get_cache()
        # Add keys
        assert r.set("foo", "bar")
        assert r.set("bar", "foo")

        res = r.mget("foo", "bar")
        # Make sure that replies was cached
        assert res == [b"bar", b"foo"]
        assert (
            cache.get(
                CacheKey(
                    command="MGET",
                    redis_keys=("foo", "bar"),
                    redis_args=("MGET", "foo", "bar"),
                )
            ).cache_value
            == res
        )

        # Make sure that objects are immutable.
        another_res = r.mget("foo", "bar")
        res.append(b"baz")
        assert another_res != res

        # Invalidate one of the keys and make sure that
        # all associated cached entries was removed
        assert r.set("foo", "baz")
        assert r.get("foo") == b"baz"
        assert (
            cache.get(
                CacheKey(
                    command="MGET",
                    redis_keys=("foo", "bar"),
                    redis_args=("MGET", "foo", "bar"),
                )
            )
            is None
        )
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            ).cache_value
            == b"baz"
        )

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": True,
            },
            {
                "cache_config": CacheConfig(max_size=128),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_cache_flushed_on_server_flush(self, r):
        cache = r.get_cache()
        # Add keys
        assert r.set("foo", "bar")
        assert r.set("bar", "foo")
        assert r.set("baz", "bar")

        # Make sure that replies was cached
        assert r.get("foo") == b"bar"
        assert r.get("bar") == b"foo"
        assert r.get("baz") == b"bar"
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            ).cache_value
            == b"bar"
        )
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("bar",), redis_args=("GET", "bar"))
            ).cache_value
            == b"foo"
        )
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("baz",), redis_args=("GET", "baz"))
            ).cache_value
            == b"bar"
        )

        # Flush server and trying to access cached entry
        assert r.flushall()
        assert r.get("foo") is None
        assert cache.size == 0

    @pytest.mark.parametrize(
        "r,expected_flags",
        [
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(max_size=128, tracking_mode=TrackingMode.PLAIN)
                    ),
                    "single_connection_client": True,
                },
                {"on"},
            ),
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(
                            max_size=128,
                            tracking_mode=TrackingMode.OPTIN,
                            cache_predicate=keys_prefixed("user:"),
                        )
                    ),
                    "single_connection_client": True,
                },
                {"on", "optin"},
            ),
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(
                            max_size=128,
                            tracking_mode=TrackingMode.OPTOUT,
                            cache_predicate=keys_prefixed("user:"),
                        )
                    ),
                    "single_connection_client": True,
                },
                {"on", "optout"},
            ),
        ],
        ids=["plain", "optin", "optout"],
        indirect=["r"],
    )
    @pytest.mark.onlynoncluster
    def test_the_tracking_mode_reaches_the_server(self, r, expected_flags):
        # The mode is sent in the tracking handshake, so this is what proves the enum reaches
        # the wire rather than only the local decision.
        assert tracking_flags(r) == expected_flags

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTIN,
                        cache_predicate=keys_prefixed("user:"),
                    )
                ),
                "single_connection_client": True,
            },
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTIN,
                        cache_predicate=keys_prefixed("user:"),
                    )
                ),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_optin_caches_and_tracks_only_the_selected_read(self, r, r2):
        """
        The assertion unit tests cannot make: that ``CLIENT CACHING YES`` actually reached the
        server ahead of its read. If it had not, the entry below would never be invalidated
        and the stale value would be served forever.
        """
        cache = r.get_cache()
        r2.set("user:42", "alice")
        r2.set("session:9", "sid")

        # Selected by the predicate: stored, and served locally on the next read.
        assert r.get("user:42") == b"alice"
        assert (
            cache.get(
                CacheKey(
                    command="GET",
                    redis_keys=("user:42",),
                    redis_args=("GET", "user:42"),
                )
            ).cache_value
            == b"alice"
        )

        # Not selected: sent alone, so nothing is stored and nothing is tracked.
        assert r.get("session:9") == b"sid"
        assert cache.size == 1

        # The entry is invalidated, which only happens if the YES was paired with the read.
        r2.set("user:42", "bob")
        assert wait_for_invalidated_value(r, "user:42", [b"bob"]) == b"bob"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTOUT,
                        cache_predicate=keys_excluding("counter:"),
                    )
                ),
                "single_connection_client": True,
            },
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTOUT,
                        cache_predicate=keys_excluding("counter:"),
                    )
                ),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_optout_exempts_the_excluded_read_and_caches_the_rest(self, r, r2):
        cache = r.get_cache()
        r2.set("counter:hits", "1")
        r2.set("user:42", "carol")

        # Excluded by the predicate: paired with ``CLIENT CACHING NO``, so not stored - and
        # its key never enters the server's invalidation table.
        assert r.get("counter:hits") == b"1"
        assert cache.size == 0

        # Everything else is stored, and tracked by default with no extra command.
        assert r.get("user:42") == b"carol"
        assert cache.size == 1

        r2.set("user:42", "dave")
        assert wait_for_invalidated_value(r, "user:42", [b"dave"]) == b"dave"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTOUT,
                        cache_predicate=keys_excluding("counter:"),
                    )
                ),
                "single_connection_client": False,
            },
        ],
        ids=["pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_a_pipeline_under_optout_bypasses_the_cache(self, r):
        # A pipelined read never reaches the cache layer, so it can carry no ``NO`` and store
        # nothing. It is tracked as in plain mode: waste, never staleness.
        cache = r.get_cache()
        r.set("foo", "bar")

        assert r.pipeline().get("foo").get("foo").execute() == [b"bar", b"bar"]
        assert cache.size == 0

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTOUT,
                        cache_predicate=keys_excluding("counter:"),
                    )
                ),
                "single_connection_client": False,
            },
        ],
        ids=["pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_optout_exempts_a_trackable_read_it_can_never_store(self, r):
        # TOUCH is read-only and keyed, so the server tracks it, but a local cache hit would
        # skip its server-side effect - so it is never eligible to store, and exempting it is
        # exactly what optout is for.
        cache = r.get_cache()
        r.set("foo", "bar")

        assert r.touch("foo") == 1
        assert cache.size == 0

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(max_size=128, tracking_mode=TrackingMode.OPTOUT)
                ),
                "single_connection_client": True,
            },
            {
                "cache": DefaultCache(
                    CacheConfig(max_size=128, tracking_mode=TrackingMode.OPTOUT)
                ),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_a_user_sent_client_caching_is_refused(self, r, r2):
        """
        A stray ``CLIENT CACHING NO`` is consumed by whatever the socket sends next. Under
        ``optout`` that is a cached read sent alone, so its reply would be stored while the
        server tracks nothing for it - stale forever. ``single_connection_client`` puts the
        stray command and the read on one socket, which is where it bites.
        """
        cache = r.get_cache()

        with pytest.raises(RedisError, match="CLIENT CACHING cannot be sent"):
            r.execute_command("CLIENT CACHING", "NO")
        with pytest.raises(RedisError, match="CLIENT CACHING cannot be sent"):
            r.pipeline(transaction=False).execute_command(
                "CLIENT", "CACHING", "NO"
            ).execute()
        with pytest.raises(RedisError, match="CLIENT CACHING cannot be sent"):
            r.pipeline().execute_command(b"CLIENT CACHING", b"NO").execute()

        # Nothing reached the socket, so the next cached read is still tracked.
        r2.set("foo", "bar")
        assert r.get("foo") == b"bar"
        assert cache.size == 1

        r2.set("foo", "baz")
        assert wait_for_invalidated_value(r, "foo", [b"baz"]) == b"baz"

    @pytest.mark.onlynoncluster
    def test_client_caching_without_a_cache_is_not_refused(self, r2):
        # The guard lives on the cache's connections only: without a cache the command
        # reaches the server, which answers it as before.
        assert r2.get_cache() is None
        with pytest.raises(ResponseError, match="tracking mode"):
            r2.execute_command("CLIENT CACHING", "NO")

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(CacheConfig(max_size=128)),
                "single_connection_client": True,
            },
            {
                "cache": DefaultCache(CacheConfig(max_size=128)),
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_the_cache_is_flushed_when_tracking_is_re_enabled(self, r):
        """
        The server destroys a connection's tracking state on disconnect, so every entry cached
        through the previous session has lost its invalidation channel. The flush therefore
        cannot live in ``disconnect`` alone: the silent reconnect inside the send path bypasses
        it, and the connect callback that re-enables tracking is the one place every reconnect
        path goes through.
        """
        cache = r.get_cache()
        r.set("foo", "bar")
        assert r.get("foo") == b"bar"
        assert cache.size == 1

        conn = r.connection_pool.get_connection()
        try:
            # Kill the socket through the wrapped connection, the way the silent reconnect
            # does, so ``CacheProxyConnection.disconnect`` never runs.
            conn._conn.disconnect()
            assert cache.size == 1

            conn.connect()
            assert cache.size == 0
        finally:
            r.connection_pool.release(conn)

    @pytest.mark.parametrize(
        "r,read_prefix,expected_tracked",
        [
            (
                {
                    "cache": DefaultCache(CacheConfig(max_size=128)),
                    "single_connection_client": True,
                },
                "user:",
                1,
            ),
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(
                            max_size=128,
                            tracking_mode=TrackingMode.OPTIN,
                            cache_predicate=keys_prefixed("user:"),
                        )
                    ),
                    "single_connection_client": True,
                },
                "user:",
                1,
            ),
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(
                            max_size=128,
                            tracking_mode=TrackingMode.OPTIN,
                            cache_predicate=keys_prefixed("user:"),
                        )
                    ),
                    "single_connection_client": True,
                },
                "session:",
                0,
            ),
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(
                            max_size=128,
                            tracking_mode=TrackingMode.OPTOUT,
                            cache_predicate=keys_excluding("counter:"),
                        )
                    ),
                    "single_connection_client": True,
                },
                "user:",
                1,
            ),
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(
                            max_size=128,
                            tracking_mode=TrackingMode.OPTOUT,
                            cache_predicate=keys_excluding("counter:"),
                        )
                    ),
                    "single_connection_client": True,
                },
                "counter:",
                0,
            ),
        ],
        ids=[
            "plain-stored",
            "optin-stored",
            "optin-not-selected",
            "optout-stored",
            "optout-excluded",
        ],
        indirect=["r"],
    )
    @pytest.mark.onlynoncluster
    def test_only_a_stored_read_enters_the_invalidation_table(
        self, r, r2, read_prefix, expected_tracked
    ):
        """
        The server-side half of every mode, read from the server's own counter.

        Invalidation tests prove that a stored read is tracked; only this proves that a read
        the client will not store is left out of the table, which is the whole point of both
        modes. A hit is read too, and must add nothing: it never reaches the server.
        """
        cache = r.get_cache()
        key = f"{read_prefix}{uuid.uuid4().hex}"
        r2.set(key, "v")
        before = tracked_key_count(r2)

        assert r.get(key) == b"v"
        assert r.get(key) == b"v"

        assert tracked_key_count(r2) - before == expected_tracked
        assert cache.size == expected_tracked

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTIN,
                        cache_predicate=keys_prefixed("user:"),
                    )
                ),
                "single_connection_client": True,
            },
        ],
        ids=["optin"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_a_failed_paired_read_leaves_the_connection_in_sync(self, r):
        """
        A ``WRONGTYPE`` on the read of a ``CLIENT CACHING YES`` pair: the ``+OK`` is consumed
        first, the placeholder goes with the failed read, and the next command on the same
        connection reads its own reply.
        """
        cache = r.get_cache()
        r.lpush("user:list", "a")

        with pytest.raises(ResponseError, match="WRONGTYPE"):
            r.get("user:list")
        assert cache.size == 0

        assert r.echo("in-sync") == b"in-sync"
        r.set("user:42", "alice")
        assert r.get("user:42") == b"alice"
        assert cache.size == 1

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTIN,
                        cache_predicate=keys_prefixed("user:"),
                    )
                ),
                "single_connection_client": True,
            },
        ],
        ids=["optin"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_a_refused_caching_command_drains_the_paired_read(self, r):
        """
        The server refuses ``CLIENT CACHING`` once tracking is off, but still executes the
        read written right behind it. That reply has to be taken off the socket, or the next
        command on this connection would read it as its own.
        """
        cache = r.get_cache()
        r.set("user:42", "alice")

        # Behind the proxy's back, so nothing on the client side knows tracking is off.
        inner = r.connection._conn
        inner.send_command("CLIENT", "TRACKING", "OFF")
        assert str_if_bytes(inner.read_response()) == "OK"

        with pytest.raises(ResponseError, match="CLIENT CACHING"):
            r.get("user:42")
        assert cache.size == 0

        assert r.echo("in-sync") == b"in-sync"

    @pytest.mark.parametrize(
        "r,expected_flags",
        [
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(
                            max_size=128,
                            tracking_mode=TrackingMode.OPTIN,
                            cache_predicate=keys_prefixed("user:"),
                        )
                    ),
                    "single_connection_client": True,
                },
                {"on", "optin"},
            ),
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(
                            max_size=128,
                            tracking_mode=TrackingMode.OPTOUT,
                            cache_predicate=keys_excluding("counter:"),
                        )
                    ),
                    "single_connection_client": True,
                },
                {"on", "optout"},
            ),
        ],
        ids=["optin", "optout"],
        indirect=["r"],
    )
    @pytest.mark.onlynoncluster
    def test_the_tracking_mode_survives_a_silent_reconnect(self, r, r2, expected_flags):
        """
        The mode lives in the handshake, so a reconnect inside the send path must send it
        again - and a read cached after the reconnect must still be invalidated.
        """
        r2.set("user:42", "alice")

        # Kill the socket through the wrapped connection, the way the silent reconnect does,
        # so ``CacheProxyConnection.disconnect`` never runs.
        r.connection._conn.disconnect()
        assert r.ping()

        assert tracking_flags(r) == expected_flags
        assert r.get("user:42") == b"alice"
        assert r.get_cache().size == 1

        r2.set("user:42", "bob")
        assert wait_for_invalidated_value(r, "user:42", [b"bob"]) == b"bob"

    # Skipped until a hit stops reading the socket of a connection another thread has
    # checked out: today readers on one pool steal each other's replies and stall in
    # ``recv`` or time out. See .agents/csc_drain_owner_checkout_deferred_task.md.
    @pytest.mark.skip(
        reason="a CSC hit drains a connection another thread may have checked out; "
        "see .agents/csc_drain_owner_checkout_deferred_task.md"
    )
    @pytest.mark.parametrize(
        "r",
        [{"cache_config": CacheConfig()}],
        indirect=True,
    )
    def test_concurrent_cached_reads_on_one_pool(self, r, r2):
        r.set("counter", 0)
        r.get("counter")

        observed = _read_while_incremented(r, r2, "counter")

        assert all(observed)
        assert int(r.get("counter")) == int(r2.get("counter"))


def _read_while_incremented(r, r2, key, readers=4, seconds=1.0):
    """
    Read ``key`` through ``r`` from several threads while ``r2`` increments it.

    Returns each reader's observed values, in the order it read them. The readers share
    ``r``'s pool, so a hit can drain a connection another reader is using.
    """
    stop = threading.Event()
    errors = []
    observed = [[] for _ in range(readers)]

    def read(values):
        try:
            while not stop.is_set():
                values.append(int(r.get(key)))
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=read, args=(values,), daemon=True)
        for values in observed
    ]
    for thread in threads:
        thread.start()

    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        r2.incr(key)

    stop.set()
    for thread in threads:
        thread.join(timeout=5)

    assert not any(thread.is_alive() for thread in threads), "a reader never finished"
    assert errors == []
    return observed


def _get_cache_key(*keys):
    return CacheKey(command="GET", redis_keys=keys, redis_args=("GET", *keys))


def _cached_value(cache, cache_key):
    """The stored reply for ``cache_key``, or None; read without touching the LRU order."""
    entry = cache.collection.get(cache_key)
    if entry is None or entry.status != CacheEntryStatus.VALID:
        return None
    return str_if_bytes(entry.cache_value)


def _server_get_calls(client) -> int:
    """How many GETs the server has run, from every client: proves a re-read happened."""
    return int(client.info("commandstats")["cmdstat_get"]["calls"])


def _wait_for_refreshes(refresher):
    # The callback submits inside the read that picked the invalidation up, so by the time
    # that read returns the keys are pending. Empty means every accepted refresh has
    # finished or was cancelled; the callers assert which by what the cache holds.
    wait_for_condition(
        lambda: not refresher._pending,
        timeout=2,
        error_message="refreshes never completed",
    )


def _refresh_config(**kwargs):
    return CacheConfig(invalidation_policy=InvalidationPolicy.REFRESH, **kwargs)


@pytest.mark.onlynoncluster
@skip_if_resp_version(2)
@skip_if_server_version_lt("7.4.0")
class TestCacheRefresh:
    """
    Refresh against a live server.

    Invalidations are read only when the tracking connection is used again, so each test
    caches through a single-connection client and then sends a ``PING`` on it to pick the
    invalidation up. The refresh itself runs on a second connection from the pool.
    """

    @pytest.mark.parametrize(
        "r,expected",
        [
            (
                {"cache_config": _refresh_config(), "single_connection_client": True},
                b"barbar",
            ),
            (
                {
                    "cache_config": _refresh_config(),
                    "single_connection_client": True,
                    "decode_responses": True,
                },
                "barbar",
            ),
        ],
        ids=["single", "decoded"],
        indirect=["r"],
    )
    def test_an_invalidated_entry_is_refreshed_without_a_read(self, r, r2, expected):
        cache = r.get_cache()
        r.set("foo", "bar")
        r.get("foo")

        r2.set("foo", "barbar")
        r.ping()
        _wait_for_refreshes(r.connection_pool._cache_refresher)

        # Stored by the refresh: no read of ``foo`` has run since the write. Compared
        # exactly, so a refresh that decoded differently from the read it replays fails.
        entry = cache.collection.get(_get_cache_key("foo"))
        assert entry.status == CacheEntryStatus.VALID
        assert entry.cache_value == expected
        assert r.get("foo") == expected

    @pytest.mark.parametrize(
        "r",
        [{"cache_config": _refresh_config(), "single_connection_client": True}],
        indirect=True,
    )
    def test_a_refreshed_entry_is_tracked_again(self, r, r2):
        cache = r.get_cache()
        refresher = r.connection_pool._cache_refresher
        r.set("foo", "v1")
        r.get("foo")

        r2.set("foo", "v2")
        r.ping()
        _wait_for_refreshes(refresher)
        assert _cached_value(cache, _get_cache_key("foo")) == "v2"

        # The refresh ran on the pool's other connection, which the server now tracks the
        # key for: the next write is reported there, and the entry is refreshed again.
        refresh_connection = cache.collection.get(_get_cache_key("foo")).connection_ref
        assert refresh_connection is not r.connection._conn

        r2.set("foo", "v3")
        # The pool's only idle connection is the one that refreshed, so a command through
        # the pool picks its invalidation up.
        redis.Redis(connection_pool=r.connection_pool).ping()
        _wait_for_refreshes(refresher)

        assert _cached_value(cache, _get_cache_key("foo")) == "v3"

    @pytest.mark.parametrize(
        "r",
        [{"cache_config": _refresh_config(), "single_connection_client": True}],
        indirect=True,
    )
    def test_a_multi_key_entry_is_refreshed(self, r, r2):
        cache = r.get_cache()
        r.mset({"a": "1", "b": "2"})
        r.mget("a", "b")
        mget_key = CacheKey(
            command="MGET", redis_keys=("a", "b"), redis_args=("MGET", "a", "b")
        )

        r2.set("b", "22")
        r.ping()
        _wait_for_refreshes(r.connection_pool._cache_refresher)

        entry = cache.collection.get(mget_key)
        assert entry is not None and entry.status == CacheEntryStatus.VALID
        assert [str_if_bytes(v) for v in entry.cache_value] == ["1", "22"]

    @pytest.mark.parametrize(
        "r",
        [{"cache_config": _refresh_config(), "single_connection_client": True}],
        indirect=True,
    )
    def test_a_server_flush_refreshes_nothing(self, r, r2):
        cache = r.get_cache()
        refresher = r.connection_pool._cache_refresher
        r.set("foo", "bar")
        r.set("baz", "qux")
        r.get("foo")
        r.get("baz")

        r2.flushall()
        # Written again before the flush is picked up, so a refresh queued by mistake
        # would read a value and store it.
        r2.set("foo", "bar")
        r2.set("baz", "qux")
        r.ping()
        _wait_for_refreshes(refresher)

        assert cache.size == 0

    @pytest.mark.parametrize(
        "r",
        [{"cache_config": _refresh_config(), "single_connection_client": True}],
        indirect=True,
    )
    def test_a_disconnect_drops_queued_refreshes(self, r, r2):
        cache = r.get_cache()
        refresher = r.connection_pool._cache_refresher
        r.set("foo", "bar")
        r.set("baz", "qux")
        r.get("foo")
        r.get("baz")

        # The first refresh is held before it checks the cache, so the second one stays
        # queued behind it while the pool disconnects.
        started = threading.Event()
        release = threading.Event()
        done = threading.Event()
        refresh = _CacheRefresher._refresh

        def held_refresh(self, pool, cache_key, generation):
            started.set()
            release.wait(5)
            try:
                return refresh(self, pool, cache_key, generation)
            finally:
                done.set()

        with patch.object(_CacheRefresher, "_refresh", held_refresh):
            r2.set("foo", "barbar")
            r2.set("baz", "quxqux")
            r.ping()
            assert started.wait(2)

            get_calls = _server_get_calls(r2)
            r.connection_pool.disconnect()
            release.set()

            # The cancel cleared ``_pending``, so completion is told by the held job
            # returning and the queued one being dequeued.
            assert done.wait(2)
            wait_for_condition(
                refresher._queue.empty,
                timeout=2,
                error_message="the queued refresh was never dequeued",
            )

        # Neither refresh read anything: the held one stopped at its generation check,
        # and the queued one was dropped unrun.
        assert _server_get_calls(r2) == get_calls
        assert cache.size == 0

    # Skipped for the same reason as
    # ``TestCache::test_concurrent_cached_reads_on_one_pool``: several readers on one pool
    # stall today, with or without refresh. See
    # .agents/csc_drain_owner_checkout_deferred_task.md.
    @pytest.mark.skip(
        reason="a CSC hit drains a connection another thread may have checked out; "
        "see .agents/csc_drain_owner_checkout_deferred_task.md"
    )
    @pytest.mark.parametrize(
        "r",
        [{"cache_config": _refresh_config()}],
        indirect=True,
    )
    def test_concurrent_readers_never_see_a_value_go_back(self, r, r2):
        cache = r.get_cache()
        refresher = r.connection_pool._cache_refresher
        r.set("counter", 0)
        r.get("counter")

        observed = _read_while_incremented(r, r2, "counter")

        # Each reader's reads are sequential, so whatever served them - the cache, a
        # refresh or the server - a later read is never older than an earlier one.
        for values in observed:
            assert values
            assert values == sorted(values)

        _wait_for_refreshes(refresher)
        server_value = int(r2.get("counter"))
        # A read drains the entry's connection first, so it finds the last invalidation.
        assert int(r.get("counter")) == server_value
        cached = _cached_value(cache, _get_cache_key("counter"))
        assert cached is None or int(cached) == server_value

    @pytest.mark.parametrize(
        "r",
        [{"cache_config": _refresh_config(), "single_connection_client": True}],
        indirect=True,
    )
    def test_a_deleted_key_is_not_stored_back(self, r, r2):
        cache = r.get_cache()
        r.set("foo", "bar")
        r.get("foo")

        r2.delete("foo")
        get_calls = _server_get_calls(r2)
        r.ping()
        _wait_for_refreshes(r.connection_pool._cache_refresher)

        # The re-read ran, answered nil, and a nil reply is never stored.
        assert _server_get_calls(r2) == get_calls + 1
        assert cache.collection.get(_get_cache_key("foo")) is None
        assert r.get("foo") is None

    @pytest.mark.parametrize(
        "r",
        [{"cache_config": _refresh_config(), "single_connection_client": True}],
        indirect=True,
    )
    def test_a_refresh_answered_with_an_error_leaves_the_entry_removed(self, r, r2):
        cache = r.get_cache()
        r.set("foo", "bar")
        r.get("foo")

        r2.delete("foo")
        r2.lpush("foo", "x")
        get_calls = _server_get_calls(r2)
        r.ping()
        _wait_for_refreshes(r.connection_pool._cache_refresher)

        assert _server_get_calls(r2) == get_calls + 1
        assert cache.collection.get(_get_cache_key("foo")) is None
        with pytest.raises(ResponseError, match="WRONGTYPE"):
            r.get("foo")
        assert r.ping()

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": _refresh_config(),
                "single_connection_client": True,
                "kwargs": {"max_connections": 1},
            }
        ],
        indirect=True,
    )
    def test_a_full_pool_skips_the_refresh(self, r, r2):
        # The single connection is the pool's only one, so a refresh would have to take
        # the connection the application holds.
        cache = r.get_cache()
        r.set("foo", "bar")
        r.get("foo")

        r2.set("foo", "barbar")
        get_calls = _server_get_calls(r2)
        recorded = []
        with patch(
            "redis.cache.record_csc_refresh",
            side_effect=lambda result, count=1: recorded.append((result, count)),
        ):
            r.ping()
            _wait_for_refreshes(r.connection_pool._cache_refresher)

        # Skipped for lack of capacity, which is told apart from a failed checkout.
        assert recorded == [(CSCRefreshResult.REJECTED, 1)]
        assert _server_get_calls(r2) == get_calls
        assert cache.collection.get(_get_cache_key("foo")) is None
        assert r.connection_pool._created_connections == 1
        assert r.get("foo") == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [{"cache_config": CacheConfig(), "single_connection_client": True}],
        indirect=True,
    )
    def test_the_evict_default_does_not_refresh(self, r, r2):
        cache = r.get_cache()
        r.set("foo", "bar")
        r.get("foo")

        r2.set("foo", "barbar")
        r.ping()

        assert r.connection_pool._cache_refresher is None
        assert cache.collection.get(_get_cache_key("foo")) is None

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": _refresh_config(
                    tracking_mode=TrackingMode.OPTIN,
                    cache_predicate=keys_prefixed("foo"),
                ),
                "single_connection_client": True,
            }
        ],
        indirect=True,
    )
    def test_optin_refreshes_what_the_predicate_selects(self, r, r2):
        cache = r.get_cache()
        r.set("foo", "1")
        r.set("bar", "1")
        r.get("foo")
        r.get("bar")
        assert cache.collection.get(_get_cache_key("bar")) is None

        r2.set("foo", "2")
        r2.set("bar", "2")
        r.ping()
        _wait_for_refreshes(r.connection_pool._cache_refresher)

        assert _cached_value(cache, _get_cache_key("foo")) == "2"
        assert cache.collection.get(_get_cache_key("bar")) is None

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": _refresh_config(tracking_mode=TrackingMode.OPTOUT),
                "single_connection_client": True,
            }
        ],
        indirect=True,
    )
    def test_optout_refreshes(self, r, r2):
        cache = r.get_cache()
        r.set("foo", "1")
        r.get("foo")

        r2.set("foo", "2")
        r.ping()
        _wait_for_refreshes(r.connection_pool._cache_refresher)

        assert _cached_value(cache, _get_cache_key("foo")) == "2"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": _refresh_config(refresh_max_inflight=1),
                "single_connection_client": True,
            }
        ],
        indirect=True,
    )
    def test_a_hot_key_settles_on_the_server_value(self, r, r2):
        cache = r.get_cache()
        r.set("counter", 0)
        r.get("counter")

        refresher = r.connection_pool._cache_refresher
        # After the first refresh the key is tracked on whichever pool connection re-read
        # it, not on the single connection, so later invalidations are picked up through
        # the pool. How many of them a given ping catches depends on which idle connection
        # the pool hands out; what must hold is the final value.
        pool_client = redis.Redis(connection_pool=r.connection_pool)
        r2.incr("counter")
        r.ping()
        for _ in range(199):
            _wait_for_refreshes(refresher)
            r2.incr("counter")
            pool_client.ping()
        _wait_for_refreshes(refresher)

        # Whatever the bound let through, nothing older than the server's value is served.
        assert int(r.get("counter")) == 200
        cached = _cached_value(cache, _get_cache_key("counter"))
        assert cached is None or int(cached) == 200


@pytest.mark.onlycluster
@skip_if_resp_version(2)
@skip_if_server_version_lt("7.4.0")
class TestClusterCache:
    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(CacheConfig(max_size=128)),
            },
            {
                "cache": DefaultCache(CacheConfig(max_size=128)),
                "decode_responses": True,
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_get_from_cache(self, r):
        cache = r.nodes_manager.get_node_from_slot(12000).redis_connection.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") in [b"bar", "bar"]
        # get key from local cache
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"barbar",
            "barbar",
        ]
        # Make sure that cache is shared between nodes.
        assert (
            cache == r.nodes_manager.get_node_from_slot(1).redis_connection.get_cache()
        )

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTIN,
                        cache_predicate=keys_prefixed("foo"),
                    )
                ),
            },
        ],
        ids=["optin"],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_optin_caches_and_tracks_through_a_node_client(self, r):
        # The pair is written by the node connection that serves the slot, so this proves the
        # CACHING command reaches the right socket in a cluster too.
        cache = r.nodes_manager.get_node_from_slot(12000).redis_connection.get_cache()
        r.set("foo", "bar")

        assert r.get("foo") == b"bar"
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            ).cache_value
            == b"bar"
        )

        # Invalidated, which only happens if the YES was paired with the read.
        r.set("foo", "barbar")
        assert wait_for_invalidated_value(r, "foo", [b"barbar"]) == b"barbar"

    @pytest.mark.parametrize(
        "r,expected_flags",
        [
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(
                            max_size=128,
                            tracking_mode=TrackingMode.OPTIN,
                            cache_predicate=keys_prefixed("foo"),
                        )
                    ),
                },
                {"on", "optin"},
            ),
            (
                {
                    "cache": DefaultCache(
                        CacheConfig(
                            max_size=128,
                            tracking_mode=TrackingMode.OPTOUT,
                            cache_predicate=keys_excluding("counter:"),
                        )
                    ),
                },
                {"on", "optout"},
            ),
        ],
        ids=["optin", "optout"],
        indirect=["r"],
    )
    @pytest.mark.onlycluster
    def test_the_tracking_mode_reaches_every_node(self, r, expected_flags):
        # One cache is shared by every node client, but each node pool enables tracking on its
        # own sockets, so the mode has to arrive at all of them - not just the one serving the
        # slot the other tests happen to read.
        primaries = r.get_primaries()

        assert primaries, "no primaries to check"
        for node in primaries:
            assert tracking_flags(node.redis_connection) == expected_flags, node.name

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTOUT,
                        cache_predicate=keys_excluding("counter:"),
                    )
                ),
            },
        ],
        ids=["optout"],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_optout_exempts_the_excluded_read_and_caches_the_rest(self, r):
        # The two keys hash to different slots, so this also shows the exemption is decided per
        # invocation on whichever node serves it, against the one cache they all share.
        cache = r.nodes_manager.get_node_from_slot(12000).redis_connection.get_cache()
        r.set("counter:hits", "1")
        r.set("foo", "bar")

        assert r.get("counter:hits") == b"1"
        assert cache.size == 0

        assert r.get("foo") == b"bar"
        assert cache.size == 1

        r.set("foo", "barbar")
        assert wait_for_invalidated_value(r, "foo", [b"barbar"]) == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTIN,
                        cache_predicate=keys_prefixed("foo"),
                    )
                ),
            },
        ],
        ids=["optin"],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_an_ask_redirected_read_is_not_paired(self, r):
        """
        The ASK suppression, through the real cluster retry loop.

        ``ASKING`` and ``CLIENT CACHING`` clear each other on the server, so pairing the
        redirected attempt would strip the ASK allowance and the read would be redirected
        again. The proxy learns about it by *observing* a command named ``ASKING`` rather than
        being handed a flag, so what needs proving here is the wiring: that the executor sends
        ``ASKING`` on the very connection it then sends the retried command on
        (``redis/cluster.py``, the ``asking`` branch of ``_execute_command``). The storage
        half of the rule - an ASK attempt caches nothing - is pinned by
        ``TestTrackingModePairing.test_the_ask_redirect_suppression_is_one_shot``.

        The ASK is pointed back at the slot's own owner on purpose: the client does not care
        where it points, and a node that is not importing the slot would answer the retry with
        MOVED and turn this into a three-attempt cascade.

        The key is deliberately left missing. A real ASK-erroring attempt stores nothing, and a
        nil reply stores nothing either, so the retry stays a genuine miss and reaches the
        pairing decision. Letting the first attempt store a value instead would make the retry
        a cache hit that returns before the pairing branch, and the test would pass even with
        the suppression removed.
        """
        slot = r.keyslot("foo")
        owner = r.nodes_manager.get_node_from_slot(slot)
        cache = r.nodes_manager.get_node_from_slot(slot).redis_connection.get_cache()

        pairings = []
        sends = []
        real_send_with_caching = CacheProxyConnection._send_with_caching
        real_send_command = CacheProxyConnection.send_command
        real_parse_response = redis.Redis.parse_response
        asked = []

        def spy_send_with_caching(self, decision, args, kwargs):
            pairings.append((id(self), decision, args[0]))
            return real_send_with_caching(self, decision, args, kwargs)

        def spy_send_command(self, *args, **kwargs):
            sends.append((id(self), args[0]))
            return real_send_command(self, *args, **kwargs)

        def ask_once(self, connection, command_name, **options):
            # Call through first: the paired attempt has a ``+OK`` and a data reply on the
            # socket, and raising before they are read would hand the pool a desynchronised
            # connection. Replacing ``parse_response`` outright - the pattern in
            # tests/test_cluster.py - is only safe for an unpaired command.
            result = real_parse_response(self, connection, command_name, **options)
            if command_name == "GET" and not asked:
                asked.append(True)
                raise AskError(f"{slot} {owner.host}:{owner.port}")
            return result

        with (
            patch.object(
                CacheProxyConnection, "_send_with_caching", spy_send_with_caching
            ),
            patch.object(CacheProxyConnection, "send_command", spy_send_command),
            patch.object(redis.Redis, "parse_response", ask_once),
        ):
            assert r.get("foo") is None

        assert asked, "the ASK redirect never fired"
        assert cache.size == 0

        # The first attempt paired; the ASK-redirected one did not.
        assert [decision for _, decision, _ in pairings] == [b"YES"]

        # ASKING and the retried GET left on one and the same connection, which is what makes
        # observing the command name sufficient.
        assert [command for _, command in sends[-2:]] == ["ASKING", "GET"]
        assert sends[-2][0] == sends[-1][0]

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTIN,
                        cache_predicate=keys_prefixed("foo"),
                    )
                ),
            },
        ],
        ids=["optin"],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_a_moved_retried_read_is_paired_again(self, r):
        """
        A redirect reply consumes the server's CACHING flag, so the retry has to send the
        CACHING command again or the read comes back untracked.

        Nothing in the client does that explicitly: ``send_command`` runs once per transmission
        attempt and the redirect loop re-enters it, so the pair is rebuilt by construction. This
        pins that construction - and it is the opposite verdict to the ASK test next to it,
        which is what makes the pair of them meaningful.

        The MOVED is pointed back at the slot's current owner, so patching the slot table
        rewrites it to what it already was and the topology is untouched. The key is left
        missing for the same reason as in the ASK test: a stored value would turn the retry
        into a cache hit that never reaches the pairing branch.
        """
        slot = r.keyslot("foo")
        owner = r.nodes_manager.get_node_from_slot(slot)

        pairings = []
        sends = []
        real_send_with_caching = CacheProxyConnection._send_with_caching
        real_send_command = CacheProxyConnection.send_command
        real_parse_response = redis.Redis.parse_response
        moved = []

        def spy_send_with_caching(self, decision, args, kwargs):
            pairings.append(decision)
            return real_send_with_caching(self, decision, args, kwargs)

        def spy_send_command(self, *args, **kwargs):
            sends.append(args[0])
            return real_send_command(self, *args, **kwargs)

        def moved_once(self, connection, command_name, **options):
            # Call through first, so the paired attempt's ``+OK`` and data reply are both off
            # the socket before the redirect unwinds the call.
            result = real_parse_response(self, connection, command_name, **options)
            if command_name == "GET" and not moved:
                moved.append(True)
                raise MovedError(f"{slot} {owner.host}:{owner.port}")
            return result

        with (
            patch.object(
                CacheProxyConnection, "_send_with_caching", spy_send_with_caching
            ),
            patch.object(CacheProxyConnection, "send_command", spy_send_command),
            patch.object(redis.Redis, "parse_response", moved_once),
        ):
            assert r.get("foo") is None

        assert moved, "the MOVED redirect never fired"

        # Both attempts paired, and no ASKING anywhere - MOVED is not ASK.
        assert pairings == [b"YES", b"YES"]
        assert "ASKING" not in sends

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "kwargs": {"metadata_resolver": StaticMetadataResolver()},
            },
        ],
        ids=["pool"],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_metadata_resolver_reaches_every_node_client(self, r):
        """
        One resolver, shared cluster-wide. Asserted by identity rather than behaviour: two
        static resolvers give the same answers, so only identity proves the object was
        distributed rather than rebuilt per node.
        """
        resolver = r._metadata_resolver

        assert r.nodes_manager._metadata_resolver is resolver
        nodes = list(r.nodes_manager.nodes_cache.values())
        assert len(nodes) > 1
        for node in nodes:
            pool = node.redis_connection.connection_pool
            assert pool.metadata_resolver is resolver, node.name
            assert pool.cache.config._metadata_resolver is resolver, node.name

        # And routing derives from it, since no policy_resolver was given.
        assert r._policy_resolver._metadata_resolver is resolver

        # Still caches what it should, through the distributed resolver.
        r.set("foo", "bar")
        assert r.get("foo") == b"bar"
        cache = r.nodes_manager.get_node_from_slot(12000).redis_connection.get_cache()
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            ).cache_value
            == b"bar"
        )

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
            },
            {
                "cache_config": CacheConfig(max_size=128),
                "decode_responses": True,
            },
        ],
        indirect=True,
    )
    def test_get_from_custom_cache(self, r, r2):
        cache = r.nodes_manager.get_node_from_slot(12000).redis_connection.get_cache()
        assert isinstance(cache.eviction_policy, LRUPolicy)
        assert cache.config.get_max_size() == 128

        # add key to redis
        assert r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") in [b"bar", "bar"]
        # get key from local cache
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert wait_for_invalidated_value(r, "foo", (b"barbar", "barbar")) in [
            b"barbar",
            "barbar",
        ]
        # Make sure that new value was cached
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"barbar",
            "barbar",
        ]

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_cache_clears_on_disconnect(self, r, r2):
        cache = r.nodes_manager.get_node_from_slot(12000).redis_connection.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            ).cache_value
            == b"bar"
        )
        # Force disconnection
        r.nodes_manager.get_node_from_slot(
            12000
        ).redis_connection.connection_pool.get_connection().disconnect()
        # Make sure cache is empty
        assert cache.size == 0

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=3),
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_cache_lru_eviction(self, r):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # add 3 keys to redis
        r.set("foo{slot}", "bar")
        r.set("foo2{slot}", "bar2")
        r.set("foo3{slot}", "bar3")
        # get 3 keys from redis and save in local cache
        assert r.get("foo{slot}") == b"bar"
        assert r.get("foo2{slot}") == b"bar2"
        assert r.get("foo3{slot}") == b"bar3"
        # get the 3 keys from local cache
        assert (
            cache.get(
                CacheKey(
                    command="GET",
                    redis_keys=("foo{slot}",),
                    redis_args=("GET", "foo{slot}"),
                )
            ).cache_value
            == b"bar"
        )
        assert (
            cache.get(
                CacheKey(
                    command="GET",
                    redis_keys=("foo2{slot}",),
                    redis_args=("GET", "foo2{slot}"),
                )
            ).cache_value
            == b"bar2"
        )
        assert (
            cache.get(
                CacheKey(
                    command="GET",
                    redis_keys=("foo3{slot}",),
                    redis_args=("GET", "foo3{slot}"),
                )
            ).cache_value
            == b"bar3"
        )
        # add 1 more key to redis (exceed the max size)
        r.set("foo4{slot}", "bar4")
        assert r.get("foo4{slot}") == b"bar4"
        # the first key is not in the local cache_data anymore
        assert (
            cache.get(
                CacheKey(
                    command="GET",
                    redis_keys=("foo{slot}",),
                    redis_args=("GET", "foo{slot}"),
                )
            )
            is None
        )

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_cache_ignore_not_allowed_command(self, r):
        cache = r.nodes_manager.get_node_from_slot(12000).redis_connection.get_cache()
        # add fields to hash
        assert r.hset("foo", "bar", "baz")
        # get random field
        assert r.hrandfield("foo") == b"bar"
        assert (
            cache.get(
                CacheKey(
                    command="HRANDFIELD",
                    redis_keys=("foo",),
                    redis_args=("HRANDFIELD", "foo"),
                )
            )
            is None
        )

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_cache_invalidate_all_related_responses(self, r, cache):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # Add keys
        assert r.set("foo{slot}", "bar")
        assert r.set("bar{slot}", "foo")

        # Make sure that replies was cached
        assert r.mget("foo{slot}", "bar{slot}") == [b"bar", b"foo"]
        assert cache.get(
            CacheKey(
                command="MGET",
                redis_keys=("foo{slot}", "bar{slot}"),
                redis_args=(
                    "MGET",
                    "foo{slot}",
                    "bar{slot}",
                ),
            ),
        ).cache_value == [b"bar", b"foo"]

        # Invalidate one of the keys and make sure
        # that all associated cached entries was removed
        assert r.set("foo{slot}", "baz")
        assert r.get("foo{slot}") == b"baz"
        assert (
            cache.get(
                CacheKey(
                    command="MGET",
                    redis_keys=("foo{slot}", "bar{slot}"),
                    redis_args=("MGET", "foo{slot}", "bar{slot}"),
                ),
            )
            is None
        )
        assert (
            cache.get(
                CacheKey(
                    command="GET",
                    redis_keys=("foo{slot}",),
                    redis_args=("GET", "foo{slot}"),
                )
            ).cache_value
            == b"baz"
        )

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_cache_flushed_on_server_flush(self, r, cache):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # Add keys
        assert r.set("foo{slot}", "bar")
        assert r.set("bar{slot}", "foo")
        assert r.set("baz{slot}", "bar")

        # Make sure that replies was cached
        assert r.get("foo{slot}") == b"bar"
        assert r.get("bar{slot}") == b"foo"
        assert r.get("baz{slot}") == b"bar"
        assert (
            cache.get(
                CacheKey(
                    command="GET",
                    redis_keys=("foo{slot}",),
                    redis_args=("GET", "foo{slot}"),
                )
            ).cache_value
            == b"bar"
        )
        assert (
            cache.get(
                CacheKey(
                    command="GET",
                    redis_keys=("bar{slot}",),
                    redis_args=("GET", "bar{slot}"),
                )
            ).cache_value
            == b"foo"
        )
        assert (
            cache.get(
                CacheKey(
                    command="GET",
                    redis_keys=("baz{slot}",),
                    redis_args=("GET", "baz{slot}"),
                )
            ).cache_value
            == b"bar"
        )

        # Flush server and trying to access cached entry
        assert r.flushall()
        assert r.get("foo{slot}") is None
        assert cache.size == 0


@pytest.mark.onlycluster
@skip_if_resp_version(2)
@skip_if_server_version_lt("7.4.0")
class TestClusterCacheRefresh:
    """
    Refresh on a cluster: each node pool refreshes on the node that owns the key, and all
    of them share one cache.

    A write through the same client reaches the node pool's idle connection - the one that
    cached the key - so the reply of the ``PING`` sent to that node afterwards carries the
    invalidation in front of it.
    """

    @staticmethod
    def _pick_up(r, key):
        node = r.get_node_from_key(key)
        r.ping(target_nodes=node)
        return node.redis_connection.connection_pool._cache_refresher

    @pytest.mark.parametrize(
        "r",
        [{"cache_config": _refresh_config(max_size=128)}],
        indirect=True,
    )
    def test_an_invalidated_entry_is_refreshed_on_its_node(self, r):
        r.set("{refresh}foo", "bar")
        r.get("{refresh}foo")
        cache = r.get_node_from_key("{refresh}foo").redis_connection.get_cache()

        r.set("{refresh}foo", "barbar")
        refresher = self._pick_up(r, "{refresh}foo")
        _wait_for_refreshes(refresher)

        assert _cached_value(cache, _get_cache_key("{refresh}foo")) == "barbar"

    @pytest.mark.parametrize(
        "r",
        [{"cache_config": _refresh_config(max_size=128)}],
        indirect=True,
    )
    def test_keys_on_different_nodes_refresh_into_one_cache(self, r):
        # Served by different primaries in the test cluster.
        keys = ["foo", "bar"]
        nodes = {r.get_node_from_key(key).name for key in keys}
        if len(nodes) < 2:
            pytest.skip("the keys share a node in this cluster")
        # The first command on a node pool can reconnect a connection the client used
        # during discovery, and a reconnect empties the shared cache. Warm every pool up
        # first, so the entries cached below survive until the writes.
        for node in r.get_primaries():
            r.ping(target_nodes=node)
        for key in keys:
            r.set(key, "1")
            r.get(key)
        cache = r.get_node_from_key("foo").redis_connection.get_cache()

        for key in keys:
            r.set(key, "2")
        refreshers = {self._pick_up(r, key) for key in keys}
        assert len(refreshers) == 2
        for refresher in refreshers:
            _wait_for_refreshes(refresher)

        for key in keys:
            assert _cached_value(cache, _get_cache_key(key)) == "2"

    @pytest.mark.parametrize(
        "r",
        [{"cache_config": _refresh_config(max_size=128)}],
        indirect=True,
    )
    def test_a_server_flush_refreshes_nothing(self, r):
        # ``flushall`` reaches every primary; a first command on a node pool would empty
        # the cache through a reconnect before the flush is even read.
        for node in r.get_primaries():
            r.ping(target_nodes=node)
        r.set("{refresh}foo", "bar")
        r.get("{refresh}foo")
        cache = r.get_node_from_key("{refresh}foo").redis_connection.get_cache()
        assert cache.collection.get(_get_cache_key("{refresh}foo")) is not None

        r.flushall()
        # Written again before the flush is picked up, so a refresh queued by mistake
        # would read a value and store it.
        r.set("{refresh}foo", "bar")
        refresher = self._pick_up(r, "{refresh}foo")
        _wait_for_refreshes(refresher)

        assert cache.collection.get(_get_cache_key("{refresh}foo")) is None


@pytest.mark.onlynoncluster
@skip_if_resp_version(2)
@skip_if_server_version_lt("7.4.0")
class TestSentinelCache:
    @pytest.mark.parametrize(
        "sentinel_setup",
        [
            {
                "cache": DefaultCache(CacheConfig(max_size=128)),
                "force_master_ip": "localhost",
            },
            {
                "cache": DefaultCache(CacheConfig(max_size=128)),
                "force_master_ip": "localhost",
                "decode_responses": True,
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_cache(self, master):
        cache = master.get_cache()
        master.set("foo", "bar")
        # get key from redis and save in local cache_data
        assert master.get("foo") in [b"bar", "bar"]
        # get key from local cache_data
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        master.set("foo", "barbar")
        # get key from redis
        assert master.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"barbar",
            "barbar",
        ]

    @pytest.mark.parametrize(
        "sentinel_setup",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTIN,
                        cache_predicate=keys_prefixed("foo"),
                    )
                ),
                "force_master_ip": "localhost",
            },
        ],
        ids=["optin"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_optin_caches_and_tracks(self, master):
        cache = master.get_cache()
        master.set("foo", "bar")

        assert master.get("foo") == b"bar"
        assert cache.size == 1

        master.set("foo", "barbar")
        assert wait_for_invalidated_value(master, "foo", [b"barbar"]) == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
            },
            {
                "cache_config": CacheConfig(max_size=128),
                "decode_responses": True,
            },
        ],
        indirect=True,
    )
    def test_get_from_default_cache(self, r, r2):
        cache = r.get_cache()
        assert isinstance(cache.eviction_policy, LRUPolicy)

        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache_data
        assert r.get("foo") in [b"bar", "bar"]
        # get key from local cache_data
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache_data it
        assert wait_for_invalidated_value(r, "foo", (b"barbar", "barbar")) in [
            b"barbar",
            "barbar",
        ]
        # Make sure that new value was cached
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"barbar",
            "barbar",
        ]

    @pytest.mark.parametrize(
        "sentinel_setup",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "force_master_ip": "localhost",
            }
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_cache_clears_on_disconnect(self, master, cache):
        cache = master.get_cache()
        # add key to redis
        master.set("foo", "bar")
        # get key from redis and save in local cache_data
        assert master.get("foo") == b"bar"
        # get key from local cache_data
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            ).cache_value
            == b"bar"
        )
        # Force disconnection
        master.connection_pool.get_connection().disconnect()
        # Make sure cache_data is empty
        assert cache.size == 0

    @pytest.mark.parametrize(
        "sentinel_setup",
        [
            {
                "cache_config": _refresh_config(max_size=128),
                "force_master_ip": "localhost",
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_an_invalidated_entry_is_refreshed(self, master):
        cache = master.get_cache()
        master.set("foo", "bar")
        master.get("foo")

        # The write and the PING reach the pool's idle connection, the one that cached
        # ``foo``, so the PING reply carries the invalidation in front of it.
        master.set("foo", "barbar")
        master.ping()
        _wait_for_refreshes(master.connection_pool._cache_refresher)

        assert _cached_value(cache, _get_cache_key("foo")) == "barbar"


@pytest.mark.onlynoncluster
@skip_if_resp_version(2)
@skip_if_server_version_lt("7.4.0")
class TestSSLCache:
    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(CacheConfig(max_size=128)),
                "ssl": True,
            },
            {
                "cache": DefaultCache(CacheConfig(max_size=128)),
                "ssl": True,
                "decode_responses": True,
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_cache(self, r, r2, cache):
        cache = r.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache_data
        assert r.get("foo") in [b"bar", "bar"]
        # get key from local cache_data
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        assert r2.set("foo", "barbar")
        # Timeout needed for SSL connection because there's timeout
        # between data appears in socket buffer
        time.sleep(0.1)
        # Retrieves a new value from server and cache_data it
        assert r.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"barbar",
            "barbar",
        ]

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": DefaultCache(
                    CacheConfig(
                        max_size=128,
                        tracking_mode=TrackingMode.OPTIN,
                        cache_predicate=keys_prefixed("foo"),
                    )
                ),
                "ssl": True,
            },
        ],
        ids=["optin"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_optin_caches_and_tracks(self, r, r2):
        cache = r.get_cache()
        r.set("foo", "bar")

        assert r.get("foo") == b"bar"
        assert cache.size == 1

        assert r2.set("foo", "barbar")
        assert wait_for_invalidated_value(r, "foo", [b"barbar"]) == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "ssl": True,
            },
            {
                "cache_config": CacheConfig(max_size=128),
                "ssl": True,
                "decode_responses": True,
            },
        ],
        indirect=True,
    )
    def test_get_from_custom_cache(self, r, r2):
        cache = r.get_cache()
        assert isinstance(cache.eviction_policy, LRUPolicy)

        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache_data
        assert r.get("foo") in [b"bar", "bar"]
        # get key from local cache_data
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache_data it
        assert wait_for_invalidated_value(r, "foo", (b"barbar", "barbar")) in [
            b"barbar",
            "barbar",
        ]
        # Make sure that new value was cached
        assert cache.get(
            CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
        ).cache_value in [
            b"barbar",
            "barbar",
        ]

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache_config": CacheConfig(max_size=128),
                "ssl": True,
            }
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_cache_invalidate_all_related_responses(self, r):
        cache = r.get_cache()
        # Add keys
        assert r.set("foo", "bar")
        assert r.set("bar", "foo")

        # Make sure that replies was cached
        assert r.mget("foo", "bar") == [b"bar", b"foo"]
        assert cache.get(
            CacheKey(
                command="MGET",
                redis_keys=("foo", "bar"),
                redis_args=("MGET", "foo", "bar"),
            )
        ).cache_value == [b"bar", b"foo"]

        # Invalidate one of the keys and make sure
        # that all associated cached entries was removed
        assert r.set("foo", "baz")
        # Timeout needed for SSL connection because there's timeout
        # between data appears in socket buffer
        time.sleep(0.1)
        assert r.get("foo") == b"baz"
        assert (
            cache.get(
                CacheKey(
                    command="MGET",
                    redis_keys=("foo", "bar"),
                    redis_args=("MGET", "foo", "bar"),
                )
            )
            is None
        )
        assert (
            cache.get(
                CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))
            ).cache_value
            == b"baz"
        )


class TestUnitDefaultCache:
    def test_get_eviction_policy(self):
        cache = DefaultCache(CacheConfig(max_size=5))
        assert isinstance(cache.eviction_policy, LRUPolicy)

    def test_get_max_size(self):
        cache = DefaultCache(CacheConfig(max_size=5))
        assert cache.config.get_max_size() == 5

    def test_get_size(self):
        cache = DefaultCache(CacheConfig(max_size=5))
        assert cache.size == 0

    @pytest.mark.parametrize(
        "cache_key", [{"command": "GET", "redis_keys": ("bar",)}], indirect=True
    )
    def test_set_non_existing_cache_key(self, cache_key, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=5))

        assert cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"val",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.get(cache_key).cache_value == b"val"

    @pytest.mark.parametrize(
        "cache_key", [{"command": "GET", "redis_keys": ("bar",)}], indirect=True
    )
    def test_set_updates_existing_cache_key(self, cache_key, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=5))

        assert cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"val",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.get(cache_key).cache_value == b"val"

        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"new_val",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.get(cache_key).cache_value == b"new_val"

    @pytest.mark.parametrize(
        "cache_key", [{"command": "HRANDFIELD", "redis_keys": ("bar",)}], indirect=True
    )
    def test_set_does_not_store_not_allowed_key(self, cache_key, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=5))

        assert not cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"val",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

    @pytest.mark.parametrize(
        "cache_key", [{"command": "GET", "redis_keys": ("bar",)}], indirect=True
    )
    def test_get_return_correct_value(self, cache_key, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=5))

        assert cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"val",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.get(cache_key).cache_value == b"val"

        wrong_key = CacheKey(
            command="HGET", redis_keys=("foo",), redis_args=("HGET", "foo", "bar")
        )
        assert cache.get(wrong_key) is None

        result = cache.get(cache_key)
        assert cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"new_val",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        # Make sure that result is immutable.
        assert result.cache_value != cache.get(cache_key).cache_value

    def test_delete_by_cache_keys_removes_associated_entries(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=5))

        cache_key1 = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_key2 = CacheKey(
            command="GET", redis_keys=("foo1",), redis_args=("GET", "foo1")
        )
        cache_key3 = CacheKey(
            command="GET", redis_keys=("foo2",), redis_args=("GET", "foo2")
        )
        cache_key4 = CacheKey(
            command="GET", redis_keys=("foo3",), redis_args=("GET", "foo3")
        )

        # Set 3 different keys
        assert cache.set(
            CacheEntry(
                cache_key=cache_key1,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.set(
            CacheEntry(
                cache_key=cache_key2,
                cache_value=b"bar1",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.set(
            CacheEntry(
                cache_key=cache_key3,
                cache_value=b"bar2",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        assert cache.delete_by_cache_keys([cache_key1, cache_key2, cache_key4]) == [
            True,
            True,
            False,
        ]
        assert len(cache.collection) == 1
        assert cache.get(cache_key3).cache_value == b"bar2"

    def test_delete_by_redis_keys_removes_associated_entries(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=5))

        cache_key1 = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_key2 = CacheKey(
            command="GET", redis_keys=("foo1",), redis_args=("GET", "foo1")
        )
        cache_key3 = CacheKey(
            command="MGET",
            redis_keys=("foo", "foo3"),
            redis_args=("MGET", "foo", "foo3"),
        )
        cache_key4 = CacheKey(
            command="MGET",
            redis_keys=("foo2", "foo3"),
            redis_args=("MGET", "foo2", "foo3"),
        )

        # Set 3 different keys
        assert cache.set(
            CacheEntry(
                cache_key=cache_key1,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.set(
            CacheEntry(
                cache_key=cache_key2,
                cache_value=b"bar1",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.set(
            CacheEntry(
                cache_key=cache_key3,
                cache_value=b"bar2",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.set(
            CacheEntry(
                cache_key=cache_key4,
                cache_value=b"bar3",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        assert cache.delete_by_redis_keys([b"foo", b"foo1"]) == [True, True, True]
        assert len(cache.collection) == 1
        assert cache.get(cache_key4).cache_value == b"bar3"

    def test_delete_by_redis_keys_with_non_utf8_bytes_key(self, mock_connection):
        """cache fails to invalidate entries when redis_keys contain non-UTF-8 bytes."""
        cache = DefaultCache(CacheConfig(max_size=5))

        # Valid UTF-8 key works
        utf8_key = b"foo"
        utf8_cache_key = CacheKey(command="GET", redis_keys=(utf8_key,))
        assert cache.set(
            CacheEntry(
                cache_key=utf8_cache_key,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        # Non-UTF-8 bytes key
        bad_key = b"f\xffoo"
        bad_cache_key = CacheKey(command="GET", redis_keys=(bad_key,))
        assert cache.set(
            CacheEntry(
                cache_key=bad_cache_key,
                cache_value=b"bar2",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        # Delete both keys: utf8 should succeed, non-utf8 exposes bug
        results = cache.delete_by_redis_keys([utf8_key, bad_key])

        assert results[0] is True
        assert results[1] is True, "Cache did not remove entry for non-UTF8 bytes key"

    def test_flush(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=5))

        cache_key1 = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_key2 = CacheKey(
            command="GET", redis_keys=("foo1",), redis_args=("GET", "foo1")
        )
        cache_key3 = CacheKey(
            command="GET", redis_keys=("foo2",), redis_args=("GET", "foo2")
        )

        # Set 3 different keys
        assert cache.set(
            CacheEntry(
                cache_key=cache_key1,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.set(
            CacheEntry(
                cache_key=cache_key2,
                cache_value=b"bar1",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.set(
            CacheEntry(
                cache_key=cache_key3,
                cache_value=b"bar2",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        assert cache.flush() == 3
        assert len(cache.collection) == 0


class TestCacheReverseIndex:
    """
    The reverse index from Redis key to the entries holding it.

    Invalidation names a key and the cache has to find every entry whose invocation touched
    it. The index replaces a full scan of the entry map, so what matters is that it is always
    *exact*: a missed entry is a reply that never gets invalidated.
    """

    @staticmethod
    def _entry(cache, command, redis_keys, value, mock_connection):
        cache_key = CacheKey(
            command=command, redis_keys=redis_keys, redis_args=(command,) + redis_keys
        )
        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=value,
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        return cache_key

    def test_the_index_finds_every_holder_of_a_key(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=10))
        single = self._entry(cache, "GET", ("foo",), b"a", mock_connection)
        multi = self._entry(cache, "MGET", ("foo", "bar"), b"b", mock_connection)
        other = self._entry(cache, "GET", ("bar",), b"c", mock_connection)

        assert cache.collection.holders_of("foo") == frozenset({single, multi})
        assert cache.collection.holders_of("bar") == frozenset({multi, other})
        assert cache.collection.holders_of("nosuchkey") == frozenset()

    def test_the_index_survives_an_eviction(self, mock_connection):
        """
        The reason the index lives on the entry map rather than on ``DefaultCache``: an
        eviction policy pops straight off ``cache.collection``, so an index maintained one
        level up would keep pointing at an entry that is gone.
        """
        cache = DefaultCache(CacheConfig(max_size=10))
        evicted = self._entry(cache, "GET", ("foo",), b"a", mock_connection)
        kept = self._entry(cache, "GET", ("bar",), b"b", mock_connection)

        assert cache.eviction_policy.evict_next() == evicted

        assert cache.collection.holders_of("foo") == frozenset()
        assert cache.collection.holders_of("bar") == frozenset({kept})
        # And an invalidation for the evicted key reports nothing rather than crashing.
        assert cache.delete_by_redis_keys([b"foo"]) == []

    def test_the_index_is_cleared_by_a_flush(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=10))
        self._entry(cache, "GET", ("foo",), b"a", mock_connection)

        assert cache.flush() == 1

        assert cache.collection.holders_of("foo") == frozenset()
        assert cache.delete_by_redis_keys([b"foo"]) == []

    def test_the_index_is_pruned_by_delete_by_cache_keys(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=10))
        cache_key = self._entry(cache, "GET", ("foo",), b"a", mock_connection)

        assert cache.delete_by_cache_keys([cache_key]) == [True]

        assert cache.collection.holders_of("foo") == frozenset()

    def test_replacing_an_entry_keeps_one_index_reference(self, mock_connection):
        # ``read_response`` sets the same cache key again to promote a placeholder to VALID.
        cache = DefaultCache(CacheConfig(max_size=10))
        cache_key = self._entry(cache, "GET", ("foo",), b"a", mock_connection)
        self._entry(cache, "GET", ("foo",), b"b", mock_connection)

        assert cache.collection.holders_of("foo") == frozenset({cache_key})
        assert cache.size == 1
        assert cache.delete_by_redis_keys(["foo"]) == [True]
        assert cache.collection.holders_of("foo") == frozenset()

    def test_both_key_spellings_resolve_to_the_same_entry(self, mock_connection):
        # Entries are indexed under the keys the invocation supplied; the server names them
        # in its own encoding.
        cache = DefaultCache(CacheConfig(max_size=10))
        self._entry(cache, "GET", ("foo",), b"a", mock_connection)

        assert cache.delete_by_redis_keys([b"foo"]) == [True]
        assert cache.size == 0

    def test_a_lone_holder_is_stored_bare_and_promoted_on_the_second(
        self, mock_connection
    ):
        # Most keys have one holder, and a set per key is most of the index's memory.
        cache = DefaultCache(CacheConfig(max_size=10))
        first = self._entry(cache, "GET", ("foo",), b"a", mock_connection)

        assert cache.collection._by_redis_key["foo"] == first

        second = self._entry(cache, "MGET", ("foo", "bar"), b"b", mock_connection)

        assert cache.collection._by_redis_key["foo"] == {first, second}
        assert cache.collection._by_redis_key["bar"] == second
        assert cache.collection.holders_of("foo") == frozenset({first, second})

    def test_a_set_dropping_to_one_holder_is_demoted(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=10))
        evicted = self._entry(cache, "GET", ("foo",), b"a", mock_connection)
        kept = self._entry(cache, "MGET", ("foo", "bar"), b"b", mock_connection)

        assert cache.eviction_policy.evict_next() == evicted

        assert cache.collection._by_redis_key["foo"] == kept
        assert cache.collection.holders_of("foo") == frozenset({kept})
        assert cache.delete_by_redis_keys([b"foo"]) == [True]
        assert cache.size == 0
        assert cache.collection._by_redis_key == {}

    def test_a_key_named_twice_by_one_invocation_is_indexed_once(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=10))
        cache_key = self._entry(cache, "MGET", ("foo", "foo"), b"a", mock_connection)

        assert cache.collection._by_redis_key["foo"] == cache_key
        assert cache.collection.holders_of("foo") == frozenset({cache_key})

        assert cache.eviction_policy.evict_next() == cache_key

        assert cache.collection.holders_of("foo") == frozenset()
        assert cache.collection._by_redis_key == {}

    def test_evicting_one_of_many_holders_keeps_the_rest(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=10))
        evicted = self._entry(cache, "GET", ("foo",), b"a", mock_connection)
        kept_a = self._entry(cache, "MGET", ("foo", "bar"), b"b", mock_connection)
        kept_b = self._entry(cache, "MGET", ("foo", "baz"), b"c", mock_connection)

        assert cache.eviction_policy.evict_next() == evicted

        assert cache.collection.holders_of("foo") == frozenset({kept_a, kept_b})
        assert cache.delete_by_redis_keys([b"foo"]) == [True, True]
        assert cache.size == 0
        assert cache.collection._by_redis_key == {}

    def test_concurrent_mutations_keep_the_index_exact(self, mock_connection):
        # The map is shared by a whole pool while each connection locks it with a lock of
        # its own, so sets and pops from different connections interleave. Every key here
        # shares the Redis key "foo", which makes each update a promotion or a demotion -
        # the read-modify-writes that lose a holder when they race.
        collection = DefaultCache(CacheConfig(max_size=10)).collection
        keys = [
            CacheKey(command="MGET", redis_keys=("foo", f"k{i}"), redis_args=(i,))
            for i in range(8)
        ]
        entry = CacheEntry(
            cache_key=keys[0],
            cache_value=b"v",
            status=CacheEntryStatus.VALID,
            connection_ref=mock_connection,
        )

        # A racing update can also raise - a demotion deleting an index slot another thread
        # already deleted - and an exception in a thread would not fail the test by itself.
        errors = []

        def churn(offset):
            try:
                for i in range(3000):
                    key = keys[(i + offset) % len(keys)]
                    if i % 2:
                        collection.pop(key, None)
                    else:
                        collection[key] = entry
            except Exception as e:
                errors.append(e)

        # A short switch interval makes the threads interleave inside the updates.
        switch_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            threads = [
                threading.Thread(target=churn, args=(offset,)) for offset in range(4)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        finally:
            sys.setswitchinterval(switch_interval)

        assert errors == []
        expected = {}
        for key in collection:
            for redis_key in key.redis_keys:
                expected.setdefault(redis_key, set()).add(key)
        actual = {
            redis_key: holders if isinstance(holders, set) else {holders}
            for redis_key, holders in collection._by_redis_key.items()
        }
        assert actual == expected


def _valid_entry(cache_key, value=b"bar"):
    return CacheEntry(
        cache_key=cache_key,
        cache_value=value,
        status=CacheEntryStatus.VALID,
        connection_ref=None,
    )


class TestTakeEntriesByRedisKeys:
    @pytest.fixture
    def cache(self):
        return DefaultCache(CacheConfig())

    @pytest.mark.parametrize(
        "indexed,named",
        [("foo", b"foo"), (b"foo", "foo")],
        ids=["str-indexed", "bytes-indexed"],
    )
    def test_either_spelling_finds_the_entry(self, cache, indexed, named):
        entry = _valid_entry(_get_cache_key(indexed))
        cache.set(entry)

        assert cache.take_entries_by_redis_keys([named]) == [entry]
        assert cache.size == 0

    def test_every_holder_is_removed_and_returned(self, cache):
        get_entry = _valid_entry(_get_cache_key("foo"))
        mget_entry = _valid_entry(
            CacheKey(command="MGET", redis_keys=("foo", "bar"), redis_args=())
        )
        other = _valid_entry(_get_cache_key("baz"))
        for entry in (get_entry, mget_entry, other):
            cache.set(entry)

        taken = cache.take_entries_by_redis_keys([b"foo"])

        # The MGET entry holds two keys but is named by one, so it comes back once.
        assert sorted(taken, key=lambda e: e.cache_key.command) == [
            get_entry,
            mget_entry,
        ]
        assert list(cache.collection) == [other.cache_key]
        # Nothing indexed under either spelling of the taken keys survives.
        for redis_key in ("foo", b"foo", "bar", b"bar"):
            assert cache.collection.holders_of(redis_key) == frozenset()

    def test_entries_keep_their_status(self, cache):
        valid = _valid_entry(_get_cache_key("foo"))
        in_progress = CacheEntry(
            cache_key=CacheKey(command="STRLEN", redis_keys=("foo",), redis_args=()),
            cache_value=b"foo",
            status=CacheEntryStatus.IN_PROGRESS,
            connection_ref=None,
        )
        cache.set(valid)
        cache.set(in_progress)

        statuses = {e.status for e in cache.take_entries_by_redis_keys([b"foo"])}

        assert statuses == {CacheEntryStatus.VALID, CacheEntryStatus.IN_PROGRESS}

    def test_an_unknown_key_takes_nothing(self, cache):
        entry = _valid_entry(_get_cache_key("foo"))
        cache.set(entry)

        assert cache.take_entries_by_redis_keys([b"nope"]) == []
        assert cache.get(entry.cache_key) is entry

    def test_the_interface_default_works_for_a_third_party_cache(self):
        # A cache implementing only the abstract methods inherits the scanning default.
        class DictCache(CacheInterface):
            def __init__(self):
                self._entries = OrderedDict()

            collection = property(lambda self: self._entries)
            config = property(lambda self: CacheConfig())
            eviction_policy = property(lambda self: None)
            size = property(lambda self: len(self._entries))

            def get(self, key):
                return self._entries.get(key)

            def set(self, entry):
                self._entries[entry.cache_key] = entry
                return True

            def delete_by_cache_keys(self, cache_keys):
                return [self._entries.pop(k, None) is not None for k in cache_keys]

            def delete_by_redis_keys(self, redis_keys):
                raise NotImplementedError

            def flush(self):
                count = len(self._entries)
                self._entries.clear()
                return count

            def is_cachable(self, key):
                return True

        cache = DictCache()
        get_entry = _valid_entry(_get_cache_key("foo"))
        mget_entry = _valid_entry(
            CacheKey(command="MGET", redis_keys=("foo", "bar"), redis_args=())
        )
        other = _valid_entry(_get_cache_key("baz"))
        for entry in (get_entry, mget_entry, other):
            cache.set(entry)

        # The server's spelling, while the entries were indexed under ``str``.
        taken = cache.take_entries_by_redis_keys([b"foo"])

        assert taken == [get_entry, mget_entry]
        assert list(cache.collection) == [other.cache_key]

    def test_the_proxy_forwards(self, cache):
        entry = _valid_entry(_get_cache_key("foo"))
        cache.set(entry)

        assert CacheProxy(cache).take_entries_by_redis_keys([b"foo"]) == [entry]


class _FakePool:
    """The pool surface the refresher uses, recording what it is asked for."""

    def __init__(self, cache=None):
        self.cache = cache if cache is not None else DefaultCache(CacheConfig())
        self.connection = MagicMock()
        self.get_connection_error = None
        # Every connection in use and the pool at its limit.
        self.full = False
        self.released = []

    def _get_connection(self, if_available=False):
        assert if_available, "a refresh must never wait for a pool connection"
        if self.full:
            return None
        if self.get_connection_error is not None:
            error, self.get_connection_error = self.get_connection_error, None
            raise error
        return self.connection

    def release(self, connection):
        self.released.append(connection)


def _sent_keys(connection):
    return [c.kwargs["keys"] for c in connection.send_command.call_args_list]


def _wait(predicate, message="Timeout waiting for the refresher"):
    wait_for_condition(predicate, timeout=2, error_message=message)


class TestCacheRefresher:
    @pytest.fixture
    def pool(self):
        return _FakePool()

    @pytest.fixture
    def make_refresher(self):
        """Build refreshers whose workers are stopped and joined after the test."""
        refreshers = []

        def make(pool, *args, **kwargs):
            refresher = _CacheRefresher(pool, *args, **kwargs)
            refreshers.append(refresher)
            return refresher

        yield make
        for refresher in refreshers:
            worker = refresher._thread
            refresher.stop()
            if worker is not None:
                worker.join(timeout=2)

    @pytest.fixture
    def gate(self, pool, make_refresher):
        """
        Hold every refresh inside ``send_command`` until the test sets the event.

        Depends on ``make_refresher`` so that it is torn down first: the gate opens before
        the workers are joined.
        """
        event = threading.Event()
        pool.connection.send_command.side_effect = lambda *a, **k: event.wait(5)
        yield event
        event.set()

    def _idle(self, refresher):
        _wait(lambda: not refresher._pending, "refresher never went idle")

    def test_no_thread_before_the_first_accepted_key(self, make_refresher, pool):
        refresher = make_refresher(pool, 4)

        assert refresher._thread is None
        assert refresher.submit([]) == []
        assert refresher._thread is None

    def test_a_refresh_replays_the_cached_command(self, make_refresher, pool):
        refresher = make_refresher(pool, 4)
        key = _get_cache_key("foo", "bar")

        assert refresher.submit([key]) == []
        self._idle(refresher)

        pool.connection.send_command.assert_called_once_with(
            "GET", "foo", "bar", keys=("foo", "bar")
        )
        pool.connection.read_response.assert_called_once_with()
        assert pool.released == [pool.connection]

    def test_keys_over_the_bound_are_rejected(self, make_refresher, pool, gate):
        refresher = make_refresher(pool, 2)
        k1, k2, k3 = _get_cache_key("k1"), _get_cache_key("k2"), _get_cache_key("k3")

        assert refresher.submit([k1, k2, k3]) == [k3]

        gate.set()
        self._idle(refresher)
        # The slots free on completion, so the rejected key fits now.
        assert refresher.submit([k3]) == []
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("k1",), ("k2",), ("k3",)]

    def test_a_pending_key_is_not_queued_twice(self, make_refresher, pool, gate):
        refresher = make_refresher(pool, 4)
        key = _get_cache_key("foo")

        refresher.submit([key])
        _wait(lambda: pool.connection.send_command.called)
        assert refresher.submit([key]) == []

        gate.set()
        self._idle(refresher)
        assert pool.connection.send_command.call_count == 1

    def test_a_duplicate_in_one_batch_is_neither_queued_nor_counted(
        self, make_refresher, pool, gate
    ):
        refresher = make_refresher(pool, 2)
        k1, k2, k3 = _get_cache_key("k1"), _get_cache_key("k2"), _get_cache_key("k3")

        assert refresher.submit([k1, k1, k2]) == []
        # Both slots are taken by two distinct keys, not by three submissions.
        assert refresher.submit([k3]) == [k3]

        gate.set()
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("k1",), ("k2",)]

    @pytest.mark.parametrize(
        "error",
        [ResponseError("WRONGTYPE"), MovedError("3999 127.0.0.1:6381")],
        ids=["wrongtype", "moved"],
    )
    def test_an_error_reply_fails_the_refresh_without_disconnecting(
        self, make_refresher, pool, error
    ):
        pool.connection.read_response.side_effect = error
        refresher = make_refresher(pool, 1)

        refresher.submit([_get_cache_key("foo")])
        self._idle(refresher)

        pool.connection.disconnect.assert_not_called()
        assert pool.released == [pool.connection]
        # The slot is free and the worker is alive.
        pool.connection.read_response.side_effect = None
        assert refresher.submit([_get_cache_key("bar")]) == []
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("foo",), ("bar",)]

    @pytest.mark.parametrize(
        "error",
        [ConnectionError("lost"), TimeoutError("slow"), OSError("reset")],
        ids=["connection", "timeout", "os"],
    )
    def test_a_connection_error_disconnects(self, make_refresher, pool, error):
        pool.connection.read_response.side_effect = error
        refresher = make_refresher(pool, 1)

        refresher.submit([_get_cache_key("foo")])
        self._idle(refresher)

        pool.connection.disconnect.assert_called_once_with()
        assert pool.released == [pool.connection]

    def test_a_full_pool_skips_the_refresh(self, make_refresher, pool, recorded):
        # Taking a connection would make an application command fail or wait.
        pool.full = True
        refresher = make_refresher(pool, 1)

        refresher.submit([_get_cache_key("foo")])
        self._idle(refresher)

        assert not pool.connection.send_command.called
        assert pool.released == []
        assert recorded == [(CSCRefreshResult.REJECTED, 1)]

        # The pool has room again, so the next refresh runs.
        pool.full = False
        refresher.submit([_get_cache_key("bar")])
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("bar",)]

    def test_a_failure_to_get_a_connection_is_survived(self, make_refresher, pool):
        pool.get_connection_error = ConnectionError("Connection refused")
        refresher = make_refresher(pool, 1)

        refresher.submit([_get_cache_key("foo")])
        self._idle(refresher)
        assert not pool.connection.send_command.called
        assert pool.released == []

        refresher.submit([_get_cache_key("bar")])
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("bar",)]

    def test_an_unexpected_failure_does_not_kill_the_worker(self, make_refresher, pool):
        class ExplodingCollection:
            calls = 0

            def __contains__(self, key):
                ExplodingCollection.calls += 1
                if ExplodingCollection.calls == 1:
                    raise RuntimeError("boom")
                return False

        pool.cache = MagicMock(collection=ExplodingCollection())
        refresher = make_refresher(pool, 1)

        refresher.submit([_get_cache_key("foo")])
        self._idle(refresher)
        refresher.submit([_get_cache_key("bar")])
        self._idle(refresher)

        assert _sent_keys(pool.connection) == [("bar",)]

    @pytest.mark.parametrize(
        "status", [CacheEntryStatus.VALID, CacheEntryStatus.IN_PROGRESS]
    )
    def test_a_key_the_cache_already_holds_is_skipped(
        self, make_refresher, pool, status
    ):
        # A user read re-fetched it, or is fetching it: refreshing would race that read.
        key = _get_cache_key("foo")
        pool.cache.set(
            CacheEntry(
                cache_key=key, cache_value=b"bar", status=status, connection_ref=None
            )
        )
        refresher = make_refresher(pool, 1)

        refresher.submit([key])
        self._idle(refresher)

        assert not pool.connection.send_command.called
        assert pool.released == []

    def test_cancel_drops_queued_keys(self, make_refresher, pool, gate):
        refresher = make_refresher(pool, 4)
        k1, k2, k3 = _get_cache_key("k1"), _get_cache_key("k2"), _get_cache_key("k3")

        refresher.submit([k1])
        _wait(lambda: pool.connection.send_command.called)
        refresher.submit([k2])
        refresher.cancel_pending()
        assert refresher.submit([k3]) == []

        gate.set()
        self._idle(refresher)
        # k2 was queued ahead of k3 and dropped unrun; k1 was already in flight.
        assert _sent_keys(pool.connection) == [("k1",), ("k3",)]

    def test_a_job_completing_after_a_cancel_keeps_newer_work_pending(
        self, make_refresher, pool
    ):
        first, second = threading.Event(), threading.Event()
        gates = iter([first, second])
        pool.connection.send_command.side_effect = lambda *a, **k: next(gates).wait(5)
        refresher = make_refresher(pool, 1)
        key = _get_cache_key("foo")

        refresher.submit([key])
        _wait(lambda: pool.connection.send_command.call_count == 1)
        refresher.cancel_pending()
        # The bound is 1 and the cancel freed the slot, so the same key is accepted again.
        assert refresher.submit([key]) == []

        first.set()
        _wait(lambda: pool.connection.send_command.call_count == 2)
        # The first job has finished, and it must not have released the newer acceptance.
        assert key in refresher._pending
        assert refresher.submit([_get_cache_key("bar")]) == [_get_cache_key("bar")]

        second.set()
        self._idle(refresher)

    def test_the_worker_exits_when_idle_and_restarts(self, make_refresher, pool):
        refresher = make_refresher(pool, 1, idle_timeout=0.05)

        refresher.submit([_get_cache_key("foo")])
        first = refresher._thread
        _wait(lambda: refresher._thread is None, "worker never exited")
        first.join(timeout=2)
        assert not first.is_alive()

        # Held inside the refresh, so the new worker cannot go idle before it is checked.
        gate = threading.Event()
        pool.connection.send_command.side_effect = lambda *a, **k: gate.wait(5)
        refresher.submit([_get_cache_key("bar")])
        second = refresher._thread
        assert second is not first
        assert second.is_alive()

        gate.set()
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("foo",), ("bar",)]

    def test_stop_cancels_queued_work_and_stays_usable(
        self, make_refresher, pool, gate
    ):
        refresher = make_refresher(pool, 4)

        refresher.submit([_get_cache_key("k1")])
        _wait(lambda: pool.connection.send_command.called)
        refresher.submit([_get_cache_key("k2")])
        refresher.stop()

        gate.set()
        _wait(lambda: refresher._thread is None, "worker never exited")
        assert _sent_keys(pool.connection) == [("k1",)]

        # A pool is reusable after close(), and so is its refresher.
        refresher.submit([_get_cache_key("k3")])
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("k1",), ("k3",)]

    def test_stop_without_a_worker_queues_nothing(self, make_refresher, pool):
        refresher = make_refresher(pool, 4)

        refresher.stop()
        refresher.stop()

        assert refresher._queue.empty()

    def test_a_worker_inherited_across_a_fork_is_replaced(self, make_refresher, pool):
        # In a forked child the registered thread object exists but never runs.
        refresher = make_refresher(pool, 4)
        dead = threading.Thread(target=lambda: None)
        dead.start()
        dead.join()
        refresher._thread = dead

        assert refresher.submit([_get_cache_key("foo")]) == []
        assert refresher._thread is not dead
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("foo",)]

    def test_a_key_accepted_right_after_stop_still_runs(
        self, make_refresher, pool, gate
    ):
        # The worker is still registered when this key arrives, so no new one is started:
        # the old one must see it behind the stop and keep going.
        refresher = make_refresher(pool, 4)

        refresher.submit([_get_cache_key("k1")])
        _wait(lambda: pool.connection.send_command.called)
        refresher.stop()
        worker = refresher._thread
        assert refresher.submit([_get_cache_key("k2")]) == []
        assert refresher._thread is worker

        gate.set()
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("k1",), ("k2",)]

    def test_a_flush_while_waiting_for_a_connection_sends_nothing(
        self, make_refresher, pool, recorded
    ):
        # Getting a connection can wait for the pool or for a handshake.
        in_get, proceed = threading.Event(), threading.Event()
        connection = pool.connection

        def slow_get_connection(if_available=False):
            assert if_available
            in_get.set()
            proceed.wait(5)
            return connection

        pool._get_connection = slow_get_connection
        refresher = make_refresher(pool, 1)

        refresher.submit([_get_cache_key("foo")])
        assert in_get.wait(2)
        refresher.cancel_pending()
        proceed.set()

        _wait(lambda: pool.released == [connection])
        assert not connection.send_command.called
        # Cancelled before any round trip, so it is no refresh outcome either.
        self._idle(refresher)
        assert recorded == []

    def test_a_worker_that_cannot_start_hands_the_keys_back(self, make_refresher, pool):
        refresher = make_refresher(pool, 4)
        k1, k2, k3 = _get_cache_key("k1"), _get_cache_key("k2"), _get_cache_key("k3")

        with patch.object(
            threading.Thread, "start", side_effect=RuntimeError("can't start")
        ):
            assert refresher.submit([k1, k2]) == [k1, k2]

        assert refresher._thread is None
        assert not refresher._pending
        # Nothing is stuck: the next submit starts a worker and runs.
        assert refresher.submit([k3]) == []
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("k3",)]

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_a_worker_ended_by_a_base_exception_is_replaced(self, make_refresher, pool):
        class Killed(BaseException):
            pass

        pool.connection.send_command.side_effect = [Killed(), None]
        refresher = make_refresher(pool, 4)

        refresher.submit([_get_cache_key("foo")])
        first = refresher._thread
        first.join(timeout=2)
        assert not first.is_alive()
        assert refresher._thread is None

        refresher.submit([_get_cache_key("bar")])
        self._idle(refresher)
        assert _sent_keys(pool.connection) == [("foo",), ("bar",)]

    @pytest.fixture
    def recorded(self):
        """The refresh outcomes the refresher reports, as ``(result, count)`` pairs."""
        calls = []
        with patch(
            "redis.cache.record_csc_refresh",
            side_effect=lambda result, count=1: calls.append((result, count)),
        ):
            yield calls

    def test_an_answered_refresh_records_a_success(
        self, make_refresher, pool, recorded
    ):
        refresher = make_refresher(pool, 4)

        refresher.submit([_get_cache_key("foo")])
        self._idle(refresher)

        assert recorded == [(CSCRefreshResult.SUCCESS, 1)]

    @pytest.mark.parametrize(
        "error",
        [ResponseError("WRONGTYPE"), ConnectionError("lost")],
        ids=["error-reply", "connection"],
    )
    def test_a_failed_refresh_records_a_failure(
        self, make_refresher, pool, recorded, error
    ):
        pool.connection.read_response.side_effect = error
        refresher = make_refresher(pool, 4)

        refresher.submit([_get_cache_key("foo")])
        self._idle(refresher)

        assert recorded == [(CSCRefreshResult.FAILURE, 1)]

    def test_a_failing_disconnect_records_one_failure(
        self, make_refresher, pool, recorded
    ):
        pool.connection.read_response.side_effect = ConnectionError("lost")
        pool.connection.disconnect.side_effect = OSError("already closed")
        refresher = make_refresher(pool, 4)

        refresher.submit([_get_cache_key("foo")])
        self._idle(refresher)

        assert recorded == [(CSCRefreshResult.FAILURE, 1)]
        assert pool.released == [pool.connection]
        # The worker outlived it.
        pool.connection.read_response.side_effect = None
        pool.connection.disconnect.side_effect = None
        refresher.submit([_get_cache_key("bar")])
        self._idle(refresher)
        assert recorded[-1] == (CSCRefreshResult.SUCCESS, 1)

    def test_no_connection_records_a_failure(self, make_refresher, pool, recorded):
        pool.get_connection_error = ConnectionError("Connection refused")
        refresher = make_refresher(pool, 4)

        refresher.submit([_get_cache_key("foo")])
        self._idle(refresher)

        assert recorded == [(CSCRefreshResult.FAILURE, 1)]

    def test_rejected_keys_are_recorded_together(
        self, make_refresher, pool, gate, recorded
    ):
        refresher = make_refresher(pool, 1)
        keys = [_get_cache_key("k1"), _get_cache_key("k2"), _get_cache_key("k3")]

        assert refresher.submit(keys) == keys[1:]

        assert recorded == [(CSCRefreshResult.REJECTED, 2)]

    def test_keys_handed_back_by_a_failed_start_are_recorded_as_rejected(
        self, make_refresher, pool, recorded
    ):
        refresher = make_refresher(pool, 4)

        with patch.object(
            threading.Thread, "start", side_effect=RuntimeError("can't start")
        ):
            refresher.submit([_get_cache_key("k1"), _get_cache_key("k2")])

        assert recorded == [(CSCRefreshResult.REJECTED, 2)]

    def test_skipped_and_cancelled_keys_record_nothing(
        self, make_refresher, pool, gate, recorded
    ):
        # Neither ran a round trip, so neither is a refresh outcome.
        held = _get_cache_key("held")
        pool.cache.set(_valid_entry(held))
        refresher = make_refresher(pool, 4)

        refresher.submit([held])
        refresher.submit([_get_cache_key("k1")])
        _wait(lambda: pool.connection.send_command.called)
        refresher.submit([_get_cache_key("k2")])
        refresher.cancel_pending()
        gate.set()
        _wait(lambda: pool.released == [pool.connection])
        self._idle(refresher)

        # Only k1, which was in flight when the cancel landed, was answered.
        assert recorded == [(CSCRefreshResult.SUCCESS, 1)]

    def test_an_idle_worker_does_not_keep_the_pool_alive(self, make_refresher):
        # Built here rather than taken from the fixture, which keeps a reference of its own.
        pool = _FakePool()
        refresher = make_refresher(pool, 1)
        refresher.submit([_get_cache_key("foo")])
        self._idle(refresher)
        pool_ref = weakref.ref(pool)
        del pool

        _wait(lambda: (gc.collect(), pool_ref())[1] is None, "the pool was kept alive")
        assert refresher._thread.is_alive()

    def test_the_worker_exits_once_its_pool_is_gone(self, make_refresher):
        pool = _FakePool()
        refresher = make_refresher(pool, 1)
        del pool
        gc.collect()

        refresher.submit([_get_cache_key("foo")])
        worker = refresher._thread
        worker.join(timeout=2)

        assert not worker.is_alive()


class TestUnitLRUPolicy:
    def test_type(self):
        policy = LRUPolicy()
        assert policy.type == EvictionPolicyType.time_based

    def test_evict_next(self, mock_connection):
        cache = DefaultCache(
            CacheConfig(max_size=5, eviction_policy=EvictionPolicy.LRU)
        )
        policy = cache.eviction_policy

        cache_key1 = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_key2 = CacheKey(
            command="GET", redis_keys=("bar",), redis_args=("GET", "bar")
        )

        assert cache.set(
            CacheEntry(
                cache_key=cache_key1,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.set(
            CacheEntry(
                cache_key=cache_key2,
                cache_value=b"foo",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        assert policy.evict_next() == cache_key1
        assert cache.get(cache_key1) is None

    def test_evict_many(self, mock_connection):
        cache = DefaultCache(
            CacheConfig(max_size=5, eviction_policy=EvictionPolicy.LRU)
        )
        policy = cache.eviction_policy
        cache_key1 = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_key2 = CacheKey(
            command="GET", redis_keys=("bar",), redis_args=("GET", "bar")
        )
        cache_key3 = CacheKey(
            command="GET", redis_keys=("baz",), redis_args=("GET", "baz")
        )

        assert cache.set(
            CacheEntry(
                cache_key=cache_key1,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.set(
            CacheEntry(
                cache_key=cache_key2,
                cache_value=b"foo",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.set(
            CacheEntry(
                cache_key=cache_key3,
                cache_value=b"baz",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        assert policy.evict_many(2) == [cache_key1, cache_key2]
        assert cache.get(cache_key1) is None
        assert cache.get(cache_key2) is None

        with pytest.raises(ValueError, match="Evictions count is above cache size"):
            policy.evict_many(99)

    def test_touch(self, mock_connection):
        cache = DefaultCache(
            CacheConfig(max_size=5, eviction_policy=EvictionPolicy.LRU)
        )
        policy = cache.eviction_policy

        cache_key1 = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_key2 = CacheKey(
            command="GET", redis_keys=("bar",), redis_args=("GET", "bar")
        )

        cache.set(
            CacheEntry(
                cache_key=cache_key1,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        cache.set(
            CacheEntry(
                cache_key=cache_key2,
                cache_value=b"foo",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        assert cache.collection.popitem(last=True)[0] == cache_key2
        cache.set(
            CacheEntry(
                cache_key=cache_key2,
                cache_value=b"foo",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        policy.touch(cache_key1)
        assert cache.collection.popitem(last=True)[0] == cache_key1

    def test_throws_error_on_invalid_cache(self):
        policy = LRUPolicy()

        with pytest.raises(
            ValueError, match="Eviction policy should be associated with valid cache."
        ):
            policy.evict_next()

        policy.cache = "wrong_type"

        with pytest.raises(
            ValueError, match="Eviction policy should be associated with valid cache."
        ):
            policy.evict_next()


class TestUnitCacheConfiguration:
    MAX_SIZE = 100
    EVICTION_POLICY = EvictionPolicy.LRU

    def test_get_max_size(self, cache_conf: CacheConfig):
        assert self.MAX_SIZE == cache_conf.get_max_size()

    def test_get_eviction_policy(self, cache_conf: CacheConfig):
        assert self.EVICTION_POLICY == cache_conf.get_eviction_policy()

    def test_is_exceeds_max_size(self, cache_conf: CacheConfig):
        assert not cache_conf.is_exceeds_max_size(self.MAX_SIZE)
        assert cache_conf.is_exceeds_max_size(self.MAX_SIZE + 1)

    def test_is_allowed_to_cache(self, cache_conf: CacheConfig):
        assert cache_conf.is_allowed_to_cache("GET")
        assert not cache_conf.is_allowed_to_cache("SET")

    @pytest.mark.parametrize(
        "command,allowed",
        [
            # Readonly, keyed, nothing that forbids caching - core and module alike, since
            # eligibility is decided by metadata rather than by command-name prefix.
            ("GET", True),
            ("HGETALL", True),
            ("JSON.GET", True),
            ("TS.GET", True),
            ("FT.SUGGET", True),
            # A write command.
            ("SET", False),
            # Readonly but with no key name argument.
            ("KEYS", False),
            ("FT.SEARCH", False),
            # Nondeterministic output.
            ("XPENDING", False),
            # The script runners.
            ("EVAL_RO", False),
            ("EVALSHA_RO", False),
            ("FCALL_RO", False),
            # A server-side effect no server flag expresses.
            ("TOUCH", False),
            # No readonly flag.
            ("XREADGROUP", False),
            # Blocking, even though it is readonly and keyed.
            ("XREAD", False),
            # Tipped dont_cache by the server.
            ("TS.INFO", False),
            # Unknown, so unproven: fails closed rather than being cached.
            ("NOSUCHCOMMAND", False),
            ("NOSUCHMODULE.NOSUCHCOMMAND", False),
            # A container command whose subcommand is in args[1], so the name alone proves
            # nothing.
            ("OBJECT", False),
        ],
    )
    def test_is_allowed_to_cache_follows_the_metadata_rules(
        self, cache_conf: CacheConfig, command, allowed
    ):
        assert cache_conf.is_allowed_to_cache(command) is allowed

    def test_is_allowed_to_cache_is_case_insensitive(self, cache_conf: CacheConfig):
        # The command methods spell a command the way they send it, and the record tables are
        # keyed lowercase.
        for command in ("get", "GET", "Get"):
            assert cache_conf.is_allowed_to_cache(command) is True

    @pytest.mark.parametrize(
        "command",
        [
            # More than one module prefix, which the record tables cannot be keyed by.
            "a.b.c",
            # ``execute_command`` accepts an arbitrary first argument, and the request encoder
            # accepts bytes, so a non-str name reaches eligibility unchanged. Regression:
            # these used to raise TypeError/AttributeError out of the command path.
            b"GET",
            bytearray(b"GET"),
            memoryview(b"GET"),
            1,
            None,
        ],
        ids=["two-dots", "bytes", "bytearray", "memoryview", "int", "none"],
    )
    def test_is_allowed_to_cache_does_not_raise_on_an_undecidable_name(
        self, cache_conf: CacheConfig, command
    ):
        # A raw command whose name eligibility cannot decide must still reach the server and
        # come back with the server's own error, not a client-side exception.
        assert cache_conf.is_allowed_to_cache(command) is False

    def test_is_allowed_to_cache_uses_an_injected_resolver(self):
        cache_conf = CacheConfig()
        assert cache_conf.is_allowed_to_cache("GET") is True

        cache_conf.set_metadata_resolver(
            DynamicMetadataResolver({"core": {"get": WRITE_KEYED}})
        )
        assert cache_conf.is_allowed_to_cache("GET") is False

    def test_the_tracking_defaults_are_todays_behaviour(self, cache_conf: CacheConfig):
        assert cache_conf.get_tracking_mode() is TrackingMode.PLAIN
        assert cache_conf.get_cache_predicate() is None

    @pytest.mark.parametrize(
        "tracking_mode,with_predicate,expected",
        [
            # Plain stores every eligible reply, and ignores the predicate.
            (TrackingMode.PLAIN, False, True),
            (TrackingMode.PLAIN, True, True),
            # Opt-in with no predicate is inert: the HLD-literal reading, warned about at
            # config time.
            (TrackingMode.OPTIN, False, False),
            (TrackingMode.OPTIN, True, False),
            # Opt-out stores by default, and the predicate is what carves out exceptions.
            (TrackingMode.OPTOUT, False, True),
            (TrackingMode.OPTOUT, True, False),
        ],
    )
    def test_should_cache_truth_table(self, tracking_mode, with_predicate, expected):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            cache_conf = CacheConfig(
                tracking_mode=tracking_mode,
                cache_predicate=(lambda command, keys: False)
                if with_predicate
                else None,
            )

        assert cache_conf.should_cache("GET", (b"foo",)) is expected

    def test_should_cache_passes_the_command_and_keys_to_the_predicate(self):
        seen = []
        cache_conf = CacheConfig(
            tracking_mode=TrackingMode.OPTOUT,
            cache_predicate=lambda command, keys: seen.append((command, keys)) or True,
        )

        assert cache_conf.should_cache("JSON.GET", ("foo", b"bar")) is True
        assert seen == [("JSON.GET", ("foo", b"bar"))]

    def test_should_cache_coerces_the_predicate_result(self):
        cache_conf = CacheConfig(
            tracking_mode=TrackingMode.OPTOUT,
            cache_predicate=lambda command, keys: keys,
        )

        # A predicate is application code, so it may answer with anything truthy. The stored
        # decision must still be a bool, because it is compared with ``is``.
        assert cache_conf.should_cache("GET", (b"foo",)) is True
        assert cache_conf.should_cache("GET", ()) is False

    def test_a_bare_string_tracking_mode_is_refused(self):
        # A silent mis-configuration here would compare equal to no enum member and behave as
        # plain mode, and its failure mode is a wrongly-cached reply.
        with pytest.raises(TypeError, match="tracking_mode must be"):
            CacheConfig(tracking_mode="optin")

    def test_optin_without_a_predicate_warns(self):
        with pytest.warns(UserWarning, match="stores nothing"):
            CacheConfig(tracking_mode=TrackingMode.OPTIN)

    def test_a_predicate_with_plain_mode_warns(self):
        with pytest.warns(UserWarning, match="ignored with tracking_mode=plain"):
            CacheConfig(cache_predicate=lambda command, keys: True)

    def test_the_accessors_round_trip(self):
        def predicate(command, keys):
            return True

        cache_conf = CacheConfig(
            tracking_mode=TrackingMode.OPTOUT, cache_predicate=predicate
        )

        assert cache_conf.get_tracking_mode() is TrackingMode.OPTOUT
        assert cache_conf.get_cache_predicate() is predicate

    def test_is_trackable_read_resolves_through_the_metadata_resolver(
        self, cache_conf: CacheConfig
    ):
        # Trackability is the readonly flag alone, so it parts company with eligibility on
        # exactly the commands optout most wants to exempt.
        assert cache_conf.is_trackable_read("GET") is True
        assert cache_conf.is_trackable_read("TOUCH") is True
        assert cache_conf.is_allowed_to_cache("TOUCH") is False
        assert cache_conf.is_trackable_read("SET") is False

        cache_conf.set_metadata_resolver(
            DynamicMetadataResolver({"core": {"get": WRITE_KEYED}})
        )
        assert cache_conf.is_trackable_read("GET") is False

    def test_a_third_party_configuration_keeps_todays_behaviour(self):
        """
        The three tracking-mode decisions and the invalidation policy are concrete defaults
        on the public ABC, so a configuration implementing only the five abstract methods
        still works - as plain mode, storing every eligible reply, never pairing a
        ``CLIENT CACHING NO``, and evicting invalidated entries without re-reading them.
        """

        class MinimalConfig(CacheConfigurationInterface):
            def get_cache_class(self):
                return DefaultCache

            def get_max_size(self) -> int:
                return 10

            def get_eviction_policy(self):
                return EvictionPolicy.LRU

            def is_exceeds_max_size(self, count: int) -> bool:
                return count > 10

            def is_allowed_to_cache(self, command: str) -> bool:
                return True

        config = MinimalConfig()

        assert config.get_tracking_mode() is TrackingMode.PLAIN
        assert config.should_cache("GET", (b"foo",)) is True
        assert config.is_trackable_read("GET") is False
        # The invalidation policy is a concrete default too, so the same configuration keeps
        # removing invalidated entries and re-reading nothing.
        assert config.get_invalidation_policy() is InvalidationPolicy.EVICT
        assert config.get_refresh_max_inflight() == (
            CacheConfig.DEFAULT_REFRESH_MAX_INFLIGHT
        )

    def test_the_invalidation_policy_defaults_to_evict(self):
        cache_conf = CacheConfig()

        assert cache_conf.get_invalidation_policy() is InvalidationPolicy.EVICT
        # Not used under evict, so no bound is set.
        assert cache_conf.get_refresh_max_inflight() is None

    def test_the_refresh_accessors_round_trip(self):
        cache_conf = CacheConfig(
            invalidation_policy=InvalidationPolicy.REFRESH, refresh_max_inflight=4
        )

        assert cache_conf.get_invalidation_policy() is InvalidationPolicy.REFRESH
        assert cache_conf.get_refresh_max_inflight() == 4

    def test_a_bare_string_invalidation_policy_is_refused(self):
        # It would compare equal to no enum member and silently behave as evict.
        with pytest.raises(TypeError, match="invalidation_policy must be"):
            CacheConfig(invalidation_policy="refresh")

    @pytest.mark.parametrize("bound", [0, -1, True, 1.5, "4", None])
    def test_an_invalid_refresh_max_inflight_is_refused(self, bound):
        with pytest.raises(ValueError, match="refresh_max_inflight must be"):
            CacheConfig(
                invalidation_policy=InvalidationPolicy.REFRESH,
                refresh_max_inflight=bound,
            )

    def test_a_refresh_bound_with_evict_is_ignored(self):
        # Not consulted under evict, so not validated or kept either: the warning says
        # it is ignored.
        with pytest.warns(UserWarning, match="ignored with invalidation_policy=evict"):
            cache_conf = CacheConfig(refresh_max_inflight=0)

        assert cache_conf.get_refresh_max_inflight() is None

    def test_a_refresh_max_inflight_of_one_is_accepted(self):
        cache_conf = CacheConfig(
            invalidation_policy=InvalidationPolicy.REFRESH, refresh_max_inflight=1
        )

        assert cache_conf.get_refresh_max_inflight() == 1

    @pytest.mark.parametrize("bound", [4, CacheConfig.DEFAULT_REFRESH_MAX_INFLIGHT])
    def test_a_refresh_bound_with_evict_warns(self, bound):
        # Any passed bound warns, the default's value included.
        with pytest.warns(UserWarning, match="ignored with invalidation_policy=evict"):
            CacheConfig(refresh_max_inflight=bound)

    def test_an_omitted_refresh_bound_with_refresh_is_the_default(self):
        cache_conf = CacheConfig(invalidation_policy=InvalidationPolicy.REFRESH)

        assert cache_conf.get_refresh_max_inflight() == (
            CacheConfig.DEFAULT_REFRESH_MAX_INFLIGHT
        )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"invalidation_policy": InvalidationPolicy.REFRESH},
            {
                "invalidation_policy": InvalidationPolicy.REFRESH,
                "refresh_max_inflight": CacheConfig.DEFAULT_REFRESH_MAX_INFLIGHT,
            },
            {
                "invalidation_policy": InvalidationPolicy.REFRESH,
                "refresh_max_inflight": 4,
            },
        ],
    )
    def test_a_consulted_refresh_bound_does_not_warn(self, kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            CacheConfig(**kwargs)


class TestUnitCacheProxy:
    """Unit tests for CacheProxy class with mocked event dispatcher."""

    @pytest.fixture
    def mock_cache(self, mock_connection):
        """Create a DefaultCache for testing."""
        return DefaultCache(CacheConfig(max_size=5))

    @pytest.fixture
    def mock_event_dispatcher(self):
        """Create a mock event dispatcher."""
        return MagicMock(spec=EventDispatcher)

    @pytest.fixture
    def cache_key(self):
        """Create a sample cache key."""
        return CacheKey(command="GET", redis_keys=("foo",), redis_args=("GET", "foo"))

    def test_initialization_creates_cache_proxy(self, mock_cache):
        """Test that CacheProxy can be initialized with a cache."""
        # Should not raise an error
        proxy = CacheProxy(mock_cache)
        assert proxy is not None

    def test_set_calls_record_csc_eviction_when_cache_exceeds_max_size(
        self, mock_connection
    ):
        """Test that record_csc_eviction is called when cache exceeds max size."""
        from unittest.mock import patch

        # Create a cache with max_size=2
        cache = DefaultCache(CacheConfig(max_size=2))
        proxy = CacheProxy(cache)

        with patch("redis.observability.recorder.record_csc_eviction") as mock_record:
            # Add 2 entries (at max capacity)
            for i in range(2):
                cache_key = CacheKey(
                    command="GET",
                    redis_keys=(f"key{i}",),
                    redis_args=("GET", f"key{i}"),
                )
                proxy.set(
                    CacheEntry(
                        cache_key=cache_key,
                        cache_value=f"value{i}".encode(),
                        status=CacheEntryStatus.VALID,
                        connection_ref=mock_connection,
                    )
                )

            # No eviction yet
            mock_record.assert_not_called()

            # Add a 3rd entry, which should trigger eviction
            cache_key = CacheKey(
                command="GET", redis_keys=("key3",), redis_args=("GET", "key3")
            )
            proxy.set(
                CacheEntry(
                    cache_key=cache_key,
                    cache_value=b"value3",
                    status=CacheEntryStatus.VALID,
                    connection_ref=mock_connection,
                )
            )

            # record_csc_eviction should be called
            mock_record.assert_called_once_with(
                count=1,
                reason=CSCReason.FULL,
            )

    def test_set_does_not_call_record_csc_eviction_when_under_max_size(
        self, mock_cache, mock_connection
    ):
        """Test that record_csc_eviction is NOT called when cache is under max size."""
        from unittest.mock import patch

        proxy = CacheProxy(mock_cache)

        with patch("redis.observability.recorder.record_csc_eviction") as mock_record:
            cache_key = CacheKey(
                command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
            )
            proxy.set(
                CacheEntry(
                    cache_key=cache_key,
                    cache_value=b"bar",
                    status=CacheEntryStatus.VALID,
                    connection_ref=mock_connection,
                )
            )

            mock_record.assert_not_called()

    def test_collection_property_delegates_to_underlying_cache(self, mock_cache):
        """Test that collection property returns the underlying cache's collection."""
        proxy = CacheProxy(mock_cache)
        assert proxy.collection is mock_cache.collection

    def test_config_property_delegates_to_underlying_cache(self, mock_cache):
        """Test that config property returns the underlying cache's config."""
        proxy = CacheProxy(mock_cache)
        assert proxy.config is mock_cache.config

    def test_eviction_policy_property_delegates_to_underlying_cache(self, mock_cache):
        """Test that eviction_policy property returns the underlying cache's eviction_policy."""
        proxy = CacheProxy(mock_cache)
        assert proxy.eviction_policy is mock_cache.eviction_policy

    def test_size_property_delegates_to_underlying_cache(
        self, mock_cache, mock_connection
    ):
        """Test that size property returns the underlying cache's size."""
        proxy = CacheProxy(mock_cache)
        assert proxy.size == 0

        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        proxy.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert proxy.size == 1

    def test_get_delegates_to_underlying_cache(self, mock_cache, mock_connection):
        """Test that get method delegates to the underlying cache."""
        proxy = CacheProxy(mock_cache)

        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        entry = CacheEntry(
            cache_key=cache_key,
            cache_value=b"bar",
            status=CacheEntryStatus.VALID,
            connection_ref=mock_connection,
        )
        proxy.set(entry)

        result = proxy.get(cache_key)
        assert result is not None
        assert result.cache_value == b"bar"

    def test_delete_by_cache_keys_delegates_to_underlying_cache(
        self, mock_cache, mock_connection
    ):
        """Test that delete_by_cache_keys method delegates to the underlying cache."""
        proxy = CacheProxy(mock_cache)

        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        proxy.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        result = proxy.delete_by_cache_keys([cache_key])
        assert result == [True]
        assert proxy.get(cache_key) is None

    def test_delete_by_redis_keys_delegates_to_underlying_cache(
        self, mock_cache, mock_connection
    ):
        """Test that delete_by_redis_keys method delegates to the underlying cache."""
        proxy = CacheProxy(mock_cache)

        cache_key = CacheKey(
            command="GET", redis_keys=(b"foo",), redis_args=("GET", "foo")
        )
        proxy.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        result = proxy.delete_by_redis_keys([b"foo"])
        assert result == [True]
        assert proxy.get(cache_key) is None

    def test_flush_delegates_to_underlying_cache(self, mock_cache, mock_connection):
        """Test that flush method delegates to the underlying cache."""
        proxy = CacheProxy(mock_cache)

        for i in range(3):
            cache_key = CacheKey(
                command="GET", redis_keys=(f"key{i}",), redis_args=("GET", f"key{i}")
            )
            proxy.set(
                CacheEntry(
                    cache_key=cache_key,
                    cache_value=f"value{i}".encode(),
                    status=CacheEntryStatus.VALID,
                    connection_ref=mock_connection,
                )
            )

        assert proxy.size == 3
        result = proxy.flush()
        assert result == 3
        assert proxy.size == 0

    def test_is_cachable_delegates_to_underlying_cache(self, mock_cache):
        """Test that is_cachable method delegates to the underlying cache."""
        proxy = CacheProxy(mock_cache)

        # GET is cachable by default
        cache_key = CacheKey(command="GET", redis_keys=("foo",), redis_args=())
        assert proxy.is_cachable(cache_key) is True

        # SET is not cachable
        cache_key = CacheKey(command="SET", redis_keys=("foo",), redis_args=())
        assert proxy.is_cachable(cache_key) is False


class TestUnitCacheProxyConnectionInvalidations:
    """A framing violation while draining invalidations now disconnects the raw
    connection (see #4291). That disconnect skips CacheProxyConnection.disconnect(),
    so the flush it performs has to happen on this path too -- otherwise the next
    connect() opens a CLIENT TRACKING session the server holds no invalidation
    state for, while the local cache keeps serving entries from the old one.
    """

    def _proxy_and_cache(self, read_response_error):
        conn = MagicMock()
        conn.can_read.return_value = True
        conn.read_response.side_effect = read_response_error
        cache = DefaultCache(CacheConfig(max_size=5))
        cache.set(
            CacheEntry(
                cache_key=CacheKey(command="GET", redis_keys=("foo",), redis_args=()),
                cache_value=b"stale",
                status=CacheEntryStatus.VALID,
                connection_ref=conn,
            )
        )
        assert cache.size == 1
        proxy = redis.connection.CacheProxyConnection(conn, cache, threading.RLock())
        return proxy, cache

    def test_framing_error_while_draining_flushes_cache(self):
        proxy, cache = self._proxy_and_cache(redis.InvalidResponse("bad framing"))

        with pytest.raises(redis.InvalidResponse):
            proxy._process_pending_invalidations()

        assert cache.size == 0

    def test_idle_connection_leaves_cache_intact(self):
        """The drain loop's normal exit must not flush anything."""
        proxy, cache = self._proxy_and_cache(redis.TimeoutError())

        proxy._process_pending_invalidations()

        assert cache.size == 1
