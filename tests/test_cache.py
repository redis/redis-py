import time
from unittest.mock import MagicMock

import pytest
import redis

from redis.cache import (
    CacheConfig,
    CacheEntry,
    CacheEntryStatus,
    CacheKey,
    CacheProxy,
    DefaultCache,
    EvictionPolicy,
    EvictionPolicyType,
    LRUPolicy,
)
from redis.commands.metadata import (
    CommandMetadata,
    DynamicMetadataResolver,
    RequestPolicy,
    ResponsePolicy,
    StaticMetadataResolver,
)
from redis.event import (
    EventDispatcher,
)
from redis.observability.attributes import CSCReason
from tests.conftest import _get_client, skip_if_resp_version, skip_if_server_version_lt

# A record for a command a resolver must report as ineligible, used to prove that eligibility
# comes from the resolver the config holds rather than from the config itself.
WRITE_KEYED = CommandMetadata(
    request_policy=RequestPolicy.DEFAULT_KEYED,
    response_policy=ResponsePolicy.DEFAULT_KEYED,
    is_readonly=False,
    has_key_argument=True,
    has_complete_metadata=True,
)


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
