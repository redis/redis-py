import time

import pytest
import redis
from redis.cache import (
    CacheConfig,
    CacheEntry,
    CacheEntryStatus,
    CacheKey,
    DefaultCache,
    EvictionPolicy,
    EvictionPolicyType,
    LRUPolicy,
)
from redis.utils import HIREDIS_AVAILABLE
from tests.conftest import _get_client, skip_if_resp_version, skip_if_server_version_lt


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


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
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
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
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
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
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
            cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value
            == b"bar"
        )
        # Force disconnection
        r.connection_pool.get_connection("_").disconnect()
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
            cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value
            == b"bar"
        )
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("foo2",))).cache_value
            == b"bar2"
        )
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("foo3",))).cache_value
            == b"bar3"
        )
        # add 1 more key to redis (exceed the max size)
        r.set("foo4", "bar4")
        assert r.get("foo4") == b"bar4"
        # the first key is not in the local cache anymore
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))) is None
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
        assert cache.get(CacheKey(command="HRANDFIELD", redis_keys=("foo",))) is None

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
            cache.get(CacheKey(command="MGET", redis_keys=("foo", "bar"))).cache_value
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
        assert cache.get(CacheKey(command="MGET", redis_keys=("foo", "bar"))) is None
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value
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
            cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value
            == b"bar"
        )
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("bar",))).cache_value
            == b"foo"
        )
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("baz",))).cache_value
            == b"bar"
        )

        # Flush server and trying to access cached entry
        assert r.flushall()
        assert r.get("foo") is None
        assert cache.size == 0


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
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
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
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
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
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
            cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value
            == b"bar"
        )
        # Force disconnection
        r.nodes_manager.get_node_from_slot(
            12000
        ).redis_connection.connection_pool.get_connection("_").disconnect()
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
            cache.get(CacheKey(command="GET", redis_keys=("foo{slot}",))).cache_value
            == b"bar"
        )
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("foo2{slot}",))).cache_value
            == b"bar2"
        )
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("foo3{slot}",))).cache_value
            == b"bar3"
        )
        # add 1 more key to redis (exceed the max size)
        r.set("foo4{slot}", "bar4")
        assert r.get("foo4{slot}") == b"bar4"
        # the first key is not in the local cache_data anymore
        assert cache.get(CacheKey(command="GET", redis_keys=("foo{slot}",))) is None

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
        assert cache.get(CacheKey(command="HRANDFIELD", redis_keys=("foo",))) is None

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
            CacheKey(command="MGET", redis_keys=("foo{slot}", "bar{slot}")),
        ).cache_value == [b"bar", b"foo"]

        # Invalidate one of the keys and make sure
        # that all associated cached entries was removed
        assert r.set("foo{slot}", "baz")
        assert r.get("foo{slot}") == b"baz"
        assert (
            cache.get(
                CacheKey(command="MGET", redis_keys=("foo{slot}", "bar{slot}")),
            )
            is None
        )
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("foo{slot}",))).cache_value
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
            cache.get(CacheKey(command="GET", redis_keys=("foo{slot}",))).cache_value
            == b"bar"
        )
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("bar{slot}",))).cache_value
            == b"foo"
        )
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("baz{slot}",))).cache_value
            == b"bar"
        )

        # Flush server and trying to access cached entry
        assert r.flushall()
        assert r.get("foo{slot}") is None
        assert cache.size == 0


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
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
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        master.set("foo", "barbar")
        # get key from redis
        assert master.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
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
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
            b"bar",
            "bar",
        ]
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache_data it
        assert r.get("foo") in [b"barbar", "barbar"]
        # Make sure that new value was cached
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
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
            cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value
            == b"bar"
        )
        # Force disconnection
        master.connection_pool.get_connection("_").disconnect()
        # Make sure cache_data is empty
        assert cache.size == 0


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
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
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
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
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
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
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
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
        assert cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value in [
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
            CacheKey(command="MGET", redis_keys=("foo", "bar"))
        ).cache_value == [b"bar", b"foo"]

        # Invalidate one of the keys and make sure
        # that all associated cached entries was removed
        assert r.set("foo", "baz")
        # Timeout needed for SSL connection because there's timeout
        # between data appears in socket buffer
        time.sleep(0.1)
        assert r.get("foo") == b"baz"
        assert cache.get(CacheKey(command="MGET", redis_keys=("foo", "bar"))) is None
        assert (
            cache.get(CacheKey(command="GET", redis_keys=("foo",))).cache_value
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

    def test_set_evict_lru_cache_key_on_reaching_max_size(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=3))
        cache_key1 = CacheKey(command="GET", redis_keys=("foo",))
        cache_key2 = CacheKey(command="GET", redis_keys=("foo1",))
        cache_key3 = CacheKey(command="GET", redis_keys=("foo2",))

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

        # Accessing key in the order that it makes 2nd key LRU
        assert cache.get(cache_key1).cache_value == b"bar"
        assert cache.get(cache_key2).cache_value == b"bar1"
        assert cache.get(cache_key3).cache_value == b"bar2"
        assert cache.get(cache_key1).cache_value == b"bar"

        cache_key4 = CacheKey(command="GET", redis_keys=("foo3",))
        assert cache.set(
            CacheEntry(
                cache_key=cache_key4,
                cache_value=b"bar3",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )

        # Make sure that new key was added and 2nd is evicted
        assert cache.get(cache_key4).cache_value == b"bar3"
        assert cache.get(cache_key2) is None

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

        wrong_key = CacheKey(command="HGET", redis_keys=("foo",))
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

        cache_key1 = CacheKey(command="GET", redis_keys=("foo",))
        cache_key2 = CacheKey(command="GET", redis_keys=("foo1",))
        cache_key3 = CacheKey(command="GET", redis_keys=("foo2",))
        cache_key4 = CacheKey(command="GET", redis_keys=("foo3",))

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

        cache_key1 = CacheKey(command="GET", redis_keys=("foo",))
        cache_key2 = CacheKey(command="GET", redis_keys=("foo1",))
        cache_key3 = CacheKey(command="MGET", redis_keys=("foo", "foo3"))
        cache_key4 = CacheKey(command="MGET", redis_keys=("foo2", "foo3"))

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

    def test_flush(self, mock_connection):
        cache = DefaultCache(CacheConfig(max_size=5))

        cache_key1 = CacheKey(command="GET", redis_keys=("foo",))
        cache_key2 = CacheKey(command="GET", redis_keys=("foo1",))
        cache_key3 = CacheKey(command="GET", redis_keys=("foo2",))

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

        cache_key1 = CacheKey(command="GET", redis_keys=("foo",))
        cache_key2 = CacheKey(command="GET", redis_keys=("bar",))

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
        cache_key1 = CacheKey(command="GET", redis_keys=("foo",))
        cache_key2 = CacheKey(command="GET", redis_keys=("bar",))
        cache_key3 = CacheKey(command="GET", redis_keys=("baz",))

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

        cache_key1 = CacheKey(command="GET", redis_keys=("foo",))
        cache_key2 = CacheKey(command="GET", redis_keys=("bar",))

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
