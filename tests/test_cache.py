import threading
import time

import pytest
import redis
from cachetools import LFUCache, LRUCache, TTLCache
from redis.cache import CacheClass, EvictionPolicy
from redis.utils import HIREDIS_AVAILABLE
from tests.conftest import _get_client, skip_if_resp_version


@pytest.fixture()
def r(request):
    use_cache = request.param.get("use_cache", False)
    cache = request.param.get("cache")
    cache_eviction = request.param.get("cache_eviction")
    cache_size = request.param.get("cache_size")
    cache_ttl = request.param.get("cache_ttl")
    kwargs = request.param.get("kwargs", {})
    protocol = request.param.get("protocol", 3)
    ssl = request.param.get("ssl", False)
    single_connection_client = request.param.get("single_connection_client", False)
    with _get_client(
        redis.Redis,
        request,
        protocol=protocol,
        ssl=ssl,
        single_connection_client=single_connection_client,
        use_cache=use_cache,
        cache=cache,
        cache_eviction=cache_eviction,
        cache_size=cache_size,
        cache_ttl=cache_ttl,
        **kwargs,
    ) as client:
        yield client


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlynoncluster
@skip_if_resp_version(2)
class TestCache:
    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "single_connection_client": True,
            },
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_given_cache(self, r, r2):
        cache = r.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") == b"barbar"
        # Make sure that new value was cached
        assert cache.get(("GET", "foo")) == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.TTL,
                "cache_size": 128,
                "cache_ttl": 300,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.LRU,
                "cache_size": 128,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.LFU,
                "cache_size": 128,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.RANDOM,
                "cache_size": 128,
            },
        ],
        ids=["TTL", "LRU", "LFU", "RANDOM"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_custom_cache(self, request, r, r2):
        cache_class = CacheClass[request.node.callspec.id]
        cache = r.get_cache()
        assert isinstance(cache, cache_class.value)

        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") == b"barbar"
        # Make sure that new value was cached
        assert cache.get(("GET", "foo")) == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "single_connection_client": True,
            },
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_cache_multithreaded(self, r):
        cache = r.get_cache()
        # Running commands over two threads
        threading.Thread(target=r.set("foo", "bar")).start()
        threading.Thread(target=r.set("bar", "foo")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        threading.Thread(target=r.get("foo")).start()
        threading.Thread(target=r.get("bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        # Make sure that responses was cached.
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "bar")) == b"foo"

        threading.Thread(target=r.set("foo", "baz")).start()
        threading.Thread(target=r.set("bar", "bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        threading.Thread(target=r.get("foo")).start()
        threading.Thread(target=r.get("bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        # Make sure that new values was cached.
        assert cache.get(("GET", "foo")) == b"baz"
        assert cache.get(("GET", "bar")) == b"bar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "single_connection_client": False,
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_health_check_invalidate_cache(self, r, r2):
        cache = r.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Wait for health check
        time.sleep(2)
        # Make sure that value was invalidated
        assert cache.get(("GET", "foo")) is None

    @pytest.mark.parametrize(
        "r", [{"cache": TTLCache(128, 300), "use_cache": True}], indirect=True
    )
    @pytest.mark.onlynoncluster
    def test_health_check_invalidate_cache_multithreaded(self, r, r2):
        cache = r.get_cache()
        # Running commands over two threads
        threading.Thread(target=r.set("foo", "bar")).start()
        threading.Thread(target=r.set("bar", "foo")).start()
        # Wait for command execution to be finished
        time.sleep(0.1)
        # get keys from server
        threading.Thread(target=r.get("foo")).start()
        threading.Thread(target=r.get("bar")).start()
        # Wait for command execution to be finished
        time.sleep(0.1)
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "bar")) == b"foo"
        # change key in redis (cause invalidation)
        threading.Thread(target=r2.set("foo", "baz")).start()
        threading.Thread(target=r2.set("bar", "bar")).start()
        # Wait for health check
        time.sleep(2)
        # Make sure that value was invalidated
        assert cache.get(("GET", "foo")) is None
        assert cache.get(("GET", "bar")) is None

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "single_connection_client": True,
            },
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
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
        assert cache.get(("GET", "foo")) == b"bar"
        # Force disconnection
        r.connection_pool.get_connection("_").disconnect()
        # Make sure cache is empty
        assert cache.currsize == 0

    @pytest.mark.parametrize(
        "r",
        [
            {"use_cache": True, "cache_size": 3, "single_connection_client": True},
            {"use_cache": True, "cache_size": 3, "single_connection_client": False},
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
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "foo2")) == b"bar2"
        assert cache.get(("GET", "foo3")) == b"bar3"
        # add 1 more key to redis (exceed the max size)
        r.set("foo4", "bar4")
        assert r.get("foo4") == b"bar4"
        # the first key is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None

    @pytest.mark.parametrize(
        "r",
        [
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.TTL,
                "cache_ttl": 1,
                "single_connection_client": True,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.TTL,
                "cache_ttl": 1,
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_cache_ttl(self, r):
        cache = r.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # wait for the key to expire
        time.sleep(1)
        # the key is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None

    @pytest.mark.parametrize(
        "r",
        [
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.LFU,
                "cache_size": 3,
                "single_connection_client": True,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.LFU,
                "cache_size": 3,
                "single_connection_client": False,
            },
        ],
        ids=["single", "pool"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_cache_lfu_eviction(self, r):
        cache = r.get_cache()
        # add 3 keys to redis
        r.set("foo", "bar")
        r.set("foo2", "bar2")
        r.set("foo3", "bar3")
        # get 3 keys from redis and save in local cache
        assert r.get("foo") == b"bar"
        assert r.get("foo2") == b"bar2"
        assert r.get("foo3") == b"bar3"
        # change the order of the keys in the cache
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "foo3")) == b"bar3"
        # add 1 more key to redis (exceed the max size)
        r.set("foo4", "bar4")
        assert r.get("foo4") == b"bar4"
        # test the eviction policy
        assert cache.currsize == 3
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "foo2")) is None

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "single_connection_client": True,
            },
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
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
        assert cache.get(("HRANDFIELD", "foo")) is None

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "single_connection_client": True,
            },
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
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
        assert cache.get(("MGET", "foo", "bar")) == res

        # Make sure that objects are immutable.
        another_res = r.mget("foo", "bar")
        res.append(b"baz")
        assert another_res != res

        # Invalidate one of the keys and make sure that
        # all associated cached entries was removed
        assert r.set("foo", "baz")
        assert r.get("foo") == b"baz"
        assert cache.get(("MGET", "foo", "bar")) is None
        assert cache.get(("GET", "foo")) == b"baz"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "single_connection_client": True,
            },
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
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
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "bar")) == b"foo"
        assert cache.get(("GET", "baz")) == b"bar"

        # Flush server and trying to access cached entry
        assert r.flushall()
        assert r.get("foo") is None
        assert cache.currsize == 0


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlycluster
@skip_if_resp_version(2)
class TestClusterCache:
    @pytest.mark.parametrize(
        "r", [{"cache": LRUCache(maxsize=128), "use_cache": True}], indirect=True
    )
    def test_get_from_cache(self, r, r2):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") == b"barbar"
        # Make sure that new value was cached
        assert cache.get(("GET", "foo")) == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.TTL,
                "cache_size": 128,
                "cache_ttl": 300,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.LRU,
                "cache_size": 128,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.LFU,
                "cache_size": 128,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.RANDOM,
                "cache_size": 128,
            },
        ],
        ids=["TTL", "LRU", "LFU", "RANDOM"],
        indirect=True,
    )
    def test_get_from_custom_cache(self, request, r, r2):
        cache_class = CacheClass[request.node.callspec.id]
        cache = r.nodes_manager.get_node_from_slot(12000).redis_connection.get_cache()
        assert isinstance(cache, cache_class.value)

        # add key to redis
        assert r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") == b"barbar"
        # Make sure that new value was cached
        assert cache.get(("GET", "foo")) == b"barbar"

    @pytest.mark.parametrize(
        "r", [{"cache": TTLCache(128, 300), "use_cache": True}], indirect=True
    )
    @pytest.mark.onlycluster
    def test_get_from_cache_multithreaded(self, r):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # Running commands over two threads
        threading.Thread(target=r.set("foo", "bar")).start()
        threading.Thread(target=r.set("bar", "foo")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        threading.Thread(target=r.get("foo")).start()
        threading.Thread(target=r.get("bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        # Make sure that both values was cached.
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "bar")) == b"foo"

        # Running commands over two threads
        threading.Thread(target=r.set("foo", "baz")).start()
        threading.Thread(target=r.set("bar", "bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        threading.Thread(target=r.get("foo")).start()
        threading.Thread(target=r.get("bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        # Make sure that new values was cached.
        assert cache.get(("GET", "foo")) == b"baz"
        assert cache.get(("GET", "bar")) == b"bar"

    @pytest.mark.parametrize(
        "r", [{"cache": TTLCache(128, 300), "use_cache": True}], indirect=True
    )
    @pytest.mark.onlycluster
    def test_health_check_invalidate_cache(self, r, r2):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Wait for health check
        time.sleep(2)
        # Make sure that value was invalidated
        assert cache.get(("GET", "foo")) is None

    @pytest.mark.parametrize(
        "r", [{"cache": TTLCache(128, 300), "use_cache": True}], indirect=True
    )
    @pytest.mark.onlycluster
    def test_health_check_invalidate_cache_multithreaded(self, r, r2):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # Running commands over two threads
        threading.Thread(target=r.set("foo", "bar")).start()
        threading.Thread(target=r.set("bar", "foo")).start()
        # Wait for command execution to be finished
        time.sleep(0.1)
        # get keys from server
        threading.Thread(target=r.get("foo")).start()
        threading.Thread(target=r.get("bar")).start()
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "bar")) == b"foo"
        # change key in redis (cause invalidation)
        threading.Thread(target=r.set("foo", "baz")).start()
        threading.Thread(target=r.set("bar", "bar")).start()
        # Wait for health check
        time.sleep(2)
        # Make sure that value was invalidated
        assert cache.get(("GET", "foo")) is None
        assert cache.get(("GET", "bar")) is None

    @pytest.mark.parametrize(
        "r", [{"cache": TTLCache(128, 300), "use_cache": True}], indirect=True
    )
    @pytest.mark.onlycluster
    def test_cache_clears_on_disconnect(self, r, r2):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # Force disconnection
        r.nodes_manager.get_node_from_slot(
            10
        ).redis_connection.connection_pool.get_connection("_").disconnect()
        # Make sure cache is empty
        assert cache.currsize == 0

    @pytest.mark.parametrize(
        "r",
        [{"cache": LRUCache(3), "use_cache": True}],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_cache_lru_eviction(self, r):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # add 3 keys to redis
        r.set("foo", "bar")
        r.set("foo2", "bar2")
        r.set("foo3", "bar3")
        # get 3 keys from redis and save in local cache
        assert r.get("foo") == b"bar"
        assert r.get("foo2") == b"bar2"
        assert r.get("foo3") == b"bar3"
        # get the 3 keys from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "foo2")) == b"bar2"
        assert cache.get(("GET", "foo3")) == b"bar3"
        # add 1 more key to redis (exceed the max size)
        r.set("foo4", "bar4")
        assert r.get("foo4") == b"bar4"
        # the first key is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None

    @pytest.mark.parametrize(
        "r", [{"cache": TTLCache(maxsize=128, ttl=1), "use_cache": True}], indirect=True
    )
    @pytest.mark.onlycluster
    def test_cache_ttl(self, r):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # wait for the key to expire
        time.sleep(1)
        # the key is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None

    @pytest.mark.parametrize(
        "r",
        [{"cache": LFUCache(3), "use_cache": True}],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_cache_lfu_eviction(self, r):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # add 3 keys to redis
        r.set("foo", "bar")
        r.set("foo2", "bar2")
        r.set("foo3", "bar3")
        # get 3 keys from redis and save in local cache
        assert r.get("foo") == b"bar"
        assert r.get("foo2") == b"bar2"
        assert r.get("foo3") == b"bar3"
        # change the order of the keys in the cache
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "foo3")) == b"bar3"
        # add 1 more key to redis (exceed the max size)
        r.set("foo4", "bar4")
        assert r.get("foo4") == b"bar4"
        # test the eviction policy
        assert cache.currsize == 3
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "foo2")) is None

    @pytest.mark.parametrize(
        "r",
        [{"cache": LRUCache(maxsize=128), "use_cache": True}],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_cache_ignore_not_allowed_command(self, r):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # add fields to hash
        assert r.hset("foo", "bar", "baz")
        # get random field
        assert r.hrandfield("foo") == b"bar"
        assert cache.get(("HRANDFIELD", "foo")) is None

    @pytest.mark.parametrize(
        "r",
        [{"cache": LRUCache(maxsize=128), "use_cache": True}],
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
        assert cache.get(("MGET", "foo{slot}", "bar{slot}")) == [b"bar", b"foo"]

        # Invalidate one of the keys and make sure
        # that all associated cached entries was removed
        assert r.set("foo{slot}", "baz")
        assert r.get("foo{slot}") == b"baz"
        assert cache.get(("MGET", "foo{slot}", "bar{slot}")) is None
        assert cache.get(("GET", "foo{slot}")) == b"baz"

    @pytest.mark.parametrize(
        "r",
        [{"cache": LRUCache(maxsize=128), "use_cache": True}],
        indirect=True,
    )
    @pytest.mark.onlycluster
    def test_cache_flushed_on_server_flush(self, r, cache):
        cache = r.nodes_manager.get_node_from_slot(10).redis_connection.get_cache()
        # Add keys
        assert r.set("foo", "bar")
        assert r.set("bar", "foo")
        assert r.set("baz", "bar")

        # Make sure that replies was cached
        assert r.get("foo") == b"bar"
        assert r.get("bar") == b"foo"
        assert r.get("baz") == b"bar"
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "bar")) == b"foo"
        assert cache.get(("GET", "baz")) == b"bar"

        # Flush server and trying to access cached entry
        assert r.flushall()
        assert r.get("foo") is None
        assert cache.currsize == 0


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlynoncluster
@skip_if_resp_version(2)
class TestSentinelCache:
    @pytest.mark.parametrize(
        "sentinel_setup",
        [
            {
                "cache": LRUCache(maxsize=128),
                "use_cache": True,
                "force_master_ip": "localhost",
            }
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_cache(self, master):
        cache = master.get_cache()
        master.set("foo", "bar")
        # get key from redis and save in local cache
        assert master.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        master.set("foo", "barbar")
        # get key from redis
        assert master.get("foo") == b"barbar"
        # Make sure that new value was cached
        assert cache.get(("GET", "foo")) == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.TTL,
                "cache_size": 128,
                "cache_ttl": 300,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.LRU,
                "cache_size": 128,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.LFU,
                "cache_size": 128,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.RANDOM,
                "cache_size": 128,
            },
        ],
        ids=["TTL", "LRU", "LFU", "RANDOM"],
        indirect=True,
    )
    def test_get_from_custom_cache(self, request, r, r2):
        cache_class = CacheClass[request.node.callspec.id]
        cache = r.get_cache()
        assert isinstance(cache, cache_class.value)

        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") == b"barbar"
        # Make sure that new value was cached
        assert cache.get(("GET", "foo")) == b"barbar"

    @pytest.mark.parametrize(
        "sentinel_setup",
        [
            {
                "cache": LRUCache(maxsize=128),
                "use_cache": True,
                "force_master_ip": "localhost",
            }
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_cache_multithreaded(self, master):
        cache = master.get_cache()

        # Running commands over two threads
        threading.Thread(target=master.set("foo", "bar")).start()
        threading.Thread(target=master.set("bar", "foo")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        # Running commands over two threads
        threading.Thread(target=master.get("foo")).start()
        threading.Thread(target=master.get("bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        # Make sure that both values was cached.
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "bar")) == b"foo"

        # Running commands over two threads
        threading.Thread(target=master.set("foo", "baz")).start()
        threading.Thread(target=master.set("bar", "bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        # Running commands over two threads
        threading.Thread(target=master.get("foo")).start()
        threading.Thread(target=master.get("bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        # Make sure that new values was cached.
        assert cache.get(("GET", "foo")) == b"baz"
        assert cache.get(("GET", "bar")) == b"bar"

    @pytest.mark.parametrize(
        "sentinel_setup",
        [
            {
                "cache": LRUCache(maxsize=128),
                "use_cache": True,
                "force_master_ip": "localhost",
            }
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_health_check_invalidate_cache(self, master, cache):
        cache = master.get_cache()
        # add key to redis
        master.set("foo", "bar")
        # get key from redis and save in local cache
        assert master.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        master.set("foo", "barbar")
        # Wait for health check
        time.sleep(2)
        # Make sure that value was invalidated
        assert cache.get(("GET", "foo")) is None

    @pytest.mark.parametrize(
        "sentinel_setup",
        [
            {
                "cache": LRUCache(maxsize=128),
                "use_cache": True,
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
        # get key from redis and save in local cache
        assert master.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # Force disconnection
        master.connection_pool.get_connection("_").disconnect()
        # Make sure cache is empty
        assert cache.currsize == 0


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlynoncluster
@skip_if_resp_version(2)
class TestSSLCache:
    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "ssl": True,
            }
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_cache(self, r, r2, cache):
        cache = r.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        assert r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") == b"barbar"
        # Make sure that new value was cached
        assert cache.get(("GET", "foo")) == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.TTL,
                "cache_size": 128,
                "cache_ttl": 300,
                "ssl": True,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.LRU,
                "cache_size": 128,
                "ssl": True,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.LFU,
                "cache_size": 128,
                "ssl": True,
            },
            {
                "use_cache": True,
                "cache_eviction": EvictionPolicy.RANDOM,
                "cache_size": 128,
                "ssl": True,
            },
        ],
        ids=["TTL", "LRU", "LFU", "RANDOM"],
        indirect=True,
    )
    def test_get_from_custom_cache(self, request, r, r2):
        cache_class = CacheClass[request.node.callspec.id]
        cache = r.get_cache()
        assert isinstance(cache, cache_class.value)

        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert r.get("foo") == b"barbar"
        # Make sure that new value was cached
        assert cache.get(("GET", "foo")) == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "ssl": True,
            }
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_get_from_cache_multithreaded(self, r):
        cache = r.get_cache()
        # Running commands over two threads
        threading.Thread(target=r.set("foo", "bar")).start()
        threading.Thread(target=r.set("bar", "foo")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        threading.Thread(target=r.get("foo")).start()
        threading.Thread(target=r.get("bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        # Make sure that responses was cached.
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "bar")) == b"foo"

        threading.Thread(target=r.set("foo", "baz")).start()
        threading.Thread(target=r.set("bar", "bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        threading.Thread(target=r.get("foo")).start()
        threading.Thread(target=r.get("bar")).start()

        # Wait for command execution to be finished
        time.sleep(0.1)

        # Make sure that new values was cached.
        assert cache.get(("GET", "foo")) == b"baz"
        assert cache.get(("GET", "bar")) == b"bar"

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
                "ssl": True,
            }
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_health_check_invalidate_cache(self, r, r2):
        cache = r.get_cache()
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # Wait for health check
        time.sleep(2)
        # Make sure that value was invalidated
        assert cache.get(("GET", "foo")) is None

    @pytest.mark.parametrize(
        "r",
        [
            {
                "cache": TTLCache(128, 300),
                "use_cache": True,
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
        assert cache.get(("MGET", "foo", "bar")) == [b"bar", b"foo"]

        # Invalidate one of the keys and make sure
        # that all associated cached entries was removed
        assert r.set("foo", "baz")
        assert r.get("foo") == b"baz"
        assert cache.get(("MGET", "foo", "bar")) is None
        assert cache.get(("GET", "foo")) == b"baz"
