import time

import pytest
from cachetools import TTLCache, LRUCache, LFUCache

import redis
from redis import Redis, RedisCluster
from redis.utils import HIREDIS_AVAILABLE
from tests.conftest import _get_client


@pytest.fixture()
def r(request):
    use_cache = request.param.get("use_cache", False)
    cache = request.param.get("cache")
    kwargs = request.param.get("kwargs", {})
    protocol = request.param.get("protocol", 3)
    single_connection_client = request.param.get("single_connection_client", False)
    with _get_client(
            redis.Redis,
            request,
            protocol=protocol,
            single_connection_client=single_connection_client,
            use_cache=use_cache,
            cache=cache,
            **kwargs,
    ) as client:
        yield client, cache


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
class TestCache:
    @pytest.mark.parametrize("r", [{"cache": TTLCache(128, 300), "use_cache": True}], indirect=True)
    @pytest.mark.onlynoncluster
    def test_get_from_cache(self, r, r2):
        r, cache = r
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
        [{"cache": LRUCache(3), "use_cache": True}],
        indirect=True,
    )
    def test_cache_lru_eviction(self, r):
        r, cache = r
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

    @pytest.mark.parametrize("r", [{"cache": TTLCache(maxsize=128, ttl=1), "use_cache": True}], indirect=True)
    def test_cache_ttl(self, r, cache):
        r, cache = r
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
    def test_cache_lfu_eviction(self, r, cache):
        r, cache = r
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
    def test_cache_ignore_not_allowed_command(self, r):
        r, cache = r
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
    def test_cache_invalidate_all_related_responses(self, r, cache):
        r, cache = r
        # Add keys
        assert r.set("foo", "bar")
        assert r.set("bar", "foo")

        # Make sure that replies was cached
        assert r.mget("foo", "bar") == [b"bar", b"foo"]
        assert cache.get(("MGET", "foo", "bar")) == [b"bar", b"foo"]

        # Invalidate one of the keys and make sure that all associated cached entries was removed
        assert r.set("foo", "baz")
        assert r.get("foo") == b"baz"
        assert cache.get(("MGET", "foo", "bar")) is None
        assert cache.get(("GET", "foo")) == b"baz"

    @pytest.mark.parametrize(
        "r",
        [{"cache": LRUCache(maxsize=128), "use_cache": True}],
        indirect=True,
    )
    def test_cache_flushed_on_server_flush(self, r, cache):
        r, cache = r
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


# def test_cluster_cached_get_and_set():
#     cluster_url = "redis://localhost:16379/0"
#
#     r = RedisCluster.from_url(cluster_url, use_cache=True, protocol=3)
#     assert r.set("key", 5)
#     assert r.get("key") == b"5"
#
#     r2 = RedisCluster.from_url(cluster_url, use_cache=True, protocol=3)
#     r2.set("key", "foo")
#
#     time.sleep(0.5)
#
#     after_invalidation = r.get("key")
#     print(f'after invalidation {after_invalidation}')
#     assert after_invalidation == b"foo"
