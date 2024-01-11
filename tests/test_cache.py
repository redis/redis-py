import time

import pytest
import redis
from redis._cache import _LocalCache
from redis.utils import HIREDIS_AVAILABLE
from tests.conftest import _get_client


@pytest.fixture()
def r(request):
    cache = request.param.get("cache")
    kwargs = request.param.get("kwargs", {})
    with _get_client(
        redis.Redis, request, protocol=3, client_cache=cache, **kwargs
    ) as client:
        yield client, cache
        # client.flushdb()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
class TestLocalCache:
    @pytest.mark.onlynoncluster
    @pytest.mark.parametrize("r", [{"cache": _LocalCache()}], indirect=True)
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
        # send any command to redis (process invalidation in background)
        r.ping()
        # the command is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None
        # get key from redis
        assert r.get("foo") == b"barbar"

    @pytest.mark.parametrize("r", [{"cache": _LocalCache(max_size=3)}], indirect=True)
    def test_cache_max_size(self, r):
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

    @pytest.mark.parametrize("r", [{"cache": _LocalCache(ttl=1)}], indirect=True)
    def test_cache_ttl(self, r):
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
        "r", [{"cache": _LocalCache(max_size=3, eviction_policy="lfu")}], indirect=True
    )
    def test_cache_lfu_eviction(self, r):
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
        assert len(cache.cache) == 3
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "foo2")) is None

    @pytest.mark.onlynoncluster
    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_cache_decode_response(self, r):
        r, cache = r
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == "bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == "bar"
        # change key in redis (cause invalidation)
        r.set("foo", "barbar")
        # send any command to redis (process invalidation in background)
        r.ping()
        # the command is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None
        # get key from redis
        assert r.get("foo") == "barbar"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"cache_blacklist": ["LLEN"]}}],
        indirect=True,
    )
    def test_cache_blacklist(self, r):
        r, cache = r
        # add list to redis
        r.lpush("mylist", "foo", "bar", "baz")
        assert r.llen("mylist") == 3
        assert r.lindex("mylist", 1) == b"bar"
        assert cache.get(("LLEN", "mylist")) is None
        assert cache.get(("LINDEX", "mylist", 1)) == b"bar"

    @pytest.mark.parametrize("r", [{"cache": _LocalCache()}], indirect=True)
    def test_cache_return_copy(self, r):
        r, cache = r
        r.lpush("mylist", "foo", "bar", "baz")
        assert r.lrange("mylist", 0, -1) == [b"baz", b"bar", b"foo"]
        res = cache.get(("LRANGE", "mylist", 0, -1))
        assert res == [b"baz", b"bar", b"foo"]
        res.append(b"new")
        check = cache.get(("LRANGE", "mylist", 0, -1))
        assert check == [b"baz", b"bar", b"foo"]


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlycluster
class TestClusterLocalCache:
    @pytest.mark.parametrize("r", [{"cache": _LocalCache()}], indirect=True)
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
        # send any command to redis (process invalidation in background)
        node = r.get_node_from_key("foo")
        r.ping(target_nodes=node)
        # the command is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None
        # get key from redis
        assert r.get("foo") == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_cache_decode_response(self, r):
        r, cache = r
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == "bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == "bar"
        # change key in redis (cause invalidation)
        r.set("foo", "barbar")
        # send any command to redis (process invalidation in background)
        node = r.get_node_from_key("foo")
        r.ping(target_nodes=node)
        # the command is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None
        # get key from redis
        assert r.get("foo") == "barbar"
