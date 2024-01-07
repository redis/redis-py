import time

import pytest
import redis
from redis._cache import _LocalCache
from redis.utils import HIREDIS_AVAILABLE


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_get_from_cache():
    cache = _LocalCache()
    r = redis.Redis(protocol=3, client_cache=cache)
    r2 = redis.Redis(protocol=3)
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

    r.flushdb()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_cache_max_size():
    cache = _LocalCache(max_size=3)
    r = redis.Redis(client_cache=cache, protocol=3)
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

    r.flushdb()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_cache_ttl():
    cache = _LocalCache(ttl=1)
    r = redis.Redis(client_cache=cache, protocol=3)
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

    r.flushdb()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_cache_lfu_eviction():
    cache = _LocalCache(max_size=3, eviction_policy="lfu")
    r = redis.Redis(client_cache=cache, protocol=3)
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

    r.flushdb()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_cache_decode_response():
    cache = _LocalCache()
    r = redis.Redis(decode_responses=True, client_cache=cache, protocol=3)
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

    r.flushdb()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_cache_blacklist():
    cache = _LocalCache()
    r = redis.Redis(client_cache=cache, cache_blacklist=["LLEN"], protocol=3)
    # add list to redis
    r.lpush("mylist", "foo", "bar", "baz")
    assert r.llen("mylist") == 3
    assert r.lindex("mylist", 1) == b"bar"
    assert cache.get(("LLEN", "mylist")) is None
    assert cache.get(("LINDEX", "mylist", 1)) == b"bar"

    r.flushdb()
