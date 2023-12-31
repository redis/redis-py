import time

import pytest
import redis
from redis.utils import HIREDIS_AVAILABLE


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_get_from_cache():
    r = redis.Redis(cache_enable=True, single_connection_client=True, protocol=3)
    r2 = redis.Redis(protocol=3)
    # add key to redis
    r.set("foo", "bar")
    # get key from redis and save in local cache
    assert r.get("foo") == b"bar"
    # get key from local cache
    assert r.client_cache.get(("GET", "foo")) == b"bar"
    # change key in redis (cause invalidation)
    r2.set("foo", "barbar")
    # send any command to redis (process invalidation in background)
    r.ping()
    # the command is not in the local cache anymore
    assert r.client_cache.get(("GET", "foo")) is None
    # get key from redis
    assert r.get("foo") == b"barbar"


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_cache_max_size():
    r = redis.Redis(
        cache_enable=True, cache_max_size=3, single_connection_client=True, protocol=3
    )
    # add 3 keys to redis
    r.set("foo", "bar")
    r.set("foo2", "bar2")
    r.set("foo3", "bar3")
    # get 3 keys from redis and save in local cache
    assert r.get("foo") == b"bar"
    assert r.get("foo2") == b"bar2"
    assert r.get("foo3") == b"bar3"
    # get the 3 keys from local cache
    assert r.client_cache.get(("GET", "foo")) == b"bar"
    assert r.client_cache.get(("GET", "foo2")) == b"bar2"
    assert r.client_cache.get(("GET", "foo3")) == b"bar3"
    # add 1 more key to redis (exceed the max size)
    r.set("foo4", "bar4")
    assert r.get("foo4") == b"bar4"
    # the first key is not in the local cache anymore
    assert r.client_cache.get(("GET", "foo")) is None


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_cache_ttl():
    r = redis.Redis(
        cache_enable=True, cache_ttl=1, single_connection_client=True, protocol=3
    )
    # add key to redis
    r.set("foo", "bar")
    # get key from redis and save in local cache
    assert r.get("foo") == b"bar"
    # get key from local cache
    assert r.client_cache.get(("GET", "foo")) == b"bar"
    # wait for the key to expire
    time.sleep(1)
    # the key is not in the local cache anymore
    assert r.client_cache.get(("GET", "foo")) is None


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_cache_lfu_eviction():
    r = redis.Redis(
        cache_enable=True,
        cache_max_size=3,
        cache_eviction_policy="lfu",
        single_connection_client=True,
        protocol=3,
    )
    # add 3 keys to redis
    r.set("foo", "bar")
    r.set("foo2", "bar2")
    r.set("foo3", "bar3")
    # get 3 keys from redis and save in local cache
    assert r.get("foo") == b"bar"
    assert r.get("foo2") == b"bar2"
    assert r.get("foo3") == b"bar3"
    # change the order of the keys in the cache
    assert r.client_cache.get(("GET", "foo")) == b"bar"
    assert r.client_cache.get(("GET", "foo")) == b"bar"
    assert r.client_cache.get(("GET", "foo3")) == b"bar3"
    # add 1 more key to redis (exceed the max size)
    r.set("foo4", "bar4")
    assert r.get("foo4") == b"bar4"
    # test the eviction policy
    assert len(r.client_cache.cache) == 3
    assert r.client_cache.get(("GET", "foo")) == b"bar"
    assert r.client_cache.get(("GET", "foo2")) is None


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
def test_cache_decode_response():
    r = redis.Redis(
        decode_responses=True,
        cache_enable=True,
        single_connection_client=True,
        protocol=3,
    )
    r2 = redis.Redis(protocol=3)
    r.set("foo", "bar")
    # get key from redis and save in local cache
    assert r.get("foo") == "bar"
    # get key from local cache
    assert r.client_cache.get(("GET", "foo")) == "bar"
    # change key in redis (cause invalidation)
    r2.set("foo", "barbar")
    # send any command to redis (process invalidation in background)
    r.ping()
    # the command is not in the local cache anymore
    assert r.client_cache.get(("GET", "foo")) is None
    # get key from redis
    assert r.get("foo") == "barbar"
