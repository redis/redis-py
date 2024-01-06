import time

import pytest
import redis.asyncio as redis
from redis._cache import _LocalCache
from redis.utils import HIREDIS_AVAILABLE


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
async def test_get_from_cache():
    cache = _LocalCache()
    r = redis.Redis(protocol=3, client_cache=cache)
    r2 = redis.Redis(protocol=3)
    # add key to redis
    await r.set("foo", "bar")
    # get key from redis and save in local cache
    assert await r.get("foo") == b"bar"
    # get key from local cache
    assert cache.get(("GET", "foo")) == b"bar"
    # change key in redis (cause invalidation)
    await r2.set("foo", "barbar")
    # send any command to redis (process invalidation in background)
    await r.ping()
    # the command is not in the local cache anymore
    assert cache.get(("GET", "foo")) is None
    # get key from redis
    assert await r.get("foo") == b"barbar"

    await r.aclose()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
async def test_cache_max_size():
    cache = _LocalCache(max_size=3)
    r = redis.Redis(client_cache=cache, protocol=3)
    # add 3 keys to redis
    await r.set("foo", "bar")
    await r.set("foo2", "bar2")
    await r.set("foo3", "bar3")
    # get 3 keys from redis and save in local cache
    assert await r.get("foo") == b"bar"
    assert await r.get("foo2") == b"bar2"
    assert await r.get("foo3") == b"bar3"
    # get the 3 keys from local cache
    assert cache.get(("GET", "foo")) == b"bar"
    assert cache.get(("GET", "foo2")) == b"bar2"
    assert cache.get(("GET", "foo3")) == b"bar3"
    # add 1 more key to redis (exceed the max size)
    await r.set("foo4", "bar4")
    assert await r.get("foo4") == b"bar4"
    # the first key is not in the local cache anymore
    assert cache.get(("GET", "foo")) is None

    await r.aclose()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
async def test_cache_ttl():
    cache = _LocalCache(ttl=1)
    r = redis.Redis(client_cache=cache, protocol=3)
    # add key to redis
    await r.set("foo", "bar")
    # get key from redis and save in local cache
    assert await r.get("foo") == b"bar"
    # get key from local cache
    assert cache.get(("GET", "foo")) == b"bar"
    # wait for the key to expire
    time.sleep(1)
    # the key is not in the local cache anymore
    assert cache.get(("GET", "foo")) is None

    await r.aclose()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
async def test_cache_lfu_eviction():
    cache = _LocalCache(max_size=3, eviction_policy="lfu")
    r = redis.Redis(client_cache=cache, protocol=3)
    # add 3 keys to redis
    await r.set("foo", "bar")
    await r.set("foo2", "bar2")
    await r.set("foo3", "bar3")
    # get 3 keys from redis and save in local cache
    assert await r.get("foo") == b"bar"
    assert await r.get("foo2") == b"bar2"
    assert await r.get("foo3") == b"bar3"
    # change the order of the keys in the cache
    assert cache.get(("GET", "foo")) == b"bar"
    assert cache.get(("GET", "foo")) == b"bar"
    assert cache.get(("GET", "foo3")) == b"bar3"
    # add 1 more key to redis (exceed the max size)
    await r.set("foo4", "bar4")
    assert await r.get("foo4") == b"bar4"
    # test the eviction policy
    assert len(cache.cache) == 3
    assert cache.get(("GET", "foo")) == b"bar"
    assert cache.get(("GET", "foo2")) is None

    await r.aclose()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
async def test_cache_decode_response():
    cache = _LocalCache()
    r = redis.Redis(decode_responses=True, client_cache=cache, protocol=3)
    await r.set("foo", "bar")
    # get key from redis and save in local cache
    assert await r.get("foo") == "bar"
    # get key from local cache
    assert cache.get(("GET", "foo")) == "bar"
    # change key in redis (cause invalidation)
    await r.set("foo", "barbar")
    # send any command to redis (process invalidation in background)
    await r.ping()
    # the command is not in the local cache anymore
    assert cache.get(("GET", "foo")) is None
    # get key from redis
    assert await r.get("foo") == "barbar"

    await r.aclose()
