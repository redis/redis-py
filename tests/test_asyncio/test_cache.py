import asyncio
import threading

import pytest
import pytest_asyncio
from cachetools import LRUCache, TTLCache

import redis.asyncio
from redis.utils import HIREDIS_AVAILABLE


@pytest_asyncio.fixture
async def r(request, create_redis, cache):
    use_cache = request.param.get('use_cache', False)
    cache = request.param.get("cache", None)
    kwargs = request.param.get("kwargs", {})
    r = await create_redis(
        protocol=3,
        use_cache=use_cache,
        cache=cache,
        **kwargs)
    yield r, cache


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
class TestLocalCache:
    @pytest.mark.parametrize("r", [{"cache": LRUCache(128), "use_cache": True}], indirect=True)
    @pytest.mark.onlynoncluster
    async def test_get_from_cache(self, r):
        r, cache = r
        # add key to redis
        await r.set("foo", "bar")
        # get key from redis and save in local cache
        assert await r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        assert await r.set("foo", "barbar")
        # Retrieves a new value from server and cache it
        assert await r.get("foo") == b"barbar"
        # Make sure that new value was cached
        assert cache.get(("GET", "foo")) == b"barbar"

    @pytest.mark.parametrize("r", [{"cache": TTLCache(128, 300), "use_cache": True}], indirect=True)
    @pytest.mark.onlynoncluster
    async def test_get_from_cache_multithreaded(self, r, cache):
        r, cache = r
        # Running commands over two threads
        threading.Thread(target=await r.set("foo", "bar")).start()
        threading.Thread(target=await r.set("bar", "foo")).start()

        # Wait for command execution to be finished
        await asyncio.sleep(0.1)

        threading.Thread(target=await r.get("foo")).start()
        threading.Thread(target=await r.get("bar")).start()

        # Wait for command execution to be finished
        await asyncio.sleep(0.1)

        # Make sure that responses was cached.
        assert cache.get(("GET", "foo")) == b"bar"
        assert cache.get(("GET", "bar")) == b"foo"

        threading.Thread(target=await r.set("foo", "baz")).start()
        threading.Thread(target=await r.set("bar", "bar")).start()

        # Wait for command execution to be finished
        await asyncio.sleep(0.1)

        threading.Thread(target=await r.get("foo")).start()
        threading.Thread(target=await r.get("bar")).start()

        # Wait for command execution to be finished
        await asyncio.sleep(0.1)

        # Make sure that new values was cached.
        assert cache.get(("GET", "foo")) == b"baz"
        assert cache.get(("GET", "bar")) == b"bar"

    @pytest.mark.parametrize("r", [
        {"cache": TTLCache(128, 300), "use_cache": True, "enable_cache_healthcheck": True}
    ], indirect=True)
    @pytest.mark.onlynoncluster
    async def test_health_check_invalidate_cache(self, r, cache):
        r, cache = r
        # add key to redis
        await r.set("foo", "bar")
        # get key from redis and save in local cache
        assert await r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        await r.set("foo", "barbar")
        # Wait for health check
        await asyncio.sleep(2)
        # Make sure that value was invalidated
        assert cache.get(("GET", "foo")) is None