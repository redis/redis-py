import time

import pytest
import pytest_asyncio
from redis._cache import EvictionPolicy, _LocalCache
from redis.utils import HIREDIS_AVAILABLE


@pytest_asyncio.fixture
async def r(request, create_redis):
    cache = request.param.get("cache")
    kwargs = request.param.get("kwargs", {})
    r = await create_redis(protocol=3, client_cache=cache, **kwargs)
    yield r, cache


@pytest_asyncio.fixture()
async def local_cache():
    yield _LocalCache()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
class TestLocalCache:
    @pytest.mark.parametrize("r", [{"cache": _LocalCache()}], indirect=True)
    @pytest.mark.onlynoncluster
    async def test_get_from_cache(self, r, r2):
        r, cache = r
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

    @pytest.mark.parametrize("r", [{"cache": _LocalCache(max_size=3)}], indirect=True)
    async def test_cache_lru_eviction(self, r):
        r, cache = r
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

    @pytest.mark.parametrize("r", [{"cache": _LocalCache(ttl=1)}], indirect=True)
    async def test_cache_ttl(self, r):
        r, cache = r
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

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(max_size=3, eviction_policy=EvictionPolicy.LFU)}],
        indirect=True,
    )
    async def test_cache_lfu_eviction(self, r):
        r, cache = r
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

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    async def test_cache_decode_response(self, r):
        r, cache = r
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

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"cache_deny_list": ["LLEN"]}}],
        indirect=True,
    )
    async def test_cache_deny_list(self, r):
        r, cache = r
        # add list to redis
        await r.lpush("mylist", "foo", "bar", "baz")
        assert await r.llen("mylist") == 3
        assert await r.lindex("mylist", 1) == b"bar"
        assert cache.get(("LLEN", "mylist")) is None
        assert cache.get(("LINDEX", "mylist", 1)) == b"bar"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"cache_allow_list": ["LLEN"]}}],
        indirect=True,
    )
    async def test_cache_allow_list(self, r):
        r, cache = r
        # add list to redis
        await r.lpush("mylist", "foo", "bar", "baz")
        assert await r.llen("mylist") == 3
        assert await r.lindex("mylist", 1) == b"bar"
        assert cache.get(("LLEN", "mylist")) == 3
        assert cache.get(("LINDEX", "mylist", 1)) is None

    @pytest.mark.parametrize("r", [{"cache": _LocalCache()}], indirect=True)
    async def test_cache_return_copy(self, r):
        r, cache = r
        await r.lpush("mylist", "foo", "bar", "baz")
        assert await r.lrange("mylist", 0, -1) == [b"baz", b"bar", b"foo"]
        res = cache.get(("LRANGE", "mylist", 0, -1))
        assert res == [b"baz", b"bar", b"foo"]
        res.append(b"new")
        check = cache.get(("LRANGE", "mylist", 0, -1))
        assert check == [b"baz", b"bar", b"foo"]

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    async def test_csc_not_cause_disconnects(self, r):
        r, cache = r
        id1 = await r.client_id()
        await r.mset({"a": 1, "b": 1, "c": 1, "d": 1, "e": 1})
        assert await r.mget("a", "b", "c", "d", "e") == ["1", "1", "1", "1", "1"]
        id2 = await r.client_id()

        # client should get value from client cache
        assert await r.mget("a", "b", "c", "d", "e") == ["1", "1", "1", "1", "1"]
        assert cache.get(("MGET", "a", "b", "c", "d", "e")) == [
            "1",
            "1",
            "1",
            "1",
            "1",
        ]

        await r.mset({"a": 2, "b": 2, "c": 2, "d": 2, "e": 2})
        id3 = await r.client_id()
        # client should get value from redis server post invalidate messages
        assert await r.mget("a", "b", "c", "d", "e") == ["2", "2", "2", "2", "2"]

        await r.mset({"a": 3, "b": 3, "c": 3, "d": 3, "e": 3})
        # need to check that we get correct value 3 and not 2
        assert await r.mget("a", "b", "c", "d", "e") == ["3", "3", "3", "3", "3"]
        # client should get value from client cache
        assert await r.mget("a", "b", "c", "d", "e") == ["3", "3", "3", "3", "3"]

        await r.mset({"a": 4, "b": 4, "c": 4, "d": 4, "e": 4})
        # need to check that we get correct value 4 and not 3
        assert await r.mget("a", "b", "c", "d", "e") == ["4", "4", "4", "4", "4"]
        # client should get value from client cache
        assert await r.mget("a", "b", "c", "d", "e") == ["4", "4", "4", "4", "4"]
        id4 = await r.client_id()
        assert id1 == id2 == id3 == id4

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    async def test_execute_command_keys_provided(self, r):
        r, cache = r
        assert await r.execute_command("SET", "b", "2") is True
        assert await r.execute_command("GET", "b", keys=["b"]) == "2"
        assert cache.get(("GET", "b")) == "2"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    async def test_execute_command_keys_not_provided(self, r):
        r, cache = r
        assert await r.execute_command("SET", "b", "2") is True
        assert (
            await r.execute_command("GET", "b") == "2"
        )  # keys not provided, not cached
        assert cache.get(("GET", "b")) is None

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    async def test_delete_one_command(self, r):
        r, cache = r
        assert await r.mset({"a{a}": 1, "b{a}": 1}) is True
        assert await r.set("c", 1) is True
        assert await r.mget("a{a}", "b{a}") == ["1", "1"]
        assert await r.get("c") == "1"
        # values should be in local cache
        assert cache.get(("MGET", "a{a}", "b{a}")) == ["1", "1"]
        assert cache.get(("GET", "c")) == "1"
        # delete one command from the cache
        r.delete_command_from_cache(("MGET", "a{a}", "b{a}"))
        # the other command is still in the local cache anymore
        assert cache.get(("MGET", "a{a}", "b{a}")) is None
        assert cache.get(("GET", "c")) == "1"
        # get from redis
        assert await r.mget("a{a}", "b{a}") == ["1", "1"]
        assert await r.get("c") == "1"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    async def test_invalidate_key(self, r):
        r, cache = r
        assert await r.mset({"a{a}": 1, "b{a}": 1}) is True
        assert await r.set("c", 1) is True
        assert await r.mget("a{a}", "b{a}") == ["1", "1"]
        assert await r.get("c") == "1"
        # values should be in local cache
        assert cache.get(("MGET", "a{a}", "b{a}")) == ["1", "1"]
        assert cache.get(("GET", "c")) == "1"
        # invalidate one key from the cache
        r.invalidate_key_from_cache("b{a}")
        # one other command is still in the local cache anymore
        assert cache.get(("MGET", "a{a}", "b{a}")) is None
        assert cache.get(("GET", "c")) == "1"
        # get from redis
        assert await r.mget("a{a}", "b{a}") == ["1", "1"]
        assert await r.get("c") == "1"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    async def test_flush_entire_cache(self, r):
        r, cache = r
        assert await r.mset({"a{a}": 1, "b{a}": 1}) is True
        assert await r.set("c", 1) is True
        assert await r.mget("a{a}", "b{a}") == ["1", "1"]
        assert await r.get("c") == "1"
        # values should be in local cache
        assert cache.get(("MGET", "a{a}", "b{a}")) == ["1", "1"]
        assert cache.get(("GET", "c")) == "1"
        # flush the local cache
        r.flush_cache()
        # the commands are not in the local cache anymore
        assert cache.get(("MGET", "a{a}", "b{a}")) is None
        assert cache.get(("GET", "c")) is None
        # get from redis
        assert await r.mget("a{a}", "b{a}") == ["1", "1"]
        assert await r.get("c") == "1"


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlycluster
class TestClusterLocalCache:
    @pytest.mark.parametrize("r", [{"cache": _LocalCache()}], indirect=True)
    async def test_get_from_cache(self, r, r2):
        r, cache = r
        # add key to redis
        await r.set("foo", "bar")
        # get key from redis and save in local cache
        assert await r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        await r2.set("foo", "barbar")
        # send any command to redis (process invalidation in background)
        node = r.get_node_from_key("foo")
        await r.ping(target_nodes=node)
        # the command is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None
        # get key from redis
        assert await r.get("foo") == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    async def test_cache_decode_response(self, r):
        r, cache = r
        await r.set("foo", "bar")
        # get key from redis and save in local cache
        assert await r.get("foo") == "bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == "bar"
        # change key in redis (cause invalidation)
        await r.set("foo", "barbar")
        # send any command to redis (process invalidation in background)
        node = r.get_node_from_key("foo")
        await r.ping(target_nodes=node)
        # the command is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None
        # get key from redis
        assert await r.get("foo") == "barbar"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    async def test_execute_command_keys_provided(self, r):
        r, cache = r
        assert await r.execute_command("SET", "b", "2") is True
        assert await r.execute_command("GET", "b", keys=["b"]) == "2"
        assert cache.get(("GET", "b")) == "2"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    async def test_execute_command_keys_not_provided(self, r):
        r, cache = r
        assert await r.execute_command("SET", "b", "2") is True
        assert (
            await r.execute_command("GET", "b") == "2"
        )  # keys not provided, not cached
        assert cache.get(("GET", "b")) is None


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlynoncluster
class TestSentinelLocalCache:

    async def test_get_from_cache(self, local_cache, master):
        await master.set("foo", "bar")
        # get key from redis and save in local cache
        assert await master.get("foo") == b"bar"
        # get key from local cache
        assert local_cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        await master.set("foo", "barbar")
        # send any command to redis (process invalidation in background)
        await master.ping()
        # the command is not in the local cache anymore
        assert local_cache.get(("GET", "foo")) is None
        # get key from redis
        assert await master.get("foo") == b"barbar"

    @pytest.mark.parametrize(
        "sentinel_setup",
        [{"kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    async def test_cache_decode_response(self, local_cache, sentinel_setup, master):
        await master.set("foo", "bar")
        # get key from redis and save in local cache
        assert await master.get("foo") == "bar"
        # get key from local cache
        assert local_cache.get(("GET", "foo")) == "bar"
        # change key in redis (cause invalidation)
        await master.set("foo", "barbar")
        # send any command to redis (process invalidation in background)
        await master.ping()
        # the command is not in the local cache anymore
        assert local_cache.get(("GET", "foo")) is None
        # get key from redis
        assert await master.get("foo") == "barbar"
