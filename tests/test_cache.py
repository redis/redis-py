import time
from collections import defaultdict
from typing import List, Sequence, Union

import cachetools
import pytest
import redis
from redis import RedisError
from redis._cache import AbstractCache, EvictionPolicy, _LocalCache
from redis.typing import KeyT, ResponseT
from redis.utils import HIREDIS_AVAILABLE
from tests.conftest import _get_client


@pytest.fixture()
def r(request):
    cache = request.param.get("cache")
    kwargs = request.param.get("kwargs", {})
    protocol = request.param.get("protocol", 3)
    single_connection_client = request.param.get("single_connection_client", False)
    with _get_client(
        redis.Redis,
        request,
        single_connection_client=single_connection_client,
        protocol=protocol,
        client_cache=cache,
        **kwargs,
    ) as client:
        yield client, cache


@pytest.fixture()
def local_cache():
    return _LocalCache()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
class TestLocalCache:
    @pytest.mark.parametrize("r", [{"cache": _LocalCache()}], indirect=True)
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
        # send any command to redis (process invalidation in background)
        r.ping()
        # the command is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None
        # get key from redis
        assert r.get("foo") == b"barbar"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(max_size=3)}],
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
        "r",
        [{"cache": _LocalCache(max_size=3, eviction_policy=EvictionPolicy.LFU)}],
        indirect=True,
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

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
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
        [{"cache": _LocalCache(), "kwargs": {"cache_deny_list": ["LLEN"]}}],
        indirect=True,
    )
    def test_cache_deny_list(self, r):
        r, cache = r
        # add list to redis
        r.lpush("mylist", "foo", "bar", "baz")
        assert r.llen("mylist") == 3
        assert r.lindex("mylist", 1) == b"bar"
        assert cache.get(("LLEN", "mylist")) is None
        assert cache.get(("LINDEX", "mylist", 1)) == b"bar"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"cache_allow_list": ["LLEN"]}}],
        indirect=True,
    )
    def test_cache_allow_list(self, r):
        r, cache = r
        r.lpush("mylist", "foo", "bar", "baz")
        assert r.llen("mylist") == 3
        assert r.lindex("mylist", 1) == b"bar"
        assert cache.get(("LLEN", "mylist")) == 3
        assert cache.get(("LINDEX", "mylist", 1)) is None

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

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_csc_not_cause_disconnects(self, r):
        r, cache = r
        id1 = r.client_id()
        r.mset({"a": 1, "b": 1, "c": 1, "d": 1, "e": 1, "f": 1})
        assert r.mget("a", "b", "c", "d", "e", "f") == ["1", "1", "1", "1", "1", "1"]
        id2 = r.client_id()

        # client should get value from client cache
        assert r.mget("a", "b", "c", "d", "e", "f") == ["1", "1", "1", "1", "1", "1"]
        assert cache.get(("MGET", "a", "b", "c", "d", "e", "f")) == [
            "1",
            "1",
            "1",
            "1",
            "1",
            "1",
        ]

        r.mset({"a": 2, "b": 2, "c": 2, "d": 2, "e": 2, "f": 2})
        id3 = r.client_id()
        # client should get value from redis server post invalidate messages
        assert r.mget("a", "b", "c", "d", "e", "f") == ["2", "2", "2", "2", "2", "2"]

        r.mset({"a": 3, "b": 3, "c": 3, "d": 3, "e": 3, "f": 3})
        # need to check that we get correct value 3 and not 2
        assert r.mget("a", "b", "c", "d", "e", "f") == ["3", "3", "3", "3", "3", "3"]
        # client should get value from client cache
        assert r.mget("a", "b", "c", "d", "e", "f") == ["3", "3", "3", "3", "3", "3"]

        r.mset({"a": 4, "b": 4, "c": 4, "d": 4, "e": 4, "f": 4})
        # need to check that we get correct value 4 and not 3
        assert r.mget("a", "b", "c", "d", "e", "f") == ["4", "4", "4", "4", "4", "4"]
        # client should get value from client cache
        assert r.mget("a", "b", "c", "d", "e", "f") == ["4", "4", "4", "4", "4", "4"]
        id4 = r.client_id()
        assert id1 == id2 == id3 == id4

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_multiple_commands_same_key(self, r):
        r, cache = r
        r.mset({"a": 1, "b": 1})
        assert r.mget("a", "b") == ["1", "1"]
        # value should be in local cache
        assert cache.get(("MGET", "a", "b")) == ["1", "1"]
        # set only one key
        r.set("a", 2)
        # send any command to redis (process invalidation in background)
        r.ping()
        # the command is not in the local cache anymore
        assert cache.get(("MGET", "a", "b")) is None
        # get from redis
        assert r.mget("a", "b") == ["2", "1"]

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_delete_one_command(self, r):
        r, cache = r
        r.mset({"a{a}": 1, "b{a}": 1})
        r.set("c", 1)
        assert r.mget("a{a}", "b{a}") == ["1", "1"]
        assert r.get("c") == "1"
        # values should be in local cache
        assert cache.get(("MGET", "a{a}", "b{a}")) == ["1", "1"]
        assert cache.get(("GET", "c")) == "1"
        # delete one command from the cache
        r.delete_command_from_cache(("MGET", "a{a}", "b{a}"))
        # the other command is still in the local cache anymore
        assert cache.get(("MGET", "a{a}", "b{a}")) is None
        assert cache.get(("GET", "c")) == "1"
        # get from redis
        assert r.mget("a{a}", "b{a}") == ["1", "1"]
        assert r.get("c") == "1"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_delete_several_commands(self, r):
        r, cache = r
        r.mset({"a{a}": 1, "b{a}": 1})
        r.set("c", 1)
        assert r.mget("a{a}", "b{a}") == ["1", "1"]
        assert r.get("c") == "1"
        # values should be in local cache
        assert cache.get(("MGET", "a{a}", "b{a}")) == ["1", "1"]
        assert cache.get(("GET", "c")) == "1"
        # delete the commands from the cache
        cache.delete_commands([("MGET", "a{a}", "b{a}"), ("GET", "c")])
        # the commands are not in the local cache anymore
        assert cache.get(("MGET", "a{a}", "b{a}")) is None
        assert cache.get(("GET", "c")) is None
        # get from redis
        assert r.mget("a{a}", "b{a}") == ["1", "1"]
        assert r.get("c") == "1"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_invalidate_key(self, r):
        r, cache = r
        r.mset({"a{a}": 1, "b{a}": 1})
        r.set("c", 1)
        assert r.mget("a{a}", "b{a}") == ["1", "1"]
        assert r.get("c") == "1"
        # values should be in local cache
        assert cache.get(("MGET", "a{a}", "b{a}")) == ["1", "1"]
        assert cache.get(("GET", "c")) == "1"
        # invalidate one key from the cache
        r.invalidate_key_from_cache("b{a}")
        # one other command is still in the local cache anymore
        assert cache.get(("MGET", "a{a}", "b{a}")) is None
        assert cache.get(("GET", "c")) == "1"
        # get from redis
        assert r.mget("a{a}", "b{a}") == ["1", "1"]
        assert r.get("c") == "1"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_flush_entire_cache(self, r):
        r, cache = r
        r.mset({"a{a}": 1, "b{a}": 1})
        r.set("c", 1)
        assert r.mget("a{a}", "b{a}") == ["1", "1"]
        assert r.get("c") == "1"
        # values should be in local cache
        assert cache.get(("MGET", "a{a}", "b{a}")) == ["1", "1"]
        assert cache.get(("GET", "c")) == "1"
        # flush the local cache
        r.flush_cache()
        # the commands are not in the local cache anymore
        assert cache.get(("MGET", "a{a}", "b{a}")) is None
        assert cache.get(("GET", "c")) is None
        # get from redis
        assert r.mget("a{a}", "b{a}") == ["1", "1"]
        assert r.get("c") == "1"

    @pytest.mark.onlynoncluster
    def test_cache_not_available_with_resp2(self, request):
        with pytest.raises(RedisError) as e:
            _get_client(redis.Redis, request, protocol=2, client_cache=_LocalCache())
        assert "protocol version 3 or higher" in str(e.value)

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_execute_command_args_not_split(self, r):
        r, cache = r
        assert r.execute_command("SET a 1") == "OK"
        assert r.execute_command("GET a") == "1"
        # "get a" is not whitelisted by default, the args should be separated
        assert cache.get(("GET a",)) is None

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_execute_command_keys_provided(self, r):
        r, cache = r
        assert r.execute_command("SET", "b", "2") is True
        assert r.execute_command("GET", "b", keys=["b"]) == "2"
        assert cache.get(("GET", "b")) == "2"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_execute_command_keys_not_provided(self, r):
        r, cache = r
        assert r.execute_command("SET", "b", "2") is True
        assert r.execute_command("GET", "b") == "2"  # keys not provided, not cached
        assert cache.get(("GET", "b")) is None

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "single_connection_client": True}],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    def test_single_connection(self, r):
        r, cache = r
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r.set("foo", "barbar")
        # send any command to redis (process invalidation in background)
        r.ping()
        # the command is not in the local cache anymore
        assert cache.get(("GET", "foo")) is None
        # get key from redis
        assert r.get("foo") == b"barbar"

    @pytest.mark.parametrize("r", [{"cache": _LocalCache()}], indirect=True)
    def test_get_from_cache_invalidate_via_get(self, r, r2):
        r, cache = r
        # add key to redis
        r.set("foo", "bar")
        # get key from redis and save in local cache
        assert r.get("foo") == b"bar"
        # get key from local cache
        assert cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        r2.set("foo", "barbar")
        # don't send any command to redis, just run another get
        # it should process the invalidation in background
        assert r.get("foo") == b"barbar"


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

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_execute_command_keys_provided(self, r):
        r, cache = r
        assert r.execute_command("SET", "b", "2") is True
        assert r.execute_command("GET", "b", keys=["b"]) == "2"
        assert cache.get(("GET", "b")) == "2"

    @pytest.mark.parametrize(
        "r",
        [{"cache": _LocalCache(), "kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_execute_command_keys_not_provided(self, r):
        r, cache = r
        assert r.execute_command("SET", "b", "2") is True
        assert r.execute_command("GET", "b") == "2"  # keys not provided, not cached
        assert cache.get(("GET", "b")) is None


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlynoncluster
class TestSentinelLocalCache:

    def test_get_from_cache(self, local_cache, master):
        master.set("foo", "bar")
        # get key from redis and save in local cache
        assert master.get("foo") == b"bar"
        # get key from local cache
        assert local_cache.get(("GET", "foo")) == b"bar"
        # change key in redis (cause invalidation)
        master.set("foo", "barbar")
        # send any command to redis (process invalidation in background)
        master.ping()
        # the command is not in the local cache anymore
        assert local_cache.get(("GET", "foo")) is None
        # get key from redis
        assert master.get("foo") == b"barbar"

    @pytest.mark.parametrize(
        "sentinel_setup",
        [{"kwargs": {"decode_responses": True}}],
        indirect=True,
    )
    def test_cache_decode_response(self, local_cache, sentinel_setup, master):
        master.set("foo", "bar")
        # get key from redis and save in local cache
        assert master.get("foo") == "bar"
        # get key from local cache
        assert local_cache.get(("GET", "foo")) == "bar"
        # change key in redis (cause invalidation)
        master.set("foo", "barbar")
        # send any command to redis (process invalidation in background)
        master.ping()
        # the command is not in the local cache anymore
        assert local_cache.get(("GET", "foo")) is None
        # get key from redis
        assert master.get("foo") == "barbar"


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlynoncluster
class TestCustomCache:
    class _CustomCache(AbstractCache):
        def __init__(self):
            self.responses = cachetools.LRUCache(maxsize=1000)
            self.keys_to_commands = defaultdict(list)
            self.commands_to_keys = defaultdict(list)

        def set(
            self,
            command: Union[str, Sequence[str]],
            response: ResponseT,
            keys_in_command: List[KeyT],
        ):
            self.responses[command] = response
            for key in keys_in_command:
                self.keys_to_commands[key].append(tuple(command))
                self.commands_to_keys[command].append(tuple(keys_in_command))

        def get(self, command: Union[str, Sequence[str]]) -> ResponseT:
            return self.responses.get(command)

        def delete_command(self, command: Union[str, Sequence[str]]):
            self.responses.pop(command, None)
            keys = self.commands_to_keys.pop(command, [])
            for key in keys:
                if command in self.keys_to_commands[key]:
                    self.keys_to_commands[key].remove(command)

        def delete_commands(self, commands: List[Union[str, Sequence[str]]]):
            for command in commands:
                self.delete_command(command)

        def flush(self):
            self.responses.clear()
            self.commands_to_keys.clear()
            self.keys_to_commands.clear()

        def invalidate_key(self, key: KeyT):
            commands = self.keys_to_commands.pop(key, [])
            for command in commands:
                self.delete_command(command)

    @pytest.mark.parametrize("r", [{"cache": _CustomCache()}], indirect=True)
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
