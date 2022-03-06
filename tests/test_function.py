import pytest

from redis.exceptions import ResponseError

from .conftest import skip_if_server_version_lt

function = "redis.register_function('myfunc', function(keys, args) return args[1] end)"
function2 = "redis.register_function('hello', function() return 'Hello World' end)"
set_function = "redis.register_function('set', function(keys, args) \
                return redis.call('SET', keys[1], args[1]) end)"
get_function = "redis.register_function('get', function(keys, args) \
                return redis.call('GET', keys[1]) end)"


@skip_if_server_version_lt("7.0.0")
class TestFunction:
    @pytest.fixture(autouse=True)
    def reset_functions(self, r):
        r.function_flush()

    def test_function_load(self, r):
        assert r.function_load("Lua", "mylib", function)
        assert r.function_load("Lua", "mylib", function, replace=True)
        with pytest.raises(ResponseError):
            r.function_load("Lua", "mylib", function)
        with pytest.raises(ResponseError):
            r.function_load("Lua", "mylib2", function)

    def test_function_delete(self, r):
        r.function_load("Lua", "mylib", set_function)
        with pytest.raises(ResponseError):
            r.function_load("Lua", "mylib", set_function)
        assert r.fcall("set", 1, "foo", "bar") == "OK"
        assert r.function_delete("mylib")
        with pytest.raises(ResponseError):
            r.fcall("set", 1, "foo", "bar")
        assert r.function_load("Lua", "mylib", set_function)

    def test_function_flush(self, r):
        r.function_load("Lua", "mylib", function)
        assert r.fcall("myfunc", 0, "hello") == "hello"
        assert r.function_flush()
        with pytest.raises(ResponseError):
            r.fcall("myfunc", 0, "hello")
        with pytest.raises(ResponseError):
            r.function_flush("ABC")

    @pytest.mark.onlynoncluster
    def test_function_list(self, r):
        r.function_load("Lua", "mylib", function)
        res = [
            [
                "library_name",
                "mylib",
                "engine",
                "LUA",
                "description",
                None,
                "functions",
                [["name", "myfunc", "description", None]],
            ],
        ]
        assert r.function_list() == res
        assert r.function_list(library="*lib") == res
        assert r.function_list(withcode=True)[0][9] == function

    @pytest.mark.onlycluster
    def test_function_list_on_cluster(self, r):
        r.function_load("Lua", "mylib", function)
        function_list = [
            [
                "library_name",
                "mylib",
                "engine",
                "LUA",
                "description",
                None,
                "functions",
                [["name", "myfunc", "description", None]],
            ],
        ]
        primaries = r.get_primaries()
        res = {}
        for node in primaries:
            res[node.name] = function_list
        assert r.function_list() == res
        assert r.function_list(library="*lib") == res
        node = primaries[0].name
        assert r.function_list(withcode=True)[node][0][9] == function

    def test_fcall(self, r):
        r.function_load("Lua", "mylib", set_function)
        r.function_load("Lua", "mylib2", get_function)
        assert r.fcall("set", 1, "foo", "bar") == "OK"
        assert r.fcall("get", 1, "foo") == "bar"
        with pytest.raises(ResponseError):
            r.fcall("myfunc", 0, "hello")

    def test_fcall_ro(self, r):
        r.function_load("Lua", "mylib", function)
        assert r.fcall_ro("myfunc", 0, "hello") == "hello"
        r.function_load("Lua", "mylib2", set_function)
        with pytest.raises(ResponseError):
            r.fcall_ro("set", 1, "foo", "bar")

    def test_function_dump_restore(self, r):
        r.function_load("Lua", "mylib", set_function)
        payload = r.function_dump()
        assert r.fcall("set", 1, "foo", "bar") == "OK"
        r.function_delete("mylib")
        with pytest.raises(ResponseError):
            r.fcall("set", 1, "foo", "bar")
        assert r.function_restore(payload)
        assert r.fcall("set", 1, "foo", "bar") == "OK"
        r.function_load("Lua", "mylib2", get_function)
        assert r.fcall("get", 1, "foo") == "bar"
        r.function_delete("mylib")
        assert r.function_restore(payload, "FLUSH")
        with pytest.raises(ResponseError):
            r.fcall("get", 1, "foo")
