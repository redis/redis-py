import pytest

from redis.exceptions import ResponseError

function = "redis.register_function('myfunc', function(keys, args) return args[1] end)"
function2 = "redis.register_function('hello', function() return 'Hello World' end)"
set_function = "redis.register_function('set', function(keys, args) \
                return redis.call('SET', keys[1], args[1]) end)"
get_function = "redis.register_function('get', function(keys, args) \
                return redis.call('GET', keys[1]) end)"


@pytest.mark.onlynoncluster
# @skip_if_server_version_lt("7.0.0") turn on after redis 7 release
class TestFunction:
    @pytest.fixture(autouse=True)
    def reset_functions(self, unstable_r):
        unstable_r.function_flush()

    def test_function_load(self, unstable_r):
        assert unstable_r.function_load("Lua", "mylib", function) == b"OK"
        assert unstable_r.function_load("Lua", "mylib", function, replace=True) == b"OK"
        with pytest.raises(ResponseError):
            unstable_r.function_load("Lua", "mylib", function)
        with pytest.raises(ResponseError):
            unstable_r.function_load("Lua", "mylib2", function)

    def test_function_delete(self, unstable_r):
        unstable_r.function_load("Lua", "mylib", set_function)
        with pytest.raises(ResponseError):
            unstable_r.function_load("Lua", "mylib", set_function)
        assert unstable_r.fcall("set", 1, "foo", "bar") == b"OK"
        assert unstable_r.function_delete("mylib") == b"OK"
        with pytest.raises(ResponseError):
            unstable_r.fcall("set", 1, "foo", "bar")
        assert unstable_r.function_load("Lua", "mylib", set_function) == b"OK"

    def test_function_flush(self, unstable_r):
        unstable_r.function_load("Lua", "mylib", function)
        assert unstable_r.fcall("myfunc", 0, "hello") == b"hello"
        assert unstable_r.function_flush() == b"OK"
        with pytest.raises(ResponseError):
            unstable_r.fcall("myfunc", 0, "hello")
        with pytest.raises(ResponseError):
            unstable_r.function_flush("ABC")

    def test_function_list(self, unstable_r):
        unstable_r.function_load("Lua", "mylib", function)
        res = [
            [
                b"library_name",
                b"mylib",
                b"engine",
                b"LUA",
                b"description",
                None,
                b"functions",
                [[b"name", b"myfunc", b"description", None]],
            ],
        ]
        assert unstable_r.function_list() == res
        assert unstable_r.function_list(library="*lib") == res
        code = function.encode("utf-8")
        assert unstable_r.function_list(withcode=True)[0][9] == code

    def test_fcall(self, unstable_r):
        unstable_r.function_load("Lua", "mylib", set_function)
        unstable_r.function_load("Lua", "mylib2", get_function)
        assert unstable_r.fcall("set", 1, "foo", "bar") == b"OK"
        # assert unstable_r.fcall("get", 1, "foo") == b"bar"
        # with pytest.raises(ResponseError):
        #     unstable_r.fcall("myfunc", 0, "hello")

    def test_fcall_ro(self, unstable_r):
        unstable_r.function_load("Lua", "mylib", function)
        assert unstable_r.fcall_ro("myfunc", 0, "hello") == b"hello"
        unstable_r.function_load("Lua", "mylib2", set_function)
        with pytest.raises(ResponseError):
            unstable_r.fcall_ro("set", 1, "foo", "bar")

    def test_function_dump_restore(self, unstable_r):
        unstable_r.function_load("Lua", "mylib", set_function)
        payload = unstable_r.function_dump()
        assert unstable_r.fcall("set", 1, "foo", "bar") == b"OK"
        unstable_r.function_delete("mylib")
        with pytest.raises(ResponseError):
            unstable_r.fcall("set", 1, "foo", "bar")
        assert unstable_r.function_restore(payload) == b"OK"
        assert unstable_r.fcall("set", 1, "foo", "bar") == b"OK"
        unstable_r.function_load("Lua", "mylib2", get_function)
        assert unstable_r.fcall("get", 1, "foo") == b"bar"
        unstable_r.function_delete("mylib")
        assert unstable_r.function_restore(payload, "FLUSH") == b"OK"
        with pytest.raises(ResponseError):
            unstable_r.fcall("get", 1, "foo")
