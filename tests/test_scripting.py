import pytest
import redis
from redis import exceptions
from redis.commands.core import Script
from tests.conftest import skip_if_redis_enterprise, skip_if_server_version_lt

multiply_script = """
local value = redis.call('GET', KEYS[1])
value = tonumber(value)
return value * ARGV[1]"""

msgpack_hello_script = """
local message = cmsgpack.unpack(ARGV[1])
local name = message['name']
return "hello " .. name
"""
msgpack_hello_script_broken = """
local message = cmsgpack.unpack(ARGV[1])
local names = message['name']
return "hello " .. name
"""


class TestScript:
    """
    We have a few tests to directly test the Script class.

    However, most of the behavioral tests are covered by `TestScripting`.
    """

    @pytest.fixture()
    def script_str(self):
        return "fake-script"

    @pytest.fixture()
    def script_bytes(self):
        return b"\xcf\x84o\xcf\x81\xce\xbdo\xcf\x82"

    def test_script_text(self, r, script_str, script_bytes):
        assert Script(r, script_str).script == "fake-script"
        assert Script(r, script_bytes).script == b"\xcf\x84o\xcf\x81\xce\xbdo\xcf\x82"

    def test_string_script_sha(self, r, script_str):
        script = Script(r, script_str)
        assert script.sha == "505e4245f0866b60552741b3cce9a0c3d3b66a87"

    def test_bytes_script_sha(self, r, script_bytes):
        script = Script(r, script_bytes)
        assert script.sha == "1329344e6bf995a35a8dc57ab1a6af8b2d54a763"

    def test_encoder(self, r, script_bytes):
        encoder = Script(r, script_bytes).get_encoder()
        assert encoder is not None
        assert encoder.encode("fake-script") == b"fake-script"


class TestScripting:
    @pytest.fixture(autouse=True)
    def reset_scripts(self, r):
        r.script_flush()

    def test_eval_multiply(self, r):
        r.set("a", 2)
        # 2 * 3 == 6
        assert r.eval(multiply_script, 1, "a", 3) == 6

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_eval_ro(self, r):
        r.set("a", "b")
        assert r.eval_ro("return redis.call('GET', KEYS[1])", 1, "a") == b"b"
        with pytest.raises(redis.ResponseError):
            r.eval_ro("return redis.call('DEL', KEYS[1])", 1, "a")

    def test_eval_msgpack(self, r):
        msgpack_message_dumped = b"\x81\xa4name\xa3Joe"
        # this is msgpack.dumps({"name": "joe"})
        assert r.eval(msgpack_hello_script, 0, msgpack_message_dumped) == b"hello Joe"

    def test_eval_same_slot(self, r):
        """
        In a clustered redis, the script keys must be in the same slot.

        This test isn't very interesting for standalone redis, but it doesn't
        hurt anything.
        """
        r.set("A{foo}", 2)
        r.set("B{foo}", 4)
        # 2 * 4 == 8

        script = """
        local value = redis.call('GET', KEYS[1])
        local value2 = redis.call('GET', KEYS[2])
        return value * value2
        """
        result = r.eval(script, 2, "A{foo}", "B{foo}")
        assert result == 8

    @pytest.mark.onlycluster
    def test_eval_crossslot(self, r):
        """
        In a clustered redis, the script keys must be in the same slot.

        This test should fail, because the two keys we send are in different
        slots. This test assumes that {foo} and {bar} will not go to the same
        server when used. In a setup with 3 primaries and 3 secondaries, this
        assumption holds.
        """
        r.set("A{foo}", 2)
        r.set("B{bar}", 4)
        # 2 * 4 == 8

        script = """
        local value = redis.call('GET', KEYS[1])
        local value2 = redis.call('GET', KEYS[2])
        return value * value2
        """
        with pytest.raises(exceptions.RedisClusterException):
            r.eval(script, 2, "A{foo}", "B{bar}")

    @skip_if_server_version_lt("6.2.0")
    def test_script_flush_620(self, r):
        r.set("a", 2)
        r.script_load(multiply_script)
        r.script_flush("ASYNC")

        r.set("a", 2)
        r.script_load(multiply_script)
        r.script_flush("SYNC")

        r.set("a", 2)
        r.script_load(multiply_script)
        r.script_flush()

        with pytest.raises(exceptions.DataError):
            r.set("a", 2)
            r.script_load(multiply_script)
            r.script_flush("NOTREAL")

    def test_script_flush(self, r):
        r.set("a", 2)
        r.script_load(multiply_script)
        r.script_flush(None)

        with pytest.raises(exceptions.DataError):
            r.set("a", 2)
            r.script_load(multiply_script)
            r.script_flush("NOTREAL")

    def test_evalsha(self, r):
        r.set("a", 2)
        sha = r.script_load(multiply_script)
        # 2 * 3 == 6
        assert r.evalsha(sha, 1, "a", 3) == 6

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_evalsha_ro(self, r):
        r.set("a", "b")
        get_sha = r.script_load("return redis.call('GET', KEYS[1])")
        del_sha = r.script_load("return redis.call('DEL', KEYS[1])")
        assert r.evalsha_ro(get_sha, 1, "a") == b"b"
        with pytest.raises(redis.ResponseError):
            r.evalsha_ro(del_sha, 1, "a")

    def test_evalsha_script_not_loaded(self, r):
        r.set("a", 2)
        sha = r.script_load(multiply_script)
        # remove the script from Redis's cache
        r.script_flush()
        with pytest.raises(exceptions.NoScriptError):
            r.evalsha(sha, 1, "a", 3)

    def test_script_loading(self, r):
        # get the sha, then clear the cache
        sha = r.script_load(multiply_script)
        r.script_flush()
        assert r.script_exists(sha) == [False]
        r.script_load(multiply_script)
        assert r.script_exists(sha) == [True]

    def test_flush_response(self, r):
        r.script_load(multiply_script)
        flush_response = r.script_flush()
        assert flush_response is True

    def test_script_object(self, r):
        r.set("a", 2)
        multiply = r.register_script(multiply_script)
        precalculated_sha = multiply.sha
        assert precalculated_sha
        assert r.script_exists(multiply.sha) == [False]
        # Test second evalsha block (after NoScriptError)
        assert multiply(keys=["a"], args=[3]) == 6
        # At this point, the script should be loaded
        assert r.script_exists(multiply.sha) == [True]
        # Test that the precalculated sha matches the one from redis
        assert multiply.sha == precalculated_sha
        # Test first evalsha block
        assert multiply(keys=["a"], args=[3]) == 6

    # Scripting is not supported in cluster pipelines
    @pytest.mark.onlynoncluster
    def test_script_object_in_pipeline(self, r):
        multiply = r.register_script(multiply_script)
        precalculated_sha = multiply.sha
        assert precalculated_sha
        pipe = r.pipeline()
        pipe.set("a", 2)
        pipe.get("a")
        multiply(keys=["a"], args=[3], client=pipe)
        assert r.script_exists(multiply.sha) == [False]
        # [SET worked, GET 'a', result of multiple script]
        assert pipe.execute() == [True, b"2", 6]
        # The script should have been loaded by pipe.execute()
        assert r.script_exists(multiply.sha) == [True]
        # The precalculated sha should have been the correct one
        assert multiply.sha == precalculated_sha

        # purge the script from redis's cache and re-run the pipeline
        # the multiply script should be reloaded by pipe.execute()
        r.script_flush()
        pipe = r.pipeline()
        pipe.set("a", 2)
        pipe.get("a")
        multiply(keys=["a"], args=[3], client=pipe)
        assert r.script_exists(multiply.sha) == [False]
        # [SET worked, GET 'a', result of multiple script]
        assert pipe.execute() == [True, b"2", 6]
        assert r.script_exists(multiply.sha) == [True]

    # Scripting is not supported in cluster pipelines
    @pytest.mark.onlynoncluster
    def test_eval_msgpack_pipeline_error_in_lua(self, r):
        msgpack_hello = r.register_script(msgpack_hello_script)
        assert msgpack_hello.sha

        pipe = r.pipeline()

        # avoiding a dependency to msgpack, this is the output of
        # msgpack.dumps({"name": "joe"})
        msgpack_message_1 = b"\x81\xa4name\xa3Joe"

        msgpack_hello(args=[msgpack_message_1], client=pipe)

        assert r.script_exists(msgpack_hello.sha) == [False]
        assert pipe.execute()[0] == b"hello Joe"
        assert r.script_exists(msgpack_hello.sha) == [True]

        msgpack_hello_broken = r.register_script(msgpack_hello_script_broken)

        msgpack_hello_broken(args=[msgpack_message_1], client=pipe)
        with pytest.raises(exceptions.ResponseError) as excinfo:
            pipe.execute()
        assert excinfo.type == exceptions.ResponseError
