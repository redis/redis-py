from __future__ import unicode_literals
import pytest

from redis import exceptions


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


class TestScripting(object):
    @pytest.fixture(autouse=True)
    def reset_scripts(self, r):
        r.script_flush()

    def test_eval(self, r):
        r.set('a', 2)
        # 2 * 3 == 6
        assert r.eval(multiply_script, 1, 'a', 3) == 6

    def test_evalsha(self, r):
        r.set('a', 2)
        sha = r.script_load(multiply_script)
        # 2 * 3 == 6
        assert r.evalsha(sha, 1, 'a', 3) == 6

    def test_evalsha_script_not_loaded(self, r):
        r.set('a', 2)
        sha = r.script_load(multiply_script)
        # remove the script from Redis's cache
        r.script_flush()
        with pytest.raises(exceptions.NoScriptError):
            r.evalsha(sha, 1, 'a', 3)

    def test_script_loading(self, r):
        # get the sha, then clear the cache
        sha = r.script_load(multiply_script)
        r.script_flush()
        assert r.script_exists(sha) == [False]
        r.script_load(multiply_script)
        assert r.script_exists(sha) == [True]

    def test_script_object(self, r):
        r.set('a', 2)
        multiply = r.register_script(multiply_script)
        precalculated_sha = multiply.sha
        assert precalculated_sha
        assert r.script_exists(multiply.sha) == [False]
        # Test second evalsha block (after NoScriptError)
        assert multiply(keys=['a'], args=[3]) == 6
        # At this point, the script should be loaded
        assert r.script_exists(multiply.sha) == [True]
        # Test that the precalculated sha matches the one from redis
        assert multiply.sha == precalculated_sha
        # Test first evalsha block
        assert multiply(keys=['a'], args=[3]) == 6

    def test_script_object_in_pipeline(self, r):
        multiply = r.register_script(multiply_script)
        precalculated_sha = multiply.sha
        assert precalculated_sha
        pipe = r.pipeline()
        pipe.set('a', 2)
        pipe.get('a')
        multiply(keys=['a'], args=[3], client=pipe)
        assert r.script_exists(multiply.sha) == [False]
        # [SET worked, GET 'a', result of multiple script]
        assert pipe.execute() == [True, b'2', 6]
        # The script should have been loaded by pipe.execute()
        assert r.script_exists(multiply.sha) == [True]
        # The precalculated sha should have been the correct one
        assert multiply.sha == precalculated_sha

        # purge the script from redis's cache and re-run the pipeline
        # the multiply script should be reloaded by pipe.execute()
        r.script_flush()
        pipe = r.pipeline()
        pipe.set('a', 2)
        pipe.get('a')
        multiply(keys=['a'], args=[3], client=pipe)
        assert r.script_exists(multiply.sha) == [False]
        # [SET worked, GET 'a', result of multiple script]
        assert pipe.execute() == [True, b'2', 6]
        assert r.script_exists(multiply.sha) == [True]

    def test_eval_msgpack_pipeline_error_in_lua(self, r):
        msgpack_hello = r.register_script(msgpack_hello_script)
        assert msgpack_hello.sha

        pipe = r.pipeline()

        # avoiding a dependency to msgpack, this is the output of
        # msgpack.dumps({"name": "joe"})
        msgpack_message_1 = b'\x81\xa4name\xa3Joe'

        msgpack_hello(args=[msgpack_message_1], client=pipe)

        assert r.script_exists(msgpack_hello.sha) == [False]
        assert pipe.execute()[0] == b'hello Joe'
        assert r.script_exists(msgpack_hello.sha) == [True]

        msgpack_hello_broken = r.register_script(msgpack_hello_script_broken)

        msgpack_hello_broken(args=[msgpack_message_1], client=pipe)
        with pytest.raises(exceptions.ResponseError) as excinfo:
            pipe.execute()
        assert excinfo.type == exceptions.ResponseError
