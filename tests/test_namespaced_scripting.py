from __future__ import with_statement
import pytest

from redis import exceptions
from redis._compat import b


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
    def reset_scripts(self, nr):
        nr.script_flush()

    def test_eval(self, nr):
        nr.set('a', 2)
        # 2 * 3 == 6
        assert nr.eval(multiply_script, 1, 'a', 3) == 6

    def test_evalsha(self, nr):
        nr.set('a', 2)
        sha = nr.script_load(multiply_script)
        # 2 * 3 == 6
        assert nr.evalsha(sha, 1, 'a', 3) == 6

    def test_evalsha_script_not_loaded(self, nr):
        nr.set('a', 2)
        sha = nr.script_load(multiply_script)
        # remove the script from Redis's cache
        nr.script_flush()
        with pytest.raises(exceptions.NoScriptError):
            nr.evalsha(sha, 1, 'a', 3)

    def test_script_loading(self, nr):
        # get the sha, then clear the cache
        sha = nr.script_load(multiply_script)
        nr.script_flush()
        assert nr.script_exists(sha) == [False]
        nr.script_load(multiply_script)
        assert nr.script_exists(sha) == [True]

    def test_script_object(self, nr):
        nr.set('a', 2)
        multiply = nr.register_script(multiply_script)
        assert not multiply.sha
        # test evalsha fail -> script load + retry
        assert multiply(keys=['a'], args=[3]) == 6
        assert multiply.sha
        assert nr.script_exists(multiply.sha) == [True]
        # test first evalsha
        assert multiply(keys=['a'], args=[3]) == 6

    def test_script_object_in_pipeline(self, nr):
        multiply = nr.register_script(multiply_script)
        assert not multiply.sha
        pipe = nr.pipeline()
        pipe.set('a', 2)
        pipe.get('a')
        multiply(keys=['a'], args=[3], client=pipe)
        # even though the pipeline wasn't executed yet, we made sure the
        # script was loaded and got a valid sha
        assert multiply.sha
        assert nr.script_exists(multiply.sha) == [True]
        # [SET worked, GET 'a', result of multiple script]
        assert pipe.execute() == [True, b('2'), 6]

        # purge the script from redis's cache and re-run the pipeline
        # the multiply script object knows it's sha, so it shouldn't get
        # reloaded until pipe.execute()
        nr.script_flush()
        pipe = nr.pipeline()
        pipe.set('a', 2)
        pipe.get('a')
        assert multiply.sha
        multiply(keys=['a'], args=[3], client=pipe)
        assert nr.script_exists(multiply.sha) == [False]
        # [SET worked, GET 'a', result of multiple script]
        assert pipe.execute() == [True, b('2'), 6]

    def test_eval_msgpack_pipeline_error_in_lua(self, nr):
        msgpack_hello = nr.register_script(msgpack_hello_script)
        assert not msgpack_hello.sha

        pipe = nr.pipeline()

        # avoiding a dependency to msgpack, this is the output of
        # msgpack.dumps({"name": "joe"})
        msgpack_message_1 = b'\x81\xa4name\xa3Joe'

        msgpack_hello(args=[msgpack_message_1], client=pipe)

        assert nr.script_exists(msgpack_hello.sha) == [True]
        assert pipe.execute()[0] == b'hello Joe'

        msgpack_hello_broken = nr.register_script(msgpack_hello_script_broken)

        msgpack_hello_broken(args=[msgpack_message_1], client=pipe)
        with pytest.raises(exceptions.ResponseError) as excinfo:
            pipe.execute()
        assert excinfo.type == exceptions.ResponseError
