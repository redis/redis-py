from __future__ import unicode_literals
from redis._compat import unicode
from .conftest import wait_for_command


class TestMonitor(object):
    def test_wait_command_not_found(self, r):
        "Make sure the wait_for_command func works when command is not found"
        with r.monitor() as m:
            response = wait_for_command(r, m, 'nothing')
            assert response is None

    def test_response_values(self, r):
        with r.monitor() as m:
            r.ping()
            response = wait_for_command(r, m, 'PING')
            assert isinstance(response['time'], float)
            assert response['db'] == 9
            assert response['client_type'] in ('tcp', 'unix')
            assert isinstance(response['client_address'], unicode)
            assert isinstance(response['client_port'], unicode)
            assert response['command'] == 'PING'

    def test_command_with_quoted_key(self, r):
        with r.monitor() as m:
            r.get('foo"bar')
            response = wait_for_command(r, m, 'GET foo"bar')
            assert response['command'] == 'GET foo"bar'

    def test_command_with_binary_data(self, r):
        with r.monitor() as m:
            byte_string = b'foo\x92'
            r.get(byte_string)
            response = wait_for_command(r, m, 'GET foo\\x92')
            assert response['command'] == 'GET foo\\x92'

    def test_lua_script(self, r):
        with r.monitor() as m:
            script = 'return redis.call("GET", "foo")'
            assert r.eval(script, 0) is None
            response = wait_for_command(r, m, 'GET foo')
            assert response['command'] == 'GET foo'
            assert response['client_type'] == 'lua'
            assert response['client_address'] == 'lua'
            assert response['client_port'] == ''
