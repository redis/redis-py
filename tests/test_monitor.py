import pytest

from .conftest import (
    skip_if_redis_enterprise,
    skip_ifnot_redis_enterprise,
    wait_for_command,
)


@pytest.mark.onlynoncluster
class TestMonitor:
    def test_wait_command_not_found(self, r):
        "Make sure the wait_for_command func works when command is not found"
        with r.monitor() as m:
            response = wait_for_command(r, m, "nothing")
            assert response is None

    def test_response_values(self, r):
        db = r.connection_pool.connection_kwargs.get("db", 0)
        with r.monitor() as m:
            r.ping()
            response = wait_for_command(r, m, "PING")
            assert isinstance(response["time"], float)
            assert response["db"] == db
            assert response["client_type"] in ("tcp", "unix")
            assert isinstance(response["client_address"], str)
            assert isinstance(response["client_port"], str)
            assert response["command"] == "PING"

    def test_command_with_quoted_key(self, r):
        with r.monitor() as m:
            r.get('foo"bar')
            response = wait_for_command(r, m, 'GET foo"bar')
            assert response["command"] == 'GET foo"bar'

    def test_command_with_binary_data(self, r):
        with r.monitor() as m:
            byte_string = b"foo\x92"
            r.get(byte_string)
            response = wait_for_command(r, m, "GET foo\\x92")
            assert response["command"] == "GET foo\\x92"

    def test_command_with_escaped_data(self, r):
        with r.monitor() as m:
            byte_string = b"foo\\x92"
            r.get(byte_string)
            response = wait_for_command(r, m, "GET foo\\\\x92")
            assert response["command"] == "GET foo\\\\x92"

    @skip_if_redis_enterprise()
    def test_lua_script(self, r):
        with r.monitor() as m:
            script = 'return redis.call("GET", "foo")'
            assert r.eval(script, 0) is None
            response = wait_for_command(r, m, "GET foo")
            assert response["command"] == "GET foo"
            assert response["client_type"] == "lua"
            assert response["client_address"] == "lua"
            assert response["client_port"] == ""

    @skip_ifnot_redis_enterprise()
    def test_lua_script_in_enterprise(self, r):
        with r.monitor() as m:
            script = 'return redis.call("GET", "foo")'
            assert r.eval(script, 0) is None
            response = wait_for_command(r, m, "GET foo")
            assert response is None
