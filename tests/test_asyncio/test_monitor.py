import pytest

from tests.conftest import skip_if_redis_enterprise, skip_ifnot_redis_enterprise

from .conftest import wait_for_command


@pytest.mark.onlynoncluster
class TestMonitor:
    async def test_wait_command_not_found(self, r):
        """Make sure the wait_for_command func works when command is not found"""
        async with r.monitor() as m:
            response = await wait_for_command(r, m, "nothing")
            assert response is None

    async def test_response_values(self, r):
        db = r.connection_pool.connection_kwargs.get("db", 0)
        async with r.monitor() as m:
            await r.ping()
            response = await wait_for_command(r, m, "PING")
            assert isinstance(response["time"], float)
            assert response["db"] == db
            assert response["client_type"] in ("tcp", "unix")
            assert isinstance(response["client_address"], str)
            assert isinstance(response["client_port"], str)
            assert response["command"] == "PING"

    async def test_command_with_quoted_key(self, r):
        async with r.monitor() as m:
            await r.get('foo"bar')
            response = await wait_for_command(r, m, 'GET foo"bar')
            assert response["command"] == 'GET foo"bar'

    async def test_command_with_binary_data(self, r):
        async with r.monitor() as m:
            byte_string = b"foo\x92"
            await r.get(byte_string)
            response = await wait_for_command(r, m, "GET foo\\x92")
            assert response["command"] == "GET foo\\x92"

    async def test_command_with_escaped_data(self, r):
        async with r.monitor() as m:
            byte_string = b"foo\\x92"
            await r.get(byte_string)
            response = await wait_for_command(r, m, "GET foo\\\\x92")
            assert response["command"] == "GET foo\\\\x92"

    @skip_if_redis_enterprise()
    async def test_lua_script(self, r):
        async with r.monitor() as m:
            script = 'return redis.call("GET", "foo")'
            assert await r.eval(script, 0) is None
            response = await wait_for_command(r, m, "GET foo")
            assert response["command"] == "GET foo"
            assert response["client_type"] == "lua"
            assert response["client_address"] == "lua"
            assert response["client_port"] == ""

    @skip_ifnot_redis_enterprise()
    async def test_lua_script_in_enterprise(self, r):
        async with r.monitor() as m:
            script = 'return redis.call("GET", "foo")'
            assert await r.eval(script, 0) is None
            response = await wait_for_command(r, m, "GET foo")
            assert response is None
