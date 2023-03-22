import asyncio
import types

import pytest

from redis.asyncio.connection import PythonParser, UnixDomainSocketConnection
from redis.exceptions import InvalidResponse
from redis.utils import HIREDIS_AVAILABLE
from tests.conftest import skip_if_server_version_lt

from .compat import mock

pytestmark = pytest.mark.asyncio


@pytest.mark.onlynoncluster
@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
async def test_invalid_response(create_redis):
    r = await create_redis(single_connection_client=True)

    raw = b"x"
    readline_mock = mock.AsyncMock(return_value=raw)

    parser: "PythonParser" = r.connection._parser
    with mock.patch.object(parser._buffer, "readline", readline_mock):
        with pytest.raises(InvalidResponse) as cm:
            await parser.read_response()
    assert str(cm.value) == f"Protocol Error: {raw!r}"


@pytest.mark.onlynoncluster
async def test_asynckills():
    from redis.asyncio.client import Redis

    for b in [True, False]:
        r = Redis(single_connection_client=b)

        await r.set("foo", "foo")
        await r.set("bar", "bar")

        t = asyncio.create_task(r.get("foo"))
        await asyncio.sleep(1)
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pytest.fail("connection left open with unread response")

        assert await r.get("bar") == b"bar"
        assert await r.ping()
        assert await r.get("foo") == b"foo"


@skip_if_server_version_lt("4.0.0")
@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def test_loading_external_modules(modclient):
    def inner():
        pass

    modclient.load_external_module("myfuncname", inner)
    assert getattr(modclient, "myfuncname") == inner
    assert isinstance(getattr(modclient, "myfuncname"), types.FunctionType)

    # and call it
    from redis.commands import RedisModuleCommands

    j = RedisModuleCommands.json
    modclient.load_external_module("sometestfuncname", j)

    # d = {'hello': 'world!'}
    # mod = j(modclient)
    # mod.set("fookey", ".", d)
    # assert mod.get('fookey') == d


async def test_socket_param_regression(r):
    """A regression test for issue #1060"""
    conn = UnixDomainSocketConnection()
    _ = await conn.disconnect() is True


async def test_can_run_concurrent_commands(r):
    assert await r.ping() is True
    assert all(await asyncio.gather(*(r.ping() for _ in range(10))))
