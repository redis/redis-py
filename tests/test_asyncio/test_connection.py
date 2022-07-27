import asyncio
import socket
import types
from unittest.mock import patch

import pytest

from redis.asyncio.connection import (
    Connection,
    PythonParser,
    UnixDomainSocketConnection,
)
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import ConnectionError, InvalidResponse, TimeoutError
from redis.utils import HIREDIS_AVAILABLE
from tests.conftest import skip_if_server_version_lt

from .compat import mock


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
    if getattr(r, "connection", None) is not None:
        # Concurrent commands are only supported on pooled or cluster connections
        # since there is no synchronization on a single connection.
        pytest.skip("pool only")
    assert await r.ping() is True
    assert all(await asyncio.gather(*(r.ping() for _ in range(10))))


async def test_connect_retry_on_timeout_error():
    """Test that the _connect function is retried in case of a timeout"""
    conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 3))
    origin_connect = conn._connect
    conn._connect = mock.AsyncMock()

    async def mock_connect():
        # connect only on the last retry
        if conn._connect.call_count <= 2:
            raise socket.timeout
        else:
            return await origin_connect()

    conn._connect.side_effect = mock_connect
    await conn.connect()
    assert conn._connect.call_count == 3


async def test_connect_without_retry_on_os_error():
    """Test that the _connect function is not being retried in case of a OSError"""
    with patch.object(Connection, "_connect") as _connect:
        _connect.side_effect = OSError("")
        conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 2))
        with pytest.raises(ConnectionError):
            await conn.connect()
        assert _connect.call_count == 1


async def test_connect_timeout_error_without_retry():
    """Test that the _connect function is not being retried if retry_on_timeout is
    set to False"""
    conn = Connection(retry_on_timeout=False)
    conn._connect = mock.AsyncMock()
    conn._connect.side_effect = socket.timeout

    with pytest.raises(TimeoutError) as e:
        await conn.connect()
    assert conn._connect.call_count == 1
    assert str(e.value) == "Timeout connecting to server"
