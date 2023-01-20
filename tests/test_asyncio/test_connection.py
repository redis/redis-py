import asyncio
import socket
import types
from unittest.mock import AsyncMock, Mock, patch

import pytest

import redis
from redis.asyncio.connection import (
    BaseParser,
    Connection,
    HiredisParser,
    PythonParser,
    UnixDomainSocketConnection,
)
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import ConnectionError, InvalidResponse, TimeoutError
from redis.utils import HIREDIS_AVAILABLE
from tests.conftest import skip_if_server_version_lt

from .compat import mock
from .mocks import MockStream


@pytest.mark.onlynoncluster
async def test_invalid_response(create_redis):
    r = await create_redis(single_connection_client=True)

    raw = b"x"
    fake_stream = MockStream(raw + b"\r\n")

    parser: BaseParser = r.connection._parser
    with mock.patch.object(parser, "_stream", fake_stream):
        with pytest.raises(InvalidResponse) as cm:
            await parser.read_response()
    if isinstance(parser, PythonParser):
        assert str(cm.value) == f"Protocol Error: {raw!r}"
    else:
        assert (
            str(cm.value) == f'Protocol error, got "{raw.decode()}" as reply type byte'
        )
    await r.connection.disconnect()


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


@pytest.mark.onlynoncluster
async def test_connection_parse_response_resume(r: redis.Redis):
    """
    This test verifies that the Connection parser,
    be that PythonParser or HiredisParser,
    can be interrupted at IO time and then resume parsing.
    """
    conn = Connection(**r.connection_pool.connection_kwargs)
    await conn.connect()
    message = (
        b"*3\r\n$7\r\nmessage\r\n$8\r\nchannel1\r\n"
        b"$25\r\nhi\r\nthere\r\n+how\r\nare\r\nyou\r\n"
    )

    conn._parser._stream = MockStream(message, interrupt_every=2)
    for i in range(100):
        try:
            response = await conn.read_response()
            break
        except MockStream.TestError:
            pass

    else:
        pytest.fail("didn't receive a response")
    assert response
    assert i > 0


@pytest.mark.xfail
@pytest.mark.onlynoncluster
async def test_connection_hiredis_disconect_race():
    """
    This test reproduces the case in issue #2349
    where a connection is closed while the parser is reading to feed the internal buffer.
    The stremam read() will succeed, but when it returns, another task has already called
    `disconnect()` and is waiting for close to finish.  When it attempts to feed the
    buffer, it will fail, since the buffer is no longer there.
    """
    if not HIREDIS_AVAILABLE:
        pytest.skip("Hiredis not available)")
    parser_class = HiredisParser

    args = {}
    args["parser_class"] = parser_class
    conn = Connection(**args)

    cond = asyncio.Condition()
    # 0 == initial
    # 1 == reader is reading
    # 2 == closer has closed and is waiting for close to finish
    state = 0

    # mock read function, which wait for a close to happen before returning
    async def read(_):
        nonlocal state
        async with cond:
            state = 1  # we are reading
            cond.notify()
            # wait until the closing task has done
            await cond.wait_for(lambda: state == 2)
        return b" "

    # function closes the connection while reader is still blocked reading
    async def do_close():
        nonlocal state
        async with cond:
            await cond.wait_for(lambda: state == 1)
            state = 2
            cond.notify()
        await conn.disconnect()

    async def do_read():
        await conn.read_response()

    reader = AsyncMock()
    writer = AsyncMock()
    writer.transport = Mock()
    writer.transport.get_extra_info.side_effect = None

    reader.read.side_effect = read

    async def open_connection(*args, **kwargs):
        return reader, writer

    with patch.object(asyncio, "open_connection", open_connection):
        await conn.connect()

    await asyncio.gather(do_read(), do_close())
