import asyncio
import socket
import types
from unittest.mock import patch

import pytest

import redis
from redis.asyncio import Redis
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


@pytest.mark.onlynoncluster
async def test_single_connection():
    """Test that concurrent requests on a single client are synchronised."""
    r = Redis(single_connection_client=True)

    init_call_count = 0
    command_call_count = 0
    in_use = False

    class Retry_:
        async def call_with_retry(self, _, __):
            # If we remove the single-client lock, this error gets raised as two
            # coroutines will be vying for the `in_use` flag due to the two
            # asymmetric sleep calls
            nonlocal command_call_count
            nonlocal in_use
            if in_use is True:
                raise ValueError("Commands should be executed one at a time.")
            in_use = True
            await asyncio.sleep(0.01)
            command_call_count += 1
            await asyncio.sleep(0.03)
            in_use = False
            return "foo"

    mock_conn = mock.MagicMock()
    mock_conn.retry = Retry_()

    async def get_conn(_):
        # Validate only one client is created in single-client mode when
        # concurrent requests are made
        nonlocal init_call_count
        await asyncio.sleep(0.01)
        init_call_count += 1
        return mock_conn

    with mock.patch.object(r.connection_pool, "get_connection", get_conn):
        with mock.patch.object(r.connection_pool, "release"):
            await asyncio.gather(r.set("a", "b"), r.set("c", "d"))

    assert init_call_count == 1
    assert command_call_count == 2


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
            response = await conn.read_response(disconnect_on_error=False)
            break
        except MockStream.TestError:
            pass

    else:
        pytest.fail("didn't receive a response")
    assert response
    assert i > 0


@pytest.mark.onlynoncluster
@pytest.mark.parametrize(
    "parser_class", [PythonParser, HiredisParser], ids=["PythonParser", "HiredisParser"]
)
async def test_connection_disconect_race(parser_class):
    """
    This test reproduces the case in issue #2349
    where a connection is closed while the parser is reading to feed the
    internal buffer.The stream `read()` will succeed, but when it returns,
    another task has already called `disconnect()` and is waiting for
    close to finish.  When we attempts to feed the buffer, we will fail
    since the buffer is no longer there.

    This test verifies that a read in progress can finish even
    if the `disconnect()` method is called.
    """
    if parser_class == HiredisParser and not HIREDIS_AVAILABLE:
        pytest.skip("Hiredis not available")

    args = {}
    args["parser_class"] = parser_class

    conn = Connection(**args)

    cond = asyncio.Condition()
    # 0 == initial
    # 1 == reader is reading
    # 2 == closer has closed and is waiting for close to finish
    state = 0

    # Mock read function, which wait for a close to happen before returning
    # Can either be invoked as two `read()` calls (HiredisParser)
    # or as a `readline()` followed by `readexact()` (PythonParser)
    chunks = [b"$13\r\n", b"Hello, World!\r\n"]

    async def read(_=None):
        nonlocal state
        async with cond:
            if state == 0:
                state = 1  # we are reading
                cond.notify()
                # wait until the closing task has done
                await cond.wait_for(lambda: state == 2)
        return chunks.pop(0)

    # function closes the connection while reader is still blocked reading
    async def do_close():
        nonlocal state
        async with cond:
            await cond.wait_for(lambda: state == 1)
            state = 2
            cond.notify()
        await conn.disconnect()

    async def do_read():
        return await conn.read_response()

    reader = mock.AsyncMock()
    writer = mock.AsyncMock()
    writer.transport = mock.Mock()
    writer.transport.get_extra_info.side_effect = None

    # for HiredisParser
    reader.read.side_effect = read
    # for PythonParser
    reader.readline.side_effect = read
    reader.readexactly.side_effect = read

    async def open_connection(*args, **kwargs):
        return reader, writer

    with patch.object(asyncio, "open_connection", open_connection):
        await conn.connect()

    vals = await asyncio.gather(do_read(), do_close())
    assert vals == [b"Hello, World!", None]


@pytest.mark.onlynoncluster
def test_create_single_connection_client_from_url():
    client = Redis.from_url("redis://localhost:6379/0?", single_connection_client=True)
    assert client.single_connection_client is True
