import asyncio
import socket
import types
from unittest.mock import patch

import pytest
import redis
from redis._parsers import (
    _AsyncHiredisParser,
    _AsyncRESP2Parser,
    _AsyncRESP3Parser,
    _AsyncRESPBase,
)
from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.connection import (
    Connection,
    SSLConnection,
    UnixDomainSocketConnection,
    parse_url,
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

    parser: _AsyncRESPBase = r.connection._parser
    with mock.patch.object(parser, "_stream", fake_stream):
        with pytest.raises(InvalidResponse) as cm:
            await parser.read_response()
    if isinstance(parser, _AsyncRESPBase):
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

    mock_conn = mock.AsyncMock(spec=Connection)
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
    r.connection = None  # it was a Mock
    await r.aclose()


@skip_if_server_version_lt("4.0.0")
@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def test_loading_external_modules(r):
    def inner():
        pass

    r.load_external_module("myfuncname", inner)
    assert getattr(r, "myfuncname") == inner
    assert isinstance(getattr(r, "myfuncname"), types.FunctionType)

    # and call it
    from redis.commands import RedisModuleCommands

    j = RedisModuleCommands.json
    r.load_external_module("sometestfuncname", j)

    # d = {'hello': 'world!'}
    # mod = j(r)
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


async def test_connect_retry_on_timeout_error(connect_args):
    """Test that the _connect function is retried in case of a timeout"""
    conn = Connection(
        retry_on_timeout=True, retry=Retry(NoBackoff(), 3), **connect_args
    )
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
    await conn.disconnect()


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
    await conn.disconnect()


@pytest.mark.onlynoncluster
@pytest.mark.parametrize(
    "parser_class",
    [_AsyncRESP2Parser, _AsyncRESP3Parser, _AsyncHiredisParser],
    ids=["AsyncRESP2Parser", "AsyncRESP3Parser", "AsyncHiredisParser"],
)
async def test_connection_disconect_race(parser_class, connect_args):
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
    if parser_class == _AsyncHiredisParser and not HIREDIS_AVAILABLE:
        pytest.skip("Hiredis not available")

    connect_args["parser_class"] = parser_class

    conn = Connection(**connect_args)

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

    reader = mock.Mock(spec=asyncio.StreamReader)
    writer = mock.Mock(spec=asyncio.StreamWriter)
    writer.transport.get_extra_info.side_effect = None

    # for HiredisParser
    reader.read.side_effect = read
    # for PythonParser
    reader.readline.side_effect = read
    reader.readexactly.side_effect = read

    async def open_connection(*args, **kwargs):
        return reader, writer

    async def dummy_method(*args, **kwargs):
        pass

    # get dummy stream objects for the connection
    with patch.object(asyncio, "open_connection", open_connection):
        # disable the initial version handshake
        with patch.multiple(
            conn, send_command=dummy_method, read_response=dummy_method
        ):
            await conn.connect()

    vals = await asyncio.gather(do_read(), do_close())
    assert vals == [b"Hello, World!", None]


@pytest.mark.onlynoncluster
def test_create_single_connection_client_from_url():
    client = Redis.from_url("redis://localhost:6379/0?", single_connection_client=True)
    assert client.single_connection_client is True


@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
async def test_pool_auto_close(request, from_url):
    """Verify that basic Redis instances have auto_close_connection_pool set to True"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    async def get_redis_connection():
        if from_url:
            return Redis.from_url(url)
        return Redis(**url_args)

    r1 = await get_redis_connection()
    assert r1.auto_close_connection_pool is True
    await r1.aclose()


async def test_close_is_aclose(request):
    """Verify close() calls aclose()"""
    calls = 0

    async def mock_aclose(self):
        nonlocal calls
        calls += 1

    url: str = request.config.getoption("--redis-url")
    r1 = await Redis.from_url(url)
    with patch.object(r1, "aclose", mock_aclose):
        with pytest.deprecated_call():
            await r1.close()
        assert calls == 1

    with pytest.deprecated_call():
        await r1.close()


async def test_pool_from_url_deprecation(request):
    url: str = request.config.getoption("--redis-url")

    with pytest.deprecated_call():
        return Redis.from_url(url, auto_close_connection_pool=False)


async def test_pool_auto_close_disable(request):
    """Verify that auto_close_connection_pool can be disabled (deprecated)"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    async def get_redis_connection():
        url_args["auto_close_connection_pool"] = False
        with pytest.deprecated_call():
            return Redis(**url_args)

    r1 = await get_redis_connection()
    assert r1.auto_close_connection_pool is False
    await r1.connection_pool.disconnect()
    await r1.aclose()


@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
async def test_redis_connection_pool(request, from_url):
    """Verify that basic Redis instances using `connection_pool`
    have auto_close_connection_pool set to False"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    pool = None

    async def get_redis_connection():
        nonlocal pool
        if from_url:
            pool = ConnectionPool.from_url(url)
        else:
            pool = ConnectionPool(**url_args)
        return Redis(connection_pool=pool)

    called = 0

    async def mock_disconnect(_):
        nonlocal called
        called += 1

    with patch.object(ConnectionPool, "disconnect", mock_disconnect):
        async with await get_redis_connection() as r1:
            assert r1.auto_close_connection_pool is False

    assert called == 0
    await pool.disconnect()


@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
async def test_redis_from_pool(request, from_url):
    """Verify that basic Redis instances created using `from_pool()`
    have auto_close_connection_pool set to True"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    pool = None

    async def get_redis_connection():
        nonlocal pool
        if from_url:
            pool = ConnectionPool.from_url(url)
        else:
            pool = ConnectionPool(**url_args)
        return Redis.from_pool(pool)

    called = 0

    async def mock_disconnect(_):
        nonlocal called
        called += 1

    with patch.object(ConnectionPool, "disconnect", mock_disconnect):
        async with await get_redis_connection() as r1:
            assert r1.auto_close_connection_pool is True

    assert called == 1
    await pool.disconnect()


@pytest.mark.parametrize("auto_close", (True, False))
async def test_redis_pool_auto_close_arg(request, auto_close):
    """test that redis instance where pool is provided have
    auto_close_connection_pool set to False, regardless of arg"""

    url: str = request.config.getoption("--redis-url")
    pool = ConnectionPool.from_url(url)

    async def get_redis_connection():
        with pytest.deprecated_call():
            client = Redis(connection_pool=pool, auto_close_connection_pool=auto_close)
        return client

    called = 0

    async def mock_disconnect(_):
        nonlocal called
        called += 1

    with patch.object(ConnectionPool, "disconnect", mock_disconnect):
        async with await get_redis_connection() as r1:
            assert r1.auto_close_connection_pool is False

    assert called == 0
    await pool.disconnect()


async def test_client_garbage_collection(request):
    """
    Test that a Redis client will call _close() on any
    connection that it holds at time of destruction
    """

    url: str = request.config.getoption("--redis-url")
    pool = ConnectionPool.from_url(url)

    # create a client with a connection from the pool
    client = Redis(connection_pool=pool, single_connection_client=True)
    await client.initialize()
    with mock.patch.object(client, "connection") as a:
        # we cannot, in unittests, or from asyncio, reliably trigger garbage collection
        # so we must just invoke the handler
        with pytest.warns(ResourceWarning):
            client.__del__()
            assert a._close.called

    await client.aclose()
    await pool.aclose()


async def test_connection_garbage_collection(request):
    """
    Test that a Connection object will call close() on the
    stream that it holds.
    """

    url: str = request.config.getoption("--redis-url")
    pool = ConnectionPool.from_url(url)

    # create a client with a connection from the pool
    client = Redis(connection_pool=pool, single_connection_client=True)
    await client.initialize()
    conn = client.connection

    with mock.patch.object(conn, "_reader"):
        with mock.patch.object(conn, "_writer") as a:
            # we cannot, in unittests, or from asyncio, reliably trigger
            # garbage collection so we must just invoke the handler
            with pytest.warns(ResourceWarning):
                conn.__del__()
                assert a.close.called

    await client.aclose()
    await pool.aclose()


@pytest.mark.parametrize(
    "conn, error, expected_message",
    [
        (SSLConnection(), OSError(), "Error connecting to localhost:6379."),
        (SSLConnection(), OSError(12), "Error 12 connecting to localhost:6379."),
        (
            SSLConnection(),
            OSError(12, "Some Error"),
            "Error 12 connecting to localhost:6379. Some Error.",
        ),
        (
            UnixDomainSocketConnection(path="unix:///tmp/redis.sock"),
            OSError(),
            "Error connecting to unix:///tmp/redis.sock.",
        ),
        (
            UnixDomainSocketConnection(path="unix:///tmp/redis.sock"),
            OSError(12),
            "Error 12 connecting to unix:///tmp/redis.sock.",
        ),
        (
            UnixDomainSocketConnection(path="unix:///tmp/redis.sock"),
            OSError(12, "Some Error"),
            "Error 12 connecting to unix:///tmp/redis.sock. Some Error.",
        ),
    ],
)
async def test_format_error_message(conn, error, expected_message):
    """Test that the _error_message function formats errors correctly"""
    error_message = conn._error_message(error)
    assert error_message == expected_message


async def test_network_connection_failure():
    with pytest.raises(ConnectionError) as e:
        redis = Redis(host="127.0.0.1", port=9999)
        await redis.set("a", "b")
    assert str(e.value).startswith("Error 111 connecting to 127.0.0.1:9999. Connect")


async def test_unix_socket_connection_failure():
    with pytest.raises(ConnectionError) as e:
        redis = Redis(unix_socket_path="unix:///tmp/a.sock")
        await redis.set("a", "b")
    assert (
        str(e.value)
        == "Error 2 connecting to unix:///tmp/a.sock. No such file or directory."
    )
