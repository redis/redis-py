import copy
import os
import platform
import select
import selectors
import socket
import ssl
import threading
import time
import types
from errno import ECONNREFUSED, EWOULDBLOCK
from typing import Any
from unittest import mock
from unittest.mock import call, patch, MagicMock, Mock

import pytest
import redis
from redis import ConnectionPool, Redis
from redis._parsers import _HiredisParser, _RESP2Parser, _RESP3Parser
from redis._parsers.hiredis import NOT_ENOUGH_DATA, _socket_can_read, _socket_is_closed
from redis._parsers.socket import SocketBuffer
from redis.backoff import NoBackoff
from redis.cache import (
    CacheConfig,
    CacheEntry,
    CacheEntryStatus,
    CacheInterface,
    CacheKey,
    CacheProxy,
    DefaultCache,
    LRUPolicy,
)
from redis.connection import (
    CacheProxyConnection,
    Connection,
    SSLConnection,
    UnixDomainSocketConnection,
    parse_url,
    BlockingConnectionPool,
)
from redis.credentials import UsernamePasswordCredentialProvider
from redis.event import (
    EventDispatcher,
)
from redis.exceptions import ConnectionError, InvalidResponse, RedisError, TimeoutError
from redis.observability.attributes import (
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_CONNECTION_STATE,
    ConnectionState,
    get_pool_name,
)
from redis.retry import Retry
from redis.utils import HIREDIS_AVAILABLE, SENTINEL

from .conftest import skip_if_redis_enterprise, skip_if_server_version_lt
from .mocks import MockSocket


class DummyHiredisReader:
    def __init__(self, response=NOT_ENOUGH_DATA, decoded_response=None, has_data=False):
        self.responses = [response]
        self.decoded_response = decoded_response
        self.has_data_value = has_data

    def has_data(self):
        return self.has_data_value

    def gets(self, *args):
        if self.responses:
            response = self.responses.pop(0)
            if args == (False,) or self.decoded_response is None:
                return response
            return self.decoded_response
        return NOT_ENOUGH_DATA


class DummyPushNotification(list):
    pass


def make_hiredis_parser(
    response=NOT_ENOUGH_DATA, decoded_response=None, has_data=False
):
    parser = _HiredisParser.__new__(_HiredisParser)
    parser._reader = DummyHiredisReader(response, decoded_response, has_data)
    parser._hiredis_PushNotificationType = None
    parser._sock = mock.Mock()
    parser._buffer = bytearray(65536)
    parser._socket_timeout = None
    return parser


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlynoncluster
def test_invalid_response(r):
    raw = b"x"
    parser = r.connection._parser
    with mock.patch.object(parser._buffer, "readline", return_value=raw):
        with pytest.raises(InvalidResponse, match=f"Protocol Error: {raw!r}"):
            parser.read_response()


def test_hiredis_can_read_detects_reader_data():
    parser = make_hiredis_parser(response=b"OK", has_data=True)

    assert parser.can_read(timeout=0) is True
    assert parser.read_response() == b"OK"


def test_hiredis_can_read_returns_true_for_readable_open_socket():
    # socket readable, reader empty, and not closed -> pending data/push, so
    # can_read() reports readable without consuming anything.
    parser = make_hiredis_parser(has_data=False)

    with (
        patch("redis._parsers.hiredis._socket_can_read", return_value=True) as ready,
        patch("redis._parsers.hiredis._socket_is_closed", return_value=False) as closed,
    ):
        assert parser.can_read(timeout=0) is True

    ready.assert_called_once_with(parser._sock, 0)
    closed.assert_called_once_with(parser._sock)


def test_hiredis_can_read_raises_on_peer_closed_socket():
    # regression for #4128: a peer-closed socket reads as ready but the reader
    # has no buffered data. can_read() must raise ConnectionError so the pool
    # recycles it, matching the pure-Python and async parsers.
    parser = make_hiredis_parser(has_data=False)

    with (
        patch("redis._parsers.hiredis._socket_can_read", return_value=True),
        patch("redis._parsers.hiredis._socket_is_closed", return_value=True),
    ):
        with pytest.raises(redis.ConnectionError):
            parser.can_read(timeout=0)


@pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="hiredis is not installed")
@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_hiredis_can_read_consumes_pending_push_before_reporting_closed():
    # a peer that sends push data and then closes reports the closed poll flags
    # while the data is still buffered. can_read() must keep reporting readable
    # so pending messages (e.g. cache invalidations) get processed, and only
    # raise once the buffer is drained, matching the pure-Python parser which
    # consumes buffered data before raising.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname())
    server, _ = listener.accept()

    parser = _HiredisParser(socket_read_size=65536)
    connection = mock.Mock()
    connection._sock = client
    connection.socket_timeout = None
    connection.encoder.encoding_errors = "strict"
    connection.encoder.decode_responses = False
    parser.on_connect(connection)
    try:
        server.sendall(b">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$5\r\nhello\r\n")
        server.close()  # graceful FIN with the push message still buffered
        time.sleep(0.1)

        assert parser.can_read(timeout=0) is True
        assert parser.read_response(push_request=True) == [b"message", b"ch", b"hello"]
        with pytest.raises(redis.ConnectionError):
            parser.can_read(timeout=0)
    finally:
        parser.on_disconnect()
        client.close()
        listener.close()


def test_socket_is_closed_reports_not_closed_with_pending_ssl_data():
    # SSL sockets buffer decrypted bytes above the OS socket layer; those must
    # be processed before the connection can be treated as closed, like
    # kernel-level pending data.
    sock = Mock(spec=["pending", "fileno"])
    sock.pending.return_value = 10

    assert _socket_is_closed(sock) is False


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
@pytest.mark.parametrize(
    "timeout,expected_poll_timeout",
    [(0, 0), (0.001, 1.0), (None, None)],
)
def test_hiredis_socket_can_read_uses_poll(timeout, expected_poll_timeout):
    sock = Mock(spec=["fileno"])
    poller = Mock()
    poller.poll.return_value = [(7, select.POLLIN)]

    with patch("redis._parsers.hiredis.select.poll", return_value=poller):
        assert _socket_can_read(sock, timeout=timeout) is True

    poller.register.assert_called_once_with(sock, select.POLLIN)
    poller.poll.assert_called_once_with(expected_poll_timeout)


@pytest.mark.fixed_client
def test_hiredis_socket_can_read_falls_back_to_default_selector():
    sock = Mock(spec=["fileno"])
    selector = MagicMock()
    selector.select.return_value = [(sock, selectors.EVENT_READ)]
    selector.__enter__.return_value = selector

    with (
        patch("redis._parsers.hiredis._HAS_POLL", False),
        patch(
            "redis._parsers.hiredis.selectors.DefaultSelector", return_value=selector
        ),
    ):
        assert _socket_can_read(sock, timeout=0.001) is True

    selector.register.assert_called_once_with(sock, selectors.EVENT_READ)
    selector.select.assert_called_once_with(0.001)
    # The selector must be used as a context manager so its fd is released; if
    # the `with` block is dropped from the implementation, this catches it.
    selector.__exit__.assert_called_once()


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(select, "poll"),
    reason="No select.poll; default selector falls back to select.select",
)
def test_hiredis_socket_can_read_handles_high_file_descriptor():
    fcntl = pytest.importorskip("fcntl")

    read_fd, write_fd = os.pipe()
    high_read_fd = None
    try:
        try:
            high_read_fd = fcntl.fcntl(read_fd, fcntl.F_DUPFD, 1024)
        except OSError as exc:
            pytest.skip(f"Could not allocate high file descriptor: {exc}")

        assert _socket_can_read(high_read_fd, timeout=0) is False
    finally:
        os.close(read_fd)
        os.close(write_fd)
        if high_read_fd is not None and high_read_fd != read_fd:
            os.close(high_read_fd)


@pytest.mark.fixed_client
@pytest.mark.forked
@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_hiredis_socket_can_read_under_fd_exhaustion():
    # Readiness checks run on every connection acquisition, and fd pressure is
    # what pushes sockets onto high fds in the first place, so they must not
    # allocate file descriptors themselves. A per-check epoll/kqueue selector
    # raises OSError (EMFILE) here; the poll() implementation passes.
    #
    # RLIMIT_NOFILE is process-wide, so @pytest.mark.forked runs this in its own
    # process to avoid starving other threads/tests of file descriptors.
    resource = pytest.importorskip("resource")

    readable, writable = socket.socketpair()
    empty_sock, peer = socket.socketpair()
    writable.sendall(b"x")

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    held_fds = []
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(soft, 256), hard))
        try:
            while True:
                held_fds.append(os.dup(0))
        except OSError:
            pass  # fd table is now full

        assert _socket_can_read(readable, timeout=0) is True
        assert _socket_can_read(empty_sock, timeout=0) is False
    finally:
        for fd in held_fds:
            os.close(fd)
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
        readable.close()
        writable.close()
        empty_sock.close()
        peer.close()


@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_socket_is_closed_detects_peer_close():
    # a peer-closed socket reads as ready (it yields EOF), so readiness alone
    # cannot tell it apart from a socket holding pending data; _socket_is_closed()
    # distinguishes them via POLLHUP without consuming data.
    alive, peer = socket.socketpair()
    closed, closing_peer = socket.socketpair()
    try:
        peer.sendall(b"pending push data")
        closing_peer.close()

        assert _socket_is_closed(alive) is False
        assert _socket_is_closed(closed) is True
    finally:
        alive.close()
        peer.close()
        closed.close()


@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_socket_is_closed_detects_tcp_peer_half_close():
    # on Linux a graceful peer close (FIN) reports POLLIN|POLLRDHUP and never
    # POLLHUP, so a POLLHUP-only flags check misses it. a real TCP socket pair
    # is required: the kernel (not a mock) produces the event, and AF_UNIX
    # socketpairs set POLLHUP on close and would hide the gap.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname())
    server, _ = listener.accept()
    try:
        assert _socket_is_closed(client) is False
        server.close()  # graceful FIN
        time.sleep(0.1)
        assert _socket_is_closed(client) is True
    finally:
        client.close()
        listener.close()


@pytest.mark.skipif(
    not hasattr(select, "poll"), reason="select.poll not available on this platform"
)
def test_socket_is_closed_defers_close_until_pending_data_is_read():
    # a peer that sends data and then closes reports the closed poll flags
    # while unread bytes remain (POLLIN|POLLHUP on macOS, POLLIN|POLLRDHUP on
    # Linux). that data may carry cache invalidations that must be processed
    # before the connection is dropped, so _socket_is_closed() must report
    # not-closed until the buffer is drained, without consuming anything.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname())
    server, _ = listener.accept()
    try:
        server.sendall(b"pending invalidation")
        server.close()  # graceful FIN with data still buffered
        time.sleep(0.1)

        assert _socket_is_closed(client) is False
        # the MSG_PEEK confirmation must not consume the pending data
        assert client.recv(65536) == b"pending invalidation"
        assert _socket_is_closed(client) is True
    finally:
        client.close()
        listener.close()


def test_socket_is_closed_without_poll_reports_not_closed(monkeypatch):
    # without select.poll (e.g. Windows) closed and has-data states are
    # indistinguishable here, so we conservatively report not-closed.
    monkeypatch.setattr("redis._parsers.hiredis._HAS_POLL", False)
    closed, closing_peer = socket.socketpair()
    try:
        closing_peer.close()
        assert _socket_is_closed(closed) is False
    finally:
        closed.close()


def test_hiredis_can_read_does_not_decide_disable_decoding():
    raw = b"\xe2\x98\x83"
    parser = make_hiredis_parser(
        response=raw,
        decoded_response=raw.decode(),
        has_data=True,
    )

    assert parser.can_read(timeout=0) is True
    assert parser.read_response(disable_decoding=True) == raw


def test_hiredis_can_read_leaves_decoding_to_read_response():
    raw = b"\xe2\x98\x83"
    parser = make_hiredis_parser(
        response=raw,
        decoded_response=raw.decode(),
        has_data=True,
    )

    assert parser.can_read(timeout=0) is True
    assert parser.read_response() == raw.decode()


def test_hiredis_read_response_returns_initial_push_notification():
    push_response = DummyPushNotification([b"message", b"channel", b"data"])
    handled_response = object()
    parser = make_hiredis_parser()
    parser._hiredis_PushNotificationType = DummyPushNotification
    parser._reader.responses = [push_response]
    parser.pubsub_push_handler_func = Mock(return_value=handled_response)

    assert parser.read_response(push_request=True) is handled_response
    parser.pubsub_push_handler_func.assert_called_once_with(push_response)


def test_hiredis_read_response_skips_initial_push_notification():
    push_response = DummyPushNotification([b"message", b"channel", b"data"])
    parser = make_hiredis_parser()
    parser._hiredis_PushNotificationType = DummyPushNotification
    parser._reader.responses = [push_response, b"OK"]
    parser.pubsub_push_handler_func = Mock(return_value=push_response)

    assert parser.read_response() == b"OK"
    parser.pubsub_push_handler_func.assert_called_once_with(push_response)


def test_hiredis_read_response_preserves_timeout_after_initial_push_notification():
    push_response = DummyPushNotification([b"message", b"channel", b"data"])
    parser = make_hiredis_parser()
    parser._hiredis_PushNotificationType = DummyPushNotification
    parser._reader.responses = [push_response, NOT_ENOUGH_DATA]
    parser._sock.recv_into.side_effect = BlockingIOError(
        EWOULDBLOCK, "Resource temporarily unavailable"
    )
    parser.pubsub_push_handler_func = Mock(return_value=push_response)

    with pytest.raises(TimeoutError):
        parser.read_response(timeout=0)

    parser.pubsub_push_handler_func.assert_called_once_with(push_response)


def test_hiredis_read_response_timeout_zero_maps_would_block_to_timeout():
    parser = make_hiredis_parser()
    parser._sock.recv_into.side_effect = BlockingIOError(
        EWOULDBLOCK, "Resource temporarily unavailable"
    )

    with pytest.raises(TimeoutError):
        parser.read_response(timeout=0)


@pytest.mark.parametrize("cleared_attr", ["_reader", "_sock"])
def test_hiredis_read_from_socket_raises_connection_error_when_disconnected(
    cleared_attr,
):
    # regression for #4003: another thread may disconnect the connection while
    # we are reading (e.g. a shared client closed via `with redis:`), which sets
    # _sock and _reader to None. read_from_socket() must raise a descriptive,
    # retryable ConnectionError rather than an AttributeError.
    parser = make_hiredis_parser()
    parser._sock.recv_into.return_value = 10
    setattr(parser, cleared_attr, None)

    with pytest.raises(ConnectionError, match="Connection closed by server"):
        parser.read_from_socket()


def test_hiredis_read_response_uses_local_reader_if_disconnected_mid_read():
    # regression for #4003: if _reader is cleared by a concurrent disconnect
    # after read_from_socket() returns, the in-flight read must still complete
    # via the locally bound reader instead of raising AttributeError on .gets().
    parser = make_hiredis_parser()
    parser._reader.responses = [NOT_ENOUGH_DATA, b"OK"]

    def fake_read_from_socket(*args, **kwargs):
        parser._reader = None  # simulate on_disconnect() from another thread
        return True

    parser.read_from_socket = fake_read_from_socket

    assert parser.read_response() == b"OK"


def test_socket_buffer_timeout_zero_maps_would_block_to_timeout():
    sock = Mock()
    sock.recv.side_effect = BlockingIOError(
        EWOULDBLOCK, "Resource temporarily unavailable"
    )
    socket_buffer = SocketBuffer(sock, socket_read_size=65536, socket_timeout=None)

    with pytest.raises(TimeoutError):
        socket_buffer.readline(timeout=0)


@skip_if_server_version_lt("4.0.0")
@pytest.mark.redismod
def test_loading_external_modules(r):
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


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "client_kwargs",
    [
        {"driver_info": None},
        {"lib_name": None, "lib_version": None},
    ],
)
def test_redis_client_preserves_explicit_none_driver_info(client_kwargs):
    if "lib_name" in client_kwargs:
        with pytest.warns(DeprecationWarning):
            client = Redis(**client_kwargs)
    else:
        client = Redis(**client_kwargs)

    assert client.connection_pool.connection_kwargs["driver_info"] is None
    client.close()


@pytest.mark.fixed_client
def test_redis_client_default_driver_info():
    client = Redis()
    driver_info = client.connection_pool.connection_kwargs["driver_info"]

    assert driver_info.formatted_name == "redis-py"
    assert driver_info.lib_version is not None
    client.close()


@pytest.mark.fixed_client
class TestConnection:
    def test_disconnect(self):
        conn = Connection()
        mock_sock = mock.Mock()
        conn._sock = mock_sock
        conn.disconnect()
        mock_sock.shutdown.assert_called_once()
        mock_sock.close.assert_called_once()
        assert conn._sock is None

    def test_disconnect__shutdown_OSError(self):
        """An OSError on socket shutdown will still close the socket."""
        conn = Connection()
        mock_sock = mock.Mock()
        conn._sock = mock_sock
        conn._sock.shutdown.side_effect = OSError
        conn.disconnect()
        mock_sock.shutdown.assert_called_once()
        mock_sock.close.assert_called_once()
        assert conn._sock is None

    def test_disconnect__close_OSError(self):
        """An OSError on socket close will still clear out the socket."""
        conn = Connection()
        mock_sock = mock.Mock()
        conn._sock = mock_sock
        conn._sock.close.side_effect = OSError
        conn.disconnect()
        mock_sock.shutdown.assert_called_once()
        mock_sock.close.assert_called_once()
        assert conn._sock is None

    @pytest.mark.parametrize(
        "connection_kwargs",
        [
            {"driver_info": None},
            {"lib_name": None, "lib_version": None},
        ],
    )
    def test_client_setinfo_skipped_with_explicit_none(self, connection_kwargs):
        if "lib_name" in connection_kwargs:
            with pytest.warns(DeprecationWarning):
                conn = Connection(protocol=2, **connection_kwargs)
        else:
            conn = Connection(protocol=2, **connection_kwargs)
        conn._parser.on_connect = mock.Mock()
        conn.send_command = mock.Mock()
        conn.read_response = mock.Mock(return_value="OK")

        conn.on_connect_check_health()

        assert conn.driver_info is None
        conn.send_command.assert_not_called()
        conn.read_response.assert_not_called()

    def clear(self, conn):
        conn.retry_on_error.clear()

    # Client-internal test: builds a default localhost Connection, so it cannot
    # target a remote managed Redis Enterprise endpoint.
    @skip_if_redis_enterprise()
    def test_retry_connect_on_timeout_error(self):
        """Test that the _connect function is retried in case of a timeout"""
        conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 3))
        origin_connect = conn._connect
        conn._connect = mock.Mock()

        def mock_connect():
            # connect only on the last retry
            if conn._connect.call_count <= 2:
                raise socket.timeout
            else:
                return origin_connect()

        conn._connect.side_effect = mock_connect
        conn.connect()
        assert conn._connect.call_count == 3
        self.clear(conn)

    def test_connect_without_retry_on_non_retryable_error(self):
        """Test that the _connect function is not being retried in case of a non-retryable error"""
        with patch.object(Connection, "_connect") as _connect:
            _connect.side_effect = RedisError("")
            conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 2))
            with pytest.raises(RedisError):
                conn.connect()
            assert _connect.call_count == 1
            self.clear(conn)

    # Client-internal test: builds a default localhost Connection and mocks the
    # socket to count handshake retries, so it needs a co-located server rather
    # than a remote managed Redis Enterprise endpoint.
    @skip_if_redis_enterprise()
    def test_connect_with_retries(self):
        """
        Validate that retries occur for the entire connect+handshake flow when OSError
        happens during the handshake phase.
        """
        with patch.object(socket.socket, "sendall") as sendall:
            sendall.side_effect = OSError(ECONNREFUSED)
            conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 2))
            with pytest.raises(ConnectionError):
                conn.connect()
            # the handshake commands are the failing ones
            # validate that we don't execute too many commands on each retry
            # 3 retries --> 3 commands
            assert sendall.call_count == 3

    def test_connect_timeout_error_without_retry(self):
        """Test that the _connect function is not being retried if retry_on_timeout is
        set to False"""
        conn = Connection(retry_on_timeout=False)
        conn._connect = mock.Mock()
        conn._connect.side_effect = socket.timeout

        with pytest.raises(TimeoutError, match="Timeout connecting to server"):
            conn.connect()
        assert conn._connect.call_count == 1
        self.clear(conn)


@pytest.mark.onlynoncluster
@pytest.mark.parametrize(
    "parser_class",
    [_RESP2Parser, _RESP3Parser, _HiredisParser],
    ids=["RESP2Parser", "RESP3Parser", "HiredisParser"],
)
def test_connection_parse_response_resume(r: redis.Redis, parser_class):
    """
    This test verifies that the Connection parser,
    be that PythonParser or HiredisParser,
    can be interrupted at IO time and then resume parsing.
    """
    if parser_class is _HiredisParser and not HIREDIS_AVAILABLE:
        pytest.skip("Hiredis not available)")
    args = dict(r.connection_pool.connection_kwargs)
    args["parser_class"] = parser_class
    conn = Connection(**args)
    conn.connect()
    message = (
        b"*3\r\n$7\r\nmessage\r\n$8\r\nchannel1\r\n"
        b"$25\r\nhi\r\nthere\r\n+how\r\nare\r\nyou\r\n"
    )
    mock_socket = MockSocket(message, interrupt_every=2)

    if isinstance(conn._parser, _RESP2Parser) or isinstance(conn._parser, _RESP3Parser):
        conn._parser._buffer._sock = mock_socket
    else:
        conn._parser._sock = mock_socket
    for i in range(100):
        try:
            response = conn.read_response(disconnect_on_error=False)
            break
        except MockSocket.TestError:
            pass

    else:
        pytest.fail("didn't receive a response")
    assert response
    assert i > 0


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "Class",
    [
        Connection,
        SSLConnection,
        UnixDomainSocketConnection,
    ],
)
def test_pack_command(Class):
    """
    This test verifies that the pack_command works
    on all supported connections. #2581
    """
    cmd = (
        "HSET",
        "foo",
        "key",
        "value1",
        b"key_b",
        b"bytes str",
        b"key_i",
        67,
        "key_f",
        3.14159265359,
    )
    expected = (
        b"*10\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$3\r\nkey\r\n$6\r\nvalue1\r\n"
        b"$5\r\nkey_b\r\n$9\r\nbytes str\r\n$5\r\nkey_i\r\n$2\r\n67\r\n$5"
        b"\r\nkey_f\r\n$13\r\n3.14159265359\r\n"
    )

    actual = Class().pack_command(*cmd)[0]
    assert actual == expected, f"actual = {actual}, expected = {expected}"


@pytest.mark.fixed_client
# Hardcodes a localhost URL, so it cannot target a remote managed Redis Enterprise endpoint.
@skip_if_redis_enterprise()
def test_create_single_connection_client_from_url():
    client = redis.Redis.from_url(
        "redis://localhost:6379/0?", single_connection_client=True
    )
    assert client.connection is not None


@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
def test_pool_auto_close(request, from_url):
    """Verify that basic Redis instances have auto_close_connection_pool set to True"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    def get_redis_connection():
        if from_url:
            return Redis.from_url(url)
        return Redis(**url_args)

    r1 = get_redis_connection()
    assert r1.auto_close_connection_pool is True
    r1.close()


@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
def test_redis_connection_pool(request, from_url):
    """Verify that basic Redis instances using `connection_pool`
    have auto_close_connection_pool set to False"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    pool = None

    def get_redis_connection():
        nonlocal pool
        if from_url:
            pool = ConnectionPool.from_url(url)
        else:
            pool = ConnectionPool(**url_args)
        return Redis(connection_pool=pool)

    called = 0

    def mock_disconnect(target_pool):
        nonlocal called
        if pool is not None and target_pool is pool:
            called += 1

    with patch.object(ConnectionPool, "disconnect", mock_disconnect):
        with get_redis_connection() as r1:
            assert r1.auto_close_connection_pool is False

    assert called == 0
    pool.disconnect()


@pytest.mark.parametrize("from_url", (True, False), ids=("from_url", "from_args"))
def test_redis_from_pool(request, from_url):
    """Verify that basic Redis instances created using `from_pool()`
    have auto_close_connection_pool set to True"""

    url: str = request.config.getoption("--redis-url")
    url_args = parse_url(url)

    pool = None

    def get_redis_connection():
        nonlocal pool
        if from_url:
            pool = ConnectionPool.from_url(url)
        else:
            pool = ConnectionPool(**url_args)
        return Redis.from_pool(pool)

    called = 0

    def mock_disconnect(target_pool):
        nonlocal called
        if pool is not None and target_pool is pool:
            called += 1

    with patch.object(ConnectionPool, "disconnect", mock_disconnect):
        with get_redis_connection() as r1:
            assert r1.auto_close_connection_pool is True

    assert called == 1
    pool.disconnect()


@pytest.mark.fixed_client
def test_create_secure_client_from_url_with_minimum_ssl_version():
    client = redis.Redis.from_url(
        "rediss://localhost:6379/0?ssl_cert_reqs=none&ssl_min_version={}".format(
            ssl.TLSVersion.TLSv1_3
        )
    )
    assert (
        client.connection_pool.connection_kwargs["ssl_min_version"]
        == ssl.TLSVersion.TLSv1_3
    )


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
def test_format_error_message(conn, error, expected_message):
    """Test that the _error_message function formats errors correctly"""
    error_message = conn._error_message(error)
    assert error_message == expected_message


@pytest.mark.fixed_client
def test_network_connection_failure():
    # Match only the stable part of the error message across OS
    exp_err = rf"Error {ECONNREFUSED} connecting to localhost:9999\."
    with pytest.raises(ConnectionError, match=exp_err):
        redis = Redis(port=9999)
        redis.set("a", "b")


@pytest.mark.fixed_client
@pytest.mark.skipif(
    not hasattr(socket, "AF_UNIX"),
    reason="Unix domain sockets not supported on this platform",
)
def test_unix_socket_connection_failure():
    exp_err = "Error 2 connecting to unix:///tmp/a.sock. No such file or directory."
    with pytest.raises(ConnectionError, match=exp_err):
        redis = Redis(unix_socket_path="unix:///tmp/a.sock")
        redis.set("a", "b")


@pytest.mark.fixed_client
class TestUnitConnectionPool:
    @pytest.mark.parametrize(
        "max_conn", (-1, "str"), ids=("non-positive", "wrong type")
    )
    def test_throws_error_on_incorrect_max_connections(self, max_conn):
        with pytest.raises(
            ValueError, match='"max_connections" must be a positive integer'
        ):
            ConnectionPool(
                max_connections=max_conn,
            )

    def test_throws_error_on_cache_enable_in_resp2(self):
        with pytest.raises(
            RedisError, match="Client caching is only supported with RESP version 3"
        ):
            ConnectionPool(protocol=2, cache_config=CacheConfig())

    def test_throws_error_on_incorrect_cache_implementation(self):
        with pytest.raises(ValueError, match="Cache must implement CacheInterface"):
            ConnectionPool(protocol=3, cache="wrong")

    def test_returns_custom_cache_implementation(self, mock_cache):
        connection_pool = ConnectionPool(protocol=3, cache=mock_cache)

        assert mock_cache == connection_pool.cache
        connection_pool.disconnect()

    def test_creates_cache_with_custom_cache_factory(
        self, mock_cache_factory, mock_cache
    ):
        mock_cache_factory.get_cache.return_value = mock_cache

        connection_pool = ConnectionPool(
            protocol=3,
            cache_config=CacheConfig(max_size=5),
            cache_factory=mock_cache_factory,
        )

        # Cache is wrapped in CacheProxy for observability
        assert isinstance(connection_pool.cache, CacheProxy)
        assert connection_pool.cache._cache == mock_cache
        connection_pool.disconnect()

    def test_creates_cache_with_given_configuration(self, mock_cache):
        connection_pool = ConnectionPool(
            protocol=3, cache_config=CacheConfig(max_size=100)
        )

        assert isinstance(connection_pool.cache, CacheInterface)
        assert connection_pool.cache.config.get_max_size() == 100
        assert isinstance(connection_pool.cache.eviction_policy, LRUPolicy)
        connection_pool.disconnect()

    def test_make_connection_proxy_connection_on_given_cache(self):
        connection_pool = ConnectionPool(protocol=3, cache_config=CacheConfig())

        assert isinstance(connection_pool.make_connection(), CacheProxyConnection)
        connection_pool.disconnect()


@pytest.mark.fixed_client
class TestUnitCacheProxyConnection:
    def test_clears_cache_on_disconnect(self, mock_connection, cache_conf):
        cache = DefaultCache(CacheConfig(max_size=10))
        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )

        cache.set(
            CacheEntry(
                cache_key=cache_key,
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            )
        )
        assert cache.get(cache_key).cache_value == b"bar"

        mock_connection.disconnect.return_value = None
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = EventDispatcher()

        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )
        proxy_connection.disconnect()

        assert len(cache.collection) == 0

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    def test_read_response_returns_cached_reply(self, mock_cache, mock_connection):
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = EventDispatcher()

        mock_cache.is_cachable.return_value = True
        mock_cache.get.side_effect = [
            None,
            None,
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                ),
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=mock_connection,
            ),
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                ),
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            ),
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                ),
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            ),
            CacheEntry(
                cache_key=CacheKey(
                    command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                ),
                cache_value=b"bar",
                status=CacheEntryStatus.VALID,
                connection_ref=mock_connection,
            ),
        ]
        mock_connection.send_command.return_value = Any
        mock_connection.read_response.return_value = b"bar"
        mock_connection.can_read.return_value = False

        proxy_connection = CacheProxyConnection(
            mock_connection, mock_cache, threading.RLock()
        )
        proxy_connection.send_command(*["GET", "foo"], **{"keys": ["foo"]})
        assert proxy_connection.read_response() == b"bar"
        assert proxy_connection._current_command_cache_key is None
        assert proxy_connection.read_response() == b"bar"

        mock_cache.set.assert_has_calls(
            [
                call(
                    CacheEntry(
                        cache_key=CacheKey(
                            command="GET",
                            redis_keys=("foo",),
                            redis_args=("GET", "foo"),
                        ),
                        cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                        status=CacheEntryStatus.IN_PROGRESS,
                        connection_ref=mock_connection,
                    )
                ),
                call(
                    CacheEntry(
                        cache_key=CacheKey(
                            command="GET",
                            redis_keys=("foo",),
                            redis_args=("GET", "foo"),
                        ),
                        cache_value=b"bar",
                        status=CacheEntryStatus.VALID,
                        connection_ref=mock_connection,
                    )
                ),
            ]
        )

        mock_cache.get.assert_has_calls(
            [
                call(
                    CacheKey(
                        command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                    )
                ),
                call(
                    CacheKey(
                        command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                    )
                ),
                call(
                    CacheKey(
                        command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
                    )
                ),
            ]
        )

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    @pytest.mark.parametrize(
        "command,redis_keys,redis_args,cached_value",
        [
            ("ZCARD", ("myset",), ("ZCARD", "myset"), 2),
            ("SCARD", ("myset",), ("SCARD", "myset"), 5),
            ("LLEN", ("mylist",), ("LLEN", "mylist"), 0),
            (
                "LRANGE",
                ("mylist",),
                ("LRANGE", "mylist", "0", "-1"),
                [b"a", b"b"],
            ),
            ("EXISTS", ("foo",), ("EXISTS", "foo"), True),
        ],
        ids=["int-zcard", "int-scard", "int-llen", "list-lrange", "bool-exists"],
    )
    def test_read_response_returns_cached_non_bytes_reply(
        self, mock_cache, mock_connection, command, redis_keys, redis_args, cached_value
    ):
        """Test that cached non-bytes responses (int, list, bool) don't crash.

        Regression test for https://github.com/redis/redis-py/issues/4009
        """
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = EventDispatcher()

        cache_key = CacheKey(
            command=command, redis_keys=redis_keys, redis_args=redis_args
        )
        valid_entry = CacheEntry(
            cache_key=cache_key,
            cache_value=cached_value,
            status=CacheEntryStatus.VALID,
            connection_ref=mock_connection,
        )
        in_progress_entry = CacheEntry(
            cache_key=cache_key,
            cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
            status=CacheEntryStatus.IN_PROGRESS,
            connection_ref=mock_connection,
        )
        mock_cache.is_cachable.return_value = True
        mock_cache.get.side_effect = [
            # 1st send_command: cache.get(key) → None (cache miss)
            None,
            # 1st read_response: cache.get(key) is not None check
            in_progress_entry,
            # 1st read_response: cache.get(key).status check
            in_progress_entry,
            # 1st read_response: cache.get(key) after wire read (to update entry)
            in_progress_entry,
            # 2nd send_command: cache.get(key) → truthy (cache hit, enter branch)
            valid_entry,
            # 2nd send_command: entry = cache.get(key)
            valid_entry,
            # 2nd send_command: re-check cache.get(key) → truthy (return early)
            valid_entry,
            # 2nd read_response: cache.get(key) is not None check
            valid_entry,
            # 2nd read_response: cache.get(key).status check (VALID != IN_PROGRESS)
            valid_entry,
            # 2nd read_response: cache.get(key).cache_value (deep copy)
            valid_entry,
        ]
        mock_connection.send_command.return_value = Any
        mock_connection.read_response.return_value = cached_value
        mock_connection.can_read.return_value = False

        proxy_connection = CacheProxyConnection(
            mock_connection, mock_cache, threading.RLock()
        )
        proxy_connection.send_command(*list(redis_args), **{"keys": list(redis_keys)})
        # First call: cache miss, reads from connection
        assert proxy_connection.read_response() == cached_value
        assert proxy_connection._current_command_cache_key is None

        # Re-issue send_command so _current_command_cache_key is set again;
        # this time send_command sees a VALID entry and returns early.
        proxy_connection.send_command(*list(redis_args), **{"keys": list(redis_keys)})
        # Second call: cache hit — this must not raise TypeError
        assert proxy_connection.read_response() == cached_value
        # Verify the second read_response used the cache, not the wire:
        # mock_connection.read_response should have been called only once
        # (during the first read_response).
        mock_connection.read_response.assert_called_once()

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    def test_triggers_invalidation_processing_on_another_connection(
        self, mock_cache, mock_connection
    ):
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = Mock(spec=EventDispatcher)

        another_conn = copy.deepcopy(mock_connection)
        another_conn.can_read.side_effect = [True, False]
        another_conn.read_response.return_value = None
        cache_entry = CacheEntry(
            cache_key=CacheKey(
                command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
            ),
            cache_value=b"bar",
            status=CacheEntryStatus.VALID,
            connection_ref=another_conn,
        )
        mock_cache.is_cachable.return_value = True
        mock_cache.get.return_value = cache_entry
        mock_connection.can_read.return_value = False

        proxy_connection = CacheProxyConnection(
            mock_connection, mock_cache, threading.RLock()
        )
        proxy_connection.send_command(*["GET", "foo"], **{"keys": ["foo"]})

        assert proxy_connection.read_response() == b"bar"
        assert another_conn.can_read.call_count == 2
        another_conn.read_response.assert_called_once_with(
            push_request=True, timeout=0, disconnect_on_error=False
        )

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    def test_sends_command_when_cache_entry_invalidated_during_drain(
        self, mock_cache, mock_connection
    ):
        """Regression test for issue #3600.

        When another connection's invalidation drain removes the cache entry,
        send_command must fall through and send the command over the wire
        instead of returning early (which would cause read_response to hang).
        """
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = Mock(spec=EventDispatcher)

        another_conn = copy.deepcopy(mock_connection)
        another_conn.can_read.side_effect = [True, False]
        another_conn.read_response.return_value = None

        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_entry = CacheEntry(
            cache_key=cache_key,
            cache_value=b"bar",
            status=CacheEntryStatus.VALID,
            connection_ref=another_conn,
        )

        mock_cache.is_cachable.return_value = True
        # get() call sequence in send_command:
        #   1st: check if entry exists (truthy → enter branch)
        #   2nd: fetch the entry
        #   3rd: re-check after drain (None → entry was invalidated)
        mock_cache.get.side_effect = [cache_entry, cache_entry, None]
        mock_connection.can_read.return_value = False
        mock_connection.send_command.return_value = None

        proxy_connection = CacheProxyConnection(
            mock_connection, mock_cache, threading.RLock()
        )
        proxy_connection.send_command(*["GET", "foo"], **{"keys": ["foo"]})

        # The drain should have happened on the other connection
        assert another_conn.can_read.call_count == 2
        another_conn.read_response.assert_called_once_with(
            push_request=True, timeout=0, disconnect_on_error=False
        )

        # The command must have been sent over the wire (not returned early)
        mock_connection.send_command.assert_called_once_with("GET", "foo", keys=["foo"])

        # An IN_PROGRESS entry must have been set for this connection
        mock_cache.set.assert_called_once_with(
            CacheEntry(
                cache_key=cache_key,
                cache_value=CacheProxyConnection.DUMMY_CACHE_VALUE,
                status=CacheEntryStatus.IN_PROGRESS,
                connection_ref=mock_connection,
            )
        )

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="Pypy doesn't support side_effect",
    )
    def test_invalidation_processing_on_another_connection_breaks_on_timeout(
        self, mock_cache, mock_connection
    ):
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection._event_dispatcher = Mock(spec=EventDispatcher)

        another_conn = copy.deepcopy(mock_connection)
        another_conn.can_read.return_value = True
        another_conn.read_response.side_effect = TimeoutError("timeout")

        cache_key = CacheKey(
            command="GET", redis_keys=("foo",), redis_args=("GET", "foo")
        )
        cache_entry = CacheEntry(
            cache_key=cache_key,
            cache_value=b"bar",
            status=CacheEntryStatus.VALID,
            connection_ref=another_conn,
        )

        mock_cache.is_cachable.return_value = True
        mock_cache.get.side_effect = [cache_entry, cache_entry, cache_entry]
        mock_connection.can_read.return_value = False

        proxy_connection = CacheProxyConnection(
            mock_connection, mock_cache, threading.RLock()
        )
        proxy_connection.send_command(*["GET", "foo"], **{"keys": ["foo"]})

        another_conn.read_response.assert_called_once_with(
            push_request=True, timeout=0, disconnect_on_error=False
        )
        mock_connection.send_command.assert_not_called()

    def test_process_pending_invalidations_breaks_on_timeout(self, mock_connection):
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.can_read.return_value = True
        mock_connection.read_response.side_effect = TimeoutError("timeout")

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        proxy_connection._process_pending_invalidations()

        mock_connection.read_response.assert_called_once_with(
            push_request=True, timeout=0, disconnect_on_error=False
        )

    def test_read_response_propagates_timeout_parameter(self, mock_connection):
        """Test that timeout parameter is propagated to underlying connection."""
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.read_response.return_value = b"OK"

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        # Test with specific timeout value
        proxy_connection.read_response(timeout=0.5)
        mock_connection.read_response.assert_called_with(
            disable_decoding=False,
            timeout=0.5,
            disconnect_on_error=True,
            push_request=False,
        )

    def test_read_response_timeout_default_is_sentinel(self, mock_connection):
        """Test that default timeout value is SENTINEL."""
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.read_response.return_value = b"OK"

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        # Test default timeout is SENTINEL
        proxy_connection.read_response()
        mock_connection.read_response.assert_called_with(
            disable_decoding=False,
            timeout=SENTINEL,
            disconnect_on_error=True,
            push_request=False,
        )

    def test_read_response_timeout_none_passed_through(self, mock_connection):
        """Test that timeout=None is passed through for blocking behavior."""
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.read_response.return_value = b"OK"

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        # Test timeout=None is passed through
        proxy_connection.read_response(timeout=None)
        mock_connection.read_response.assert_called_with(
            disable_decoding=False,
            timeout=None,
            disconnect_on_error=True,
            push_request=False,
        )

    def test_read_response_timeout_zero_passed_through(self, mock_connection):
        """Test that timeout=0 is passed through for non-blocking behavior."""
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.read_response.return_value = b"OK"

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        # Test timeout=0 is passed through
        proxy_connection.read_response(timeout=0)
        mock_connection.read_response.assert_called_with(
            disable_decoding=False,
            timeout=0,
            disconnect_on_error=True,
            push_request=False,
        )

    def test_read_response_all_params_with_timeout(self, mock_connection):
        """Test that all parameters including timeout are correctly passed."""
        mock_connection.retry = "mock"
        mock_connection.host = "mock"
        mock_connection.port = "mock"
        mock_connection.db = 0
        mock_connection._event_dispatcher = EventDispatcher()
        mock_connection.credential_provider = UsernamePasswordCredentialProvider()
        mock_connection.read_response.return_value = b"OK"

        cache = DefaultCache(CacheConfig(max_size=10))
        proxy_connection = CacheProxyConnection(
            mock_connection, cache, threading.RLock()
        )

        # Test all parameters together
        proxy_connection.read_response(
            disable_decoding=True,
            timeout=1.5,
            disconnect_on_error=False,
            push_request=True,
        )
        mock_connection.read_response.assert_called_with(
            disable_decoding=True,
            timeout=1.5,
            disconnect_on_error=False,
            push_request=True,
        )


class TestConnectionPoolGetConnectionCount:
    """Tests for ConnectionPool.get_connection_count() method."""

    def test_get_connection_count_returns_idle_and_used_counts(self):
        """Test that get_connection_count returns both idle and used connection counts."""
        pool = ConnectionPool(max_connections=10)

        # Initially, no connections exist
        counts = pool.get_connection_count()
        assert len(counts) == 2

        # Check idle connections count
        idle_count, idle_attrs = counts[0]
        assert idle_count == 0
        assert DB_CLIENT_CONNECTION_POOL_NAME in idle_attrs
        assert idle_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.IDLE.value

        # Check used connections count
        used_count, used_attrs = counts[1]
        assert used_count == 0
        assert DB_CLIENT_CONNECTION_POOL_NAME in used_attrs
        assert used_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.USED.value

        pool.disconnect()

    def test_get_connection_count_with_connections_in_use(self):
        """Test get_connection_count when connections are in use."""

        pool = ConnectionPool(max_connections=10)

        # Create mock connections
        mock_conn1 = MagicMock()
        mock_conn1.pid = pool.pid

        mock_conn2 = MagicMock()
        mock_conn2.pid = pool.pid

        # Simulate connections in use
        pool._in_use_connections.add(mock_conn1)
        pool._in_use_connections.add(mock_conn2)

        counts = pool.get_connection_count()

        idle_count, idle_attrs = counts[0]
        used_count, used_attrs = counts[1]

        assert idle_count == 0
        assert used_count == 2
        assert idle_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.IDLE.value
        assert used_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.USED.value

        pool.disconnect()

    def test_get_connection_count_with_available_connections(self):
        """Test get_connection_count when connections are available (idle)."""

        pool = ConnectionPool(max_connections=10)

        # Create mock connections
        mock_conn1 = MagicMock()
        mock_conn1.pid = pool.pid

        mock_conn2 = MagicMock()
        mock_conn2.pid = pool.pid

        mock_conn3 = MagicMock()
        mock_conn3.pid = pool.pid

        # Simulate available connections
        pool._available_connections.append(mock_conn1)
        pool._available_connections.append(mock_conn2)
        pool._available_connections.append(mock_conn3)

        counts = pool.get_connection_count()

        idle_count, idle_attrs = counts[0]
        used_count, used_attrs = counts[1]

        assert idle_count == 3
        assert used_count == 0
        assert idle_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.IDLE.value
        assert used_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.USED.value

        pool.disconnect()

    def test_get_connection_count_mixed_connections(self):
        """Test get_connection_count with both idle and used connections."""

        pool = ConnectionPool(max_connections=10)

        # Create mock connections
        mock_idle = MagicMock()
        mock_idle.pid = pool.pid

        mock_used1 = MagicMock()
        mock_used1.pid = pool.pid

        mock_used2 = MagicMock()
        mock_used2.pid = pool.pid

        # Simulate mixed state
        pool._available_connections.append(mock_idle)
        pool._in_use_connections.add(mock_used1)
        pool._in_use_connections.add(mock_used2)

        counts = pool.get_connection_count()

        idle_count, _ = counts[0]
        used_count, _ = counts[1]

        assert idle_count == 1
        assert used_count == 2

        pool.disconnect()

    def test_get_connection_count_includes_pool_name_in_attributes(self):
        """Test that get_connection_count includes pool name in attributes."""
        from redis.observability.attributes import get_pool_name

        pool = ConnectionPool(max_connections=10)

        counts = pool.get_connection_count()

        _, idle_attrs = counts[0]
        _, used_attrs = counts[1]

        # Both should have the pool name
        assert DB_CLIENT_CONNECTION_POOL_NAME in idle_attrs
        assert DB_CLIENT_CONNECTION_POOL_NAME in used_attrs

        # Pool name should match the format from get_pool_name() (host:port_uniqueID)
        expected_pool_name = get_pool_name(pool)
        assert idle_attrs[DB_CLIENT_CONNECTION_POOL_NAME] == expected_pool_name
        assert used_attrs[DB_CLIENT_CONNECTION_POOL_NAME] == expected_pool_name

        # Verify the pool name has the expected format (host:port_uniqueID)
        assert "unknown:6379_" in expected_pool_name

        # Verify the unique ID is 8 hex characters (matching go-redis)
        parts = expected_pool_name.split("_")
        assert len(parts) == 2, (
            f"Pool name should have format host:port_id, got: {expected_pool_name}"
        )
        unique_id = parts[1]
        assert len(unique_id) == 8, (
            f"Unique ID should be 8 characters, got: {unique_id}"
        )

        pool.disconnect()


class TestBlockingConnectionPoolGetConnectionCount:
    """Tests for BlockingConnectionPool.get_connection_count() method."""

    def test_get_connection_count_returns_idle_and_used_counts(self):
        """Test that BlockingConnectionPool.get_connection_count returns both counts."""

        pool = BlockingConnectionPool(max_connections=10)

        # Initially, no connections exist
        counts = pool.get_connection_count()
        assert len(counts) == 2

        idle_count, idle_attrs = counts[0]
        used_count, used_attrs = counts[1]

        assert idle_count == 0
        assert used_count == 0
        assert idle_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.IDLE.value
        assert used_attrs[DB_CLIENT_CONNECTION_STATE] == ConnectionState.USED.value

        pool.disconnect()

    def test_get_connection_count_with_connections_in_queue(self):
        """Test get_connection_count when connections are in the queue (idle)."""

        pool = BlockingConnectionPool(max_connections=10)

        # Create mock connections and add to queue
        mock_conn1 = MagicMock()
        mock_conn1.pid = pool.pid

        mock_conn2 = MagicMock()
        mock_conn2.pid = pool.pid

        # Add connections to the pool's internal list and queue
        pool._connections.append(mock_conn1)
        pool._connections.append(mock_conn2)

        # Clear the queue and add our connections
        while not pool.pool.empty():
            try:
                pool.pool.get_nowait()
            except Exception:
                break

        pool.pool.put_nowait(mock_conn1)
        pool.pool.put_nowait(mock_conn2)

        counts = pool.get_connection_count()

        idle_count, _ = counts[0]
        used_count, _ = counts[1]

        assert idle_count == 2
        assert used_count == 0

        pool.disconnect()


class _DummyConnection:
    """Minimal connection stub for pool metric tests (no real socket)."""

    description_format = "DummyConnection<>"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()
        self._sock = None

    def connect(self):
        self._sock = MagicMock()

    def disconnect(self):
        self._sock = None

    def can_read(self):
        return False

    def should_reconnect(self):
        return False

    def re_auth(self):
        pass


def _pool_metric_calls(mock_fn, pool_name):
    """Extract (state, delta) tuples from record_connection_count calls for a pool.

    Filters by pool_name to avoid interference from GC of other pools.
    """
    result = []
    for c in mock_fn.call_args_list:
        p = c.kwargs.get("pool_name", c.args[0] if c.args else None)
        if p != pool_name:
            continue
        state = c.kwargs.get(
            "connection_state", c.args[1] if len(c.args) > 1 else None
        )
        counter = c.kwargs.get("counter", c.args[2] if len(c.args) > 2 else 1)
        result.append((state, counter))
    return result


def _net(calls):
    """Return (idle_net, used_net) from a list of (state, delta) tuples."""
    idle = sum(d for s, d in calls if s == ConnectionState.IDLE)
    used = sum(d for s, d in calls if s == ConnectionState.USED)
    return idle, used


class TestConnectionPoolMetricCount:
    """Tests for db.client.connection.count UpDownCounter accuracy.

    Verifies that get_connection / release produce balanced IDLE and USED
    counter updates across ConnectionPool and BlockingConnectionPool.
    """

    @patch("redis.connection.record_connection_count")
    def test_new_connection_records_only_used(self, mock_rec):
        """A new connection should record USED +1 only (never was idle)."""
        pool = ConnectionPool(connection_class=_DummyConnection, max_connections=10)
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        conn = pool.get_connection()

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert idle_net == 0, f"New conn should not touch IDLE, got {idle_net}"
        assert used_net == 1
        pool.release(conn)

    @patch("redis.connection.record_connection_count")
    def test_reused_connection_transitions_idle_to_used(self, mock_rec):
        """A reused connection should record IDLE -1, USED +1."""
        pool = ConnectionPool(connection_class=_DummyConnection, max_connections=10)
        pn = get_pool_name(pool)
        conn = pool.get_connection()
        pool.release(conn)
        mock_rec.reset_mock()

        conn2 = pool.get_connection()
        assert conn2 is conn

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert idle_net == -1
        assert used_net == 1
        pool.release(conn2)

    @patch("redis.connection.record_connection_count")
    def test_full_lifecycle_nets_to_zero(self, mock_rec):
        """create -> use -> release -> reuse -> release -> destroy = net 0."""
        pool = ConnectionPool(connection_class=_DummyConnection, max_connections=10)
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        conn = pool.get_connection()
        pool.release(conn)
        conn = pool.get_connection()
        pool.release(conn)

        # Simulate destruction (what __del__ / reset does)
        idle_count = len(pool._available_connections)
        if idle_count:
            mock_rec(
                pool_name=pn,
                connection_state=ConnectionState.IDLE,
                counter=-idle_count,
            )

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert idle_net == 0, f"Lifecycle IDLE should net 0, got {idle_net}"
        assert used_net == 0, f"Lifecycle USED should net 0, got {used_net}"

    @patch("redis.connection.record_connection_count")
    def test_release_unowned_uses_real_pool_name(self, mock_rec):
        """release() with owns_connection()=False must use the real pool name."""
        pool = ConnectionPool(connection_class=_DummyConnection, max_connections=10)
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        conn = pool.get_connection()
        mock_rec.reset_mock()
        conn.pid = -1  # simulate fork
        pool.release(conn)

        used_decs = [
            c for c in mock_rec.call_args_list
            if c.kwargs.get("connection_state") == ConnectionState.USED
            and c.kwargs.get("counter", 1) == -1
        ]
        assert len(used_decs) == 1
        assert used_decs[0].kwargs["pool_name"] == pn


class TestBlockingConnectionPoolMetricCount:
    """Same metric-count tests for BlockingConnectionPool."""

    def _pool(self):
        return BlockingConnectionPool(
            connection_class=_DummyConnection, max_connections=10, timeout=0.1,
        )

    @patch("redis.connection.record_connection_count")
    def test_new_connection_records_only_used(self, mock_rec):
        pool = self._pool()
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        conn = pool.get_connection()

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert idle_net == 0
        assert used_net == 1
        pool.release(conn)

    @patch("redis.connection.record_connection_count")
    def test_release_full_queue_decrements_used(self, mock_rec):
        """When queue is full, release() must still decrement USED."""
        pool = BlockingConnectionPool(
            connection_class=_DummyConnection, max_connections=1, timeout=0.1,
        )
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        conn = pool.get_connection()
        mock_rec.reset_mock()

        pool.pool.put_nowait(None)  # fill the queue
        pool.release(conn)

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert used_net == -1, f"USED must be decremented, got {used_net}"
        assert idle_net == 0, f"IDLE must not increase for dropped conn, got {idle_net}"

    @patch("redis.connection.record_connection_count")
    def test_release_full_queue_no_double_decrement_on_reset(self, mock_rec):
        """A connection dropped on a full queue must be removed from
        _connections so a later reset() does not decrement USED again."""
        pool = BlockingConnectionPool(
            connection_class=_DummyConnection, max_connections=1, timeout=0.1,
        )
        pn = get_pool_name(pool)

        conn = pool.get_connection()
        pool.pool.put_nowait(None)  # fill the queue
        pool.release(conn)

        # The dropped connection must no longer be tracked.
        assert conn not in pool._connections

        mock_rec.reset_mock()
        pool.reset()

        calls = _pool_metric_calls(mock_rec, pn)
        idle_net, used_net = _net(calls)
        assert used_net == 0, f"reset() must not decrement USED again, got {used_net}"
        assert idle_net == 0, f"reset() must not touch IDLE, got {idle_net}"

    @patch("redis.connection.record_connection_count")
    def test_release_unowned_uses_real_pool_name(self, mock_rec):
        pool = self._pool()
        pn = get_pool_name(pool)
        mock_rec.reset_mock()

        conn = pool.get_connection()
        mock_rec.reset_mock()
        conn.pid = -1
        pool.release(conn)

        used_decs = [
            c for c in mock_rec.call_args_list
            if c.kwargs.get("connection_state") == ConnectionState.USED
            and c.kwargs.get("counter", 1) == -1
        ]
        assert len(used_decs) == 1
        assert used_decs[0].kwargs["pool_name"] == pn
