import socket
import types
from unittest import mock
from unittest.mock import patch

import pytest

import redis
from redis.backoff import NoBackoff
from redis.connection import (
    Connection,
    HiredisParser,
    PythonParser,
    SSLConnection,
    UnixDomainSocketConnection,
)
from redis.exceptions import ConnectionError, InvalidResponse, TimeoutError
from redis.retry import Retry
from redis.utils import HIREDIS_AVAILABLE

from .conftest import skip_if_server_version_lt
from .mocks import MockSocket


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
@pytest.mark.onlynoncluster
def test_invalid_response(r):
    raw = b"x"
    parser = r.connection._parser
    with mock.patch.object(parser._buffer, "readline", return_value=raw):
        with pytest.raises(InvalidResponse) as cm:
            parser.read_response()
    assert str(cm.value) == f"Protocol Error: {raw!r}"


@skip_if_server_version_lt("4.0.0")
@pytest.mark.redismod
def test_loading_external_modules(modclient):
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

    def clear(self, conn):
        conn.retry_on_error.clear()

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

    def test_connect_without_retry_on_os_error(self):
        """Test that the _connect function is not being retried in case of a OSError"""
        with patch.object(Connection, "_connect") as _connect:
            _connect.side_effect = OSError("")
            conn = Connection(retry_on_timeout=True, retry=Retry(NoBackoff(), 2))
            with pytest.raises(ConnectionError):
                conn.connect()
            assert _connect.call_count == 1
            self.clear(conn)

    def test_connect_timeout_error_without_retry(self):
        """Test that the _connect function is not being retried if retry_on_timeout is
        set to False"""
        conn = Connection(retry_on_timeout=False)
        conn._connect = mock.Mock()
        conn._connect.side_effect = socket.timeout

        with pytest.raises(TimeoutError) as e:
            conn.connect()
        assert conn._connect.call_count == 1
        assert str(e.value) == "Timeout connecting to server"
        self.clear(conn)


@pytest.mark.onlynoncluster
@pytest.mark.parametrize(
    "parser_class", [PythonParser, HiredisParser], ids=["PythonParser", "HiredisParser"]
)
def test_connection_parse_response_resume(r: redis.Redis, parser_class):
    """
    This test verifies that the Connection parser,
    be that PythonParser or HiredisParser,
    can be interrupted at IO time and then resume parsing.
    """
    if parser_class is HiredisParser and not HIREDIS_AVAILABLE:
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

    if isinstance(conn._parser, PythonParser):
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


@pytest.mark.onlynoncluster
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


@pytest.mark.onlynoncluster
def test_create_single_connection_client_from_url():
    client = redis.Redis.from_url(
        "redis://localhost:6379/0?", single_connection_client=True
    )
    assert client.connection is not None
