import random
import socket
import string
import types
from unittest import mock
from unittest.mock import patch

import pytest

import redis
from redis.backoff import NoBackoff
from redis.connection import Connection, CredentialsProvider
from redis.exceptions import (
    ConnectionError,
    InvalidResponse,
    ResponseError,
    TimeoutError,
)
from redis.retry import Retry
from redis.utils import HIREDIS_AVAILABLE

from .conftest import _get_client, skip_if_redis_enterprise, skip_if_server_version_lt


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


class TestCredentialsProvider:
    @skip_if_redis_enterprise()
    def test_credentials_provider_without_supplier(self, r, request):
        # first, test for default user (`username` is supposed to be optional)
        default_username = "default"
        temp_pass = "temp_pass"
        creds_provider = CredentialsProvider(default_username, temp_pass)
        r.config_set("requirepass", temp_pass)
        creds = creds_provider.get_credentials()
        assert r.auth(creds[1], creds[0]) is True
        assert r.auth(creds_provider.get_password()) is True

        # test for other users
        username = "redis-py-auth"
        password = "strong_password"

        def teardown():
            try:
                r.auth(temp_pass)
            except ResponseError:
                r.auth("default", "")
            r.config_set("requirepass", "")
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        assert r.acl_setuser(
            username,
            enabled=True,
            passwords=["+" + password],
            keys="~*",
            commands=["+ping", "+command", "+info", "+select", "+flushdb", "+cluster"],
        )

        creds_provider2 = CredentialsProvider(username, password)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, credentials_provider=creds_provider2
        )

        assert r2.ping() is True

    @skip_if_redis_enterprise()
    def test_credentials_provider_with_supplier(self, r, request):
        import functools

        @functools.lru_cache(maxsize=10)
        def auth_supplier(user, endpoint):
            def get_random_string(length):
                letters = string.ascii_lowercase
                result_str = "".join(random.choice(letters) for i in range(length))
                return result_str

            auth_token = get_random_string(5) + user + "_" + endpoint
            return user, auth_token

        username = "redis-py-auth"
        creds_provider = CredentialsProvider(
            supplier=auth_supplier,
            user=username,
            endpoint="localhost",
        )
        password = creds_provider.get_password()

        assert r.acl_setuser(
            username,
            enabled=True,
            passwords=["+" + password],
            keys="~*",
            commands=["+ping", "+command", "+info", "+select", "+flushdb", "+cluster"],
        )

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        r2 = _get_client(
            redis.Redis, request, flushdb=False, credentials_provider=creds_provider
        )

        assert r2.ping() is True
