import datetime
import gc
import os
import re
import sys
import time
import weakref
from contextlib import closing
from threading import Thread
from unittest import mock

import pytest
import redis
from redis.cache import CacheConfig
from redis.connection import CacheProxyConnection, Connection, to_bool
from redis.utils import SSL_AVAILABLE

from .conftest import (
    _get_client,
    skip_if_redis_enterprise,
    skip_if_resp_version,
    skip_if_server_version_lt,
)
from .test_pubsub import wait_for_message

if SSL_AVAILABLE:
    import ssl


class DummyConnection:
    description_format = "DummyConnection<>"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()
        self._sock = None
        self._disconnected = False

    def connect(self):
        self._sock = mock.Mock()
        self._disconnected = False

    def can_read(self):
        return False

    def should_reconnect(self):
        return False

    def disconnect(self):
        self._sock = None
        self._disconnected = True

    def re_auth(self):
        pass


class TestConnectionPool:
    def get_pool(
        self,
        connection_kwargs=None,
        max_connections=None,
        connection_class=redis.Connection,
    ):
        connection_kwargs = connection_kwargs or {}
        pool = redis.ConnectionPool(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs,
        )
        return pool

    def test_connection_creation(self):
        connection_kwargs = {
            "foo": "bar",
            "biz": "baz",
        }
        pool = self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=DummyConnection
        )

        connection = pool.get_connection()
        assert isinstance(connection, DummyConnection)
        assert connection.kwargs == connection_kwargs

    def test_closing(self):
        connection_kwargs = {"foo": "bar", "biz": "baz"}
        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            max_connections=None,
            **connection_kwargs,
        )
        with closing(pool):
            pass

    def test_multiple_connections(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = pool.get_connection()
        c2 = pool.get_connection()
        assert c1 != c2

    def test_max_connections(self, master_host):
        # Use DummyConnection to avoid actual connection to Redis
        # This prevents authentication issues and makes the test more reliable
        # while still properly testing the MaxConnectionsError behavior
        pool = self.get_pool(max_connections=2, connection_class=DummyConnection)
        pool.get_connection()
        pool.get_connection()
        with pytest.raises(redis.MaxConnectionsError):
            pool.get_connection()

    def test_reuse_previously_released_connection(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = pool.get_connection()
        pool.release(c1)
        c2 = pool.get_connection()
        assert c1 == c2

    def test_release_not_owned_connection(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool1 = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = pool1.get_connection()
        pool2 = self.get_pool(
            connection_kwargs={"host": master_host[0], "port": master_host[1]}
        )
        c2 = pool2.get_connection()
        pool2.release(c2)

        assert len(pool2._available_connections) == 1

        pool2.release(c1)
        assert len(pool2._available_connections) == 1

    def test_repr_contains_db_info_tcp(self):
        connection_kwargs = {
            "host": "localhost",
            "port": 6379,
            "db": 1,
            "client_name": "test-client",
        }
        pool = self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=redis.Connection
        )
        expected = "host=localhost,port=6379,db=1,client_name=test-client"
        assert expected in repr(pool)

    def test_repr_contains_db_info_unix(self):
        connection_kwargs = {"path": "/abc", "db": 1, "client_name": "test-client"}
        pool = self.get_pool(
            connection_kwargs=connection_kwargs,
            connection_class=redis.UnixDomainSocketConnection,
        )
        expected = "path=/abc,db=1,client_name=test-client"
        assert expected in repr(pool)

    def test_pool_disconnect(self, master_host):
        connection_kwargs = {
            "host": master_host[0],
            "port": master_host[1],
        }
        pool = self.get_pool(connection_kwargs=connection_kwargs)

        conn = pool.get_connection()
        pool.disconnect()
        assert not conn._sock

        conn.connect()
        pool.disconnect(inuse_connections=False)
        assert conn._sock


class TestBlockingConnectionPool:
    def get_pool(self, connection_kwargs=None, max_connections=10, timeout=20):
        connection_kwargs = connection_kwargs or {}
        pool = redis.BlockingConnectionPool(
            connection_class=DummyConnection,
            max_connections=max_connections,
            timeout=timeout,
            **connection_kwargs,
        )
        return pool

    def test_connection_creation(self, master_host):
        connection_kwargs = {
            "foo": "bar",
            "biz": "baz",
            "host": master_host[0],
            "port": master_host[1],
        }

        pool = self.get_pool(connection_kwargs=connection_kwargs)
        connection = pool.get_connection()
        assert isinstance(connection, DummyConnection)
        assert connection.kwargs == connection_kwargs

    def test_multiple_connections(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = pool.get_connection()
        c2 = pool.get_connection()
        assert c1 != c2

    def test_connection_pool_blocks_until_timeout(self, master_host):
        "When out of connections, block for timeout seconds, then raise"
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(
            max_connections=1, timeout=0.1, connection_kwargs=connection_kwargs
        )
        pool.get_connection()

        start = time.monotonic()
        with pytest.raises(redis.ConnectionError):
            pool.get_connection()
        # we should have waited at least 0.1 seconds
        assert time.monotonic() - start >= 0.1

    def test_connection_pool_blocks_until_conn_available(self, master_host):
        """
        When out of connections, block until another connection is released
        to the pool
        """
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(
            max_connections=1, timeout=2, connection_kwargs=connection_kwargs
        )
        c1 = pool.get_connection()

        def target():
            time.sleep(0.1)
            pool.release(c1)

        start = time.monotonic()
        Thread(target=target).start()
        pool.get_connection()
        assert time.monotonic() - start >= 0.1

    def test_reuse_previously_released_connection(self, master_host):
        connection_kwargs = {"host": master_host[0], "port": master_host[1]}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        c1 = pool.get_connection()
        pool.release(c1)
        c2 = pool.get_connection()
        assert c1 == c2

    def test_repr_contains_db_info_tcp(self):
        pool = redis.ConnectionPool(
            host="localhost", port=6379, client_name="test-client"
        )
        expected = "host=localhost,port=6379,client_name=test-client"
        assert expected in repr(pool)

    def test_repr_contains_db_info_unix(self):
        pool = redis.ConnectionPool(
            connection_class=redis.UnixDomainSocketConnection,
            path="abc",
            db=0,
            client_name="test-client",
        )
        expected = "path=abc,db=0,client_name=test-client"
        assert expected in repr(pool)

    @pytest.mark.onlynoncluster
    @skip_if_resp_version(2)
    @skip_if_server_version_lt("7.4.0")
    def test_initialise_pool_with_cache(self, master_host):
        pool = redis.BlockingConnectionPool(
            connection_class=Connection,
            host=master_host[0],
            port=master_host[1],
            protocol=3,
            cache_config=CacheConfig(),
        )
        assert isinstance(pool.get_connection(), CacheProxyConnection)

    def test_pool_disconnect(self, master_host):
        connection_kwargs = {
            "foo": "bar",
            "biz": "baz",
            "host": master_host[0],
            "port": master_host[1],
        }
        pool = self.get_pool(connection_kwargs=connection_kwargs)

        conn = pool.get_connection()
        pool.disconnect()
        assert not conn._sock

        conn.connect()
        pool.disconnect(inuse_connections=False)
        assert conn._sock


class TestConnectionPoolURLParsing:
    def test_hostname(self):
        pool = redis.ConnectionPool.from_url("redis://my.host")
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {"host": "my.host"}

    def test_quoted_hostname(self):
        pool = redis.ConnectionPool.from_url("redis://my %2F host %2B%3D+")
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {"host": "my / host +=+"}

    def test_port(self):
        pool = redis.ConnectionPool.from_url("redis://localhost:6380")
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {"host": "localhost", "port": 6380}

    @skip_if_server_version_lt("6.0.0")
    def test_username(self):
        pool = redis.ConnectionPool.from_url("redis://myuser:@localhost")
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {"host": "localhost", "username": "myuser"}

    @skip_if_server_version_lt("6.0.0")
    def test_quoted_username(self):
        pool = redis.ConnectionPool.from_url(
            "redis://%2Fmyuser%2F%2B name%3D%24+:@localhost"
        )
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "username": "/myuser/+ name=$+",
        }

    def test_password(self):
        pool = redis.ConnectionPool.from_url("redis://:mypassword@localhost")
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {"host": "localhost", "password": "mypassword"}

    def test_quoted_password(self):
        pool = redis.ConnectionPool.from_url(
            "redis://:%2Fmypass%2F%2B word%3D%24+@localhost"
        )
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "password": "/mypass/+ word=$+",
        }

    @skip_if_server_version_lt("6.0.0")
    def test_username_and_password(self):
        pool = redis.ConnectionPool.from_url("redis://myuser:mypass@localhost")
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "username": "myuser",
            "password": "mypass",
        }

    def test_db_as_argument(self):
        pool = redis.ConnectionPool.from_url("redis://localhost", db=1)
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {"host": "localhost", "db": 1}

    def test_db_in_path(self):
        pool = redis.ConnectionPool.from_url("redis://localhost/2", db=1)
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {"host": "localhost", "db": 2}

    def test_db_in_querystring(self):
        pool = redis.ConnectionPool.from_url("redis://localhost/2?db=3", db=1)
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {"host": "localhost", "db": 3}

    def test_extra_typed_querystring_options(self):
        pool = redis.ConnectionPool.from_url(
            "redis://localhost/2?socket_timeout=20&socket_connect_timeout=10"
            "&socket_keepalive=&retry_on_timeout=Yes&max_connections=10"
        )

        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "db": 2,
            "socket_timeout": 20.0,
            "socket_connect_timeout": 10.0,
            "retry_on_timeout": True,
        }
        assert pool.max_connections == 10

    def test_boolean_parsing(self):
        for expected, value in (
            (None, None),
            (None, ""),
            (False, 0),
            (False, "0"),
            (False, "f"),
            (False, "F"),
            (False, "False"),
            (False, "n"),
            (False, "N"),
            (False, "No"),
            (True, 1),
            (True, "1"),
            (True, "y"),
            (True, "Y"),
            (True, "Yes"),
        ):
            assert expected is to_bool(value)

    def test_client_name_in_querystring(self):
        pool = redis.ConnectionPool.from_url("redis://location?client_name=test-client")
        assert pool.connection_kwargs["client_name"] == "test-client"

    def test_invalid_extra_typed_querystring_options(self):
        with pytest.raises(ValueError):
            redis.ConnectionPool.from_url(
                "redis://localhost/2?socket_timeout=_&socket_connect_timeout=abc"
            )

    def test_extra_querystring_options(self):
        pool = redis.ConnectionPool.from_url("redis://localhost?a=1&b=2")
        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {"host": "localhost", "a": "1", "b": "2"}

    def test_calling_from_subclass_returns_correct_instance(self):
        pool = redis.BlockingConnectionPool.from_url("redis://localhost")
        assert isinstance(pool, redis.BlockingConnectionPool)

    def test_client_creates_connection_pool(self):
        r = redis.Redis.from_url("redis://myhost")
        assert r.connection_pool.connection_class == redis.Connection
        assert r.connection_pool.connection_kwargs == {"host": "myhost"}

    def test_invalid_scheme_raises_error(self):
        with pytest.raises(ValueError) as cm:
            redis.ConnectionPool.from_url("localhost")
        assert str(cm.value) == (
            "Redis URL must specify one of the following schemes "
            "(redis://, rediss://, unix://)"
        )

    def test_invalid_scheme_raises_error_when_double_slash_missing(self):
        with pytest.raises(ValueError) as cm:
            redis.ConnectionPool.from_url("redis:foo.bar.com:12345")
        assert str(cm.value) == (
            "Redis URL must specify one of the following schemes "
            "(redis://, rediss://, unix://)"
        )


class TestBlockingConnectionPoolURLParsing:
    def test_extra_typed_querystring_options(self):
        pool = redis.BlockingConnectionPool.from_url(
            "redis://localhost/2?socket_timeout=20&socket_connect_timeout=10"
            "&socket_keepalive=&retry_on_timeout=Yes&max_connections=10&timeout=42"
        )

        assert pool.connection_class == redis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "db": 2,
            "socket_timeout": 20.0,
            "socket_connect_timeout": 10.0,
            "retry_on_timeout": True,
        }
        assert pool.max_connections == 10
        assert pool.timeout == 42.0

    def test_invalid_extra_typed_querystring_options(self):
        with pytest.raises(ValueError):
            redis.BlockingConnectionPool.from_url(
                "redis://localhost/2?timeout=_not_a_float_"
            )


class TestConnectionPoolUnixSocketURLParsing:
    def test_defaults(self):
        pool = redis.ConnectionPool.from_url("unix:///socket")
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {"path": "/socket"}

    @skip_if_server_version_lt("6.0.0")
    def test_username(self):
        pool = redis.ConnectionPool.from_url("unix://myuser:@/socket")
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {"path": "/socket", "username": "myuser"}

    @skip_if_server_version_lt("6.0.0")
    def test_quoted_username(self):
        pool = redis.ConnectionPool.from_url(
            "unix://%2Fmyuser%2F%2B name%3D%24+:@/socket"
        )
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "username": "/myuser/+ name=$+",
        }

    def test_password(self):
        pool = redis.ConnectionPool.from_url("unix://:mypassword@/socket")
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {"path": "/socket", "password": "mypassword"}

    def test_quoted_password(self):
        pool = redis.ConnectionPool.from_url(
            "unix://:%2Fmypass%2F%2B word%3D%24+@/socket"
        )
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "password": "/mypass/+ word=$+",
        }

    def test_quoted_path(self):
        pool = redis.ConnectionPool.from_url(
            "unix://:mypassword@/my%2Fpath%2Fto%2F..%2F+_%2B%3D%24ocket"
        )
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/my/path/to/../+_+=$ocket",
            "password": "mypassword",
        }

    def test_db_as_argument(self):
        pool = redis.ConnectionPool.from_url("unix:///socket", db=1)
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {"path": "/socket", "db": 1}

    def test_db_in_querystring(self):
        pool = redis.ConnectionPool.from_url("unix:///socket?db=2", db=1)
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {"path": "/socket", "db": 2}

    def test_client_name_in_querystring(self):
        pool = redis.ConnectionPool.from_url("redis://location?client_name=test-client")
        assert pool.connection_kwargs["client_name"] == "test-client"

    def test_extra_querystring_options(self):
        pool = redis.ConnectionPool.from_url("unix:///socket?a=1&b=2")
        assert pool.connection_class == redis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {"path": "/socket", "a": "1", "b": "2"}

    def test_connection_class_override(self):
        class MyConnection(redis.UnixDomainSocketConnection):
            pass

        pool = redis.ConnectionPool.from_url(
            "unix:///socket", connection_class=MyConnection
        )
        assert pool.connection_class == MyConnection


@pytest.mark.skipif(not SSL_AVAILABLE, reason="SSL not installed")
class TestSSLConnectionURLParsing:
    def test_host(self):
        pool = redis.ConnectionPool.from_url("rediss://my.host")
        assert pool.connection_class == redis.SSLConnection
        assert pool.connection_kwargs == {"host": "my.host"}

    def test_connection_class_override(self):
        class MyConnection(redis.SSLConnection):
            pass

        pool = redis.ConnectionPool.from_url(
            "rediss://my.host", connection_class=MyConnection
        )
        assert pool.connection_class == MyConnection

    def test_cert_reqs_options(self):
        class DummyConnectionPool(redis.ConnectionPool):
            def get_connection(self):
                return self.make_connection()

        pool = DummyConnectionPool.from_url("rediss://?ssl_cert_reqs=none")
        assert pool.get_connection().cert_reqs == ssl.CERT_NONE

        pool = DummyConnectionPool.from_url("rediss://?ssl_cert_reqs=optional")
        assert pool.get_connection().cert_reqs == ssl.CERT_OPTIONAL

        pool = DummyConnectionPool.from_url("rediss://?ssl_cert_reqs=required")
        assert pool.get_connection().cert_reqs == ssl.CERT_REQUIRED

        pool = DummyConnectionPool.from_url("rediss://?ssl_check_hostname=False")
        assert pool.get_connection().check_hostname is False

        pool = DummyConnectionPool.from_url("rediss://?ssl_check_hostname=True")
        assert pool.get_connection().check_hostname is True

    def test_ssl_flags_config_parsing(self):
        class DummyConnectionPool(redis.ConnectionPool):
            def get_connection(self):
                return self.make_connection()

        pool = DummyConnectionPool.from_url(
            "rediss://?ssl_include_verify_flags=VERIFY_X509_STRICT,VERIFY_CRL_CHECK_CHAIN"
        )

        assert pool.get_connection().ssl_include_verify_flags == [
            ssl.VerifyFlags.VERIFY_X509_STRICT,
            ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN,
        ]

        pool = DummyConnectionPool.from_url(
            "rediss://?ssl_include_verify_flags=[VERIFY_X509_STRICT, VERIFY_CRL_CHECK_CHAIN]"
        )

        assert pool.get_connection().ssl_include_verify_flags == [
            ssl.VerifyFlags.VERIFY_X509_STRICT,
            ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN,
        ]

        pool = DummyConnectionPool.from_url(
            "rediss://?ssl_exclude_verify_flags=VERIFY_X509_STRICT, VERIFY_CRL_CHECK_CHAIN"
        )

        assert pool.get_connection().ssl_exclude_verify_flags == [
            ssl.VerifyFlags.VERIFY_X509_STRICT,
            ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN,
        ]

        pool = DummyConnectionPool.from_url(
            "rediss://?ssl_include_verify_flags=VERIFY_X509_STRICT, VERIFY_CRL_CHECK_CHAIN&ssl_exclude_verify_flags=VERIFY_CRL_CHECK_LEAF"
        )

        assert pool.get_connection().ssl_include_verify_flags == [
            ssl.VerifyFlags.VERIFY_X509_STRICT,
            ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN,
        ]
        assert pool.get_connection().ssl_exclude_verify_flags == [
            ssl.VerifyFlags.VERIFY_CRL_CHECK_LEAF,
        ]

    def test_ssl_flags_config_invalid_flag(self):
        class DummyConnectionPool(redis.ConnectionPool):
            def get_connection(self):
                return self.make_connection()

        with pytest.raises(ValueError):
            DummyConnectionPool.from_url(
                "rediss://?ssl_include_verify_flags=[VERIFY_X509,VERIFY_CRL_CHECK_CHAIN]"
            )

        with pytest.raises(ValueError):
            DummyConnectionPool.from_url(
                "rediss://?ssl_exclude_verify_flags=[VERIFY_X509_STRICT1, VERIFY_CRL_CHECK_CHAIN]"
            )


class TestConnection:
    def test_on_connect_error(self):
        """
        An error in Connection.on_connect should disconnect from the server
        see for details: https://github.com/andymccurdy/redis-py/issues/368
        """
        # this assumes the Redis server being tested against doesn't have
        # 9999 databases ;)
        bad_connection = redis.Redis(db=9999)
        # an error should be raised on connect
        with pytest.raises(redis.RedisError):
            bad_connection.info()
        pool = bad_connection.connection_pool
        assert len(pool._available_connections) == 1
        assert not pool._available_connections[0].connection._sock

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    def test_busy_loading_disconnects_socket(self, r):
        """
        If Redis raises a LOADING error, the connection should be
        disconnected and a BusyLoadingError raised
        """
        with pytest.raises(redis.BusyLoadingError):
            r.execute_command("DEBUG", "ERROR", "LOADING fake message")
        assert not r.connection._sock

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    def test_busy_loading_from_pipeline_immediate_command(self, r):
        """
        BusyLoadingErrors should raise from Pipelines that execute a
        command immediately, like WATCH does.
        """
        pipe = r.pipeline()
        with pytest.raises(redis.BusyLoadingError):
            pipe.immediate_execute_command("DEBUG", "ERROR", "LOADING fake message")
        pool = r.connection_pool
        assert pipe.connection
        assert pipe.connection in pool._in_use_connections
        assert not pipe.connection._sock

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    def test_busy_loading_from_pipeline(self, r):
        """
        BusyLoadingErrors should be raised from a pipeline execution
        regardless of the raise_on_error flag.
        """
        pipe = r.pipeline()
        pipe.execute_command("DEBUG", "ERROR", "LOADING fake message")
        with pytest.raises(redis.BusyLoadingError):
            pipe.execute()
        pool = r.connection_pool
        assert not pipe.connection
        assert len(pool._available_connections) == 1
        assert not pool._available_connections[0].connection._sock

    @skip_if_server_version_lt("2.8.8")
    @skip_if_redis_enterprise()
    def test_read_only_error(self, r):
        "READONLY errors get turned into ReadOnlyError exceptions"
        with pytest.raises(redis.ReadOnlyError):
            r.execute_command("DEBUG", "ERROR", "READONLY blah blah")

    def test_oom_error(self, r):
        "OOM errors get turned into OutOfMemoryError exceptions"
        with pytest.raises(redis.OutOfMemoryError):
            # note: don't use the DEBUG OOM command since it's not the same
            # as the db being full
            r.execute_command("DEBUG", "ERROR", "OOM blah blah")

    def test_connect_from_url_tcp(self):
        connection = redis.Redis.from_url("redis://localhost:6379?db=0")
        pool = connection.connection_pool

        assert re.match(
            r"< .*?([^\.]+) \( < .*?([^\.]+) \( (.+) \) > \) >", repr(pool), re.VERBOSE
        ).groups() == (
            "ConnectionPool",
            "Connection",
            "db=0,host=localhost,port=6379",
        )

    def test_connect_from_url_unix(self):
        connection = redis.Redis.from_url("unix:///path/to/socket")
        pool = connection.connection_pool

        assert re.match(
            r"< .*?([^\.]+) \( < .*?([^\.]+) \( (.+) \) > \) >", repr(pool), re.VERBOSE
        ).groups() == (
            "ConnectionPool",
            "UnixDomainSocketConnection",
            "path=/path/to/socket",
        )

    @skip_if_redis_enterprise()
    def test_connect_no_auth_configured(self, r):
        """
        AuthenticationError should be raised when the server is not configured with auth
        but credentials are supplied by the user.
        """
        # Redis < 6
        with pytest.raises(redis.AuthenticationError):
            r.execute_command(
                "DEBUG", "ERROR", "ERR Client sent AUTH, but no password is set"
            )

        # Redis >= 6
        with pytest.raises(redis.AuthenticationError):
            r.execute_command(
                "DEBUG",
                "ERROR",
                "ERR AUTH <password> called without any password "
                "configured for the default user. Are you sure "
                "your configuration is correct?",
            )

    @skip_if_redis_enterprise()
    def test_connect_invalid_auth_credentials_supplied(self, r):
        """
        AuthenticationError should be raised when sending invalid username/password
        """
        # Redis < 6
        with pytest.raises(redis.AuthenticationError):
            r.execute_command("DEBUG", "ERROR", "ERR invalid password")

        # Redis >= 6
        with pytest.raises(redis.AuthenticationError):
            r.execute_command("DEBUG", "ERROR", "WRONGPASS")


@pytest.mark.onlynoncluster
class TestMultiConnectionClient:
    @pytest.fixture()
    def r(self, request):
        return _get_client(redis.Redis, request, single_connection_client=False)

    def test_multi_connection_command(self, r):
        assert not r.connection
        assert r.set("a", "123")
        assert r.get("a") == b"123"


@pytest.mark.onlynoncluster
class TestHealthCheck:
    interval = 60

    @pytest.fixture()
    def r(self, request):
        return _get_client(redis.Redis, request, health_check_interval=self.interval)

    def assert_interval_advanced(self, connection):
        diff = connection.next_health_check - time.monotonic()
        assert self.interval > diff > (self.interval - 1)

    def test_health_check_runs(self, r):
        r.connection.next_health_check = time.monotonic() - 1
        r.connection.check_health()
        self.assert_interval_advanced(r.connection)

    def test_arbitrary_command_invokes_health_check(self, r):
        # invoke a command to make sure the connection is entirely setup
        r.get("foo")
        r.connection.next_health_check = time.monotonic()
        with mock.patch.object(
            r.connection, "send_command", wraps=r.connection.send_command
        ) as m:
            r.get("foo")
            m.assert_called_with("PING", check_health=False)

        self.assert_interval_advanced(r.connection)

    def test_arbitrary_command_advances_next_health_check(self, r):
        r.get("foo")
        next_health_check = r.connection.next_health_check
        r.get("foo")
        assert next_health_check < r.connection.next_health_check

    def test_health_check_not_invoked_within_interval(self, r):
        r.get("foo")
        with mock.patch.object(
            r.connection, "send_command", wraps=r.connection.send_command
        ) as m:
            r.get("foo")
            ping_call_spec = (("PING",), {"check_health": False})
            assert ping_call_spec not in m.call_args_list

    def test_health_check_in_pipeline(self, r):
        with r.pipeline(transaction=False) as pipe:
            pipe.connection = pipe.connection_pool.get_connection()
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                responses = pipe.set("foo", "bar").get("foo").execute()
                m.assert_any_call("PING", check_health=False)
                assert responses == [True, b"bar"]

    def test_health_check_in_transaction(self, r):
        with r.pipeline(transaction=True) as pipe:
            pipe.connection = pipe.connection_pool.get_connection()
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                responses = pipe.set("foo", "bar").get("foo").execute()
                m.assert_any_call("PING", check_health=False)
                assert responses == [True, b"bar"]

    def test_health_check_in_watched_pipeline(self, r):
        r.set("foo", "bar")
        with r.pipeline(transaction=False) as pipe:
            pipe.connection = pipe.connection_pool.get_connection()
            pipe.connection.next_health_check = 0
            with mock.patch.object(
                pipe.connection, "send_command", wraps=pipe.connection.send_command
            ) as m:
                pipe.watch("foo")
                # the health check should be called when watching
                m.assert_called_with("PING", check_health=False)
                self.assert_interval_advanced(pipe.connection)
                assert pipe.get("foo") == b"bar"

                # reset the mock to clear the call list and schedule another
                # health check
                m.reset_mock()
                pipe.connection.next_health_check = 0

                pipe.multi()
                responses = pipe.set("foo", "not-bar").get("foo").execute()
                assert responses == [True, b"not-bar"]
                m.assert_any_call("PING", check_health=False)

    def test_health_check_in_pubsub_before_subscribe(self, r):
        "A health check happens before the first [p]subscribe"
        p = r.pubsub()
        p.connection = p.connection_pool.get_connection()
        p.connection.next_health_check = 0
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            assert not p.subscribed
            p.subscribe("foo")
            # the connection is not yet in pubsub mode, so the normal
            # ping/pong within connection.send_command should check
            # the health of the connection
            m.assert_any_call("PING", check_health=False)
            self.assert_interval_advanced(p.connection)

            subscribe_message = wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"

    def test_health_check_in_pubsub_after_subscribed(self, r):
        """
        Pubsub can handle a new subscribe when it's time to check the
        connection health
        """
        p = r.pubsub()
        p.connection = p.connection_pool.get_connection()
        p.connection.next_health_check = 0
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            p.subscribe("foo")
            subscribe_message = wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            self.assert_interval_advanced(p.connection)
            # because we weren't subscribed when sending the subscribe
            # message to 'foo', the connection's standard check_health ran
            # prior to subscribing.
            m.assert_any_call("PING", check_health=False)

            p.connection.next_health_check = 0
            m.reset_mock()

            p.subscribe("bar")
            # the second subscribe issues exactly only command (the subscribe)
            # and the health check is not invoked
            m.assert_called_once_with("SUBSCRIBE", "bar", check_health=False)

            # since no message has been read since the health check was
            # reset, it should still be 0
            assert p.connection.next_health_check == 0

            subscribe_message = wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            assert wait_for_message(p) is None
            # now that the connection is subscribed, the pubsub health
            # check should have taken over and include the HEALTH_CHECK_MESSAGE
            m.assert_any_call("PING", p.HEALTH_CHECK_MESSAGE, check_health=False)
            self.assert_interval_advanced(p.connection)

    def test_health_check_in_pubsub_poll(self, r):
        """
        Polling a pubsub connection that's subscribed will regularly
        check the connection's health.
        """
        p = r.pubsub()
        p.connection = p.connection_pool.get_connection()
        with mock.patch.object(
            p.connection, "send_command", wraps=p.connection.send_command
        ) as m:
            p.subscribe("foo")
            subscribe_message = wait_for_message(p)
            assert subscribe_message["type"] == "subscribe"
            self.assert_interval_advanced(p.connection)

            # polling the connection before the health check interval
            # doesn't result in another health check
            m.reset_mock()
            next_health_check = p.connection.next_health_check
            assert wait_for_message(p) is None
            assert p.connection.next_health_check == next_health_check
            m.assert_not_called()

            # reset the health check and poll again
            # we should not receive a pong message, but the next_health_check
            # should be advanced
            p.connection.next_health_check = 0
            assert wait_for_message(p) is None
            m.assert_called_with("PING", p.HEALTH_CHECK_MESSAGE, check_health=False)
            self.assert_interval_advanced(p.connection)


class MockDateTime:
    """Context manager for mocking datetime.datetime.now() and time.time()."""

    def __init__(self, start_time=None):
        if start_time is None:
            start_time = datetime.datetime(2024, 1, 1, 12, 0, 0)
        self.current_time = start_time
        self.start_time = start_time

    def advance(self, seconds):
        """Advance the mocked time by the given number of seconds."""
        self.current_time = self.current_time + datetime.timedelta(seconds=seconds)

    def __enter__(self):
        self._datetime_patcher = mock.patch("redis.connection.datetime")
        self._time_patcher = mock.patch("redis.connection.time")

        mock_datetime = self._datetime_patcher.__enter__()
        mock_time = self._time_patcher.__enter__()

        mock_datetime.datetime.now = lambda: self.current_time
        mock_datetime.datetime.side_effect = (
            lambda *args, **kwargs: datetime.datetime(*args, **kwargs)
        )
        mock_time.time = lambda: self.current_time.timestamp()

        return self

    def __exit__(self, *args):
        self._time_patcher.__exit__(*args)
        return self._datetime_patcher.__exit__(*args)


class TestIdleConnectionTimeout:
    """Tests for idle connection timeout functionality."""

    def test_idle_timeout_parameters_validation(self):
        """Test that idle timeout parameters are validated properly."""
        # Valid parameters should work
        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            idle_connection_timeout=10.0,
            idle_check_interval=5.0,
        )
        assert pool.idle_connection_timeout == 10.0
        assert pool.idle_check_interval == 5.0
        pool.close()

        # None for idle_connection_timeout should work (disables feature)
        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            idle_connection_timeout=None,
        )
        assert pool.idle_connection_timeout is None
        pool.close()

        # Invalid idle_connection_timeout should raise ValueError
        with pytest.raises(ValueError, match="idle_connection_timeout"):
            redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=-1.0,
            )

        with pytest.raises(ValueError, match="idle_connection_timeout"):
            redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=0,
            )

        # Invalid idle_check_interval should raise ValueError
        with pytest.raises(ValueError, match="idle_check_interval"):
            redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_check_interval=-1.0,
            )

        with pytest.raises(ValueError, match="idle_check_interval"):
            redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_check_interval=0,
            )

    def test_pool_not_registered_without_timeout(self):
        """Test that pool is not registered when idle_connection_timeout is None."""
        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            idle_connection_timeout=None,
        )
        manager = redis.connection.IdleConnectionCleanupManager.get_instance()
        assert id(pool) not in manager._registered_pools
        pool.close()

    def test_pool_registered_with_timeout(self):
        """Test that pool is registered with manager when idle_connection_timeout is set."""
        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            idle_connection_timeout=10.0,
            idle_check_interval=1.0,
        )
        manager = redis.connection.IdleConnectionCleanupManager.get_instance()
        assert id(pool) in manager._registered_pools
        assert manager._worker_thread is not None
        assert manager._worker_thread.is_alive()
        pool.close()

    def test_pool_unregistered_on_close(self):
        """Test that pool is unregistered from manager when closed."""
        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            idle_connection_timeout=10.0,
            idle_check_interval=1.0,
        )
        manager = redis.connection.IdleConnectionCleanupManager.get_instance()
        pool_id = id(pool)
        assert pool_id in manager._registered_pools
        pool.close()
        # After close, pool should be unregistered
        assert pool_id not in manager._registered_pools

    def test_idle_connections_cleaned_up(self):
        """Test that idle connections are actually cleaned up."""
        with MockDateTime() as mock_time:
            pool = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=1.0,  # 1 second timeout
                idle_check_interval=0.5,  # Check every 0.5 seconds
            )

            # Get and release a connection
            conn1 = pool.get_connection()
            pool.release(conn1)

            # Should have 1 available connection
            assert len(pool._available_connections) == 1
            assert pool._created_connections == 1

            # Advance time past the idle timeout
            mock_time.advance(1.5)

            # Manually trigger cleanup
            pool._cleanup_idle_connections()

            # The idle connection should have been cleaned up
            assert len(pool._available_connections) == 0
            assert pool._created_connections == 0

            # Pool should still work after cleanup
            conn2 = pool.get_connection()
            assert conn2 is not None
            pool.release(conn2)

            pool.close()

    def test_fresh_connections_not_cleaned_up(self):
        """Test that recently used connections are not cleaned up."""
        with MockDateTime() as mock_time:
            pool = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=2.0,
                idle_check_interval=0.5,
            )

            # Get and release a connection
            conn1 = pool.get_connection()
            pool.release(conn1)

            # Advance time less than the timeout
            mock_time.advance(0.8)

            # Manually trigger cleanup
            pool._cleanup_idle_connections()

            # Connection should still be available
            assert len(pool._available_connections) == 1

            pool.close()

    def test_blocking_pool_idle_timeout(self):
        """Test idle timeout with BlockingConnectionPool."""
        with MockDateTime() as mock_time:
            pool = redis.BlockingConnectionPool(
                connection_class=DummyConnection,
                max_connections=5,
                timeout=1,
                idle_connection_timeout=1.0,
                idle_check_interval=0.5,
            )

            # Get and release some connections
            conn1 = pool.get_connection()
            conn2 = pool.get_connection()
            pool.release(conn1)
            pool.release(conn2)

            # Should have 2 connections
            assert len(pool._connections) == 2

            # Advance time past the idle timeout
            mock_time.advance(1.5)

            # Manually trigger cleanup
            pool._cleanup_idle_connections()

            # Connections should be cleaned up
            assert len(pool._connections) == 0

            # Pool should still work
            conn3 = pool.get_connection()
            assert conn3 is not None
            pool.release(conn3)

            pool.close()

    def test_blocking_pool_parameters(self):
        """Test that BlockingConnectionPool accepts idle timeout parameters."""
        pool = redis.BlockingConnectionPool(
            connection_class=DummyConnection,
            max_connections=5,
            timeout=1,
            idle_connection_timeout=10.0,
            idle_check_interval=5.0,
        )
        assert pool.idle_connection_timeout == 10.0
        assert pool.idle_check_interval == 5.0
        pool.close()

    def test_multiple_pools_independent_cleanup(self):
        """Test that multiple pools clean up independently."""
        with MockDateTime() as mock_time:
            pool1 = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=1.0,
                idle_check_interval=0.5,
            )
            pool2 = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=2.0,
                idle_check_interval=0.5,
            )

            # Create connections in both pools
            conn1 = pool1.get_connection()
            conn2 = pool2.get_connection()
            pool1.release(conn1)
            pool2.release(conn2)

            # Advance time past pool1's timeout but not pool2's
            mock_time.advance(1.5)

            # Trigger cleanup for both pools
            pool1._cleanup_idle_connections()
            pool2._cleanup_idle_connections()

            # Pool1 should be cleaned up, pool2 should not
            assert len(pool1._available_connections) == 0
            assert len(pool2._available_connections) == 1

            pool1.close()
            pool2.close()

    def test_pool_garbage_collection(self):
        """Test that pool can be garbage collected when no longer referenced."""
        manager = redis.connection.IdleConnectionCleanupManager.get_instance()

        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            idle_connection_timeout=10.0,
            idle_check_interval=0.5,
        )

        pool_id = id(pool)
        # Pool should be registered with manager
        assert pool_id in manager._registered_pools

        # Create a weak reference to the pool
        pool_weak_ref = weakref.ref(pool)

        # Drop the pool reference
        del pool

        # Force garbage collection
        gc.collect()

        # Pool should be garbage collected
        assert pool_weak_ref() is None

        # Manager should eventually clean up the dead pool reference
        # (this happens in _cleanup_dead_pools which is called in the worker loop)

    def test_manager_singleton(self):
        """Test that IdleConnectionCleanupManager is a singleton."""
        manager1 = redis.connection.IdleConnectionCleanupManager.get_instance()
        manager2 = redis.connection.IdleConnectionCleanupManager.get_instance()
        assert manager1 is manager2

    def test_manager_shared_across_pools(self):
        """Test that multiple pools share the same cleanup manager."""
        pool1 = redis.ConnectionPool(
            connection_class=DummyConnection,
            idle_connection_timeout=10.0,
        )
        pool2 = redis.ConnectionPool(
            connection_class=DummyConnection,
            idle_connection_timeout=5.0,
        )

        manager = redis.connection.IdleConnectionCleanupManager.get_instance()

        # Both pools should be registered with the same manager
        assert id(pool1) in manager._registered_pools
        assert id(pool2) in manager._registered_pools

        # Manager should have only one worker thread
        assert manager._worker_thread is not None
        assert manager._worker_thread.is_alive()

        pool1.close()
        pool2.close()

    def test_manager_connection_release_notification(self):
        """Test that manager is notified when connections are released."""
        with MockDateTime() as mock_time:
            pool = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=10.0,
                idle_check_interval=5.0,
            )

            manager = redis.connection.IdleConnectionCleanupManager.get_instance()
            pool_id = id(pool)

            # Get and release a connection
            conn = pool.get_connection()
            release_time = time.time()
            pool.release(conn)

            # Manager should have metadata for this pool
            assert pool_id in manager._registered_pools
            metadata = manager._registered_pools[pool_id]

            # Check that idle_timeout and check_interval are stored correctly
            assert metadata.idle_timeout == 10.0
            assert metadata.check_interval == 5.0

            pool.close()

    def test_manager_schedules_multiple_pools(self):
        """Test that manager correctly schedules cleanup for multiple pools."""
        with MockDateTime() as mock_time:
            # Create pools with different timeouts
            pool1 = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=5.0,
                idle_check_interval=1.0,
            )
            pool2 = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=10.0,
                idle_check_interval=2.0,
            )

            manager = redis.connection.IdleConnectionCleanupManager.get_instance()

            # Both pools should be in the schedule
            pool_ids_in_schedule = {entry.pool_id for entry in manager._schedule}
            assert id(pool1) in pool_ids_in_schedule
            assert id(pool2) in pool_ids_in_schedule

            pool1.close()
            pool2.close()

    def test_manager_schedules_empty_pool_on_release(self):
        """Test that manager re-registers an empty pool when a connection is released."""
        pool = redis.ConnectionPool(
            connection_class=DummyConnection,
            idle_connection_timeout=10.0,
            idle_check_interval=5.0,
        )

        manager = redis.connection.IdleConnectionCleanupManager.get_instance()
        pool_id = id(pool)

        # Pool should initially be registered and scheduled
        assert pool_id in manager._registered_pools
        initial_schedule = [
            entry for entry in manager._schedule if entry.pool_id == pool_id
        ]
        assert len(initial_schedule) == 1

        # Manually remove from both dict and schedule to simulate empty pool
        with manager._condition:
            manager._registered_pools.pop(pool_id, None)
            manager._schedule = [
                entry for entry in manager._schedule if entry.pool_id != pool_id
            ]

        # Now pool should not be tracked
        assert pool_id not in manager._registered_pools
        schedule = [entry for entry in manager._schedule if entry.pool_id == pool_id]
        assert len(schedule) == 0

        # Release a connection
        conn = pool.get_connection()
        pool.release(conn)

        # Pool should now be re-registered and scheduled
        assert pool_id in manager._registered_pools
        schedule = [entry for entry in manager._schedule if entry.pool_id == pool_id]
        assert len(schedule) == 1

        pool.close()

    def test_no_per_pool_threads(self):
        """Test that creating many pools doesn't create many threads."""
        import threading

        initial_thread_count = threading.active_count()

        pools = []
        for i in range(10):
            pool = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=10.0,
                idle_check_interval=1.0,
            )
            pools.append(pool)

        # Should only have one additional thread (the manager's worker)
        # Not 10 threads (one per pool)
        final_thread_count = threading.active_count()
        new_threads = final_thread_count - initial_thread_count

        # Allow some tolerance for test infrastructure threads, but should be much less than 10
        assert new_threads <= 2, f"Expected at most 2 new threads, got {new_threads}"

        for pool in pools:
            pool.close()

    def test_manager_automatically_cleans_idle_connections(self):
        """Integration test: Manager automatically cleans up idle connections without manual trigger."""
        import time

        with MockDateTime() as mock_time:
            pool = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=1.0,  # 1 second timeout
                idle_check_interval=0.5,  # Check every 0.5 seconds
            )

            try:
                # Get and release a connection
                conn1 = pool.get_connection()
                pool.release(conn1)

                # Should have 1 available connection
                assert len(pool._available_connections) == 1
                assert pool._created_connections == 1

                # Advance time past the idle timeout
                mock_time.advance(1.5)

                # Notify the manager to wake up and check (simulates time passing)
                manager = redis.connection.IdleConnectionCleanupManager.get_instance()
                with manager._condition:
                    manager._condition.notify()

                # Poll until the worker thread processes (with timeout)
                deadline = time.time() + 1.0  # 1 second timeout
                while time.time() < deadline:
                    if len(pool._available_connections) == 0:
                        break
                    time.sleep(0.01)

                # The manager should have cleaned it up automatically
                assert len(pool._available_connections) == 0
                assert pool._created_connections == 0
            finally:
                pool.close()

    def test_manager_reschedules_pools_after_cleanup(self):
        """Integration test: Manager reschedules pools that still have connections after cleanup."""
        import time

        with MockDateTime() as mock_time:
            pool = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=1.5,  # 1.5 seconds timeout
                idle_check_interval=0.5,  # Check every 0.5 seconds
            )

            try:
                manager = redis.connection.IdleConnectionCleanupManager.get_instance()

                # Get and release two connections
                conn1 = pool.get_connection()
                conn2 = pool.get_connection()
                pool.release(conn1)

                # Advance time, then release conn2
                mock_time.advance(1.0)
                pool.release(conn2)

                # Now we have:
                # - conn1: idle for 1.0s
                # - conn2: idle for 0s

                # Advance time for first cleanup cycle
                mock_time.advance(0.6)  # Total: conn1 at 1.6s, conn2 at 0.6s

                # Wake up manager
                with manager._condition:
                    manager._condition.notify()

                # Poll until first cleanup happens
                deadline = time.time() + 1.0
                while time.time() < deadline:
                    if len(pool._available_connections) == 1:
                        break
                    time.sleep(0.01)

                # conn1 should be cleaned (>1.5s), conn2 should remain (<1.5s)
                assert len(pool._available_connections) == 1
                assert pool._created_connections == 1

                # Verify pool was rescheduled by advancing time again
                mock_time.advance(1.0)  # Total: conn2 at 1.6s

                # Wake up manager
                with manager._condition:
                    manager._condition.notify()

                # Poll until second cleanup happens
                deadline = time.time() + 1.0
                while time.time() < deadline:
                    if len(pool._available_connections) == 0:
                        break
                    time.sleep(0.01)

                # Now conn2 should also be cleaned
                assert len(pool._available_connections) == 0
                assert pool._created_connections == 0
            finally:
                pool.close()

    def test_manager_removes_empty_pools_from_tracking(self):
        """Integration test: Manager removes empty pools from its internal tracking."""
        import time

        with MockDateTime() as mock_time:
            pool = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=1.0,  # 1 second timeout
                idle_check_interval=0.5,  # Check every 0.5 seconds
            )

            try:
                # Get and release a connection
                conn = pool.get_connection()
                pool.release(conn)

                # Pool should be registered
                manager = redis.connection.IdleConnectionCleanupManager.get_instance()
                pool_id = id(pool)
                assert pool_id in manager._registered_pools

                # Advance time past timeout
                mock_time.advance(1.5)

                # Wake up manager
                with manager._condition:
                    manager._condition.notify()

                # Poll until cleanup happens
                deadline = time.time() + 1.0
                while time.time() < deadline:
                    if pool_id not in manager._registered_pools:
                        break
                    time.sleep(0.01)

                # Pool should be empty
                assert len(pool._available_connections) == 0

                # Pool should be removed from manager's tracking
                assert pool_id not in manager._registered_pools
            finally:
                pool.close()

    def test_manager_schedules_at_correct_time(self):
        """Integration test: Manager schedules cleanups at the correct time based on idle_timeout."""
        import time

        with MockDateTime() as mock_time:
            pool = redis.ConnectionPool(
                connection_class=DummyConnection,
                idle_connection_timeout=2.0,  # 2 seconds timeout
                idle_check_interval=0.5,  # Check every 0.5 seconds
            )

            try:
                manager = redis.connection.IdleConnectionCleanupManager.get_instance()

                # Get and release a connection
                conn = pool.get_connection()
                pool.release(conn)

                # Connection should NOT be cleaned up before timeout
                mock_time.advance(1.0)  # 1 second - less than 2 second timeout

                # Wake up manager
                with manager._condition:
                    manager._condition.notify()

                # Give worker thread time to process, but it shouldn't clean anything
                time.sleep(0.05)

                assert len(pool._available_connections) == 1
                assert pool._created_connections == 1

                # Connection SHOULD be cleaned up after timeout
                mock_time.advance(1.5)  # Total 2.5 seconds - more than 2 second timeout

                # Wake up manager
                with manager._condition:
                    manager._condition.notify()

                # Poll until cleanup happens
                deadline = time.time() + 1.0
                while time.time() < deadline:
                    if len(pool._available_connections) == 0:
                        break
                    time.sleep(0.01)

                assert len(pool._available_connections) == 0
                assert pool._created_connections == 0
            finally:
                pool.close()
