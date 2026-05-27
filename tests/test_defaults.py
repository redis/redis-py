import inspect
import socket

from redis._defaults import get_default_socket_keepalive_options
from redis._parsers.socket import SENTINEL
from redis.asyncio.client import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.asyncio.connection import AbstractConnection as AsyncAbstractConnection
from redis.asyncio.connection import Connection as AsyncConnection
from redis.client import Redis
from redis.connection import AbstractConnection
from redis.connection import Connection
from redis.connection import UnixDomainSocketConnection


def test_socket_keepalive_signature_defaults_are_true():
    classes = (Redis, Connection, AsyncRedis, AsyncConnection, AsyncRedisCluster)

    for cls in classes:
        default = inspect.signature(cls.__init__).parameters["socket_keepalive"].default
        assert default is True


def test_connection_socket_keepalive_defaults_to_true():
    assert Connection().socket_keepalive is True
    assert AsyncConnection().socket_keepalive is True


def test_connection_socket_read_size_defaults_to_32kb():
    assert (
        inspect.signature(AbstractConnection.__init__)
        .parameters["socket_read_size"]
        .default
        == 32768
    )
    assert (
        inspect.signature(AsyncAbstractConnection.__init__)
        .parameters["socket_read_size"]
        .default
        == 32768
    )
    assert Connection()._socket_read_size == 32768
    assert AsyncConnection()._socket_read_size == 32768


def test_socket_timeout_default():
    classes = (Redis, AbstractConnection, AsyncRedis, AsyncAbstractConnection)

    for cls in classes:
        parameters = inspect.signature(cls.__init__).parameters
        assert parameters["socket_timeout"].default == 5
        assert parameters["socket_connect_timeout"].default == 5

    cluster_parameters = inspect.signature(AsyncRedisCluster.__init__).parameters
    assert cluster_parameters["socket_timeout"].default == 5
    assert cluster_parameters["socket_connect_timeout"].default == 5

    assert Connection().socket_timeout == 5
    assert Connection().socket_connect_timeout == 5
    assert AsyncConnection().socket_timeout == 5
    assert AsyncConnection().socket_connect_timeout == 5
    assert UnixDomainSocketConnection().socket_timeout == 5


def test_default_socket_keepalive_options():
    options = get_default_socket_keepalive_options()

    tcp_keepidle = getattr(socket, "TCP_KEEPIDLE", None)
    if tcp_keepidle is None:
        tcp_keepidle = getattr(socket, "TCP_KEEPALIVE", None)
    if tcp_keepidle is not None:
        assert options[tcp_keepidle] == 30

    tcp_keepintvl = getattr(socket, "TCP_KEEPINTVL", None)
    if tcp_keepintvl is not None:
        assert options[tcp_keepintvl] == 5

    tcp_keepcnt = getattr(socket, "TCP_KEEPCNT", None)
    if tcp_keepcnt is not None:
        assert options[tcp_keepcnt] == 3


def test_socket_keepalive_options_signature_defaults_use_sentinel():
    sync_classes = (Redis, Connection)

    for cls in sync_classes:
        default = (
            inspect.signature(cls.__init__)
            .parameters["socket_keepalive_options"]
            .default
        )
        assert default is SENTINEL

    async_classes = (AsyncRedis, AsyncConnection, AsyncRedisCluster)

    for cls in async_classes:
        default = (
            inspect.signature(cls.__init__)
            .parameters["socket_keepalive_options"]
            .default
        )
        assert default is SENTINEL


def test_connection_socket_keepalive_options_resolve_sentinel_default():
    assert (
        Connection().socket_keepalive_options == get_default_socket_keepalive_options()
    )
    assert (
        AsyncConnection().socket_keepalive_options
        == get_default_socket_keepalive_options()
    )


def test_connection_socket_keepalive_options_default_is_not_shared():
    assert (
        Connection().socket_keepalive_options
        is not Connection().socket_keepalive_options
    )
    assert (
        AsyncConnection().socket_keepalive_options
        is not AsyncConnection().socket_keepalive_options
    )


def test_connection_socket_keepalive_options_none_stays_empty():
    assert Connection(socket_keepalive_options=None).socket_keepalive_options == {}
    assert AsyncConnection(socket_keepalive_options=None).socket_keepalive_options == {}
