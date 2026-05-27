import inspect
import socket

from redis._defaults import get_default_socket_keepalive_options
from redis._parsers.socket import SENTINEL
from redis.asyncio.client import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.asyncio.connection import Connection as AsyncConnection
from redis.client import Redis
from redis.connection import Connection


def test_socket_keepalive_signature_defaults_are_true():
    classes = (Redis, Connection, AsyncRedis, AsyncConnection, AsyncRedisCluster)

    for cls in classes:
        default = inspect.signature(cls.__init__).parameters["socket_keepalive"].default
        assert default is True


def test_connection_socket_keepalive_defaults_to_true():
    assert Connection().socket_keepalive is True
    assert AsyncConnection().socket_keepalive is True


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
