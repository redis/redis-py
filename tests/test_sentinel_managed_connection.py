import socket
from unittest import mock

from redis import Connection
from redis.retry import Retry
from redis.sentinel import SentinelManagedConnection
from redis.backoff import NoBackoff


def test_connect_retry_on_timeout_error(master_host):
    """Test that the _connect function is retried in case of a timeout"""
    connection_pool = mock.Mock()
    connection_pool.get_master_address = mock.Mock(
        return_value=(master_host[0], master_host[1])
    )
    conn = SentinelManagedConnection(
        retry_on_timeout=True,
        retry=Retry(NoBackoff(), 3),
        connection_pool=connection_pool,
    )
    original_super_connect = Connection._connect.__get__(conn, Connection)

    with mock.patch.object(
        Connection, "_connect", new_callable=mock.Mock
    ) as mock_super_connect:

        def side_effect(*args, **kwargs):
            if mock_super_connect.call_count <= 2:
                raise socket.timeout()
            return original_super_connect(*args, **kwargs)

        mock_super_connect.side_effect = side_effect

        conn.connect()
        assert mock_super_connect.call_count == 3
        assert connection_pool.get_master_address.call_count == 3
        conn.disconnect()


def test_connect_check_health_retry_on_timeout_error(master_host):
    """Test that the _connect function is retried in case of a timeout"""
    connection_pool = mock.Mock()
    connection_pool.get_master_address = mock.Mock(
        return_value=(master_host[0], master_host[1])
    )
    conn = SentinelManagedConnection(
        retry_on_timeout=True,
        retry=Retry(NoBackoff(), 3),
        connection_pool=connection_pool,
    )
    original_super_connect = Connection._connect.__get__(conn, Connection)

    with mock.patch.object(
        Connection, "_connect", new_callable=mock.Mock
    ) as mock_super_connect:

        def side_effect(*args, **kwargs):
            if mock_super_connect.call_count <= 2:
                raise socket.timeout()
            return original_super_connect(*args, **kwargs)

        mock_super_connect.side_effect = side_effect

        conn.connect_check_health()
        assert mock_super_connect.call_count == 3
        assert connection_pool.get_master_address.call_count == 3
        conn.disconnect()
