import socket

from redis._parsers.socket import SENTINEL
from redis.retry import Retry
from redis.sentinel import SentinelManagedConnection
from redis.backoff import NoBackoff
from unittest import mock


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
    assert connection_pool.get_master_address.call_count == 3
    conn.disconnect()


class TestSentinelManagedConnectionReadResponseTimeout:
    """
    Tests for timeout parameter propagation in SentinelManagedConnection.read_response().
    """

    def test_read_response_accepts_timeout_parameter(self, master_host):
        """
        Test that SentinelManagedConnection.read_response() accepts a timeout parameter.
        """
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        conn = SentinelManagedConnection(connection_pool=connection_pool)

        # Mock the parent class's read_response to verify timeout is passed
        with mock.patch.object(
            SentinelManagedConnection.__bases__[0],
            "read_response",
            return_value=b"OK",
        ) as mock_read_response:
            conn.read_response(timeout=0.5)
            mock_read_response.assert_called_once_with(
                disable_decoding=False,
                timeout=0.5,
                disconnect_on_error=False,
                push_request=False,
            )

    def test_read_response_timeout_default_is_sentinel(self, master_host):
        """
        Test that the default timeout value is SENTINEL (not modified).
        """
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        conn = SentinelManagedConnection(connection_pool=connection_pool)

        # Mock the parent class's read_response to verify default timeout
        with mock.patch.object(
            SentinelManagedConnection.__bases__[0],
            "read_response",
            return_value=b"OK",
        ) as mock_read_response:
            conn.read_response()
            mock_read_response.assert_called_once_with(
                disable_decoding=False,
                timeout=SENTINEL,
                disconnect_on_error=False,
                push_request=False,
            )

    def test_read_response_timeout_none_passed_through(self, master_host):
        """
        Test that timeout=None is passed through (for blocking behavior).
        """
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        conn = SentinelManagedConnection(connection_pool=connection_pool)

        # Mock the parent class's read_response to verify timeout=None is passed
        with mock.patch.object(
            SentinelManagedConnection.__bases__[0],
            "read_response",
            return_value=b"OK",
        ) as mock_read_response:
            conn.read_response(timeout=None)
            mock_read_response.assert_called_once_with(
                disable_decoding=False,
                timeout=None,
                disconnect_on_error=False,
                push_request=False,
            )

    def test_read_response_timeout_zero_passed_through(self, master_host):
        """
        Test that timeout=0 is passed through (for non-blocking behavior).
        """
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        conn = SentinelManagedConnection(connection_pool=connection_pool)

        # Mock the parent class's read_response to verify timeout=0 is passed
        with mock.patch.object(
            SentinelManagedConnection.__bases__[0],
            "read_response",
            return_value=b"OK",
        ) as mock_read_response:
            conn.read_response(timeout=0)
            mock_read_response.assert_called_once_with(
                disable_decoding=False,
                timeout=0,
                disconnect_on_error=False,
                push_request=False,
            )

    def test_read_response_all_parameters_passed_through(self, master_host):
        """
        Test that all parameters including timeout are correctly passed to parent.
        """
        connection_pool = mock.Mock()
        connection_pool.get_master_address = mock.Mock(
            return_value=(master_host[0], master_host[1])
        )
        connection_pool.is_master = True
        conn = SentinelManagedConnection(connection_pool=connection_pool)

        # Mock the parent class's read_response to verify all params
        with mock.patch.object(
            SentinelManagedConnection.__bases__[0],
            "read_response",
            return_value=b"OK",
        ) as mock_read_response:
            conn.read_response(
                disable_decoding=True,
                timeout=1.5,
                disconnect_on_error=True,
                push_request=True,
            )
            mock_read_response.assert_called_once_with(
                disable_decoding=True,
                timeout=1.5,
                disconnect_on_error=True,
                push_request=True,
            )
