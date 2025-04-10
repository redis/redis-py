import socket
import threading
from typing import List, Union
from unittest.mock import patch

import pytest
from time import sleep

from redis import Redis
from redis.connection import (
    AbstractConnection,
    ConnectionPool,
    BlockingConnectionPool,
    MaintenanceState,
)
from redis.exceptions import ResponseError
from redis.maint_notifications import (
    EndpointType,
    MaintNotificationsConfig,
    NodeMigratingNotification,
    NodeMigratedNotification,
    NodeFailingOverNotification,
    NodeFailedOverNotification,
    MaintNotificationsPoolHandler,
    NodeMovingNotification,
)


AFTER_MOVING_ADDRESS = "1.2.3.4:6379"
DEFAULT_ADDRESS = "12.45.34.56:6379"
MOVING_TIMEOUT = 1

MOVING_NOTIFICATION = NodeMovingNotification(
    id=1,
    new_node_host=AFTER_MOVING_ADDRESS.split(":")[0],
    new_node_port=int(AFTER_MOVING_ADDRESS.split(":")[1]),
    ttl=MOVING_TIMEOUT,
)

MOVING_NONE_NOTIFICATION = NodeMovingNotification(
    id=1,
    new_node_host=None,
    new_node_port=None,
    ttl=MOVING_TIMEOUT,
)


class Helpers:
    """Helper class containing static methods for validation in maintenance notifications tests."""

    @staticmethod
    def validate_in_use_connections_state(
        in_use_connections: List[AbstractConnection],
        expected_state=MaintenanceState.NONE,
        expected_should_reconnect: Union[bool, str] = True,
        expected_host_address=DEFAULT_ADDRESS.split(":")[0],
        expected_socket_timeout=None,
        expected_socket_connect_timeout=None,
        expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
        expected_orig_socket_timeout=None,
        expected_orig_socket_connect_timeout=None,
        expected_current_socket_timeout=None,
        expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
    ):
        """Helper method to validate state of in-use connections."""

        # validate in use connections are still working with set flag for reconnect
        # and timeout is updated
        for connection in in_use_connections:
            if expected_should_reconnect != "any":
                assert connection._should_reconnect == expected_should_reconnect
            assert connection.host == expected_host_address
            assert connection.socket_timeout == expected_socket_timeout
            assert connection.socket_connect_timeout == expected_socket_connect_timeout
            assert connection.orig_host_address == expected_orig_host_address
            assert connection.orig_socket_timeout == expected_orig_socket_timeout
            assert (
                connection.orig_socket_connect_timeout
                == expected_orig_socket_connect_timeout
            )
            if connection._sock is not None:
                assert connection._sock.gettimeout() == expected_current_socket_timeout
                assert connection._sock.connected is True
                if expected_current_peername != "any":
                    assert (
                        connection._sock.getpeername()[0] == expected_current_peername
                    )
            assert connection.maintenance_state == expected_state

    @staticmethod
    def validate_free_connections_state(
        pool,
        should_be_connected_count=0,
        connected_to_tmp_address=False,
        tmp_address=AFTER_MOVING_ADDRESS.split(":")[0],
        expected_state=MaintenanceState.MOVING,
        expected_host_address=DEFAULT_ADDRESS.split(":")[0],
        expected_socket_timeout=None,
        expected_socket_connect_timeout=None,
        expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
        expected_orig_socket_timeout=None,
        expected_orig_socket_connect_timeout=None,
    ):
        """Helper method to validate state of free/available connections."""

        if isinstance(pool, BlockingConnectionPool):
            free_connections = [conn for conn in pool.pool.queue if conn is not None]
        elif isinstance(pool, ConnectionPool):
            free_connections = pool._available_connections
        else:
            raise ValueError(f"Unsupported pool type: {type(pool)}")

        connected_count = 0
        for connection in free_connections:
            assert connection._should_reconnect is False
            assert connection.host == expected_host_address
            assert connection.socket_timeout == expected_socket_timeout
            assert connection.socket_connect_timeout == expected_socket_connect_timeout
            assert connection.orig_host_address == expected_orig_host_address
            assert connection.orig_socket_timeout == expected_orig_socket_timeout
            assert (
                connection.orig_socket_connect_timeout
                == expected_orig_socket_connect_timeout
            )
            assert connection.maintenance_state == expected_state
            if expected_state == MaintenanceState.NONE:
                assert connection.maintenance_notification_hash is None

            if connection._sock is not None:
                assert connection._sock.connected is True
                if connected_to_tmp_address and tmp_address != "any":
                    assert connection._sock.getpeername()[0] == tmp_address
                connected_count += 1
        assert connected_count == should_be_connected_count

    @staticmethod
    def validate_conn_kwargs(
        pool,
        expected_maintenance_state,
        expected_maintenance_notification_hash,
        expected_host_address,
        expected_port,
        expected_socket_timeout,
        expected_socket_connect_timeout,
        expected_orig_host_address,
        expected_orig_socket_timeout,
        expected_orig_socket_connect_timeout,
    ):
        """Helper method to validate connection kwargs."""
        assert pool.connection_kwargs["maintenance_state"] == expected_maintenance_state
        assert (
            pool.connection_kwargs["maintenance_notification_hash"]
            == expected_maintenance_notification_hash
        )
        assert pool.connection_kwargs["host"] == expected_host_address
        assert pool.connection_kwargs["port"] == expected_port
        assert pool.connection_kwargs["socket_timeout"] == expected_socket_timeout
        assert (
            pool.connection_kwargs["socket_connect_timeout"]
            == expected_socket_connect_timeout
        )
        assert (
            pool.connection_kwargs.get("orig_host_address", None)
            == expected_orig_host_address
        )
        assert (
            pool.connection_kwargs.get("orig_socket_timeout", None)
            == expected_orig_socket_timeout
        )
        assert (
            pool.connection_kwargs.get("orig_socket_connect_timeout", None)
            == expected_orig_socket_connect_timeout
        )


class MockSocket:
    """Mock socket that simulates Redis protocol responses."""

    def __init__(self):
        self.connected = False
        self.address = None
        self.sent_data = []
        self.closed = False
        self.command_count = 0
        self.pending_responses = []
        # Track socket timeout changes for maintenance notifications validation
        self.timeout = None
        self.thread_timeouts = {}  # Track last applied timeout per thread
        self.moving_sent = False

    def connect(self, address):
        """Simulate socket connection."""
        self.connected = True
        self.address = address

    def send(self, data):
        """Simulate sending data to Redis."""
        if self.closed:
            raise ConnectionError("Socket is closed")
        self.sent_data.append(data)

        # Analyze the command and prepare appropriate response
        if b"HELLO" in data:
            response = b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.0.0\r\n$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:1\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"
            self.pending_responses.append(response)
        elif b"MAINT_NOTIFICATIONS" in data and b"internal-ip" in data:
            # Simulate error response - activate it only for internal-ip tests
            response = b"+ERROR\r\n"
            self.pending_responses.append(response)
        elif b"SET" in data:
            response = b"+OK\r\n"

            # Check if this is a key that should trigger a push message
            if b"key_receive_migrating_" in data or b"key_receive_migrating" in data:
                # MIGRATING push message before SET key_receive_migrating_X response
                # Format: >3\r\n$9\r\nMIGRATING\r\n:1\r\n:10\r\n (3 elements: MIGRATING, id, ttl)
                migrating_push = ">3\r\n$9\r\nMIGRATING\r\n:1\r\n:10\r\n"
                response = migrating_push.encode() + response
            elif b"key_receive_migrated_" in data or b"key_receive_migrated" in data:
                # MIGRATED push message before SET key_receive_migrated_X response
                # Format: >2\r\n$8\r\nMIGRATED\r\n:1\r\n (2 elements: MIGRATED, id)
                migrated_push = ">2\r\n$8\r\nMIGRATED\r\n:1\r\n"
                response = migrated_push.encode() + response
            elif (
                b"key_receive_failing_over_" in data
                or b"key_receive_failing_over" in data
            ):
                # FAILING_OVER push message before SET key_receive_failing_over_X response
                # Format: >3\r\n$12\r\nFAILING_OVER\r\n:1\r\n:10\r\n (3 elements: FAILING_OVER, id, ttl)
                failing_over_push = ">3\r\n$12\r\nFAILING_OVER\r\n:1\r\n:10\r\n"

                response = failing_over_push.encode() + response
            elif (
                b"key_receive_failed_over_" in data
                or b"key_receive_failed_over" in data
            ):
                # FAILED_OVER push message before SET key_receive_failed_over_X response
                # Format: >2\r\n$11\r\nFAILED_OVER\r\n:1\r\n (2 elements: FAILED_OVER, id)
                failed_over_push = ">2\r\n$11\r\nFAILED_OVER\r\n:1\r\n"
                response = failed_over_push.encode() + response
            elif b"key_receive_moving_none_" in data:
                # MOVING push message before SET key_receive_moving_none_X response
                # Format: >4\r\n$6\r\nMOVING\r\n:1\r\n:1\r\n+null\r\n (4 elements: MOVING, id, ttl, null)
                # Note: Using + instead of $ to send as simple string instead of bulk string
                moving_push = f">4\r\n$6\r\nMOVING\r\n:1\r\n:{MOVING_TIMEOUT}\r\n_\r\n"
                response = moving_push.encode() + response
            elif b"key_receive_moving_" in data:
                # MOVING push message before SET key_receive_moving_X response
                # Format: >4\r\n$6\r\nMOVING\r\n:1\r\n:1\r\n+1.2.3.4:6379\r\n (4 elements: MOVING, id, ttl, host:port)
                # Note: Using + instead of $ to send as simple string instead of bulk string
                moving_push = f">4\r\n$6\r\nMOVING\r\n:1\r\n:{MOVING_TIMEOUT}\r\n+{AFTER_MOVING_ADDRESS}\r\n"
                response = moving_push.encode() + response

            self.pending_responses.append(response)
        elif b"GET" in data:
            # Extract key and provide appropriate response
            if b"hello" in data:
                response = b"$5\r\nworld\r\n"
                self.pending_responses.append(response)
            # Handle specific keys used in tests
            elif b"key_receive_moving_0" in data:
                self.pending_responses.append(b"$8\r\nvalue3_0\r\n")
            elif b"key_receive_migrated_0" in data:
                self.pending_responses.append(b"$13\r\nmigrated_value\r\n")
            elif b"key_receive_migrating" in data:
                self.pending_responses.append(b"$6\r\nvalue2\r\n")
            elif b"key_receive_migrated" in data:
                self.pending_responses.append(b"$6\r\nvalue3\r\n")
            elif b"key_receive_failing_over" in data:
                self.pending_responses.append(b"$6\r\nvalue4\r\n")
            elif b"key_receive_failed_over" in data:
                self.pending_responses.append(b"$6\r\nvalue5\r\n")
            elif b"key1" in data:
                self.pending_responses.append(b"$6\r\nvalue1\r\n")
            else:
                self.pending_responses.append(b"$-1\r\n")  # NULL response
        elif b"PING" in data:
            self.pending_responses.append(b"+PONG\r\n")
        else:
            self.pending_responses.append(b"+OK\r\n")  # Default response

        self.command_count += 1
        return len(data)

    def sendall(self, data):
        """Simulate sending all data to Redis."""
        return self.send(data)

    def recv(self, bufsize):
        """Simulate receiving data from Redis."""
        if self.closed:
            raise ConnectionError("Socket is closed")

        # Use pending responses that were prepared when commands were sent
        if self.pending_responses:
            response = self.pending_responses.pop(0)
            if b"MOVING" in response:
                self.moving_sent = True
            return response[:bufsize]  # Respect buffer size
        else:
            # No data available - this should block or raise an exception
            # For can_read checks, we should indicate no data is available
            import errno

            raise BlockingIOError(errno.EAGAIN, "Resource temporarily unavailable")

    def fileno(self):
        """Return a fake file descriptor for select/poll operations."""
        return 1  # Fake file descriptor

    def close(self):
        """Simulate closing the socket."""
        self.closed = True
        self.connected = False
        self.address = None
        self.timeout = None
        self.thread_timeouts = {}

    def settimeout(self, timeout):
        """Simulate setting socket timeout and track changes per thread."""
        self.timeout = timeout

        # Track last applied timeout with thread_id information added
        thread_id = threading.current_thread().ident
        self.thread_timeouts[thread_id] = timeout

    def gettimeout(self):
        """Simulate getting socket timeout."""
        return self.timeout

    def setsockopt(self, level, optname, value):
        """Simulate setting socket options."""
        pass

    def getpeername(self):
        """Simulate getting peer name."""
        return self.address

    def getsockname(self):
        """Simulate getting socket name."""
        return (self.address.split(":")[0], 12345)

    def shutdown(self, how):
        """Simulate socket shutdown."""
        pass


class TestMaintenanceNotificationsBase:
    """Base class for maintenance notifications handling tests."""

    def setup_method(self):
        """Set up test fixtures with mocked sockets."""
        self.mock_sockets = []
        self.original_socket = socket.socket

        # Mock socket creation to return our mock sockets
        def mock_socket_factory(*args, **kwargs):
            mock_sock = MockSocket()
            self.mock_sockets.append(mock_sock)
            return mock_sock

        self.socket_patcher = patch("socket.socket", side_effect=mock_socket_factory)
        self.socket_patcher.start()

        # Mock select.select to simulate data availability for reading
        def mock_select(rlist, wlist, xlist, timeout=0):
            # Check if any of the sockets in rlist have data available
            ready_sockets = []
            for sock in rlist:
                if hasattr(sock, "connected") and sock.connected and not sock.closed:
                    # Only return socket as ready if it actually has data to read
                    if hasattr(sock, "pending_responses") and sock.pending_responses:
                        ready_sockets.append(sock)
                    # Don't return socket as ready just because it received commands
                    # Only when there are actual responses available
            return (ready_sockets, [], [])

        self.select_patcher = patch("select.select", side_effect=mock_select)
        self.select_patcher.start()

        # Create maintenance notifications config
        self.config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=30
        )

    def teardown_method(self):
        """Clean up test fixtures."""
        self.socket_patcher.stop()
        self.select_patcher.stop()

    def _get_client(
        self,
        pool_class,
        max_connections=10,
        maint_notifications_config=None,
        setup_pool_handler=False,
    ):
        """Helper method to create a pool and Redis client with maintenance notifications configuration.

        Args:
            pool_class: The connection pool class (ConnectionPool or BlockingConnectionPool)
            max_connections: Maximum number of connections in the pool (default: 10)
            maint_notifications_config: Optional MaintNotificationsConfig to use. If not provided,
                                    uses self.config from setup_method (default: None)
            setup_pool_handler: Whether to set up pool handler for moving notifications (default: False)

        Returns:
            tuple: (test_pool, test_redis_client)
        """
        config = (
            maint_notifications_config
            if maint_notifications_config is not None
            else self.config
        )

        test_pool = pool_class(
            host=DEFAULT_ADDRESS.split(":")[0],
            port=int(DEFAULT_ADDRESS.split(":")[1]),
            max_connections=max_connections,
            protocol=3,  # Required for maintenance notifications
            maint_notifications_config=config,
        )
        test_redis_client = Redis(connection_pool=test_pool)

        # Set up pool handler for moving notifications if requested
        if setup_pool_handler:
            pool_handler = MaintNotificationsPoolHandler(
                test_redis_client.connection_pool, config
            )
            test_redis_client.connection_pool.set_maint_notifications_pool_handler(
                pool_handler
            )

        return test_redis_client


class TestMaintenanceNotificationsHandshake(TestMaintenanceNotificationsBase):
    """Integration tests for maintenance notifications handling with real connection pool."""

    def test_handshake_success_when_enabled(self):
        """Test that handshake is performed correctly."""
        maint_notifications_config = MaintNotificationsConfig(
            enabled=True, endpoint_type=EndpointType.EXTERNAL_IP
        )
        test_redis_client = self._get_client(
            ConnectionPool, maint_notifications_config=maint_notifications_config
        )

        try:
            # Perform Redis operations that should work with our improved mock responses
            result_set = test_redis_client.set("hello", "world")
            result_get = test_redis_client.get("hello")

            # Verify operations completed successfully
            assert result_set is True
            assert result_get == b"world"

        finally:
            test_redis_client.close()

    def test_handshake_success_when_auto_and_command_not_supported(self):
        """Test that when maintenance notifications are set to 'auto', the client gracefully handles unsupported MAINT_NOTIFICATIONS commands and normal Redis operations succeed."""
        maint_notifications_config = MaintNotificationsConfig(
            enabled="auto", endpoint_type=EndpointType.INTERNAL_IP
        )
        test_redis_client = self._get_client(
            ConnectionPool, maint_notifications_config=maint_notifications_config
        )

        try:
            # Perform Redis operations that should work with our improved mock responses
            result_set = test_redis_client.set("hello", "world")
            result_get = test_redis_client.get("hello")

            # Verify operations completed successfully
            assert result_set is True
            assert result_get == b"world"

        finally:
            test_redis_client.close()

    def test_handshake_failure_when_enabled(self):
        """Test that handshake is performed correctly."""
        maint_notifications_config = MaintNotificationsConfig(
            enabled=True, endpoint_type=EndpointType.INTERNAL_IP
        )
        test_redis_client = self._get_client(
            ConnectionPool, maint_notifications_config=maint_notifications_config
        )
        try:
            with pytest.raises(ResponseError):
                test_redis_client.set("hello", "world")

        finally:
            test_redis_client.close()


class TestMaintenanceNotificationsHandlingSingleProxy(TestMaintenanceNotificationsBase):
    """Integration tests for maintenance notifications handling with real connection pool."""

    def _validate_connection_handlers(self, conn, pool_handler, config):
        """Helper method to validate connection handlers are properly set."""
        # Test that the node moving handler function is correctly set
        parser_handler = conn._parser.node_moving_push_handler_func
        assert parser_handler is not None
        assert hasattr(parser_handler, "__self__")
        assert hasattr(parser_handler, "__func__")
        assert parser_handler.__self__ is pool_handler
        assert parser_handler.__func__ is pool_handler.handle_notification.__func__

        # Test that the maintenance handler function is correctly set
        maintenance_handler = conn._parser.maintenance_push_handler_func
        assert maintenance_handler is not None
        assert hasattr(maintenance_handler, "__self__")
        assert hasattr(maintenance_handler, "__func__")
        # The maintenance handler should be bound to the connection's
        # maintenance notification connection handler
        assert (
            maintenance_handler.__self__ is conn._maint_notifications_connection_handler
        )
        assert (
            maintenance_handler.__func__
            is conn._maint_notifications_connection_handler.handle_notification.__func__
        )

        # Validate that the connection's maintenance handler has the same config object
        assert conn._maint_notifications_connection_handler.config is config

    def _validate_current_timeout(self, expected_timeout, error_msg=None):
        """Helper method to validate the current timeout for the calling thread."""
        actual_timeout = None
        # Get the actual thread ID from the current thread
        current_thread_id = threading.current_thread().ident
        for sock in self.mock_sockets:
            if current_thread_id in sock.thread_timeouts:
                actual_timeout = sock.thread_timeouts[current_thread_id]
                break

        assert actual_timeout == expected_timeout, (
            f"{error_msg or ''}"
            f"Expected timeout ({expected_timeout}), "
            f"but found timeout: {actual_timeout}. "
            f"All thread timeouts: {[sock.thread_timeouts for sock in self.mock_sockets]}",
        )

    def _validate_disconnected(self, expected_count):
        """Helper method to validate all socket timeouts"""
        disconnected_sockets_count = 0
        for sock in self.mock_sockets:
            if sock.closed:
                disconnected_sockets_count += 1
        assert disconnected_sockets_count == expected_count

    def _validate_connected(self, expected_count):
        """Helper method to validate all socket timeouts"""
        connected_sockets_count = 0
        for sock in self.mock_sockets:
            if sock.connected:
                connected_sockets_count += 1
        assert connected_sockets_count == expected_count

    def test_client_initialization(self):
        """Test that Redis client is created with maintenance notifications configuration."""
        # Create a pool and Redis client with maintenance notifications

        test_redis_client = Redis(
            protocol=3,  # Required for maintenance notifications
            maint_notifications_config=self.config,
        )

        pool_handler = test_redis_client.connection_pool.connection_kwargs.get(
            "maint_notifications_pool_handler"
        )
        assert pool_handler is not None
        assert pool_handler.config == self.config

        conn = test_redis_client.connection_pool.get_connection()
        assert conn._should_reconnect is False
        assert conn.orig_host_address == "localhost"
        assert conn.orig_socket_timeout is None

        # Test that the node moving handler function is correctly set by
        # comparing the underlying function and instance
        parser_handler = conn._parser.node_moving_push_handler_func
        assert parser_handler is not None
        assert hasattr(parser_handler, "__self__")
        assert hasattr(parser_handler, "__func__")
        assert parser_handler.__self__ is pool_handler
        assert parser_handler.__func__ is pool_handler.handle_notification.__func__

        # Test that the maintenance handler function is correctly set
        maintenance_handler = conn._parser.maintenance_push_handler_func
        assert maintenance_handler is not None
        assert hasattr(maintenance_handler, "__self__")
        assert hasattr(maintenance_handler, "__func__")
        # The maintenance handler should be bound to the connection's
        # maintenance notification connection handler
        assert (
            maintenance_handler.__self__ is conn._maint_notifications_connection_handler
        )
        assert (
            maintenance_handler.__func__
            is conn._maint_notifications_connection_handler.handle_notification.__func__
        )

        # Validate that the connection's maintenance handler has the same config object
        assert conn._maint_notifications_connection_handler.config is self.config

    def test_maint_handler_init_for_existing_connections(self):
        """Test that maintenance notification handlers are properly set on existing and new connections
        when configuration is enabled after client creation."""

        # Create a Redis client with disabled maintenance notifications configuration
        disabled_config = MaintNotificationsConfig(enabled=False)
        test_redis_client = Redis(
            protocol=3,  # Required for maintenance notifications
            maint_notifications_config=disabled_config,
        )

        # Extract an existing connection before enabling maintenance notifications
        existing_conn = test_redis_client.connection_pool.get_connection()

        # Verify that maintenance notifications are initially disabled
        assert existing_conn._parser.node_moving_push_handler_func is None
        assert existing_conn._maint_notifications_connection_handler is None
        assert existing_conn._parser.maintenance_push_handler_func is None

        # Create a new enabled configuration and set up pool handler
        enabled_config = MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True, relaxed_timeout=30
        )
        pool_handler = MaintNotificationsPoolHandler(
            test_redis_client.connection_pool, enabled_config
        )
        test_redis_client.connection_pool.set_maint_notifications_pool_handler(
            pool_handler
        )

        # Validate the existing connection after enabling maintenance notifications
        # Both existing and new connections should now have full handler setup
        self._validate_connection_handlers(existing_conn, pool_handler, enabled_config)

        # Create a new connection and validate it has full handlers
        new_conn = test_redis_client.connection_pool.get_connection()
        self._validate_connection_handlers(new_conn, pool_handler, enabled_config)

        # Clean up connections
        test_redis_client.connection_pool.release(existing_conn)
        test_redis_client.connection_pool.release(new_conn)

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_connection_pool_creation_with_maintenance_notifications(self, pool_class):
        """Test that connection pools are created with maintenance notifications configuration."""
        # Create a pool and Redis client with maintenance notifications
        max_connections = 3 if pool_class == BlockingConnectionPool else 10
        test_redis_client = self._get_client(
            pool_class, max_connections=max_connections
        )
        test_pool = test_redis_client.connection_pool

        try:
            assert (
                test_pool.connection_kwargs.get("maint_notifications_config")
                == self.config
            )
            # Pool should have maintenance notifications enabled
            assert test_pool.maint_notifications_pool_handler_enabled() is True

            # Create and set a pool handler
            pool_handler = MaintNotificationsPoolHandler(test_pool, self.config)
            test_pool.set_maint_notifications_pool_handler(pool_handler)

            # Validate that the handler is properly set on the pool
            assert (
                test_pool.connection_kwargs.get("maint_notifications_pool_handler")
                == pool_handler
            )
            assert (
                test_pool.connection_kwargs.get("maint_notifications_config")
                == pool_handler.config
            )

            # Verify that the pool handler has the correct configuration
            assert pool_handler.pool == test_pool
            assert pool_handler.config == self.config

        finally:
            if hasattr(test_pool, "disconnect"):
                test_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_redis_operations_with_mock_sockets(self, pool_class):
        """
        Test basic Redis operations work with mocked sockets and proper response parsing.
        Basically with test - the mocked socket is validated.
        """
        # Create a pool and Redis client with maintenance notifications
        test_redis_client = self._get_client(pool_class, max_connections=5)

        try:
            # Perform Redis operations that should work with our improved mock responses
            result_set = test_redis_client.set("hello", "world")
            result_get = test_redis_client.get("hello")

            # Verify operations completed successfully
            assert result_set is True
            assert result_get == b"world"

            # Verify socket interactions
            assert len(self.mock_sockets) >= 1
            assert self.mock_sockets[0].connected
            assert len(self.mock_sockets[0].sent_data) >= 2  # HELLO, SET, GET commands

            # Verify that the connection has maintenance notification handler
            connection = test_redis_client.connection_pool.get_connection()
            assert hasattr(connection, "_maint_notifications_connection_handler")
            test_redis_client.connection_pool.release(connection)

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    def test_pool_handler_with_migrating_notification(self):
        """Test that pool handler correctly handles migrating notifications."""
        # Create a pool and Redis client with maintenance notifications
        test_redis_client = self._get_client(ConnectionPool)
        test_pool = test_redis_client.connection_pool

        try:
            # Create and set a pool handler
            pool_handler = MaintNotificationsPoolHandler(test_pool, self.config)

            # Create a migrating notification (not handled by pool handler)
            migrating_notification = NodeMigratingNotification(id=1, ttl=5)

            # Mock the required functions
            with (
                patch.object(
                    pool_handler, "remove_expired_notifications"
                ) as mock_remove_expired,
                patch.object(
                    pool_handler, "handle_node_moving_notification"
                ) as mock_handle_moving,
                patch("redis.maint_notifications.logging.error") as mock_logging_error,
            ):
                # Pool handler should return None for migrating notifications (not its responsibility)
                pool_handler.handle_notification(migrating_notification)

                # Validate that remove_expired_notifications has been called once
                mock_remove_expired.assert_called_once()

                # Validate that handle_node_moving_notification hasn't been called
                mock_handle_moving.assert_not_called()

                # Validate that logging.error has been called once
                mock_logging_error.assert_called_once()

        finally:
            if hasattr(test_pool, "disconnect"):
                test_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_migration_related_notifications_handling_integration(self, pool_class):
        """
        Test full integration of migration-related notifications (MIGRATING/MIGRATED) handling.

        This test validates the complete migration lifecycle:
        1. Executes 5 Redis commands sequentially
        2. Injects MIGRATING push message before command 2 (SET key_receive_migrating)
        3. Validates socket timeout is updated to relaxed value (30s) after MIGRATING
        4. Executes commands 3-4 while timeout remains relaxed
        5. Injects MIGRATED push message before command 5 (SET key_receive_migrated)
        6. Validates socket timeout is restored after MIGRATED
        7. Tests both ConnectionPool and BlockingConnectionPool implementations
        8. Uses proper RESP3 push message format for realistic protocol simulation
        """
        # Create a pool and Redis client with maintenance notifications
        test_redis_client = self._get_client(pool_class, max_connections=10)

        try:
            # Command 1: Initial command
            key1 = "key1"
            value1 = "value1"
            result1 = test_redis_client.set(key1, value1)

            # Validate Command 1 result
            assert result1 is True, "Command 1 (SET key1) failed"

            # Command 2: This SET command will receive MIGRATING push message before response
            key_migrating = "key_receive_migrating"
            value_migrating = "value2"
            result2 = test_redis_client.set(key_migrating, value_migrating)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_migrating) failed"

            # Step 4: Validate timeout was updated to relaxed value after MIGRATING
            self._validate_current_timeout(30, "Right after MIGRATING is received. ")

            # Command 3: Another command while timeout is still relaxed
            result3 = test_redis_client.get(key1)

            # Validate Command 3 result
            expected_value3 = value1.encode()
            assert result3 == expected_value3, (
                f"Command 3 (GET key1) failed. Expected {expected_value3}, got {result3}"
            )

            # Command 4: Execute command (step 5)
            result4 = test_redis_client.get(key_migrating)

            # Validate Command 4 result
            expected_value4 = value_migrating.encode()
            assert result4 == expected_value4, (
                f"Command 4 (GET key_receive_migrating) failed. Expected {expected_value4}, got {result4}"
            )

            # Step 6: Validate socket timeout is still relaxed during commands 3-4
            self._validate_current_timeout(
                30,
                "Execute a command with a connection extracted from the pool (after it has received MIGRATING)",
            )

            # Command 5: This SET command will receive
            # MIGRATED push message before actual response
            key_migrated = "key_receive_migrated"
            value_migrated = "value3"
            result5 = test_redis_client.set(key_migrated, value_migrated)

            # Validate Command 5 result
            assert result5 is True, "Command 5 (SET key_receive_migrated) failed"

            # Step 8: Validate socket timeout is reversed back to original after MIGRATED
            self._validate_current_timeout(None)

            # Verify maintenance notifications were processed correctly
            # The key is that we have at least 1 socket and all operations succeeded
            assert len(self.mock_sockets) >= 1, (
                f"Expected at least 1 socket for operations, got {len(self.mock_sockets)}"
            )

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_migrating_notification_with_disabled_relaxed_timeout(self, pool_class):
        """
        Test maintenance notifications handling when relaxed timeout is disabled.

        This test validates that when relaxed_timeout is disabled (-1):
        1. MIGRATING, MIGRATED, FAILING_OVER, and FAILED_OVER notifications are received and processed
        2. No timeout updates are applied to connections
        3. Socket timeouts remain unchanged during all maintenance notifications
        4. Tests both ConnectionPool and BlockingConnectionPool implementations
        5. Tests the complete lifecycle: MIGRATING -> MIGRATED -> FAILING_OVER -> FAILED_OVER
        """
        # Create config with disabled relaxed timeout
        disabled_config = MaintNotificationsConfig(
            enabled=True,
            relaxed_timeout=-1,  # This means the relaxed timeout is Disabled
        )

        # Create a pool and Redis client with disabled relaxed timeout config
        test_redis_client = self._get_client(
            pool_class, max_connections=5, maint_notifications_config=disabled_config
        )

        try:
            # Command 1: Initial command
            key1 = "key1"
            value1 = "value1"
            result1 = test_redis_client.set(key1, value1)

            # Validate Command 1 result
            assert result1 is True, "Command 1 (SET key1) failed"

            # Command 2: This SET command will receive MIGRATING push message before response
            key_migrating = "key_receive_migrating"
            value_migrating = "value2"
            result2 = test_redis_client.set(key_migrating, value_migrating)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_migrating) failed"

            # Validate timeout was NOT updated (relaxed is disabled)
            # Should remain at default timeout (None), not relaxed to 30s
            self._validate_current_timeout(None)

            # Command 3: Another command to verify timeout remains unchanged
            result3 = test_redis_client.get(key1)

            # Validate Command 3 result
            expected_value3 = value1.encode()
            assert result3 == expected_value3, (
                f"Command 3 (GET key1) failed. Expected: {expected_value3}, Got: {result3}"
            )

            # Command 4: This SET command will receive MIGRATED push message before response
            key_migrated = "key_receive_migrated"
            value_migrated = "value3"
            result4 = test_redis_client.set(key_migrated, value_migrated)

            # Validate Command 4 result
            assert result4 is True, "Command 4 (SET key_receive_migrated) failed"

            # Validate timeout is still NOT updated after MIGRATED (relaxed is disabled)
            self._validate_current_timeout(None)

            # Command 5: This SET command will receive FAILING_OVER push message before response
            key_failing_over = "key_receive_failing_over"
            value_failing_over = "value4"
            result5 = test_redis_client.set(key_failing_over, value_failing_over)

            # Validate Command 5 result
            assert result5 is True, "Command 5 (SET key_receive_failing_over) failed"

            # Validate timeout is still NOT updated after FAILING_OVER (relaxed is disabled)
            self._validate_current_timeout(None)

            # Command 6: Another command to verify timeout remains unchanged during failover
            result6 = test_redis_client.get(key_failing_over)

            # Validate Command 6 result
            expected_value6 = value_failing_over.encode()
            assert result6 == expected_value6, (
                f"Command 6 (GET key_receive_failing_over) failed. Expected: {expected_value6}, Got: {result6}"
            )

            # Command 7: This SET command will receive FAILED_OVER push message before response
            key_failed_over = "key_receive_failed_over"
            value_failed_over = "value5"
            result7 = test_redis_client.set(key_failed_over, value_failed_over)

            # Validate Command 7 result
            assert result7 is True, "Command 7 (SET key_receive_failed_over) failed"

            # Validate timeout is still NOT updated after FAILED_OVER (relaxed is disabled)
            self._validate_current_timeout(None)

            # Command 8: Final command to verify timeout remains unchanged after all notifications
            result8 = test_redis_client.get(key_failed_over)

            # Validate Command 8 result
            expected_value8 = value_failed_over.encode()
            assert result8 == expected_value8, (
                f"Command 8 (GET key_receive_failed_over) failed. Expected: {expected_value8}, Got: {result8}"
            )

            # Verify maintenance notifications were processed correctly
            # The key is that we have at least 1 socket and all operations succeeded
            assert len(self.mock_sockets) >= 1, (
                f"Expected at least 1 socket for operations, got {len(self.mock_sockets)}"
            )

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_failing_over_related_notifications_handling_integration(self, pool_class):
        """
        Test full integration of failing-over-related notifications (FAILING_OVER/FAILED_OVER) handling.

        This test validates the complete FAILING_OVER -> FAILED_OVER lifecycle:
        1. Executes 5 Redis commands sequentially
        2. Injects FAILING_OVER push message before command 2 (SET key_receive_failing_over)
        3. Validates socket timeout is updated to relaxed value (30s) after FAILING_OVER
        4. Executes commands 3-4 while timeout remains relaxed
        5. Injects FAILED_OVER push message before command 5 (SET key_receive_failed_over)
        6. Validates socket timeout is restored after FAILED_OVER
        7. Tests both ConnectionPool and BlockingConnectionPool implementations
        8. Uses proper RESP3 push message format for realistic protocol simulation
        """
        # Create a pool and Redis client with maintenance notifications
        test_redis_client = self._get_client(pool_class, max_connections=10)

        try:
            # Command 1: Initial command
            key1 = "key1"
            value1 = "value1"
            result1 = test_redis_client.set(key1, value1)

            # Validate Command 1 result
            assert result1 is True, "Command 1 (SET key1) failed"

            # Command 2: This SET command will receive FAILING_OVER push message before response
            key_failing_over = "key_receive_failing_over"
            value_failing_over = "value4"
            result2 = test_redis_client.set(key_failing_over, value_failing_over)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_failing_over) failed"

            # Step 4: Validate timeout was updated to relaxed value after MIGRATING
            self._validate_current_timeout(30, "Right after FAILING_OVER is received. ")

            # Command 3: Another command while timeout is still relaxed
            result3 = test_redis_client.get(key1)

            # Validate Command 3 result
            expected_value3 = value1.encode()
            assert result3 == expected_value3, (
                f"Command 3 (GET key1) failed. Expected {expected_value3}, got {result3}"
            )

            # Command 4: Execute command (step 5)
            result4 = test_redis_client.get(key_failing_over)

            # Validate Command 4 result
            expected_value4 = value_failing_over.encode()
            assert result4 == expected_value4, (
                f"Command 4 (GET key_receive_failing_over) failed. Expected {expected_value4}, got {result4}"
            )

            # Step 6: Validate socket timeout is still relaxed during commands 3-4
            self._validate_current_timeout(
                30,
                "Execute a command with a connection extracted from the pool (after it has received FAILING_OVER)",
            )

            # Command 5: This SET command will receive
            # FAILED_OVER push message before actual response
            key_failed_over = "key_receive_failed_over"
            value_migrated = "value3"
            result5 = test_redis_client.set(key_failed_over, value_migrated)

            # Validate Command 5 result
            assert result5 is True, "Command 5 (SET key_receive_failed_over) failed"

            # Step 8: Validate socket timeout is reversed back to original after FAILED_OVER
            self._validate_current_timeout(None)

            # Verify maintenance notifications were processed correctly
            # The key is that we have at least 1 socket and all operations succeeded
            assert len(self.mock_sockets) >= 1, (
                f"Expected at least 1 socket for operations, got {len(self.mock_sockets)}"
            )

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_moving_related_notifications_handling_integration(self, pool_class):
        """
        Test full integration of moving-related notifications (MOVING) handling with Redis commands.

        This test validates the complete MOVING notification lifecycle:
        1. Creates multiple connections in the pool
        2. Executes a Redis command that triggers a MOVING push message
        3. Validates that pool configuration is updated with temporary
           address and timeout - for new connections creation
        4. Validates that existing connections are marked for disconnection
        5. Tests both ConnectionPool and BlockingConnectionPool implementations
        """
        # Create a pool and Redis client with maintenance notifications and pool handler
        test_redis_client = self._get_client(
            pool_class, max_connections=10, setup_pool_handler=True
        )

        try:
            # Create several connections and return them in the pool
            connections = []
            for _ in range(10):
                connection = test_redis_client.connection_pool.get_connection()
                connections.append(connection)

            for connection in connections:
                test_redis_client.connection_pool.release(connection)

            # Take 5 connections to be "in use"
            in_use_connections = []
            for _ in range(5):
                connection = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(connection)

            # Validate all connections are connected prior MOVING notification
            self._validate_disconnected(0)

            # Run command that will receive and handle MOVING notification
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            # the connection used for the command is expected to be reconnected to the new address
            # before it is returned to the pool
            result2 = test_redis_client.set(key_moving, value_moving)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_moving) failed"

            # Validate pool and connections settings were updated according to MOVING notification
            expected_notification_hash = hash(MOVING_NOTIFICATION)

            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=expected_notification_hash,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
            )
            self._validate_disconnected(5)
            self._validate_connected(6)
            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                expected_current_socket_timeout=self.config.relaxed_timeout,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[
                    0
                ],  # the in use connections reconnect when they complete their current task
            )
            Helpers.validate_free_connections_state(
                pool=test_redis_client.connection_pool,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                should_be_connected_count=1,
                connected_to_tmp_address=True,
            )
            # Wait for MOVING timeout to expire and the moving completed handler to run
            sleep(MOVING_TIMEOUT + 0.5)

            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_state=MaintenanceState.NONE,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=None,
                expected_socket_connect_timeout=None,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                expected_current_socket_timeout=None,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
            )
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.NONE,
                expected_maintenance_notification_hash=None,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=None,
                expected_socket_connect_timeout=None,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
            )
            Helpers.validate_free_connections_state(
                pool=test_redis_client.connection_pool,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=None,
                expected_socket_connect_timeout=None,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                should_be_connected_count=1,
                connected_to_tmp_address=True,
                expected_state=MaintenanceState.NONE,
            )
        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_moving_none_notifications_handling_integration(self, pool_class):
        """
        Test full integration of moving-related notifications (MOVING) handling with Redis commands.

        This test validates the complete MOVING notification lifecycle,
        when the push notification doesn't contain host and port:
        1. Creates multiple connections in the pool
        2. Executes a Redis command that triggers a MOVING with "null" push message
        3. Validates that pool configuration is updated with temporary
           address and timeout - for new connections creation
        4. Validates that existing connections are marked for disconnection after ttl/2 seconds
        5. Tests both ConnectionPool and BlockingConnectionPool implementations
        """
        # Create a pool and Redis client with maintenance notifications and pool handler
        test_redis_client = self._get_client(
            pool_class, max_connections=10, setup_pool_handler=True
        )

        try:
            # Create several connections and return them in the pool
            connections = []
            for _ in range(10):
                connection = test_redis_client.connection_pool.get_connection()
                connections.append(connection)

            for connection in connections:
                test_redis_client.connection_pool.release(connection)

            # Take 5 connections to be "in use"
            in_use_connections = []
            for _ in range(5):
                connection = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(connection)

            # Validate all connections are connected prior MOVING notification
            self._validate_disconnected(0)

            # Run command that will receive and handle MOVING notification
            key_moving = "key_receive_moving_none_0"
            value_moving = "value3_0"

            # the connection used for the command is expected to be reconnected to the new address
            # before it is returned to the pool
            result2 = test_redis_client.set(key_moving, value_moving)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_moving) failed"

            # Validate pool and connections settings were updated according to MOVING notification
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=hash(MOVING_NONE_NOTIFICATION),
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
            )
            self._validate_disconnected(0)
            self._validate_connected(10)
            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_should_reconnect=False,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                expected_current_socket_timeout=self.config.relaxed_timeout,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[
                    0
                ],  # the in use connections reconnect when they complete their current task
            )
            Helpers.validate_free_connections_state(
                pool=test_redis_client.connection_pool,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                should_be_connected_count=5,
                connected_to_tmp_address=False,
            )
            # Wait for half of MOVING timeout to expire and the proactive reconnect to run
            sleep(MOVING_TIMEOUT / 2 + 0.2)
            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_should_reconnect=True,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                expected_current_socket_timeout=self.config.relaxed_timeout,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[
                    0
                ],  # the in use connections reconnect when they complete their current task
            )
            self._validate_disconnected(5)
            self._validate_connected(5)

            # Wait for MOVING timeout to expire and the moving completed handler to run
            sleep(MOVING_TIMEOUT / 2 + 0.2)
            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_state=MaintenanceState.NONE,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=None,
                expected_socket_connect_timeout=None,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                expected_current_socket_timeout=None,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
            )
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.NONE,
                expected_maintenance_notification_hash=None,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=None,
                expected_socket_connect_timeout=None,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
            )
            Helpers.validate_free_connections_state(
                pool=test_redis_client.connection_pool,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_socket_timeout=None,
                expected_socket_connect_timeout=None,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                should_be_connected_count=0,
                connected_to_tmp_address=True,
                expected_state=MaintenanceState.NONE,
            )
        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_create_new_conn_while_moving_not_expired(self, pool_class):
        """
        Test creating new connections while MOVING notification is active (not expired).

        This test validates that:
        1. After MOVING notification is processed, new connections are created with temporary address
        2. New connections inherit the relaxed timeout settings
        3. Pool configuration is properly applied to newly created connections
        """
        # Create a pool and Redis client with maintenance notifications and pool handler
        test_redis_client = self._get_client(
            pool_class, max_connections=10, setup_pool_handler=True
        )

        try:
            # Create several connections and return them in the pool
            connections = []
            for _ in range(5):
                connection = test_redis_client.connection_pool.get_connection()
                connections.append(connection)

            for connection in connections:
                test_redis_client.connection_pool.release(connection)

            # Take 3 connections to be "in use"
            in_use_connections = []
            for _ in range(3):
                connection = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(connection)

            # Validate all connections are connected prior MOVING notification
            self._validate_disconnected(0)

            # Run command that will receive and handle MOVING notification
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            result = test_redis_client.set(key_moving, value_moving)

            # Validate command result
            assert result is True, "SET key_receive_moving command failed"

            # Validate pool and connections settings were updated according to MOVING notification
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=hash(MOVING_NOTIFICATION),
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
            )

            # Now get several more connections to force creation of new ones
            # This should create new connections with the temporary address
            old_connections = []
            for _ in range(2):
                connection = test_redis_client.connection_pool.get_connection()
                old_connections.append(connection)

            new_connection = test_redis_client.connection_pool.get_connection()

            # Validate that new connections are created with temporary address and relaxed timeout
            # and when connecting those configs are used
            # get_connection() returns a connection that is already connected
            assert new_connection.host == AFTER_MOVING_ADDRESS.split(":")[0]
            assert new_connection.socket_timeout is self.config.relaxed_timeout
            # New connections should be connected to the temporary address
            assert new_connection._sock is not None
            assert new_connection._sock.connected is True
            assert (
                new_connection._sock.getpeername()[0]
                == AFTER_MOVING_ADDRESS.split(":")[0]
            )
            assert new_connection._sock.gettimeout() == self.config.relaxed_timeout

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_create_new_conn_after_moving_expires(self, pool_class):
        """
        Test creating new connections after MOVING notification expires.

        This test validates that:
        1. After MOVING timeout expires, new connections use original address
        2. Pool configuration is reset to original values
        3. New connections don't inherit temporary settings
        """
        # Create a pool and Redis client with maintenance notifications and pool handler
        test_redis_client = self._get_client(
            pool_class, max_connections=10, setup_pool_handler=True
        )

        try:
            # Create several connections and return them in the pool
            connections = []
            for _ in range(5):
                connection = test_redis_client.connection_pool.get_connection()
                connections.append(connection)

            for connection in connections:
                test_redis_client.connection_pool.release(connection)

            # Take 3 connections to be "in use"
            in_use_connections = []
            for _ in range(3):
                connection = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(connection)

            # Run command that will receive and handle MOVING notification
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            result = test_redis_client.set(key_moving, value_moving)

            # Validate command result
            assert result is True, "SET key_receive_moving command failed"

            # Wait for MOVING timeout to expire
            sleep(MOVING_TIMEOUT + 0.5)

            # Now get several new connections after expiration
            old_connections = []
            for _ in range(2):
                connection = test_redis_client.connection_pool.get_connection()
                old_connections.append(connection)

            new_connection = test_redis_client.connection_pool.get_connection()

            # Validate that new connections are created with original address (no temporary settings)
            assert new_connection.orig_host_address == DEFAULT_ADDRESS.split(":")[0]
            assert new_connection.orig_socket_timeout is None
            # New connections should be connected to the original address
            assert new_connection._sock is not None
            assert new_connection._sock.connected is True
            # Socket timeout should be None (original timeout)
            assert new_connection._sock.gettimeout() is None

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_receive_migrated_after_moving(self, pool_class):
        """
        Test receiving MIGRATED notification after MOVING notification.

        This test validates the complete MOVING -> MIGRATED lifecycle:
        1. MOVING notification is processed and temporary settings are applied
        2. MIGRATED notification is received during command execution
        3. Temporary settings are cleared after MIGRATED
        4. Pool configuration is restored to original values

        Note: When MIGRATED comes after MOVING and MOVING hasn't yet expired,
        it should not decrease timeouts (future refactoring consideration).
        """
        # Create a pool and Redis client with maintenance notifications and pool handler
        test_redis_client = self._get_client(
            pool_class, max_connections=10, setup_pool_handler=True
        )

        try:
            # Create several connections and return them in the pool
            connections = []
            for _ in range(5):
                connection = test_redis_client.connection_pool.get_connection()
                connections.append(connection)

            for connection in connections:
                test_redis_client.connection_pool.release(connection)

            # Take 3 connections to be "in use"
            in_use_connections = []
            for _ in range(3):
                connection = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(connection)

            # Validate all connections are connected prior MOVING notification
            self._validate_disconnected(0)

            # Step 1: Run command that will receive and handle MOVING notification
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            result_moving = test_redis_client.set(key_moving, value_moving)

            # Validate MOVING command result
            assert result_moving is True, "SET key_receive_moving command failed"

            # Validate pool and connections settings were updated according to MOVING notification
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=hash(MOVING_NOTIFICATION),
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
            )

            # TODO validate current socket timeout

            # Step 2: Run command that will receive and handle MIGRATED notification
            # This should clear the temporary settings
            key_migrated = "key_receive_migrated_0"
            value_migrated = "migrated_value"
            result_migrated = test_redis_client.set(key_migrated, value_migrated)

            # Validate MIGRATED command result
            assert result_migrated is True, "SET key_receive_migrated command failed"

            # Step 3: Validate that MIGRATED notification was processed but MOVING settings remain
            # (MIGRATED doesn't automatically clear MOVING settings - they are separate notifications)
            # MOVING settings should still be active
            # MOVING timeout should still be active
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=hash(MOVING_NOTIFICATION),
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
            )

            # Step 4: Create new connections after MIGRATED to verify they still use MOVING settings
            # (since MOVING settings are still active)
            new_connections = []
            for _ in range(2):
                connection = test_redis_client.connection_pool.get_connection()
                new_connections.append(connection)

            # Validate that new connections are created with MOVING settings (still active)
            for connection in new_connections:
                assert connection.host == AFTER_MOVING_ADDRESS.split(":")[0]
                # Note: New connections may not inherit the exact relaxed timeout value
                # but they should have the temporary host address
                # New connections should be connected
                if connection._sock is not None:
                    assert connection._sock.connected is True

            # Release the new connections
            for connection in new_connections:
                test_redis_client.connection_pool.release(connection)

            # Validate free connections state with MOVING settings still active
            # Note: We'll validate with the pool's current settings rather than individual connection settings
            # since new connections may have different timeout values but still use the temporary address

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_overlapping_moving_notifications(self, pool_class):
        """
        Test handling of overlapping/duplicate MOVING notifications (e.g., two MOVING notifications before the first expires).
        Ensures that the second MOVING notification updates the pool and connections as expected, and that expiry/cleanup works.
        """
        global AFTER_MOVING_ADDRESS
        test_redis_client = self._get_client(
            pool_class, max_connections=5, setup_pool_handler=True
        )
        try:
            # Create and release some connections
            in_use_connections = []
            for _ in range(3):
                in_use_connections.append(
                    test_redis_client.connection_pool.get_connection()
                )

            for conn in in_use_connections:
                test_redis_client.connection_pool.release(conn)

            # Take 2 connections to be in use
            in_use_connections = []
            for _ in range(2):
                conn = test_redis_client.connection_pool.get_connection()
                in_use_connections.append(conn)

            # Trigger first MOVING notification
            key_moving1 = "key_receive_moving_0"
            value_moving1 = "value3_0"
            result1 = test_redis_client.set(key_moving1, value_moving1)
            assert result1 is True
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.MOVING,
                expected_maintenance_notification_hash=hash(MOVING_NOTIFICATION),
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
            )
            # Validate all connections reflect the first MOVING notification
            Helpers.validate_in_use_connections_state(
                in_use_connections,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
                expected_current_socket_timeout=self.config.relaxed_timeout,
                expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
            )
            Helpers.validate_free_connections_state(
                pool=test_redis_client.connection_pool,
                should_be_connected_count=1,
                connected_to_tmp_address=True,
                expected_state=MaintenanceState.MOVING,
                expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
                expected_socket_timeout=self.config.relaxed_timeout,
                expected_socket_connect_timeout=self.config.relaxed_timeout,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
            )
            # Reconnect in use connections
            for conn in in_use_connections:
                conn.disconnect()
                conn.connect()

            # Before the first MOVING expires, trigger a second MOVING notification (simulate new address)
            # Validate the orig properties are not changed!
            second_moving_address = "5.6.7.8:6380"
            orig_after_moving = AFTER_MOVING_ADDRESS
            # Temporarily modify the global constant for this test
            AFTER_MOVING_ADDRESS = second_moving_address
            second_moving_notification = NodeMovingNotification(
                id=1,
                new_node_host=second_moving_address.split(":")[0],
                new_node_port=int(second_moving_address.split(":")[1]),
                ttl=MOVING_TIMEOUT,
            )
            try:
                key_moving2 = "key_receive_moving_1"
                value_moving2 = "value3_1"
                result2 = test_redis_client.set(key_moving2, value_moving2)
                assert result2 is True
                Helpers.validate_conn_kwargs(
                    pool=test_redis_client.connection_pool,
                    expected_maintenance_state=MaintenanceState.MOVING,
                    expected_maintenance_notification_hash=hash(
                        second_moving_notification
                    ),
                    expected_host_address=second_moving_address.split(":")[0],
                    expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                    expected_socket_timeout=self.config.relaxed_timeout,
                    expected_socket_connect_timeout=self.config.relaxed_timeout,
                    expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                    expected_orig_socket_timeout=None,
                    expected_orig_socket_connect_timeout=None,
                )
                # Validate all connections reflect the second MOVING notification
                Helpers.validate_in_use_connections_state(
                    in_use_connections,
                    expected_state=MaintenanceState.MOVING,
                    expected_host_address=second_moving_address.split(":")[0],
                    expected_socket_timeout=self.config.relaxed_timeout,
                    expected_socket_connect_timeout=self.config.relaxed_timeout,
                    expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                    expected_orig_socket_timeout=None,
                    expected_orig_socket_connect_timeout=None,
                    expected_current_socket_timeout=self.config.relaxed_timeout,
                    expected_current_peername=orig_after_moving.split(":")[0],
                )
                # print(test_redis_client.connection_pool._available_connections)
                Helpers.validate_free_connections_state(
                    test_redis_client.connection_pool,
                    should_be_connected_count=1,
                    connected_to_tmp_address=True,
                    tmp_address=second_moving_address.split(":")[0],
                    expected_state=MaintenanceState.MOVING,
                    expected_host_address=second_moving_address.split(":")[0],
                    expected_socket_timeout=self.config.relaxed_timeout,
                    expected_socket_connect_timeout=self.config.relaxed_timeout,
                    expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                    expected_orig_socket_timeout=None,
                    expected_orig_socket_connect_timeout=None,
                )
            finally:
                AFTER_MOVING_ADDRESS = orig_after_moving

            # Wait for both MOVING timeouts to expire
            sleep(MOVING_TIMEOUT + 0.5)
            Helpers.validate_conn_kwargs(
                pool=test_redis_client.connection_pool,
                expected_maintenance_state=MaintenanceState.NONE,
                expected_maintenance_notification_hash=None,
                expected_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
                expected_socket_timeout=None,
                expected_socket_connect_timeout=None,
                expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
                expected_orig_socket_timeout=None,
                expected_orig_socket_connect_timeout=None,
            )
        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_thread_safety_concurrent_notification_handling(self, pool_class):
        """
        Test thread-safety under concurrent maintenance notification handling.
        Simulates multiple threads triggering MOVING notifications and performing operations concurrently.
        """
        import threading

        test_redis_client = self._get_client(
            pool_class, max_connections=5, setup_pool_handler=True
        )
        results = []
        errors = []

        def worker(idx):
            try:
                key = f"key_receive_moving_{idx}"
                value = f"value3_{idx}"
                result = test_redis_client.set(key, value)
                results.append(result)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(results), f"Not all threads succeeded: {results}"
        assert not errors, f"Errors occurred in threads: {errors}"
        # After all threads, MOVING notification should have been handled safely
        Helpers.validate_conn_kwargs(
            pool=test_redis_client.connection_pool,
            expected_maintenance_state=MaintenanceState.MOVING,
            expected_maintenance_notification_hash=hash(MOVING_NOTIFICATION),
            expected_host_address=AFTER_MOVING_ADDRESS.split(":")[0],
            expected_port=int(DEFAULT_ADDRESS.split(":")[1]),
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
        )

        if hasattr(test_redis_client.connection_pool, "disconnect"):
            test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_moving_migrating_migrated_moved_state_transitions(self, pool_class):
        """
        Test moving configs are not lost if the per connection notifications get picked up after moving is handled.
        Sequence of notifications: MOVING, MIGRATING, MIGRATED, FAILING_OVER, FAILED_OVER, MOVED.
        Note: FAILING_OVER and FAILED_OVER notifications do not change the connection state when already in MOVING state.
        Checks the state after each notification for all connections and for new connections created during each state.
        """
        # Setup
        test_redis_client = self._get_client(
            pool_class, max_connections=5, setup_pool_handler=True
        )
        pool = test_redis_client.connection_pool
        pool_handler = pool.connection_kwargs["maint_notifications_pool_handler"]

        # Create and release some connections
        in_use_connections = []
        for _ in range(3):
            in_use_connections.append(pool.get_connection())

        pool_handler.set_connection(in_use_connections[0])

        while len(in_use_connections) > 0:
            pool.release(in_use_connections.pop())

        # Take 2 connections to be in use
        in_use_connections = []
        for _ in range(2):
            conn = pool.get_connection()
            in_use_connections.append(conn)

        # 1. MOVING notification
        tmp_address = "22.23.24.25"
        moving_notification = NodeMovingNotification(
            id=1, new_node_host=tmp_address, new_node_port=6379, ttl=1
        )
        pool_handler.handle_notification(moving_notification)

        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )
        Helpers.validate_free_connections_state(
            pool=pool,
            should_be_connected_count=0,
            connected_to_tmp_address=False,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
        )

        # 2. MIGRATING notification (simulate direct connection handler call)
        for conn in in_use_connections:
            conn._maint_notifications_connection_handler.handle_notification(
                NodeMigratingNotification(id=2, ttl=1)
            )
        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )

        # 3. MIGRATED notification (simulate direct connection handler call)
        for conn in in_use_connections:
            conn._maint_notifications_connection_handler.handle_notification(
                NodeMigratedNotification(id=2)
            )
        # State should not change for connections that are in MOVING state
        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )

        # 4. FAILING_OVER notification (simulate direct connection handler call)
        for conn in in_use_connections:
            conn._maint_notifications_connection_handler.handle_notification(
                NodeFailingOverNotification(id=3, ttl=1)
            )
        # State should not change for connections that are in MOVING state
        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )

        # 5. FAILED_OVER notification (simulate direct connection handler call)
        for conn in in_use_connections:
            conn._maint_notifications_connection_handler.handle_notification(
                NodeFailedOverNotification(id=3)
            )
        # State should not change for connections that are in MOVING state
        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.MOVING,
            expected_host_address=tmp_address,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )

        # 6. MOVED notification (simulate timer expiry)
        pool_handler.handle_node_moved_notification(moving_notification)
        Helpers.validate_in_use_connections_state(
            in_use_connections,
            expected_state=MaintenanceState.NONE,
            expected_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_socket_timeout=None,
            expected_socket_connect_timeout=None,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=None,
            expected_current_peername=DEFAULT_ADDRESS.split(":")[0],
        )
        Helpers.validate_free_connections_state(
            pool=pool,
            should_be_connected_count=0,
            connected_to_tmp_address=False,
            expected_state=MaintenanceState.NONE,
            expected_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_socket_timeout=None,
            expected_socket_connect_timeout=None,
            expected_orig_host_address=DEFAULT_ADDRESS.split(":")[0],
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
        )
        # New connection after MOVED
        new_conn_none = pool.get_connection()
        assert new_conn_none.maintenance_state == MaintenanceState.NONE
        pool.release(new_conn_none)
        # Cleanup
        for conn in in_use_connections:
            pool.release(conn)
        if hasattr(pool, "disconnect"):
            pool.disconnect()


class TestMaintenanceNotificationsHandlingMultipleProxies(
    TestMaintenanceNotificationsBase
):
    """Integration tests for maintenance notifications handling with real connection pool."""

    def setup_method(self):
        """Set up test fixtures with mocked sockets."""
        super().setup_method()
        self.orig_host = "test.address.com"

        ips = ["1.2.3.4", "5.6.7.8", "9.10.11.12"]
        ips = ips * 3

        # Mock socket creation to return our mock sockets
        def mock_socket_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
            if host == self.orig_host:
                ip_address = ips.pop(0)
            else:
                ip_address = host

            # Return the standard getaddrinfo format
            # (family, type, proto, canonname, sockaddr)
            return [
                (
                    socket.AF_INET,
                    socket.SOCK_STREAM,
                    socket.IPPROTO_TCP,
                    "",
                    (ip_address, port),
                )
            ]

        self.getaddrinfo_patcher = patch(
            "socket.getaddrinfo", side_effect=mock_socket_getaddrinfo
        )
        self.getaddrinfo_patcher.start()

    def teardown_method(self):
        """Clean up test fixtures."""
        super().teardown_method()
        self.getaddrinfo_patcher.stop()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_migrating_after_moving_multiple_proxies(self, pool_class):
        """ """
        # Setup

        pool = pool_class(
            host=self.orig_host,
            port=12345,
            max_connections=10,
            protocol=3,  # Required for maintenance notifications
            maint_notifications_config=self.config,
        )
        pool.set_maint_notifications_pool_handler(
            MaintNotificationsPoolHandler(pool, self.config)
        )
        pool_handler = pool.connection_kwargs["maint_notifications_pool_handler"]

        # Create and release some connections
        key1 = "1.2.3.4"
        key2 = "5.6.7.8"
        key3 = "9.10.11.12"
        in_use_connections = {key1: [], key2: [], key3: []}
        # Create 7 connections
        for _ in range(7):
            conn = pool.get_connection()
            in_use_connections[conn.getpeername()].append(conn)

        for _, conns in in_use_connections.items():
            while len(conns) > 1:
                pool.release(conns.pop())

        # Send MOVING notification to con with ip = key1
        conn = in_use_connections[key1][0]
        pool_handler.set_connection(conn)
        new_ip = "13.14.15.16"
        pool_handler.handle_notification(
            NodeMovingNotification(
                id=1, new_node_host=new_ip, new_node_port=6379, ttl=1
            )
        )

        # validate in use connection and ip1
        Helpers.validate_in_use_connections_state(
            in_use_connections[key1],
            expected_state=MaintenanceState.MOVING,
            expected_host_address=new_ip,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=key1,
        )
        # validate free connections for ip1
        changed_free_connections = 0
        if isinstance(pool, BlockingConnectionPool):
            free_connections = [conn for conn in pool.pool.queue if conn is not None]
        elif isinstance(pool, ConnectionPool):
            free_connections = pool._available_connections
        for conn in free_connections:
            if conn.host == new_ip:
                changed_free_connections += 1
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.host == new_ip
                assert conn.socket_timeout == self.config.relaxed_timeout
                assert conn.socket_connect_timeout == self.config.relaxed_timeout
                assert conn.orig_host_address == self.orig_host
                assert conn.orig_socket_timeout is None
                assert conn.orig_socket_connect_timeout is None
            else:
                assert conn.maintenance_state == MaintenanceState.NONE
                assert conn.host == self.orig_host
                assert conn.socket_timeout is None
                assert conn.socket_connect_timeout is None
                assert conn.orig_host_address == self.orig_host
                assert conn.orig_socket_timeout is None
                assert conn.orig_socket_connect_timeout is None
        assert changed_free_connections == 2
        assert len(free_connections) == 4

        # Send second MOVING notification to con with ip = key2
        conn = in_use_connections[key2][0]
        pool_handler.set_connection(conn)
        new_ip_2 = "17.18.19.20"
        pool_handler.handle_notification(
            NodeMovingNotification(
                id=2, new_node_host=new_ip_2, new_node_port=6379, ttl=2
            )
        )

        # validate in use connection and ip2
        Helpers.validate_in_use_connections_state(
            in_use_connections[key2],
            expected_state=MaintenanceState.MOVING,
            expected_host_address=new_ip_2,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=key2,
        )
        # validate free connections for ip2
        changed_free_connections = 0
        if isinstance(pool, BlockingConnectionPool):
            free_connections = [conn for conn in pool.pool.queue if conn is not None]
        elif isinstance(pool, ConnectionPool):
            free_connections = pool._available_connections
        for conn in free_connections:
            if conn.host == new_ip_2:
                changed_free_connections += 1
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.host == new_ip_2
                assert conn.socket_timeout == self.config.relaxed_timeout
                assert conn.socket_connect_timeout == self.config.relaxed_timeout
                assert conn.orig_host_address == self.orig_host
                assert conn.orig_socket_timeout is None
                assert conn.orig_socket_connect_timeout is None
            # here I can't validate the other connections since some of
            # them are in MOVING state from the first notification
            # and some are in NONE state
        assert changed_free_connections == 1

        # MIGRATING notification on connection that has already been marked as MOVING
        conn = in_use_connections[key2][0]
        conn_notification_handler = conn._maint_notifications_connection_handler
        conn_notification_handler.handle_notification(
            NodeMigratingNotification(id=3, ttl=1)
        )
        # validate connection does not lose its MOVING state
        assert conn.maintenance_state == MaintenanceState.MOVING
        # MIGRATED notification
        conn_notification_handler.handle_notification(NodeMigratedNotification(id=3))
        # validate connection does not lose its MOVING state and relaxed timeout
        assert conn.maintenance_state == MaintenanceState.MOVING
        assert conn.socket_timeout == self.config.relaxed_timeout

        # Send Migrating notification to con with ip = key3
        conn = in_use_connections[key3][0]
        conn_notification_handler = conn._maint_notifications_connection_handler
        conn_notification_handler.handle_notification(
            NodeMigratingNotification(id=3, ttl=1)
        )
        # validate connection is in MIGRATING state
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE

        assert conn.socket_timeout == self.config.relaxed_timeout

        # Send MIGRATED notification to con with ip = key3
        conn_notification_handler.handle_notification(NodeMigratedNotification(id=3))
        # validate connection is in MOVING state
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout is None

        # sleep to expire only the first MOVING notifications
        sleep(1.3)
        # validate only the connections affected by the first MOVING notification
        # have lost their MOVING state
        Helpers.validate_in_use_connections_state(
            in_use_connections[key1],
            expected_state=MaintenanceState.NONE,
            expected_host_address=self.orig_host,
            expected_socket_timeout=None,
            expected_socket_connect_timeout=None,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=None,
            expected_current_peername=key1,
        )
        Helpers.validate_in_use_connections_state(
            in_use_connections[key2],
            expected_state=MaintenanceState.MOVING,
            expected_host_address=new_ip_2,
            expected_socket_timeout=self.config.relaxed_timeout,
            expected_socket_connect_timeout=self.config.relaxed_timeout,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=self.config.relaxed_timeout,
            expected_current_peername=key2,
        )
        Helpers.validate_in_use_connections_state(
            in_use_connections[key3],
            expected_state=MaintenanceState.NONE,
            expected_should_reconnect=False,
            expected_host_address=self.orig_host,
            expected_socket_timeout=None,
            expected_socket_connect_timeout=None,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=None,
            expected_current_peername=key3,
        )
        # TODO validate free connections

        # sleep to expire the second MOVING notifications
        sleep(1)
        # validate all connections have lost their MOVING state
        Helpers.validate_in_use_connections_state(
            [
                *in_use_connections[key1],
                *in_use_connections[key2],
                *in_use_connections[key3],
            ],
            expected_state=MaintenanceState.NONE,
            expected_should_reconnect="any",
            expected_host_address=self.orig_host,
            expected_socket_timeout=None,
            expected_socket_connect_timeout=None,
            expected_orig_host_address=self.orig_host,
            expected_orig_socket_timeout=None,
            expected_orig_socket_connect_timeout=None,
            expected_current_socket_timeout=None,
            expected_current_peername="any",
        )
        # TODO validate free connections
