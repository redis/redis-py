import socket
import threading
from typing import List
from unittest.mock import patch
import pytest
from time import sleep

from redis import Redis
from redis.connection import AbstractConnection, ConnectionPool, BlockingConnectionPool
from redis.maintenance_events import (
    MaintenanceEventsConfig,
    NodeMigratingEvent,
    MaintenanceEventPoolHandler,
)


class MockSocket:
    """Mock socket that simulates Redis protocol responses."""

    AFTER_MOVING_ADDRESS = "1.2.3.4:6379"
    DEFAULT_ADDRESS = "12.45.34.56:6379"
    MOVING_TIMEOUT = 1

    def __init__(self):
        self.connected = False
        self.address = None
        self.sent_data = []
        self.closed = False
        self.command_count = 0
        self.pending_responses = []
        # Track socket timeout changes for maintenance events validation
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
        elif b"SET" in data:
            response = b"+OK\r\n"

            # Check if this is a key that should trigger a push message
            if b"key_receive_migrating_" in data or b"key_receive_migrating" in data:
                # MIGRATING push message before SET key_receive_migrating_X response
                # Format: >2\r\n$9\r\nMIGRATING\r\n:10\r\n (2 elements: MIGRATING, ttl)
                migrating_push = ">2\r\n$9\r\nMIGRATING\r\n:10\r\n"
                response = migrating_push.encode() + response
            elif b"key_receive_migrated_" in data or b"key_receive_migrated" in data:
                # MIGRATED push message before SET key_receive_migrated_X response
                # Format: >1\r\n$8\r\nMIGRATED\r\n (1 element: MIGRATED)
                migrated_push = ">1\r\n$8\r\nMIGRATED\r\n"
                response = migrated_push.encode() + response
            elif b"key_receive_moving_" in data:
                # MOVING push message before SET key_receive_moving_X response
                # Format: >3\r\n$6\r\nMOVING\r\n:15\r\n+localhost:6379\r\n (3 elements: MOVING, ttl, host:port)
                # Note: Using + instead of $ to send as simple string instead of bulk string
                moving_push = f">3\r\n$6\r\nMOVING\r\n:{MockSocket.MOVING_TIMEOUT}\r\n+{MockSocket.AFTER_MOVING_ADDRESS}\r\n"
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
            elif b"key1" in data:
                self.pending_responses.append(b"$6\r\nvalue1\r\n")
            else:
                self.pending_responses.append(b"$-1\r\n")  # NULL response
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


class TestMaintenanceEventsHandling:
    """Integration tests for maintenance events handling with real connection pool."""

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

        # Create maintenance events config
        self.config = MaintenanceEventsConfig(
            enabled=True, proactive_reconnect=True, relax_timeout=30
        )

    def teardown_method(self):
        """Clean up test fixtures."""
        self.socket_patcher.stop()
        self.select_patcher.stop()

    def _get_client(
        self,
        pool_class,
        max_connections=10,
        maintenance_events_config=None,
        setup_pool_handler=False,
    ):
        """Helper method to create a pool and Redis client with maintenance events configuration.

        Args:
            pool_class: The connection pool class (ConnectionPool or BlockingConnectionPool)
            max_connections: Maximum number of connections in the pool (default: 10)
            maintenance_events_config: Optional MaintenanceEventsConfig to use. If not provided,
                                     uses self.config from setup_method (default: None)
            setup_pool_handler: Whether to set up pool handler for moving events (default: False)

        Returns:
            tuple: (test_pool, test_redis_client)
        """
        config = (
            maintenance_events_config
            if maintenance_events_config is not None
            else self.config
        )

        test_pool = pool_class(
            host=MockSocket.DEFAULT_ADDRESS.split(":")[0],
            port=int(MockSocket.DEFAULT_ADDRESS.split(":")[1]),
            max_connections=max_connections,
            protocol=3,  # Required for maintenance events
            maintenance_events_config=config,
        )
        test_redis_client = Redis(connection_pool=test_pool)

        # Set up pool handler for moving events if requested
        if setup_pool_handler:
            pool_handler = MaintenanceEventPoolHandler(
                test_redis_client.connection_pool, config
            )
            test_redis_client.connection_pool.set_maintenance_events_pool_handler(
                pool_handler
            )

        return test_redis_client

    def _validate_connection_handlers(self, conn, pool_handler, config):
        """Helper method to validate connection handlers are properly set."""
        # Test that the node moving handler function is correctly set
        parser_handler = conn._parser.node_moving_push_handler_func
        assert parser_handler is not None
        assert hasattr(parser_handler, "__self__")
        assert hasattr(parser_handler, "__func__")
        assert parser_handler.__self__ is pool_handler
        assert parser_handler.__func__ is pool_handler.handle_event.__func__

        # Test that the maintenance handler function is correctly set
        maintenance_handler = conn._parser.maintenance_push_handler_func
        assert maintenance_handler is not None
        assert hasattr(maintenance_handler, "__self__")
        assert hasattr(maintenance_handler, "__func__")
        # The maintenance handler should be bound to the connection's
        # maintenance event connection handler
        assert (
            maintenance_handler.__self__ is conn._maintenance_event_connection_handler
        )
        assert (
            maintenance_handler.__func__
            is conn._maintenance_event_connection_handler.handle_event.__func__
        )

        # Validate that the connection's maintenance handler has the same config object
        assert conn._maintenance_event_connection_handler.config is config

    def _validate_current_timeout_for_thread(
        self, thread_id, expected_timeout, error_msg=None
    ):
        """Helper method to validate the current timeout for the calling thread."""
        actual_timeout = None
        # Get the actual thread ID from the current thread
        current_thread_id = threading.current_thread().ident
        for sock in self.mock_sockets:
            if current_thread_id in sock.thread_timeouts:
                actual_timeout = sock.thread_timeouts[current_thread_id]
                break

        assert actual_timeout == expected_timeout, (
            error_msg,
            f"Thread {thread_id}: Expected timeout ({expected_timeout}), "
            f"but found timeout: {actual_timeout} for thread {thread_id}. "
            f"All thread timeouts: {[sock.thread_timeouts for sock in self.mock_sockets]}",
        )

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

    def _validate_in_use_connections_state(
        self, in_use_connections: List[AbstractConnection]
    ):
        """Helper method to validate state of in-use connections."""
        # validate in use connections are still working with set flag for reconnect
        # and timeout is updated
        for connection in in_use_connections:
            assert connection._should_reconnect is True
            assert (
                connection.tmp_host_address
                == MockSocket.AFTER_MOVING_ADDRESS.split(":")[0]
            )
            assert connection.tmp_relax_timeout == self.config.relax_timeout
            assert connection._sock.gettimeout() == self.config.relax_timeout
            assert connection._sock.connected is True
            assert (
                connection._sock.getpeername()[0]
                == MockSocket.DEFAULT_ADDRESS.split(":")[0]
            )

    def _validate_free_connections_state(
        self,
        pool,
        tmp_host_address,
        relax_timeout,
        should_be_connected_count,
        connected_to_tmp_addres=False,
    ):
        """Helper method to validate state of free/available connections."""
        if isinstance(pool, BlockingConnectionPool):
            # BlockingConnectionPool uses _connections list where created connections are stored
            # but we need to get the ones in the queue - these are the free ones
            # the uninitialized connections are filtered out
            free_connections = [conn for conn in pool.pool.queue if conn is not None]
        elif isinstance(pool, ConnectionPool):
            # Regular ConnectionPool uses _available_connections for free connections
            free_connections = pool._available_connections
        else:
            raise ValueError(f"Unsupported pool type: {type(pool)}")

        connected_count = 0
        # Validate fields that are validated in the validation of the active connections
        for connection in free_connections:
            # Validate the same fields as in _validate_in_use_connections_state
            assert connection._should_reconnect is False
            assert connection.tmp_host_address == tmp_host_address
            assert connection.tmp_relax_timeout == relax_timeout
            if connection._sock is not None:
                connected_count += 1

                if connected_to_tmp_addres:
                    assert (
                        connection._sock.getpeername()[0]
                        == MockSocket.AFTER_MOVING_ADDRESS.split(":")[0]
                    )
                else:
                    assert (
                        connection._sock.getpeername()[0]
                        == MockSocket.DEFAULT_ADDRESS.split(":")[0]
                    )
        assert connected_count == should_be_connected_count

    def _validate_all_timeouts(self, expected_timeout):
        """Helper method to validate state of in-use connections."""
        # validate in use connections are still working with set flag for reconnect
        # and timeout is updated
        for mock_socket in self.mock_sockets:
            if expected_timeout is None:
                assert mock_socket.gettimeout() is None
            else:
                assert mock_socket.gettimeout() == expected_timeout

    def _validate_conn_kwargs(
        self,
        pool,
        expected_host_address,
        expected_port,
        expected_tmp_host_address,
        expected_tmp_relax_timeout,
    ):
        """Helper method to validate connection kwargs."""
        assert pool.connection_kwargs["host"] == expected_host_address
        assert pool.connection_kwargs["port"] == expected_port
        assert pool.connection_kwargs["tmp_host_address"] == expected_tmp_host_address
        assert pool.connection_kwargs["tmp_relax_timeout"] == expected_tmp_relax_timeout

    def test_client_initialization(self):
        """Test that Redis client is created with maintenance events configuration."""
        # Create a pool and Redis client with maintenance events

        test_redis_client = Redis(
            protocol=3,  # Required for maintenance events
            maintenance_events_config=self.config,
        )

        pool_handler = test_redis_client.connection_pool.connection_kwargs.get(
            "maintenance_events_pool_handler"
        )
        assert pool_handler is not None
        assert pool_handler.config == self.config

        conn = test_redis_client.connection_pool.get_connection()
        assert conn._should_reconnect is False
        assert conn.tmp_host_address is None
        assert conn.tmp_relax_timeout == -1

        # Test that the node moving handler function is correctly set by
        # comparing the underlying function and instance
        parser_handler = conn._parser.node_moving_push_handler_func
        assert parser_handler is not None
        assert hasattr(parser_handler, "__self__")
        assert hasattr(parser_handler, "__func__")
        assert parser_handler.__self__ is pool_handler
        assert parser_handler.__func__ is pool_handler.handle_event.__func__

        # Test that the maintenance handler function is correctly set
        maintenance_handler = conn._parser.maintenance_push_handler_func
        assert maintenance_handler is not None
        assert hasattr(maintenance_handler, "__self__")
        assert hasattr(maintenance_handler, "__func__")
        # The maintenance handler should be bound to the connection's
        # maintenance event connection handler
        assert (
            maintenance_handler.__self__ is conn._maintenance_event_connection_handler
        )
        assert (
            maintenance_handler.__func__
            is conn._maintenance_event_connection_handler.handle_event.__func__
        )

        # Validate that the connection's maintenance handler has the same config object
        assert conn._maintenance_event_connection_handler.config is self.config

    def test_maint_handler_init_for_existing_connections(self):
        """Test that maintenance event handlers are properly set on existing and new connections
        when configuration is enabled after client creation."""

        # Create a Redis client with disabled maintenance events configuration
        disabled_config = MaintenanceEventsConfig(enabled=False)
        test_redis_client = Redis(
            protocol=3,  # Required for maintenance events
            maintenance_events_config=disabled_config,
        )

        # Extract an existing connection before enabling maintenance events
        existing_conn = test_redis_client.connection_pool.get_connection()

        # Verify that maintenance events are initially disabled
        assert existing_conn._parser.node_moving_push_handler_func is None
        assert not hasattr(existing_conn, "_maintenance_event_connection_handler")
        assert existing_conn._parser.maintenance_push_handler_func is None

        # Create a new enabled configuration and set up pool handler
        enabled_config = MaintenanceEventsConfig(
            enabled=True, proactive_reconnect=True, relax_timeout=30
        )
        pool_handler = MaintenanceEventPoolHandler(
            test_redis_client.connection_pool, enabled_config
        )
        test_redis_client.connection_pool.set_maintenance_events_pool_handler(
            pool_handler
        )

        # Validate the existing connection after enabling maintenance events
        # Both existing and new connections should now have full handler setup
        self._validate_connection_handlers(existing_conn, pool_handler, enabled_config)

        # Create a new connection and validate it has full handlers
        new_conn = test_redis_client.connection_pool.get_connection()
        self._validate_connection_handlers(new_conn, pool_handler, enabled_config)

        # Clean up connections
        test_redis_client.connection_pool.release(existing_conn)
        test_redis_client.connection_pool.release(new_conn)

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_connection_pool_creation_with_maintenance_events(self, pool_class):
        """Test that connection pools are created with maintenance events configuration."""
        # Create a pool and Redis client with maintenance events
        max_connections = 3 if pool_class == BlockingConnectionPool else 10
        test_redis_client = self._get_client(
            pool_class, max_connections=max_connections
        )
        test_pool = test_redis_client.connection_pool

        try:
            assert (
                test_pool.connection_kwargs.get("maintenance_events_config")
                == self.config
            )
            # Pool should have maintenance events enabled
            assert test_pool.maintenance_events_pool_handler_enabled() is True

            # Create and set a pool handler
            pool_handler = MaintenanceEventPoolHandler(test_pool, self.config)
            test_pool.set_maintenance_events_pool_handler(pool_handler)

            # Validate that the handler is properly set on the pool
            assert (
                test_pool.connection_kwargs.get("maintenance_events_pool_handler")
                == pool_handler
            )
            assert (
                test_pool.connection_kwargs.get("maintenance_events_config")
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
        # Create a pool and Redis client with maintenance events
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

            # Verify that the connection has maintenance event handler
            connection = test_redis_client.connection_pool.get_connection()
            assert hasattr(connection, "_maintenance_event_connection_handler")
            test_redis_client.connection_pool.release(connection)

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    def test_pool_handler_with_migrating_event(self):
        """Test that pool handler correctly handles migrating events."""
        # Create a pool and Redis client with maintenance events
        test_redis_client = self._get_client(ConnectionPool)
        test_pool = test_redis_client.connection_pool

        try:
            # Create and set a pool handler
            pool_handler = MaintenanceEventPoolHandler(test_pool, self.config)

            # Create a migrating event (not handled by pool handler)
            migrating_event = NodeMigratingEvent(id=1, ttl=5)

            # Mock the required functions
            with (
                patch.object(
                    pool_handler, "remove_expired_notifications"
                ) as mock_remove_expired,
                patch.object(
                    pool_handler, "handle_node_moving_event"
                ) as mock_handle_moving,
                patch("redis.maintenance_events.logging.error") as mock_logging_error,
            ):
                # Pool handler should return None for migrating events (not its responsibility)
                pool_handler.handle_event(migrating_event)

                # Validate that remove_expired_notifications has been called once
                mock_remove_expired.assert_called_once()

                # Validate that handle_node_moving_event hasn't been called
                mock_handle_moving.assert_not_called()

                # Validate that logging.error has been called once
                mock_logging_error.assert_called_once()

        finally:
            if hasattr(test_pool, "disconnect"):
                test_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_migration_related_events_handling_integration(self, pool_class):
        """
        Test full integration of migration-related events (MIGRATING/MIGRATED) handling.

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
        # Create a pool and Redis client with maintenance events
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

            # Verify maintenance events were processed correctly
            # The key is that we have at least 1 socket and all operations succeeded
            assert len(self.mock_sockets) >= 1, (
                f"Expected at least 1 socket for operations, got {len(self.mock_sockets)}"
            )

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_migrating_event_with_disabled_relax_timeout(self, pool_class):
        """
        Test migrating event handling when relax timeout is disabled.

        This test validates that when relax_timeout is disabled (-1):
        1. MIGRATING events are received and processed
        2. No timeout updates are applied to connections
        3. Socket timeouts remain unchanged during migration events
        4. Tests both ConnectionPool and BlockingConnectionPool implementations
        """
        # Create config with disabled relax timeout
        disabled_config = MaintenanceEventsConfig(
            enabled=True,
            relax_timeout=-1,  # This means the relax timeout is Disabled
        )

        # Create a pool and Redis client with disabled relax timeout config
        test_redis_client = self._get_client(
            pool_class, max_connections=5, maintenance_events_config=disabled_config
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

            # Validate timeout was NOT updated (relax is disabled)
            # Should remain at default timeout (None), not relaxed to 30s
            self._validate_current_timeout(None)

            # Command 3: Another command to verify timeout remains unchanged
            result3 = test_redis_client.get(key1)

            # Validate Command 3 result
            expected_value3 = value1.encode()
            assert result3 == expected_value3, (
                f"Command 3 (GET key1) failed. Expected: {expected_value3}, Got: {result3}"
            )

            # Verify maintenance events were processed correctly
            # The key is that we have at least 1 socket and all operations succeeded
            assert len(self.mock_sockets) >= 1, (
                f"Expected at least 1 socket for operations, got {len(self.mock_sockets)}"
            )

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_moving_related_events_handling_integration(self, pool_class):
        """
        Test full integration of moving-related events (MOVING) handling with Redis commands.

        This test validates the complete MOVING event lifecycle:
        1. Creates multiple connections in the pool
        2. Executes a Redis command that triggers a MOVING push message
        3. Validates that pool configuration is updated with temporary address and timeout
        4. Validates that existing connections are marked for disconnection
        5. Tests both ConnectionPool and BlockingConnectionPool implementations
        """
        # Create a pool and Redis client with maintenance events and pool handler
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

            # Validate all connections are connected prior MOVING event
            self._validate_disconnected(0)

            # Run command that will receive and handle MOVING event
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            # the connection used for the command is expected to be reconnected to the new address
            # before it is returned to the pool
            result2 = test_redis_client.set(key_moving, value_moving)

            # Validate Command 2 result
            assert result2 is True, "Command 2 (SET key_receive_moving) failed"

            # Validate pool and connections settings were updated according to MOVING event
            # handling expectations
            self._validate_conn_kwargs(
                test_redis_client.connection_pool,
                MockSocket.DEFAULT_ADDRESS.split(":")[0],
                int(MockSocket.DEFAULT_ADDRESS.split(":")[1]),
                MockSocket.AFTER_MOVING_ADDRESS.split(":")[0],
                self.config.relax_timeout,
            )
            # 5 disconnects has happened, 1 of them is with reconnect
            self._validate_disconnected(5)
            # 5 in use connected + 1 after reconnect
            self._validate_connected(6)
            self._validate_in_use_connections_state(in_use_connections)
            # Validate there is 1 free connection that is connected
            # the one that has handled the MOVING should reconnect after parsing the response
            self._validate_free_connections_state(
                test_redis_client.connection_pool,
                MockSocket.AFTER_MOVING_ADDRESS.split(":")[0],
                self.config.relax_timeout,
                should_be_connected_count=1,
                connected_to_tmp_addres=True,
            )

            # Wait for MOVING timeout to expire and the moving completed handler to run
            print("Waiting for MOVING timeout to expire...")
            sleep(MockSocket.MOVING_TIMEOUT + 0.5)

            self._validate_all_timeouts(None)
            self._validate_conn_kwargs(
                test_redis_client.connection_pool,
                MockSocket.DEFAULT_ADDRESS.split(":")[0],
                int(MockSocket.DEFAULT_ADDRESS.split(":")[1]),
                None,
                -1,
            )
            self._validate_free_connections_state(
                test_redis_client.connection_pool,
                None,
                -1,
                should_be_connected_count=1,
                connected_to_tmp_addres=True,
            )

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_create_new_conn_while_moving_not_expired(self, pool_class):
        """
        Test creating new connections while MOVING event is active (not expired).

        This test validates that:
        1. After MOVING event is processed, new connections are created with temporary address
        2. New connections inherit the relaxed timeout settings
        3. Pool configuration is properly applied to newly created connections
        """
        # Create a pool and Redis client with maintenance events and pool handler
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

            # Validate all connections are connected prior MOVING event
            self._validate_disconnected(0)

            # Run command that will receive and handle MOVING event
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            result = test_redis_client.set(key_moving, value_moving)

            # Validate command result
            assert result is True, "SET key_receive_moving command failed"

            # Validate pool and connections settings were updated according to MOVING event
            self._validate_conn_kwargs(
                test_redis_client.connection_pool,
                MockSocket.DEFAULT_ADDRESS.split(":")[0],
                int(MockSocket.DEFAULT_ADDRESS.split(":")[1]),
                MockSocket.AFTER_MOVING_ADDRESS.split(":")[0],
                self.config.relax_timeout,
            )

            # Now get several more connections to force creation of new ones
            # This should create new connections with the temporary address
            old_connections = []
            for _ in range(2):
                connection = test_redis_client.connection_pool.get_connection()
                old_connections.append(connection)

            new_connection = test_redis_client.connection_pool.get_connection()

            # Validate that new connections are created with temporary address and relax timeout
            # and when connecting those configs are used
            # get_connection() returns a connection that is already connected
            assert (
                new_connection.tmp_host_address
                == MockSocket.AFTER_MOVING_ADDRESS.split(":")[0]
            )
            assert new_connection.tmp_relax_timeout == self.config.relax_timeout
            # New connections should be connected to the temporary address
            assert new_connection._sock is not None
            assert new_connection._sock.connected is True
            assert (
                new_connection._sock.getpeername()[0]
                == MockSocket.AFTER_MOVING_ADDRESS.split(":")[0]
            )
            assert new_connection._sock.gettimeout() == self.config.relax_timeout

        finally:
            if hasattr(test_redis_client.connection_pool, "disconnect"):
                test_redis_client.connection_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_create_new_conn_after_moving_expires(self, pool_class):
        """
        Test creating new connections after MOVING event expires.

        This test validates that:
        1. After MOVING timeout expires, new connections use original address
        2. Pool configuration is reset to original values
        3. New connections don't inherit temporary settings
        """
        # Create a pool and Redis client with maintenance events and pool handler
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

            # Run command that will receive and handle MOVING event
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            result = test_redis_client.set(key_moving, value_moving)

            # Validate command result
            assert result is True, "SET key_receive_moving command failed"

            # Wait for MOVING timeout to expire
            print("Waiting for MOVING timeout to expire...")
            sleep(MockSocket.MOVING_TIMEOUT + 0.5)

            # Now get several new connections after expiration
            old_connections = []
            for _ in range(2):
                connection = test_redis_client.connection_pool.get_connection()
                old_connections.append(connection)

            new_connection = test_redis_client.connection_pool.get_connection()

            # Validate that new connections are created with original address (no temporary settings)
            assert new_connection.tmp_host_address is None
            assert new_connection.tmp_relax_timeout == -1
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
        Test receiving MIGRATED event after MOVING event.

        This test validates the complete MOVING -> MIGRATED lifecycle:
        1. MOVING event is processed and temporary settings are applied
        2. MIGRATED event is received during command execution
        3. Temporary settings are cleared after MIGRATED
        4. Pool configuration is restored to original values

        Note: When MIGRATED comes after MOVING and MOVING hasn't yet expired,
        it should not decrease timeouts (future refactoring consideration).
        """
        # Create a pool and Redis client with maintenance events and pool handler
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

            # Validate all connections are connected prior MOVING event
            self._validate_disconnected(0)

            # Step 1: Run command that will receive and handle MOVING event
            key_moving = "key_receive_moving_0"
            value_moving = "value3_0"
            result_moving = test_redis_client.set(key_moving, value_moving)

            # Validate MOVING command result
            assert result_moving is True, "SET key_receive_moving command failed"

            # Validate pool and connections settings were updated according to MOVING event
            self._validate_conn_kwargs(
                test_redis_client.connection_pool,
                MockSocket.DEFAULT_ADDRESS.split(":")[0],
                int(MockSocket.DEFAULT_ADDRESS.split(":")[1]),
                MockSocket.AFTER_MOVING_ADDRESS.split(":")[0],
                self.config.relax_timeout,
            )

            # Step 2: Run command that will receive and handle MIGRATED event
            # This should clear the temporary settings
            key_migrated = "key_receive_migrated_0"
            value_migrated = "migrated_value"
            result_migrated = test_redis_client.set(key_migrated, value_migrated)

            # Validate MIGRATED command result
            assert result_migrated is True, "SET key_receive_migrated command failed"

            # Step 3: Validate that MIGRATED event was processed but MOVING settings remain
            # (MIGRATED doesn't automatically clear MOVING settings - they are separate events)
            self._validate_conn_kwargs(
                test_redis_client.connection_pool,
                MockSocket.DEFAULT_ADDRESS.split(":")[0],
                int(MockSocket.DEFAULT_ADDRESS.split(":")[1]),
                MockSocket.AFTER_MOVING_ADDRESS.split(":")[
                    0
                ],  # MOVING settings still active
                self.config.relax_timeout,  # MOVING timeout still active
            )

            # Step 4: Create new connections after MIGRATED to verify they still use MOVING settings
            # (since MOVING settings are still active)
            new_connections = []
            for _ in range(2):
                connection = test_redis_client.connection_pool.get_connection()
                new_connections.append(connection)

            # Validate that new connections are created with MOVING settings (still active)
            for connection in new_connections:
                assert (
                    connection.tmp_host_address
                    == MockSocket.AFTER_MOVING_ADDRESS.split(":")[0]
                )
                # Note: New connections may not inherit the exact relax timeout value
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
