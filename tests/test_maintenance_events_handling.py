import socket
import threading
from unittest.mock import Mock, patch
import pytest

from redis import Redis
from redis.connection import ConnectionPool, BlockingConnectionPool
from redis.maintenance_events import (
    MaintenanceEventsConfig,
    NodeMigratingEvent,
    MaintenanceEventPoolHandler,
)


class MockSocket:
    """Mock socket that simulates Redis protocol responses."""

    def __init__(self):
        self.connected = False
        self.address = None
        self.sent_data = []
        self.response_queue = []
        self.closed = False
        self.command_count = 0
        self.pending_responses = []
        self.current_response_index = 0
        # Track socket timeout changes for maintenance events validation
        self.timeout = None
        self.thread_timeouts = {}  # Track last applied timeout per thread

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
            if b"key_receive_migrating_" in data:
                # MIGRATING push message before SET key_receive_migrating_X response
                # Format: >2\r\n$9\r\nMIGRATING\r\n:10\r\n (2 elements: MIGRATING, ttl)
                migrating_push = ">2\r\n$9\r\nMIGRATING\r\n:10\r\n"
                response = migrating_push.encode() + response
            elif b"key_receive_migrated_" in data:
                # MIGRATED push message before SET key_receive_migrated_X response
                # Format: >1\r\n$8\r\nMIGRATED\r\n (1 element: MIGRATED)
                migrated_push = ">1\r\n$8\r\nMIGRATED\r\n"
                response = migrated_push.encode() + response

            self.pending_responses.append(response)
        elif b"GET" in data:
            # Extract key and provide appropriate response
            if b"hello" in data:
                response = b"$5\r\nworld\r\n"
                self.pending_responses.append(response)
            # Handle thread-specific keys for integration test first (more specific)
            elif b"key1_0" in data:
                self.pending_responses.append(b"$8\r\nvalue1_0\r\n")
            elif b"key_receive_migrating_0" in data:
                self.pending_responses.append(b"$8\r\nvalue2_0\r\n")
            elif b"key1_1" in data:
                self.pending_responses.append(b"$8\r\nvalue1_1\r\n")
            elif b"key_receive_migrating_1" in data:
                self.pending_responses.append(b"$8\r\nvalue2_1\r\n")
            elif b"key1_2" in data:
                self.pending_responses.append(b"$8\r\nvalue1_2\r\n")
            elif b"key_receive_migrating_2" in data:
                self.pending_responses.append(b"$8\r\nvalue2_2\r\n")
            # Generic keys (less specific, should come after thread-specific)
            elif b"key0" in data:
                self.pending_responses.append(b"$6\r\nvalue0\r\n")
            elif b"key1" in data:
                self.pending_responses.append(b"$6\r\nvalue1\r\n")
            elif b"key2" in data:
                self.pending_responses.append(b"$6\r\nvalue2\r\n")
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
        if self.response_queue:
            response = self.response_queue.pop(0)
            return response[:bufsize]  # Respect buffer size

        # Use pending responses that were prepared when commands were sent
        if self.pending_responses:
            response = self.pending_responses.pop(0)
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

    def settimeout(self, timeout):
        """Simulate setting socket timeout and track changes per thread."""
        self.timeout = timeout

        # Track last applied timeout per thread
        thread_id = threading.current_thread().ident
        self.thread_timeouts[thread_id] = timeout

    def setsockopt(self, level, optname, value):
        """Simulate setting socket options."""
        pass

    def getpeername(self):
        """Simulate getting peer name."""
        return ("127.0.0.1", 6379)

    def getsockname(self):
        """Simulate getting socket name."""
        return ("127.0.0.1", 12345)

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
                    if (
                        hasattr(sock, "pending_responses") and sock.pending_responses
                    ) or (hasattr(sock, "response_queue") and sock.response_queue):
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
        self, pool_class, max_connections=10, maintenance_events_config=None
    ):
        """Helper method to create a pool and Redis client with maintenance events configuration.

        Args:
            pool_class: The connection pool class (ConnectionPool or BlockingConnectionPool)
            max_connections: Maximum number of connections in the pool (default: 10)
            maintenance_events_config: Optional MaintenanceEventsConfig to use. If not provided,
                                     uses self.config from setup_method (default: None)

        Returns:
            tuple: (test_pool, test_redis_client)
        """
        config = (
            maintenance_events_config
            if maintenance_events_config is not None
            else self.config
        )

        test_pool = pool_class(
            host="localhost",
            port=6379,
            max_connections=max_connections,
            protocol=3,  # Required for maintenance events
            maintenance_events_config=config,
        )
        test_redis_client = Redis(connection_pool=test_pool)
        return test_pool, test_redis_client

    def _validate_current_timeout_for_thread(self, thread_id, expected_timeout):
        """Helper method to validate the current timeout for the calling thread."""
        current_thread_id = threading.current_thread().ident
        actual_timeout = None
        for sock in self.mock_sockets:
            if current_thread_id in sock.thread_timeouts:
                actual_timeout = sock.thread_timeouts[current_thread_id]
                break

        assert actual_timeout == expected_timeout, (
            f"Thread {thread_id}: Expected timeout ({expected_timeout}), "
            f"but found timeout: {actual_timeout} for thread {current_thread_id}. "
            f"All thread timeouts: {[sock.thread_timeouts for sock in self.mock_sockets]}"
        )

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_connection_pool_creation_with_maintenance_events(self, pool_class):
        """Test that connection pools are created with maintenance events configuration."""
        # Create a pool and Redis client with maintenance events
        max_connections = 3 if pool_class == BlockingConnectionPool else 10
        test_pool, _ = self._get_client(pool_class, max_connections=max_connections)

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
        test_pool, test_redis_client = self._get_client(pool_class, max_connections=5)

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
            connection = test_pool.get_connection()
            assert hasattr(connection, "_maintenance_event_connection_handler")
            test_pool.release(connection)

        finally:
            if hasattr(test_pool, "disconnect"):
                test_pool.disconnect()

    @pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
    def test_multiple_connections_in_pool(self, pool_class):
        """Test that multiple connections can be created and used for Redis operations in multiple threads."""
        # Create a pool and Redis client with maintenance events
        test_pool, test_redis_client = self._get_client(pool_class, max_connections=5)

        try:
            # Results storage for thread operations
            results = []
            errors = []

            def redis_operation(key_suffix):
                """Perform Redis operations in a thread."""
                try:
                    # SET operation
                    set_result = test_redis_client.set(
                        f"key{key_suffix}", f"value{key_suffix}"
                    )
                    # GET operation
                    get_result = test_redis_client.get(f"key{key_suffix}")
                    results.append((set_result, get_result))
                except Exception as e:
                    errors.append(e)

            # Run operations in multiple threads to force multiple connections
            threads = []
            for i in range(3):
                thread = threading.Thread(target=redis_operation, args=(i,))
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Verify no errors occurred
            assert len(errors) == 0, f"Errors occurred: {errors}"

            # Verify all operations completed successfully
            assert len(results) == 3
            for set_result, get_result in results:
                assert set_result is True
                assert get_result in [b"value0", b"value1", b"value2"]

            # Verify that multiple connections were created with mock sockets
            # With threading, both pool types should create multiple sockets for concurrent access
            assert len(self.mock_sockets) >= 2, (
                f"Expected multiple sockets due to threading, got {len(self.mock_sockets)}"
            )

            # Verify each connection has maintenance event handler
            connection = test_pool.get_connection()
            assert hasattr(connection, "_maintenance_event_connection_handler")
            test_pool.release(connection)

        finally:
            if hasattr(test_pool, "disconnect"):
                test_pool.disconnect()

    def test_pool_handler_with_migrating_event(self):
        """Test that pool handler correctly handles migrating events."""
        # Create a pool and Redis client with maintenance events
        test_pool, _ = self._get_client(ConnectionPool)

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
        Test full integration of migration-related events (MIGRATING/MIGRATED) handling with multiple threads and commands.

        This test validates the complete migration lifecycle:
        1. Creates 3 concurrent threads, each executing 5 Redis commands
        2. Injects MIGRATING push message before command 2 (SET key_receive_migrating_X)
        3. Validates socket timeout is updated to relaxed value (30s) after MIGRATING
        4. Executes commands 3-4 while timeout remains relaxed
        5. Injects MIGRATED push message before command 5 (SET key_receive_migrated_X)
        6. Validates socket timeout is restored after MIGRATED
        7. Tests both ConnectionPool and BlockingConnectionPool implementations
        8. Uses proper RESP3 push message format for realistic protocol simulation
        """
        # Create a pool and Redis client with maintenance events
        test_pool, test_redis_client = self._get_client(pool_class, max_connections=10)

        try:
            # Results storage for thread operations
            results = []
            errors = []

            def redis_operations_with_maintenance_events(thread_id):
                """Perform Redis operations with maintenance events in a thread."""
                try:
                    # Command 1: Initial command
                    key1 = f"key1_{thread_id}"
                    value1 = f"value1_{thread_id}"
                    result1 = test_redis_client.set(key1, value1)

                    # Validate Command 1 result
                    erros_msg = f"Thread {thread_id}: Command 1 (SET key1) failed"
                    assert result1 is True, erros_msg

                    # Command 2: This SET command will receive MIGRATING push message before response
                    key_migrating = f"key_receive_migrating_{thread_id}"
                    value_migrating = f"value2_{thread_id}"
                    result2 = test_redis_client.set(key_migrating, value_migrating)

                    # Validate Command 2 result
                    erros_msg = f"Thread {thread_id}: Command 2 (SET key_receive_migrating) failed"
                    assert result2 is True, erros_msg

                    # Step 4: Validate timeout was updated to relaxed value after MIGRATING
                    self._validate_current_timeout_for_thread(thread_id, 30)

                    # Command 3: Another command while timeout is still relaxed
                    result3 = test_redis_client.get(key1)

                    # Validate Command 3 result
                    expected_value3 = value1.encode()
                    errors_msg = (
                        f"Thread {thread_id}: Command 3 (GET key1) failed. "
                        f"Expected {expected_value3}, got {result3}"
                    )
                    assert result3 == expected_value3, errors_msg

                    # Command 4: Execute command (step 5)
                    result4 = test_redis_client.get(key_migrating)

                    # Validate Command 4 result
                    expected_value4 = value_migrating.encode()
                    errors_msg = (
                        f"Thread {thread_id}: Command 4 (GET key_receive_migrating) failed. "
                        f"Expected {expected_value4}, got {result4}"
                    )
                    assert result4 == expected_value4, errors_msg

                    # Step 6: Validate socket timeout is still relaxed during commands 3-4
                    self._validate_current_timeout_for_thread(thread_id, 30)

                    # Command 5: This SET command will receive
                    # MIGRATED push message before actual response
                    key_migrated = f"key_receive_migrated_{thread_id}"
                    value_migrated = f"value3_{thread_id}"
                    result5 = test_redis_client.set(key_migrated, value_migrated)

                    # Validate Command 5 result
                    errors_msg = f"Thread {thread_id}: Command 5 (SET key_receive_migrated) failed"
                    assert result5 is True, errors_msg

                    # Step 8: Validate socket timeout is reversed back to original after MIGRATED
                    self._validate_current_timeout_for_thread(thread_id, None)

                    results.append(
                        {
                            "thread_id": thread_id,
                            "success": True,
                        }
                    )

                except Exception as e:
                    errors.append(f"Thread {thread_id}: {e}")

            # Run operations in multiple threads (step 1)
            threads = []
            for i in range(3):
                thread = threading.Thread(
                    target=redis_operations_with_maintenance_events,
                    args=(i,),
                    name=str(i),
                )
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Verify all threads completed successfully
            successful_threads = len(results)
            assert successful_threads == 3, (
                f"Expected 3 successful threads, got {successful_threads}. "
                f"Errors: {errors}"
            )

            # Verify maintenance events were processed correctly across all threads
            # Note: Different pool types may create different numbers of sockets
            # The key is that we have at least 1 socket and all threads succeeded
            assert len(self.mock_sockets) >= 1, (
                f"Expected at least 1 socket for operations, got {len(self.mock_sockets)}"
            )

        finally:
            if hasattr(test_pool, "disconnect"):
                test_pool.disconnect()

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
        test_pool, test_redis_client = self._get_client(
            pool_class, max_connections=5, maintenance_events_config=disabled_config
        )

        try:
            # Results storage for thread operations
            results = []
            errors = []

            def redis_operations_with_disabled_relax(thread_id):
                """Perform Redis operations with disabled relax timeout in a thread."""
                try:
                    # Command 1: Initial command
                    key1 = f"key1_{thread_id}"
                    value1 = f"value1_{thread_id}"
                    result1 = test_redis_client.set(key1, value1)

                    # Validate Command 1 result
                    errors_msg = f"Thread {thread_id}: Command 1 (SET key1) failed"
                    assert result1 is True, errors_msg

                    # Command 2: This SET command will receive MIGRATING push message before response
                    key_migrating = f"key_receive_migrating_{thread_id}"
                    value_migrating = f"value2_{thread_id}"
                    result2 = test_redis_client.set(key_migrating, value_migrating)

                    # Validate Command 2 result
                    errors_msg = f"Thread {thread_id}: Command 2 (SET key_receive_migrating) failed"
                    assert result2 is True, errors_msg

                    # Validate timeout was NOT updated (relax is disabled)
                    # Should remain at default timeout (None), not relaxed to 30s
                    self._validate_current_timeout_for_thread(thread_id, None)

                    # Command 3: Another command to verify timeout remains unchanged
                    result3 = test_redis_client.get(key1)

                    # Validate Command 3 result
                    expected_value3 = value1.encode()
                    errors_msg = (
                        f"Thread {thread_id}: Command 3 (GET key1) failed. "
                        f"Expected: {expected_value3}, Got: {result3}"
                    )
                    assert result3 == expected_value3, errors_msg

                    results.append(
                        {
                            "thread_id": thread_id,
                            "success": True,
                        }
                    )

                except Exception as e:
                    errors.append(f"Thread {thread_id}: {str(e)}")

            # Run operations in multiple threads to test concurrent behavior
            threads = []
            for i in range(3):
                thread = threading.Thread(
                    target=redis_operations_with_disabled_relax, args=(i,)
                )
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Verify no errors occurred
            assert len(errors) == 0, f"Errors occurred: {errors}"

            # Verify all operations completed successfully
            assert len(results) == 3, (
                f"Expected 3 successful threads, got {len(results)}"
            )

            # Verify maintenance events were processed correctly across all threads
            # Note: Different pool types may create different numbers of sockets
            # The key is that we have at least 1 socket and all threads succeeded
            assert len(self.mock_sockets) >= 1, (
                f"Expected at least 1 socket for operations, got {len(self.mock_sockets)}"
            )

        finally:
            if hasattr(test_pool, "disconnect"):
                test_pool.disconnect()
