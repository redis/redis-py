import pytest
import redis
from unittest import mock
from redis.connection import ConnectionInterface


class DummyConnection(ConnectionInterface):
    """A dummy connection class for testing that doesn't actually connect to Redis"""

    def __init__(self, *args, **kwargs):
        self.connected = False

    @property
    def is_connected(self) -> bool:
        return self.connected

    def connect(self):
        self.connected = True

    def disconnect(self):
        self.connected = False

    def register_connect_callback(self, callback):
        pass

    def deregister_connect_callback(self, callback):
        pass

    def set_parser(self, parser_class):
        pass

    def get_protocol(self):
        return 2

    def on_connect(self):
        pass

    def check_health(self):
        return True

    def send_packed_command(self, command, check_health=True):
        pass

    def send_command(self, *args, **kwargs):
        pass

    def can_read(self, timeout: float = 0) -> bool:
        return False

    def read_response(self, disable_decoding=False, **kwargs):
        return "PONG"


@pytest.mark.onlynoncluster
def test_max_connections_error_inheritance():
    """Test that MaxConnectionsError is a subclass of ConnectionError"""
    assert issubclass(redis.MaxConnectionsError, redis.ConnectionError)


@pytest.mark.onlynoncluster
def test_connection_pool_raises_max_connections_error():
    """Test that ConnectionPool raises MaxConnectionsError and not ConnectionError"""
    # Use a dummy connection class that doesn't try to connect to a real Redis server
    pool = redis.ConnectionPool(max_connections=1, connection_class=DummyConnection)
    pool.get_connection()

    with pytest.raises(redis.MaxConnectionsError):
        pool.get_connection()


@pytest.mark.skipif(
    not hasattr(redis, "RedisCluster"), reason="RedisCluster not available"
)
def test_cluster_handles_max_connections_error():
    """
    Test that RedisCluster doesn't reinitialize when MaxConnectionsError is raised
    """
    # Create a more complete mock cluster
    cluster = mock.MagicMock(spec=redis.RedisCluster)
    cluster.cluster_response_callbacks = {}
    cluster.RedisClusterRequestTTL = 3  # Set the TTL to avoid infinite loops
    cluster.nodes_manager = mock.MagicMock()
    cluster.node_health_manager = redis.cluster.NodeHealthManager()
    node = mock.MagicMock()

    # Mock get_redis_connection to return a mock Redis client
    redis_conn = mock.MagicMock()
    cluster.get_redis_connection.return_value = redis_conn

    # Setup get_connection to be called and return a connection that will raise
    connection = mock.MagicMock()

    # Patch the get_connection function in the cluster module
    with mock.patch("redis.cluster.get_connection", return_value=connection):
        # Test MaxConnectionsError
        connection.send_command.side_effect = redis.MaxConnectionsError(
            "Too many connections"
        )

        # Call the method and check that the exception is raised
        with pytest.raises(redis.MaxConnectionsError):
            redis.RedisCluster._execute_command(cluster, node, "GET", "key")

        # Verify nodes_manager.initialize was NOT called
        cluster.nodes_manager.initialize.assert_not_called()

        # Reset the mock for the next test
        cluster.nodes_manager.initialize.reset_mock()

        # Now test with regular ConnectionError to ensure it DOES reinitialize
        connection.send_command.side_effect = redis.ConnectionError("Connection lost")

        with pytest.raises(redis.ConnectionError):
            redis.RedisCluster._execute_command(cluster, node, "GET", "key")

        # Verify nodes_manager.initialize WAS called
        cluster.nodes_manager.initialize.assert_called_once()


class TestNodeHealthManager:
    def test_node_starts_available(self):
        hm = redis.cluster.NodeHealthManager(failure_threshold=3)
        assert hm.is_available("node1:6379") is True

    def test_node_unavailable_after_threshold(self):
        hm = redis.cluster.NodeHealthManager(failure_threshold=3)
        hm.record_failure("node1:6379")
        hm.record_failure("node1:6379")
        assert hm.is_available("node1:6379") is True
        hm.record_failure("node1:6379")
        assert hm.is_available("node1:6379") is False

    def test_node_recovers_on_success(self):
        hm = redis.cluster.NodeHealthManager(failure_threshold=2)
        hm.record_failure("node1:6379")
        hm.record_failure("node1:6379")
        assert hm.is_available("node1:6379") is False
        hm.record_success("node1:6379")
        assert hm.is_available("node1:6379") is True

    def test_node_recovers_after_recovery_time(self):
        hm = redis.cluster.NodeHealthManager(failure_threshold=2, recovery_time=0.1)
        hm.record_failure("node1:6379")
        hm.record_failure("node1:6379")
        assert hm.is_available("node1:6379") is False
        import time
        time.sleep(0.15)
        # After recovery_time, node goes half-open (allows one probe)
        assert hm.is_available("node1:6379") is True
        # Only one probe allowed — second call is blocked
        assert hm.is_available("node1:6379") is False

    def test_failure_count_resets_on_success(self):
        hm = redis.cluster.NodeHealthManager(failure_threshold=3)
        hm.record_failure("node1:6379")
        hm.record_failure("node1:6379")
        hm.record_success("node1:6379")
        # Counter reset, so 2 more failures should not trigger unavailable
        hm.record_failure("node1:6379")
        hm.record_failure("node1:6379")
        assert hm.is_available("node1:6379") is True

    def test_per_node_isolation(self):
        hm = redis.cluster.NodeHealthManager(failure_threshold=2)
        hm.record_failure("node1:6379")
        hm.record_failure("node1:6379")
        assert hm.is_available("node1:6379") is False
        assert hm.is_available("node2:6379") is True

    def test_cluster_fails_fast_on_unavailable_node(self):
        """Cluster _execute_command raises immediately for unavailable nodes."""
        from redis.exceptions import NodeUnavailableError

        cluster = mock.MagicMock(spec=redis.RedisCluster)
        cluster.cluster_response_callbacks = {}
        cluster.RedisClusterRequestTTL = 3
        cluster.nodes_manager = mock.MagicMock()
        hm = redis.cluster.NodeHealthManager(failure_threshold=1)
        hm.record_failure("testnode:6379")
        cluster.node_health_manager = hm

        node = mock.MagicMock()
        node.name = "testnode:6379"

        with mock.patch("redis.cluster.get_connection") as mock_get_conn:
            with pytest.raises(NodeUnavailableError, match="marked unavailable"):
                redis.RedisCluster._execute_command(cluster, node, "GET", "key")
            # get_connection should never be called — we failed fast
            mock_get_conn.assert_not_called()
            # initialize should NOT be called — no real network failure
            cluster.nodes_manager.initialize.assert_not_called()

    def test_half_open_allows_single_probe(self):
        """Only one request goes through in half-open state."""
        hm = redis.cluster.NodeHealthManager(failure_threshold=2, recovery_time=0.1)
        hm.record_failure("node1:6379")
        hm.record_failure("node1:6379")
        assert hm.is_available("node1:6379") is False

        import time
        time.sleep(0.15)

        # First call transitions to half-open — one probe allowed
        assert hm.is_available("node1:6379") is True
        # Subsequent calls blocked while probe is in-flight
        assert hm.is_available("node1:6379") is False
        assert hm.is_available("node1:6379") is False

    def test_half_open_probe_success_recovers_node(self):
        """Successful probe in half-open state fully recovers the node."""
        hm = redis.cluster.NodeHealthManager(failure_threshold=2, recovery_time=0.1)
        hm.record_failure("node1:6379")
        hm.record_failure("node1:6379")

        import time
        time.sleep(0.15)

        assert hm.is_available("node1:6379") is True  # probe allowed
        hm.record_success("node1:6379")
        # Node is now fully available
        assert hm.is_available("node1:6379") is True
        assert hm.is_available("node1:6379") is True

    def test_half_open_probe_failure_resets_timer(self):
        """Failed probe in half-open state resets recovery timer."""
        hm = redis.cluster.NodeHealthManager(failure_threshold=2, recovery_time=0.1)
        hm.record_failure("node1:6379")
        hm.record_failure("node1:6379")

        import time
        time.sleep(0.15)

        assert hm.is_available("node1:6379") is True  # probe allowed
        hm.record_failure("node1:6379")  # probe failed
        # Node is unavailable again, timer reset
        assert hm.is_available("node1:6379") is False
        # Must wait another recovery_time
        time.sleep(0.15)
        assert hm.is_available("node1:6379") is True
