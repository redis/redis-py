import pytest
import redis
from unittest import mock
from redis.connection import ConnectionInterface


class DummyConnection(ConnectionInterface):
    """A dummy connection class for testing that doesn't actually connect to Redis"""

    def __init__(self, *args, **kwargs):
        self.connected = False

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

    def can_read(self, timeout=0):
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
