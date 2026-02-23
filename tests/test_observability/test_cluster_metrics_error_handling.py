"""
Unit tests for cluster metrics recording during error handling.

These tests verify that the cluster error handling correctly sets the connection
attribute on exceptions for metrics reporting, even when the connection is not
yet established or when using ClusterNode objects.
"""

import pytest
from unittest.mock import MagicMock, patch
from redis.cluster import RedisCluster, ClusterNode
from redis.exceptions import (
    AuthenticationError,
    MaxConnectionsError,
    ConnectionError as RedisConnectionError,
    ResponseError,
)


@pytest.mark.onlycluster
class TestClusterErrorHandlingMetrics:
    """Tests for cluster error handling with metrics."""

    def test_authentication_error_uses_connection_when_available(self):
        """
        Test that AuthenticationError uses connection when available, otherwise target_node.

        This validates the error handling in cluster.py lines 1558-1564.
        The code prefers the actual connection object when available.
        """
        # Create a real ClusterNode
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        # Create cluster with mocked NodesManager
        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                # Mock get_redis_connection to return a redis connection
                mock_redis_conn = MagicMock()
                # Make parse_response raise AuthenticationError (simulates auth failure)
                mock_redis_conn.parse_response.side_effect = AuthenticationError(
                    "Auth failed"
                )

                mock_connection = MagicMock()

                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection", return_value=mock_connection
                    ):
                        # Execute command and expect AuthenticationError
                        with pytest.raises(AuthenticationError) as exc_info:
                            cluster._execute_command(target_node, "GET", "key")

                        # Verify the library code set connection attribute to the connection
                        # (prefers connection over target_node when connection is available)
                        assert hasattr(exc_info.value, "connection")
                        assert exc_info.value.connection == mock_connection

    def test_max_connections_error_uses_target_node_for_metrics(self):
        """
        Test that MaxConnectionsError uses target_node for metrics when connection
        pool is exhausted.

        This validates the error handling in cluster.py lines 1565-1574.
        """
        # Create a real ClusterNode
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        # Create cluster with mocked NodesManager
        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                # Mock get_redis_connection to return a redis connection
                mock_redis_conn = MagicMock()
                # Make get_connection raise MaxConnectionsError (simulates pool exhaustion)
                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection",
                        side_effect=MaxConnectionsError("Pool exhausted"),
                    ):
                        # Execute command and expect MaxConnectionsError
                        with pytest.raises(MaxConnectionsError) as exc_info:
                            cluster._execute_command(target_node, "GET", "key")

                        # Verify the library code set connection attribute to target_node
                        assert hasattr(exc_info.value, "connection")
                        assert exc_info.value.connection == target_node

    def test_connection_error_uses_connection_if_available(self):
        """
        Test that ConnectionError uses actual connection if available.

        This validates the error handling in cluster.py lines 1575-1605 where
        ConnectionError is caught and e.connection is set to connection if available.
        """
        # Create a real ClusterNode
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        # Create cluster with mocked NodesManager
        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                # Mock get_redis_connection to return a redis connection
                mock_redis_conn = MagicMock()
                # Make parse_response raise ConnectionError after connection is obtained
                mock_redis_conn.parse_response.side_effect = RedisConnectionError(
                    "Connection lost"
                )

                mock_connection = MagicMock()
                mock_connection.host = "127.0.0.1"
                mock_connection.port = 7000

                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection", return_value=mock_connection
                    ):
                        # Execute command and expect ConnectionError
                        with pytest.raises(RedisConnectionError) as exc_info:
                            cluster._execute_command(target_node, "GET", "key")

                        # Verify the library code set connection attribute to the actual connection
                        assert hasattr(exc_info.value, "connection")
                        assert exc_info.value.connection == mock_connection

    def test_connection_error_uses_target_node_when_no_connection(self):
        """
        Test that ConnectionError uses target_node when connection is not available.

        This validates the error handling in cluster.py lines 1575-1605 where
        ConnectionError is caught and e.connection is set to target_node when connection is None.
        """
        # Create a real ClusterNode
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        # Create cluster with mocked NodesManager
        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                # Mock get_redis_connection to return a redis connection
                mock_redis_conn = MagicMock()

                # Make get_connection raise ConnectionError before connection is obtained
                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection",
                        side_effect=RedisConnectionError("Cannot connect"),
                    ):
                        # Execute command and expect ConnectionError
                        with pytest.raises(RedisConnectionError) as exc_info:
                            cluster._execute_command(target_node, "GET", "key")

                        # Verify the library code set connection attribute to target_node
                        assert hasattr(exc_info.value, "connection")
                        assert exc_info.value.connection == target_node

    def test_response_error_uses_connection(self):
        """
        Test that ResponseError uses the actual connection for metrics.

        This validates the error handling in cluster.py lines 1704-1713.
        """
        # Create a real ClusterNode
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        # Create cluster with mocked NodesManager
        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                # Mock get_redis_connection to return a redis connection
                mock_redis_conn = MagicMock()
                # Make parse_response raise ResponseError
                mock_redis_conn.parse_response.side_effect = ResponseError("WRONGTYPE")

                mock_connection = MagicMock()
                mock_connection.host = "127.0.0.1"
                mock_connection.port = 7000

                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection", return_value=mock_connection
                    ):
                        # Execute command and expect ResponseError
                        with pytest.raises(ResponseError) as exc_info:
                            cluster._execute_command(target_node, "GET", "key")

                        # Verify the library code set connection attribute to the actual connection
                        assert hasattr(exc_info.value, "connection")
                        assert exc_info.value.connection == mock_connection
