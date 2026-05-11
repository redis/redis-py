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
    TimeoutError as RedisTimeoutError,
    ClusterDownError,
    SlotNotCoveredError,
)


@pytest.mark.fixed_client
class TestClusterMetricsRecordingDuringErrorHandling:
    """
    Tests for cluster metrics recording during error handling.

    These tests verify that when exceptions occur during command execution,
    metrics are recorded correctly using either the Connection object (when
    available) or the ClusterNode (as fallback when connection is None).
    """

    def test_authentication_error_uses_target_node_for_metrics(self):
        """
        Test that AuthenticationError uses target_node for metrics when connection is None.

        AuthenticationError typically occurs during get_connection() when the connection
        is being established and authenticated. Since the error is raised before
        get_connection() returns, the connection variable is still None, so we
        fall back to target_node for metrics.
        """
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                mock_redis_conn = MagicMock()

                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection",
                        side_effect=AuthenticationError("Auth failed"),
                    ):
                        with patch(
                            "redis.cluster.record_operation_duration"
                        ) as mock_record:
                            with pytest.raises(AuthenticationError) as exc_info:
                                cluster.execute_command(
                                    "GET", "key", target_nodes=target_node
                                )

                            assert hasattr(exc_info.value, "connection")
                            assert exc_info.value.connection == target_node

                            mock_record.assert_called_once()
                            call_kwargs = mock_record.call_args.kwargs
                            assert call_kwargs["command_name"] == "GET"
                            assert call_kwargs["server_address"] == "127.0.0.1"
                            assert call_kwargs["server_port"] == 7000
                            assert call_kwargs["db_namespace"] == "0"
                            assert isinstance(call_kwargs["error"], AuthenticationError)

    def test_connection_error_uses_connection_when_available(self):
        """
        Test that ConnectionError uses actual connection if available.

        When the error occurs AFTER get_connection() returns (e.g., during
        parse_response), the connection object is available for metrics.
        """
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            mock_nodes_manager.move_node_to_end_of_cached_nodes = MagicMock()
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                mock_redis_conn = MagicMock()
                mock_redis_conn.parse_response.side_effect = RedisConnectionError(
                    "Connection lost"
                )

                mock_connection = MagicMock()
                mock_connection.host = "192.168.1.100"
                mock_connection.port = 6379
                mock_connection.db = 3

                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection", return_value=mock_connection
                    ):
                        with patch(
                            "redis.cluster.record_operation_duration"
                        ) as mock_record:
                            with pytest.raises(RedisConnectionError) as exc_info:
                                cluster.execute_command(
                                    "GET", "key", target_nodes=target_node
                                )

                            assert hasattr(exc_info.value, "connection")
                            assert exc_info.value.connection == mock_connection

                            mock_record.assert_called_once()
                            call_kwargs = mock_record.call_args.kwargs
                            assert call_kwargs["command_name"] == "GET"
                            assert call_kwargs["server_address"] == "192.168.1.100"
                            assert call_kwargs["server_port"] == 6379
                            assert call_kwargs["db_namespace"] == "3"
                            assert isinstance(
                                call_kwargs["error"], RedisConnectionError
                            )

    def test_connection_error_uses_target_node_when_no_connection(self):
        """
        Test that ConnectionError uses target_node when connection is not available.

        When ConnectionError occurs DURING get_connection() (before it returns),
        the connection variable is None. The code should fall back to using
        target_node for metrics to provide valid host/port information.
        """
        target_node = ClusterNode(host="10.0.0.50", port=7001, server_type="primary")

        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            mock_nodes_manager.move_node_to_end_of_cached_nodes = MagicMock()
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                mock_redis_conn = MagicMock()

                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection",
                        side_effect=RedisConnectionError("Cannot connect"),
                    ):
                        with patch(
                            "redis.cluster.record_operation_duration"
                        ) as mock_record:
                            with pytest.raises(RedisConnectionError) as exc_info:
                                cluster.execute_command(
                                    "GET", "key", target_nodes=target_node
                                )

                            assert hasattr(exc_info.value, "connection")
                            assert exc_info.value.connection == target_node

                            mock_record.assert_called_once()
                            call_kwargs = mock_record.call_args.kwargs
                            assert call_kwargs["command_name"] == "GET"
                            assert call_kwargs["server_address"] == "10.0.0.50"
                            assert call_kwargs["server_port"] == 7001
                            assert call_kwargs["db_namespace"] == "0"
                            assert isinstance(
                                call_kwargs["error"], RedisConnectionError
                            )

    def test_response_error_uses_connection(self):
        """
        Test that ResponseError uses the actual connection for metrics.

        ResponseError typically occurs after get_connection() succeeds (during
        parse_response), so we should have a valid connection for metrics.
        """
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                mock_redis_conn = MagicMock()
                mock_redis_conn.parse_response.side_effect = ResponseError("WRONGTYPE")

                mock_connection = MagicMock()
                mock_connection.host = "172.16.0.10"
                mock_connection.port = 6380
                mock_connection.db = 2

                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection", return_value=mock_connection
                    ):
                        with patch(
                            "redis.cluster.record_operation_duration"
                        ) as mock_record:
                            with pytest.raises(ResponseError) as exc_info:
                                cluster.execute_command(
                                    "GET", "key", target_nodes=target_node
                                )

                            assert hasattr(exc_info.value, "connection")
                            assert exc_info.value.connection == mock_connection

                            mock_record.assert_called_once()
                            call_kwargs = mock_record.call_args.kwargs
                            assert call_kwargs["command_name"] == "GET"
                            assert call_kwargs["server_address"] == "172.16.0.10"
                            assert call_kwargs["server_port"] == 6380
                            assert call_kwargs["db_namespace"] == "2"
                            assert isinstance(call_kwargs["error"], ResponseError)

    def test_max_connections_error_records_metrics_with_cluster_node(self):
        """
        Test that MaxConnectionsError records metrics using ClusterNode info.

        When MaxConnectionsError occurs, connection is None because we couldn't
        get a connection from the pool. The code sets e.connection = target_node
        and metrics should be recorded using the ClusterNode's host/port.
        """
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        assert not hasattr(target_node, "db")
        assert hasattr(target_node, "host")
        assert hasattr(target_node, "port")

        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                mock_redis_conn = MagicMock()
                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection",
                        side_effect=MaxConnectionsError("Pool exhausted"),
                    ):
                        with patch(
                            "redis.cluster.record_operation_duration"
                        ) as mock_record_duration:
                            with pytest.raises(MaxConnectionsError):
                                cluster.execute_command(
                                    "GET", "key", target_nodes=target_node
                                )

                            mock_record_duration.assert_called()
                            call_kwargs = mock_record_duration.call_args[1]
                            assert call_kwargs["server_address"] == "127.0.0.1"
                            assert call_kwargs["server_port"] == 7000
                            assert call_kwargs["db_namespace"] == "0"

    def test_successful_command_records_metrics_with_connection_db(self):
        """
        Test that successful command execution records metrics with Connection's db.

        When a command succeeds, we have an actual Connection object which has
        a db attribute. Verify the metrics use the actual db value.
        """
        target_node = ClusterNode(host="127.0.0.1", port=7000, server_type="primary")

        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                mock_redis_conn = MagicMock()
                mock_redis_conn.parse_response.return_value = b"value"

                mock_connection = MagicMock()
                mock_connection.host = "127.0.0.1"
                mock_connection.port = 7000
                mock_connection.db = 5

                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection", return_value=mock_connection
                    ):
                        with patch(
                            "redis.cluster.record_operation_duration"
                        ) as mock_record_duration:
                            cluster.execute_command(
                                "GET", "key", target_nodes=target_node
                            )

                            call_kwargs = mock_record_duration.call_args[1]
                            assert call_kwargs["db_namespace"] == "5"

    def test_timeout_error_uses_target_node_for_metrics(self):
        """
        Test that TimeoutError uses target_node for metrics when connection is None.

        When TimeoutError occurs during get_connection(), connection is None.
        The code uses target_node for metrics to provide valid host/port info.
        """
        target_node = ClusterNode(host="10.0.0.100", port=7003, server_type="primary")

        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            mock_nodes_manager.move_node_to_end_of_cached_nodes = MagicMock()
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                mock_redis_conn = MagicMock()
                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection",
                        side_effect=RedisTimeoutError("Timeout connecting to server"),
                    ):
                        with patch(
                            "redis.cluster.record_operation_duration"
                        ) as mock_record:
                            with pytest.raises(RedisTimeoutError) as exc_info:
                                cluster.execute_command(
                                    "GET", "key", target_nodes=target_node
                                )

                            assert "Timeout" in str(exc_info.value)

                            mock_record.assert_called_once()
                            call_kwargs = mock_record.call_args[1]
                            assert call_kwargs["server_address"] == "10.0.0.100"
                            assert call_kwargs["server_port"] == 7003
                            assert call_kwargs["db_namespace"] == "0"

    def test_cluster_down_error_with_cluster_node_metrics(self):
        """
        Test that ClusterDownError records metrics correctly when connection is None.

        When ClusterDownError occurs before connection is established,
        e.connection is set to target_node (ClusterNode), and metrics should
        be recorded with valid host/port from target_node.
        """
        target_node = ClusterNode(host="172.20.0.10", port=7004, server_type="primary")

        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                mock_redis_conn = MagicMock()
                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection",
                        side_effect=ClusterDownError("CLUSTERDOWN"),
                    ):
                        with patch(
                            "redis.cluster.record_operation_duration"
                        ) as mock_record:
                            with pytest.raises(ClusterDownError):
                                cluster.execute_command(
                                    "GET", "key", target_nodes=target_node
                                )

                            mock_record.assert_called_once()
                            call_kwargs = mock_record.call_args[1]
                            assert call_kwargs["server_address"] == "172.20.0.10"
                            assert call_kwargs["server_port"] == 7004
                            assert call_kwargs["db_namespace"] == "0"
                            assert isinstance(call_kwargs["error"], ClusterDownError)

    def test_slot_not_covered_error_with_cluster_node_metrics(self):
        """
        Test that SlotNotCoveredError records metrics correctly when connection is None.

        When SlotNotCoveredError occurs before connection is established,
        e.connection is set to target_node, and metrics should be recorded
        with valid host/port from target_node.
        """
        target_node = ClusterNode(host="172.20.0.20", port=7005, server_type="primary")

        with patch("redis.cluster.NodesManager") as MockNodesManager:
            mock_nodes_manager = MagicMock()
            mock_nodes_manager.initialize.return_value = None
            mock_nodes_manager.default_node = target_node
            MockNodesManager.return_value = mock_nodes_manager

            with patch("redis.cluster.CommandsParser"):
                cluster = RedisCluster(host="127.0.0.1", port=7000)

                mock_redis_conn = MagicMock()
                with patch.object(
                    cluster, "get_redis_connection", return_value=mock_redis_conn
                ):
                    with patch(
                        "redis.cluster.get_connection",
                        side_effect=SlotNotCoveredError("Slot 1234 not covered"),
                    ):
                        with patch(
                            "redis.cluster.record_operation_duration"
                        ) as mock_record:
                            with pytest.raises(SlotNotCoveredError):
                                cluster.execute_command(
                                    "GET", "key", target_nodes=target_node
                                )

                            mock_record.assert_called_once()
                            call_kwargs = mock_record.call_args[1]
                            assert call_kwargs["server_address"] == "172.20.0.20"
                            assert call_kwargs["server_port"] == 7005
                            assert call_kwargs["db_namespace"] == "0"
                            assert isinstance(call_kwargs["error"], SlotNotCoveredError)
