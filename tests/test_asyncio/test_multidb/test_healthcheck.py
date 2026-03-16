import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from redis.asyncio.multidb.database import Database
from redis.asyncio.multidb.healthcheck import (
    PingHealthCheck,
    LagAwareHealthCheck,
    HealthCheck,
    HealthyAllPolicy,
    HealthyMajorityPolicy,
    HealthyAnyPolicy,
)
from redis.asyncio import Redis
from redis.http.http_client import HttpError
from redis.multidb.circuit import State as CBState
from redis.exceptions import ConnectionError
from redis.multidb.exception import UnhealthyDatabaseException


def _configure_mock_health_check(mock_hc, probes=3, delay=0.01, timeout=1.0):
    """Helper to configure mock health check with required properties."""
    mock_hc.health_check_probes = probes
    mock_hc.health_check_delay = delay
    mock_hc.health_check_timeout = timeout
    # check_health is async, use AsyncMock
    mock_hc.check_health = AsyncMock(return_value=True)
    return mock_hc


@pytest.mark.onlynoncluster
class TestHealthyAllPolicy:
    @pytest.mark.asyncio
    async def test_policy_returns_true_for_all_successful_probes(self):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc1.check_health.return_value = True
        mock_hc2.check_health.return_value = True
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()
        assert await policy.execute([mock_hc1, mock_hc2], mock_db)
        # Both health checks run in parallel, each with 3 probes
        assert mock_hc1.check_health.call_count == 3
        assert mock_hc2.check_health.call_count == 3

    @pytest.mark.asyncio
    async def test_policy_returns_false_on_failed_probe(self):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc1.check_health.side_effect = [True, True, False]
        mock_hc2.check_health.return_value = True
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()
        # Policy returns False because mock_hc1 fails on the third probe
        assert not await policy.execute([mock_hc1, mock_hc2], mock_db)
        # Both health checks run in parallel
        assert mock_hc1.check_health.call_count == 3
        assert mock_hc2.check_health.call_count == 3

    @pytest.mark.asyncio
    async def test_policy_raise_unhealthy_database_exception(self):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc1.check_health.side_effect = [True, True, ConnectionError]
        mock_hc2.check_health.return_value = True
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()
        with pytest.raises(UnhealthyDatabaseException, match="Unhealthy database"):
            await policy.execute([mock_hc1, mock_hc2], mock_db)

    @pytest.mark.asyncio
    async def test_policy_raises_exception_when_health_check_times_out(self):
        """
        Verify that health_check_timeout is respected and raises UnhealthyDatabaseException
        when a health check takes longer than the configured timeout.
        """
        import asyncio

        async def slow_health_check(database, connection=None):
            await asyncio.sleep(0.5)  # Sleep longer than the timeout
            return True

        # Configure with a very short timeout (0.1 seconds)
        mock_hc = _configure_mock_health_check(
            Mock(spec=HealthCheck), probes=1, delay=0.01, timeout=0.1
        )
        mock_hc.check_health.side_effect = slow_health_check
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()
        with pytest.raises(
            UnhealthyDatabaseException, match="Unhealthy database"
        ) as exc_info:
            await policy.execute([mock_hc], mock_db)

        # Verify the original exception is a TimeoutError
        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)

    @pytest.mark.asyncio
    async def test_policy_succeeds_when_health_check_completes_within_timeout(self):
        """
        Verify that health checks that complete within the timeout succeed normally.
        """
        import asyncio

        async def fast_health_check(database, connection=None):
            await asyncio.sleep(0.01)  # Sleep much less than the timeout
            return True

        # Configure with a generous timeout (1 second)
        mock_hc = _configure_mock_health_check(
            Mock(spec=HealthCheck), probes=1, delay=0.01, timeout=1.0
        )
        mock_hc.check_health.side_effect = fast_health_check
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()
        result = await policy.execute([mock_hc], mock_db)
        assert result is True


@pytest.mark.onlynoncluster
class TestHealthyMajorityPolicy:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "probes,hc1_side_effect,hc2_side_effect,expected_result",
        [
            (3, [True, False, False], [True, True, True], False),
            (3, [True, True, True], [True, False, False], False),
            (3, [True, False, True], [True, True, True], True),
            (3, [True, True, True], [True, False, True], True),
            (3, [True, True, False], [True, False, True], True),
            (4, [True, True, False, False], [True, True, True, True], False),
            (4, [True, True, True, True], [True, True, False, False], False),
            (4, [False, True, True, True], [True, True, True, True], True),
            (4, [True, True, True, True], [True, False, True, True], True),
            (4, [False, True, True, True], [True, True, False, True], True),
        ],
        ids=[
            "HC1 - no majority - odd",
            "HC2 - no majority - odd",
            "HC1 - majority- odd",
            "HC2 - majority - odd",
            "HC1 + HC2 - majority - odd",
            "HC1 - no majority - even",
            "HC2 - no majority - even",
            "HC1 - majority - even",
            "HC2 - majority - even",
            "HC1 + HC2 - majority - even",
        ],
    )
    async def test_policy_returns_true_for_majority_successful_probes(
        self,
        probes,
        hc1_side_effect,
        hc2_side_effect,
        expected_result,
    ):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=probes)
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=probes)
        mock_hc1.check_health.side_effect = hc1_side_effect
        mock_hc2.check_health.side_effect = hc2_side_effect
        mock_db = Mock(spec=Database)

        policy = HealthyMajorityPolicy()
        assert await policy.execute([mock_hc1, mock_hc2], mock_db) == expected_result
        # Both health checks run in parallel; call counts may vary due to early returns
        assert mock_hc1.check_health.call_count >= 1
        assert mock_hc2.check_health.call_count >= 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "probes,hc1_side_effect,hc2_side_effect",
        [
            (3, [True, ConnectionError, ConnectionError], [True, True, True]),
            (3, [True, True, True], [True, ConnectionError, ConnectionError]),
            (
                4,
                [True, ConnectionError, ConnectionError, True],
                [True, True, True, True],
            ),
            (
                4,
                [True, True, True, True],
                [True, ConnectionError, ConnectionError, False],
            ),
        ],
        ids=[
            "HC1 - majority- odd",
            "HC2 - majority - odd",
            "HC1 - majority - even",
            "HC2 - majority - even",
        ],
    )
    async def test_policy_raise_unhealthy_database_exception_on_majority_probes_exceptions(
        self, probes, hc1_side_effect, hc2_side_effect
    ):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=probes)
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=probes)
        mock_hc1.check_health.side_effect = hc1_side_effect
        mock_hc2.check_health.side_effect = hc2_side_effect
        mock_db = Mock(spec=Database)

        policy = HealthyMajorityPolicy()
        with pytest.raises(UnhealthyDatabaseException, match="Unhealthy database"):
            await policy.execute([mock_hc1, mock_hc2], mock_db)


@pytest.mark.onlynoncluster
class TestHealthyAnyPolicy:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "hc1_side_effect,hc2_side_effect,expected_result",
        [
            # HC1 fails all probes, HC2 succeeds - overall False (both HCs must pass)
            ([False, False, False], [True, True, True], False),
            # Both fail all probes - overall False
            ([False, False, False], [False, False, False], False),
            # Both succeed on at least one probe - overall True
            ([False, True, True], [False, False, True], True),
            # Both succeed on first probe - overall True
            ([True, True, True], [True, True, True], True),
        ],
        ids=[
            "HC1 fails all, HC2 succeeds",
            "Both fail all probes",
            "Both succeed on at least one",
            "Both succeed on first",
        ],
    )
    async def test_policy_returns_true_for_any_successful_probe(
        self,
        hc1_side_effect,
        hc2_side_effect,
        expected_result,
    ):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc1.check_health.side_effect = hc1_side_effect
        mock_hc2.check_health.side_effect = hc2_side_effect
        mock_db = Mock(spec=Database)

        policy = HealthyAnyPolicy()
        assert await policy.execute([mock_hc1, mock_hc2], mock_db) == expected_result
        # Both health checks run in parallel; call counts depend on when success is found
        assert mock_hc1.check_health.call_count >= 1
        assert mock_hc2.check_health.call_count >= 1

    @pytest.mark.asyncio
    async def test_policy_raise_unhealthy_database_exception_if_exception_occurs_on_failed_health_check(
        self,
    ):
        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck))
        mock_hc1.check_health.side_effect = [False, False, ConnectionError]
        mock_hc2.check_health.side_effect = [False, False, False]
        mock_db = Mock(spec=Database)

        policy = HealthyAnyPolicy()
        with pytest.raises(UnhealthyDatabaseException, match="Unhealthy database"):
            await policy.execute([mock_hc1, mock_hc2], mock_db)


@pytest.mark.onlynoncluster
class TestPingHealthCheck:
    @pytest.mark.asyncio
    async def test_database_is_healthy_on_echo_response(self, mock_client, mock_cb):
        """
        Mocking responses to mix error and actual responses to ensure that health check retry
        according to given configuration.
        """
        mock_conn = AsyncMock()
        mock_conn.send_command = AsyncMock()
        mock_conn.read_response = AsyncMock(return_value="PONG")
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert await hc.check_health(db, mock_conn)
        mock_conn.send_command.assert_called_once_with("PING")
        mock_conn.read_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_database_is_unhealthy_on_incorrect_echo_response(
        self, mock_client, mock_cb
    ):
        """
        Mocking responses to mix error and actual responses to ensure that health check retry
        according to given configuration.
        """
        mock_conn = AsyncMock()
        mock_conn.send_command = AsyncMock()
        mock_conn.read_response = AsyncMock(return_value="NOT_PONG")
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert not await hc.check_health(db, mock_conn)
        mock_conn.send_command.assert_called_once_with("PING")

    @pytest.mark.asyncio
    async def test_database_close_circuit_on_successful_healthcheck(
        self, mock_client, mock_cb
    ):
        mock_conn = AsyncMock()
        mock_conn.send_command = AsyncMock()
        mock_conn.read_response = AsyncMock(return_value="PONG")
        mock_cb.state = CBState.HALF_OPEN
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert await hc.check_health(db, mock_conn)
        mock_conn.send_command.assert_called_once_with("PING")


@pytest.mark.onlynoncluster
class TestLagAwareHealthCheck:
    @pytest.mark.asyncio
    async def test_database_is_healthy_when_bdb_matches_by_dns_name(
        self, mock_client, mock_cb
    ):
        """
        Ensures health check succeeds when /v1/bdbs contains an endpoint whose dns_name
        matches database host, and availability endpoint returns success.
        """
        host = "db1.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        # Mock HttpClient used inside LagAwareHealthCheck
        mock_http = AsyncMock()
        mock_http.get.side_effect = [
            # First call: list of bdbs
            [
                {
                    "uid": "bdb-1",
                    "endpoints": [
                        {"dns_name": host, "addr": ["10.0.0.1", "10.0.0.2"]},
                    ],
                }
            ],
            # Second call: availability check (no JSON expected)
            None,
        ]

        hc = LagAwareHealthCheck(rest_api_port=1234, lag_aware_tolerance=150)
        # Inject our mocked http client
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")
        mock_conn = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        assert await hc.check_health(db, mock_conn) is True
        # Base URL must be set correctly
        assert hc._http_client.client.base_url == "https://healthcheck.example.com:1234"
        # Calls: first to list bdbs, then to availability
        assert mock_http.get.call_count == 2
        first_call = mock_http.get.call_args_list[0]
        second_call = mock_http.get.call_args_list[1]
        assert first_call.args[0] == "/v1/bdbs"
        assert (
            second_call.args[0]
            == "/v1/bdbs/bdb-1/availability?extend_check=lag&availability_lag_tolerance_ms=150"
        )
        assert second_call.kwargs.get("expect_json") is False

    @pytest.mark.asyncio
    async def test_database_is_healthy_when_bdb_matches_by_addr(
        self, mock_client, mock_cb
    ):
        """
        Ensures health check succeeds when endpoint addr list contains the database host.
        """
        host_ip = "203.0.113.5"
        mock_client.get_connection_kwargs.return_value = {"host": host_ip}

        mock_http = AsyncMock()
        mock_http.get.side_effect = [
            [
                {
                    "uid": "bdb-42",
                    "endpoints": [
                        {"dns_name": "not-matching.example.com", "addr": [host_ip]},
                    ],
                }
            ],
            None,
        ]

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")
        mock_conn = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        assert await hc.check_health(db, mock_conn) is True
        assert mock_http.get.call_count == 2
        assert (
            mock_http.get.call_args_list[1].args[0]
            == "/v1/bdbs/bdb-42/availability?extend_check=lag&availability_lag_tolerance_ms=5000"
        )

    @pytest.mark.asyncio
    async def test_raises_value_error_when_no_matching_bdb(self, mock_client, mock_cb):
        """
        Ensures health check raises ValueError when there's no bdb matching the database host.
        """
        host = "db2.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        mock_http = AsyncMock()
        # Return bdbs that do not match host by dns_name nor addr
        mock_http.get.return_value = [
            {
                "uid": "a",
                "endpoints": [{"dns_name": "other.example.com", "addr": ["10.0.0.9"]}],
            },
            {
                "uid": "b",
                "endpoints": [
                    {"dns_name": "another.example.com", "addr": ["10.0.0.10"]}
                ],
            },
        ]

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")
        mock_conn = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        with pytest.raises(ValueError, match="Could not find a matching bdb"):
            await hc.check_health(db, mock_conn)

        # Only the listing call should have happened
        mock_http.get.assert_called_once_with("/v1/bdbs")

    @pytest.mark.asyncio
    async def test_propagates_http_error_from_availability(self, mock_client, mock_cb):
        """
        Ensures that any HTTP error raised by the availability endpoint is propagated.
        """
        host = "db3.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        mock_http = AsyncMock()
        # First: list bdbs -> match by dns_name
        mock_http.get.side_effect = [
            [{"uid": "bdb-err", "endpoints": [{"dns_name": host, "addr": []}]}],
            # Second: availability -> raise HttpError
            HttpError(
                url=f"https://{host}:9443/v1/bdbs/bdb-err/availability",
                status=503,
                message="busy",
            ),
        ]

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")
        mock_conn = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        with pytest.raises(HttpError, match="busy") as e:
            await hc.check_health(db, mock_conn)
            assert e.status == 503

        # Ensure both calls were attempted
        assert mock_http.get.call_count == 2


@pytest.mark.onlynoncluster
@pytest.mark.no_mock_connections
class TestAbstractHealthCheckPolicy:
    """
    Tests for AbstractHealthCheckPolicy public interface methods.
    These tests use explicit patching to test actual connection pool
    creation and management behavior.

    Uses @pytest.mark.no_mock_connections to disable the autouse fixture
    that mocks get_connections.
    """

    @pytest.mark.asyncio
    async def test_get_connections_creates_pool_for_standalone_redis(self):
        """
        Verify that get_connections creates a ConnectionPool for a standalone Redis client.
        """
        mock_client = Mock(spec=Redis)
        mock_client.get_connection_kwargs.return_value = {
            "host": "localhost",
            "port": 6379,
            "db": 0,
        }

        mock_db = Mock(spec=Database)
        mock_db.client = mock_client

        policy = HealthyAllPolicy()

        # Patch ConnectionPool to avoid real connection
        with patch(
            "redis.asyncio.multidb.healthcheck.ConnectionPool"
        ) as MockConnectionPool:
            mock_pool_instance = AsyncMock()
            MockConnectionPool.return_value = mock_pool_instance

            pools = await policy.get_connections(mock_db)

            # Should create exactly one pool for standalone Redis
            assert len(pools) == 1
            assert pools[0] == mock_pool_instance
            MockConnectionPool.assert_called_once_with(
                host="localhost", port=6379, db=0
            )

    @pytest.mark.asyncio
    async def test_get_connections_creates_pool_per_cluster_node(self):
        """
        Verify that get_connections creates a ConnectionPool for each cluster node.
        """
        # Create mock cluster nodes
        mock_node1 = Mock()
        mock_node1.redis_connection = Mock()
        mock_node1.redis_connection.get_connection_kwargs.return_value = {
            "host": "node1.cluster.local",
            "port": 6379,
        }

        mock_node2 = Mock()
        mock_node2.redis_connection = Mock()
        mock_node2.redis_connection.get_connection_kwargs.return_value = {
            "host": "node2.cluster.local",
            "port": 6379,
        }

        mock_node3 = Mock()
        mock_node3.redis_connection = Mock()
        mock_node3.redis_connection.get_connection_kwargs.return_value = {
            "host": "node3.cluster.local",
            "port": 6379,
        }

        # Create mock cluster client (not Redis instance)
        mock_cluster_client = Mock()
        mock_cluster_client.get_nodes.return_value = [
            mock_node1,
            mock_node2,
            mock_node3,
        ]

        mock_db = Mock(spec=Database)
        mock_db.client = mock_cluster_client

        policy = HealthyAllPolicy()

        with patch(
            "redis.asyncio.multidb.healthcheck.ConnectionPool"
        ) as MockConnectionPool:
            mock_pools = [AsyncMock(), AsyncMock(), AsyncMock()]
            MockConnectionPool.side_effect = mock_pools

            pools = await policy.get_connections(mock_db)

            # Should create one pool per cluster node
            assert len(pools) == 3
            assert MockConnectionPool.call_count == 3

    @pytest.mark.asyncio
    async def test_get_connections_caches_pools_by_database_id(self):
        """
        Verify that connection pools are cached and reused across multiple calls.
        """
        mock_client = Mock(spec=Redis)
        mock_client.get_connection_kwargs.return_value = {"host": "localhost"}

        mock_db = Mock(spec=Database)
        mock_db.client = mock_client

        policy = HealthyAllPolicy()

        with patch(
            "redis.asyncio.multidb.healthcheck.ConnectionPool"
        ) as MockConnectionPool:
            mock_pool = AsyncMock()
            MockConnectionPool.return_value = mock_pool

            # First call creates the pool
            pools1 = await policy.get_connections(mock_db)
            # Second call should return cached pool
            pools2 = await policy.get_connections(mock_db)

            # Pool should only be created once
            assert MockConnectionPool.call_count == 1
            assert pools1 is pools2

    @pytest.mark.asyncio
    async def test_get_connections_creates_separate_pools_for_different_databases(self):
        """
        Verify that different databases get separate connection pools.
        """
        mock_client1 = Mock(spec=Redis)
        mock_client1.get_connection_kwargs.return_value = {"host": "db1.local"}

        mock_client2 = Mock(spec=Redis)
        mock_client2.get_connection_kwargs.return_value = {"host": "db2.local"}

        mock_db1 = Mock(spec=Database)
        mock_db1.client = mock_client1

        mock_db2 = Mock(spec=Database)
        mock_db2.client = mock_client2

        policy = HealthyAllPolicy()

        with patch(
            "redis.asyncio.multidb.healthcheck.ConnectionPool"
        ) as MockConnectionPool:
            mock_pool1 = AsyncMock()
            mock_pool2 = AsyncMock()
            MockConnectionPool.side_effect = [mock_pool1, mock_pool2]

            pools1 = await policy.get_connections(mock_db1)
            pools2 = await policy.get_connections(mock_db2)

            # Two different pools should be created
            assert MockConnectionPool.call_count == 2
            assert pools1[0] == mock_pool1
            assert pools2[0] == mock_pool2

    @pytest.mark.asyncio
    async def test_close_disconnects_all_pools(self):
        """
        Verify that close() disconnects all cached connection pools.
        """
        mock_client = Mock(spec=Redis)
        mock_client.get_connection_kwargs.return_value = {"host": "localhost"}

        mock_db = Mock(spec=Database)
        mock_db.client = mock_client

        policy = HealthyAllPolicy()

        with patch(
            "redis.asyncio.multidb.healthcheck.ConnectionPool"
        ) as MockConnectionPool:
            mock_pool = AsyncMock()
            mock_pool.disconnect = AsyncMock()
            MockConnectionPool.return_value = mock_pool

            # Create connections
            await policy.get_connections(mock_db)

            # Close the policy
            await policy.close()

            # Pool should be disconnected
            mock_pool.disconnect.assert_called_once()

            # Internal connections dict should be cleared
            assert len(policy._connections) == 0

    @pytest.mark.asyncio
    async def test_close_disconnects_multiple_database_pools(self):
        """
        Verify that close() disconnects pools for all databases.
        """
        mock_client1 = Mock(spec=Redis)
        mock_client1.get_connection_kwargs.return_value = {"host": "db1.local"}

        mock_client2 = Mock(spec=Redis)
        mock_client2.get_connection_kwargs.return_value = {"host": "db2.local"}

        mock_db1 = Mock(spec=Database)
        mock_db1.client = mock_client1

        mock_db2 = Mock(spec=Database)
        mock_db2.client = mock_client2

        policy = HealthyAllPolicy()

        with patch(
            "redis.asyncio.multidb.healthcheck.ConnectionPool"
        ) as MockConnectionPool:
            mock_pool1 = AsyncMock()
            mock_pool1.disconnect = AsyncMock()
            mock_pool2 = AsyncMock()
            mock_pool2.disconnect = AsyncMock()
            MockConnectionPool.side_effect = [mock_pool1, mock_pool2]

            # Create connections for both databases
            await policy.get_connections(mock_db1)
            await policy.get_connections(mock_db2)

            # Close the policy
            await policy.close()

            # Both pools should be disconnected
            mock_pool1.disconnect.assert_called_once()
            mock_pool2.disconnect.assert_called_once()
            assert len(policy._connections) == 0

    @pytest.mark.asyncio
    async def test_close_handles_disconnect_exceptions_gracefully(self):
        """
        Verify that close() completes even if some pools fail to disconnect.
        """
        mock_client1 = Mock(spec=Redis)
        mock_client1.get_connection_kwargs.return_value = {"host": "db1.local"}

        mock_client2 = Mock(spec=Redis)
        mock_client2.get_connection_kwargs.return_value = {"host": "db2.local"}

        mock_db1 = Mock(spec=Database)
        mock_db1.client = mock_client1

        mock_db2 = Mock(spec=Database)
        mock_db2.client = mock_client2

        policy = HealthyAllPolicy()

        with patch(
            "redis.asyncio.multidb.healthcheck.ConnectionPool"
        ) as MockConnectionPool:
            mock_pool1 = AsyncMock()
            mock_pool1.disconnect = AsyncMock(
                side_effect=ConnectionError("Disconnect failed")
            )
            mock_pool2 = AsyncMock()
            mock_pool2.disconnect = AsyncMock()
            MockConnectionPool.side_effect = [mock_pool1, mock_pool2]

            await policy.get_connections(mock_db1)
            await policy.get_connections(mock_db2)

            # Should not raise even if one pool fails to disconnect
            await policy.close()

            # Both disconnect attempts should have been made
            mock_pool1.disconnect.assert_called_once()
            mock_pool2.disconnect.assert_called_once()
            assert len(policy._connections) == 0

    @pytest.mark.asyncio
    async def test_execute_runs_health_checks_concurrently(self):
        """
        Verify that execute() runs multiple health checks concurrently.
        """

        # Track concurrent execution
        concurrent_count = 0
        max_concurrent = 0

        async def tracking_health_check(database, connection=None):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.01)  # Small delay to allow overlap detection
            concurrent_count -= 1
            return True

        mock_hc1 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=1)
        mock_hc1.check_health.side_effect = tracking_health_check

        mock_hc2 = _configure_mock_health_check(Mock(spec=HealthCheck), probes=1)
        mock_hc2.check_health.side_effect = tracking_health_check

        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()

        # Mock get_connections to avoid real connections
        async def mock_get_connections(database):
            mock_pool = AsyncMock()
            mock_pool.get_connection = AsyncMock(return_value=AsyncMock())
            mock_pool.release = AsyncMock()
            return [mock_pool]

        policy.get_connections = mock_get_connections

        result = await policy.execute([mock_hc1, mock_hc2], mock_db)

        assert result is True
        # Both health checks should have been called
        assert mock_hc1.check_health.call_count == 1
        assert mock_hc2.check_health.call_count == 1
        # If running concurrently, max_concurrent should be 2
        assert max_concurrent == 2, (
            f"Expected 2 concurrent executions, got max {max_concurrent}"
        )

    @pytest.mark.asyncio
    async def test_execute_applies_timeout_per_health_check(self):
        """
        Verify that execute() applies individual timeouts per health check.
        """
        import asyncio

        async def slow_health_check(database, connection=None):
            await asyncio.sleep(1.0)  # Takes 1 second
            return True

        async def fast_health_check(database, connection=None):
            await asyncio.sleep(0.01)
            return True

        # First health check times out
        mock_hc1 = _configure_mock_health_check(
            Mock(spec=HealthCheck), probes=1, timeout=0.05
        )
        mock_hc1.check_health.side_effect = slow_health_check

        # Second health check completes within timeout
        mock_hc2 = _configure_mock_health_check(
            Mock(spec=HealthCheck), probes=1, timeout=1.0
        )
        mock_hc2.check_health.side_effect = fast_health_check

        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()

        async def mock_get_connections(database):
            mock_pool = AsyncMock()
            mock_pool.get_connection = AsyncMock(return_value=AsyncMock())
            mock_pool.release = AsyncMock()
            return [mock_pool]

        policy.get_connections = mock_get_connections

        # Should raise because first health check times out
        with pytest.raises(UnhealthyDatabaseException) as exc_info:
            await policy.execute([mock_hc1, mock_hc2], mock_db)

        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)

    @pytest.mark.asyncio
    async def test_execute_releases_connections_on_success(self):
        """
        Verify that connections are released back to the pool after successful health check.
        """
        mock_hc = _configure_mock_health_check(Mock(spec=HealthCheck), probes=1)
        mock_hc.check_health.return_value = True
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()

        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.get_connection = AsyncMock(return_value=mock_conn)
        mock_pool.release = AsyncMock()

        async def mock_get_connections(database):
            return [mock_pool]

        policy.get_connections = mock_get_connections

        await policy.execute([mock_hc], mock_db)

        # Connection should be released
        mock_pool.release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_execute_disconnects_connection_on_error(self):
        """
        Verify that connections are disconnected (not just released) on error.
        """
        mock_hc = _configure_mock_health_check(Mock(spec=HealthCheck), probes=1)
        mock_hc.check_health.side_effect = ConnectionError("Connection lost")
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy()

        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.disconnect = AsyncMock()
        mock_pool.get_connection = AsyncMock(return_value=mock_conn)
        mock_pool.release = AsyncMock()

        async def mock_get_connections(database):
            return [mock_pool]

        policy.get_connections = mock_get_connections

        with pytest.raises(UnhealthyDatabaseException):
            await policy.execute([mock_hc], mock_db)

        # Connection should be disconnected on error
        mock_conn.disconnect.assert_called_once()
        # Connection should still be released to pool
        mock_pool.release.assert_called_once_with(mock_conn)
