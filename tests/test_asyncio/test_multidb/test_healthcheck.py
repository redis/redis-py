import asyncio
from unittest.mock import AsyncMock, Mock

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
    """Tests for PingHealthCheck with standalone and cluster clients."""

    @pytest.mark.asyncio
    async def test_standalone_database_is_healthy_on_ping_true(
        self, mock_client, mock_cb
    ):
        """
        Verify that PingHealthCheck returns True for standalone client when PING succeeds.
        """
        from redis.asyncio import Redis as AsyncRedis

        mock_hc_client = AsyncMock(spec=AsyncRedis)
        mock_hc_client.execute_command = AsyncMock(return_value=True)
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert await hc.check_health(db, mock_hc_client)
        mock_hc_client.execute_command.assert_called_once_with("PING")

    @pytest.mark.asyncio
    async def test_standalone_database_is_unhealthy_on_ping_false(
        self, mock_client, mock_cb
    ):
        """
        Verify that PingHealthCheck returns False for standalone client when PING fails.
        """
        from redis.asyncio import Redis as AsyncRedis

        mock_hc_client = AsyncMock(spec=AsyncRedis)
        mock_hc_client.execute_command = AsyncMock(return_value=False)
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert not await hc.check_health(db, mock_hc_client)
        mock_hc_client.execute_command.assert_called_once_with("PING")

    @pytest.mark.asyncio
    async def test_standalone_database_close_circuit_on_successful_healthcheck(
        self, mock_client, mock_cb
    ):
        """
        Verify health check succeeds for standalone client with HALF_OPEN circuit.
        """
        from redis.asyncio import Redis as AsyncRedis

        mock_hc_client = AsyncMock(spec=AsyncRedis)
        mock_hc_client.execute_command = AsyncMock(return_value=True)
        mock_cb.state = CBState.HALF_OPEN
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert await hc.check_health(db, mock_hc_client)
        mock_hc_client.execute_command.assert_called_once_with("PING")

    @pytest.mark.asyncio
    async def test_cluster_database_is_healthy_when_all_nodes_respond(
        self, mock_client, mock_cb
    ):
        """
        Verify that PingHealthCheck returns True for cluster when all nodes respond.
        """
        from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

        # Create mock nodes
        mock_node1 = Mock()
        mock_node1.redis_connection = AsyncMock()
        mock_node1.redis_connection.execute_command = AsyncMock(return_value=True)

        mock_node2 = Mock()
        mock_node2.redis_connection = AsyncMock()
        mock_node2.redis_connection.execute_command = AsyncMock(return_value=True)

        mock_node3 = Mock()
        mock_node3.redis_connection = AsyncMock()
        mock_node3.redis_connection.execute_command = AsyncMock(return_value=True)

        # Create mock cluster client (not AsyncRedis, so isinstance check fails)
        mock_hc_client = Mock(spec=AsyncRedisCluster)
        mock_hc_client.get_nodes = Mock(
            return_value=[mock_node1, mock_node2, mock_node3]
        )

        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert await hc.check_health(db, mock_hc_client)

        # All nodes should have been pinged
        mock_node1.redis_connection.execute_command.assert_called_once_with("PING")
        mock_node2.redis_connection.execute_command.assert_called_once_with("PING")
        mock_node3.redis_connection.execute_command.assert_called_once_with("PING")

    @pytest.mark.asyncio
    async def test_cluster_database_is_unhealthy_when_one_node_fails(
        self, mock_client, mock_cb
    ):
        """
        Verify that PingHealthCheck returns False for cluster when any node fails.
        """
        from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

        # Create mock nodes - second node fails
        mock_node1 = Mock()
        mock_node1.redis_connection = AsyncMock()
        mock_node1.redis_connection.execute_command = AsyncMock(return_value=True)

        mock_node2 = Mock()
        mock_node2.redis_connection = AsyncMock()
        mock_node2.redis_connection.execute_command = AsyncMock(return_value=False)

        mock_node3 = Mock()
        mock_node3.redis_connection = AsyncMock()
        mock_node3.redis_connection.execute_command = AsyncMock(return_value=True)

        # Create mock cluster client
        mock_hc_client = Mock(spec=AsyncRedisCluster)
        mock_hc_client.get_nodes = Mock(
            return_value=[mock_node1, mock_node2, mock_node3]
        )

        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert not await hc.check_health(db, mock_hc_client)

        # Should stop after the failing node
        mock_node1.redis_connection.execute_command.assert_called_once_with("PING")
        mock_node2.redis_connection.execute_command.assert_called_once_with("PING")
        # Node 3 should not be checked (early exit on failure)
        mock_node3.redis_connection.execute_command.assert_not_called()


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
        mock_hc_client = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        assert await hc.check_health(db, mock_hc_client) is True
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
        mock_hc_client = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        assert await hc.check_health(db, mock_hc_client) is True
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
        mock_hc_client = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        with pytest.raises(ValueError, match="Could not find a matching bdb"):
            await hc.check_health(db, mock_hc_client)

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
        mock_hc_client = (
            AsyncMock()
        )  # Not used by LagAwareHealthCheck but required by signature

        with pytest.raises(HttpError, match="busy") as e:
            await hc.check_health(db, mock_hc_client)
            assert e.status == 503

        # Ensure both calls were attempted
        assert mock_http.get.call_count == 2


@pytest.mark.onlynoncluster
@pytest.mark.no_mock_connections
class TestAbstractHealthCheckPolicy:
    """
    Tests for AbstractHealthCheckPolicy public interface methods.
    These tests verify client lifecycle and caching behavior by
    directly manipulating the internal _clients dictionary.

    Uses @pytest.mark.no_mock_connections to disable the autouse fixture
    that mocks get_client.
    """

    @pytest.mark.asyncio
    async def test_get_client_caches_clients_by_database_id(self):
        """
        Verify that clients are cached and reused across multiple calls.
        Uses a real sync Redis client to trigger the standalone path.
        """
        # Create a real sync Redis client (without connecting)
        from redis import Redis as SyncRedis

        sync_client = SyncRedis(host="localhost", port=6379)

        mock_db = Mock(spec=Database)
        mock_db.client = sync_client

        policy = HealthyAllPolicy()

        # Manually inject a mock client to test caching behavior
        mock_redis = AsyncMock()
        db_id = id(mock_db)
        policy._clients[db_id] = mock_redis

        # First call should return cached client
        client1 = await policy.get_client(mock_db)
        # Second call should return same client
        client2 = await policy.get_client(mock_db)

        # Should be the same instance
        assert client1 is client2
        assert client1 is mock_redis

    @pytest.mark.asyncio
    async def test_get_client_creates_separate_clients_for_different_databases(self):
        """
        Verify that different databases get separate clients.
        """
        from redis import Redis as SyncRedis

        sync_client1 = SyncRedis(host="db1.local", port=6379)
        sync_client2 = SyncRedis(host="db2.local", port=6379)

        mock_db1 = Mock(spec=Database)
        mock_db1.client = sync_client1

        mock_db2 = Mock(spec=Database)
        mock_db2.client = sync_client2

        policy = HealthyAllPolicy()

        # Manually inject mock clients
        mock_redis1 = AsyncMock()
        mock_redis2 = AsyncMock()
        policy._clients[id(mock_db1)] = mock_redis1
        policy._clients[id(mock_db2)] = mock_redis2

        client1 = await policy.get_client(mock_db1)
        client2 = await policy.get_client(mock_db2)

        # Should get different clients
        assert client1 is mock_redis1
        assert client2 is mock_redis2
        assert client1 is not client2

    @pytest.mark.asyncio
    async def test_close_closes_all_clients(self):
        """
        Verify that close() closes all cached clients.
        """
        policy = HealthyAllPolicy()

        # Manually inject a mock client
        mock_redis = AsyncMock()
        mock_redis.aclose = AsyncMock()
        policy._clients[123] = mock_redis

        # Close the policy
        await policy.close()

        # Client should be closed
        mock_redis.aclose.assert_called_once()

        # Internal clients dict should be cleared
        assert len(policy._clients) == 0

    @pytest.mark.asyncio
    async def test_close_closes_multiple_database_clients(self):
        """
        Verify that close() closes clients for all databases.
        """
        policy = HealthyAllPolicy()

        # Manually inject mock clients
        mock_redis1 = AsyncMock()
        mock_redis1.aclose = AsyncMock()
        mock_redis2 = AsyncMock()
        mock_redis2.aclose = AsyncMock()
        policy._clients[123] = mock_redis1
        policy._clients[456] = mock_redis2

        # Close the policy
        await policy.close()

        # Both clients should be closed
        mock_redis1.aclose.assert_called_once()
        mock_redis2.aclose.assert_called_once()
        assert len(policy._clients) == 0

    @pytest.mark.asyncio
    async def test_close_handles_close_exceptions_gracefully(self):
        """
        Verify that close() completes even if some clients fail to close.
        """
        policy = HealthyAllPolicy()

        # Manually inject mock clients
        mock_redis1 = AsyncMock()
        mock_redis1.aclose = AsyncMock(side_effect=ConnectionError("Close failed"))
        mock_redis2 = AsyncMock()
        mock_redis2.aclose = AsyncMock()
        policy._clients[123] = mock_redis1
        policy._clients[456] = mock_redis2

        # Should not raise even if one client fails to close
        await policy.close()

        # Both close attempts should have been made
        mock_redis1.aclose.assert_called_once()
        mock_redis2.aclose.assert_called_once()
        assert len(policy._clients) == 0

    @pytest.mark.asyncio
    async def test_execute_runs_health_checks_concurrently(self):
        """
        Verify that execute() runs multiple health checks concurrently.
        """

        # Track concurrent execution
        concurrent_count = 0
        max_concurrent = 0

        async def tracking_health_check(database, client=None):
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

        # Mock get_client to avoid real connections
        async def mock_get_client(database):
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock(return_value=True)
            return mock_client

        policy.get_client = mock_get_client

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

        async def slow_health_check(database, client=None):
            await asyncio.sleep(1.0)  # Takes 1 second
            return True

        async def fast_health_check(database, client=None):
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

        async def mock_get_client(database):
            mock_client = AsyncMock()
            return mock_client

        policy.get_client = mock_get_client

        # Should raise because first health check times out
        with pytest.raises(UnhealthyDatabaseException) as exc_info:
            await policy.execute([mock_hc1, mock_hc2], mock_db)

        assert isinstance(exc_info.value.original_exception, asyncio.TimeoutError)
