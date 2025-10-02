import asyncio
from unittest.mock import patch, AsyncMock, Mock

import pybreaker
import pytest

from redis.asyncio.multidb.client import MultiDBClient
from redis.asyncio.multidb.database import AsyncDatabase
from redis.asyncio.multidb.failover import WeightBasedFailoverStrategy
from redis.asyncio.multidb.failure_detector import AsyncFailureDetector
from redis.asyncio.multidb.healthcheck import EchoHealthCheck, HealthCheck
from redis.event import EventDispatcher, AsyncOnCommandsFailEvent
from redis.multidb.circuit import State as CBState, PBCircuitBreakerAdapter
from redis.multidb.exception import NoValidDatabaseException
from tests.test_asyncio.test_multidb.conftest import create_weighted_list


class TestMultiDbClient:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_against_correct_db_on_successful_initialization(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert await client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            assert mock_db.circuit.state == CBState.CLOSED
            assert mock_db1.circuit.state == CBState.CLOSED
            assert mock_db2.circuit.state == CBState.CLOSED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_against_correct_db_and_closed_circuit(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_db1.client.execute_command = AsyncMock(return_value="OK1")

            mock_hc.check_health.side_effect = [
                False,
                True,
                True,
                True,
                True,
                True,
                True,
            ]

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert await client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 7

            assert mock_db.circuit.state == CBState.CLOSED
            assert mock_db1.circuit.state == CBState.CLOSED
            assert mock_db2.circuit.state == CBState.OPEN

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"health_check_probes": 1},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_against_correct_db_on_background_health_check_determine_active_db_unhealthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        cb = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=5))
        cb.database = mock_db
        mock_db.circuit = cb

        cb1 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=5))
        cb1.database = mock_db1
        mock_db1.circuit = cb1

        cb2 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=5))
        cb2.database = mock_db2
        mock_db2.circuit = cb2

        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config,
                "default_health_checks",
                return_value=[EchoHealthCheck()],
            ),
        ):
            mock_db.client.execute_command.side_effect = [
                "healthcheck",
                "healthcheck",
                "healthcheck",
                "OK",
                "error",
            ]
            mock_db1.client.execute_command.side_effect = [
                "healthcheck",
                "OK1",
                "error",
                "error",
                "healthcheck",
                "OK1",
            ]
            mock_db2.client.execute_command.side_effect = [
                "healthcheck",
                "healthcheck",
                "OK2",
                "error",
                "error",
            ]
            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            client = MultiDBClient(mock_multi_db_config)
            assert await client.set("key", "value") == "OK1"
            await asyncio.sleep(0.15)
            assert await client.set("key", "value") == "OK2"
            await asyncio.sleep(0.1)
            assert await client.set("key", "value") == "OK"
            await asyncio.sleep(0.1)
            assert await client.set("key", "value") == "OK1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"health_check_probes": 1},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_auto_fallback_to_highest_weight_db(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config,
                "default_health_checks",
                return_value=[EchoHealthCheck()],
            ),
        ):
            mock_db.client.execute_command.side_effect = [
                "healthcheck",
                "healthcheck",
                "healthcheck",
                "healthcheck",
                "healthcheck",
            ]
            mock_db1.client.execute_command.side_effect = [
                "healthcheck",
                "OK1",
                "error",
                "healthcheck",
                "healthcheck",
                "OK1",
            ]
            mock_db2.client.execute_command.side_effect = [
                "healthcheck",
                "healthcheck",
                "OK2",
                "healthcheck",
                "healthcheck",
                "healthcheck",
            ]
            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.auto_fallback_interval = 0.2
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            client = MultiDBClient(mock_multi_db_config)
            assert await client.set("key", "value") == "OK1"
            await asyncio.sleep(0.15)
            assert await client.set("key", "value") == "OK2"
            await asyncio.sleep(0.22)
            assert await client.set("key", "value") == "OK1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.OPEN}},
                {"weight": 0.7, "circuit": {"state": CBState.OPEN}},
                {"weight": 0.5, "circuit": {"state": CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_command_throws_exception_on_failed_initialization(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            with pytest.raises(
                NoValidDatabaseException,
                match="Initial connection failed - no active database found",
            ):
                await client.set("key", "value")
                assert mock_hc.check_health.call_count == 9

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_add_database_throws_exception_on_same_database(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            with pytest.raises(ValueError, match="Given database already exists"):
                await client.add_database(mock_db)
                assert mock_hc.check_health.call_count == 9

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_add_database_makes_new_database_active(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert await client.set("key", "value") == "OK2"
            assert mock_hc.check_health.call_count == 6

            await client.add_database(mock_db1)
            assert mock_hc.check_health.call_count == 9

            assert await client.set("key", "value") == "OK1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_remove_highest_weighted_database(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert await client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            await client.remove_database(mock_db1)
            assert await client.set("key", "value") == "OK2"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_update_database_weight_to_be_highest(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert await client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            await client.update_database_weight(mock_db2, 0.8)
            assert mock_db2.weight == 0.8

            assert await client.set("key", "value") == "OK2"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_add_new_failure_detector(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_multi_db_config.event_dispatcher = EventDispatcher()
            mock_fd = mock_multi_db_config.failure_detectors[0]

            # Event fired if command against mock_db1 would fail
            command_fail_event = AsyncOnCommandsFailEvent(
                commands=("SET", "key", "value"),
                exception=Exception(),
            )

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert await client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            # Simulate failing command events that lead to a failure detection
            for i in range(5):
                await mock_multi_db_config.event_dispatcher.dispatch_async(
                    command_fail_event
                )

            assert mock_fd.register_failure.call_count == 5

            another_fd = Mock(spec=AsyncFailureDetector)
            client.add_failure_detector(another_fd)

            # Simulate failing command events that lead to a failure detection
            for i in range(5):
                await mock_multi_db_config.event_dispatcher.dispatch_async(
                    command_fail_event
                )

            assert mock_fd.register_failure.call_count == 10
            assert another_fd.register_failure.call_count == 5

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_add_new_health_check(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_db1.client.execute_command.return_value = "OK1"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert await client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            another_hc = Mock(spec=HealthCheck)
            another_hc.check_health.return_value = True

            await client.add_health_check(another_hc)
            await client._check_db_health(mock_db1)

            assert mock_hc.check_health.call_count == 12
            assert another_hc.check_health.call_count == 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_set_active_database(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db.client.execute_command.return_value = "OK"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert await client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            await client.set_active_database(mock_db)
            assert await client.set("key", "value") == "OK"

            with pytest.raises(
                ValueError, match="Given database is not a member of database list"
            ):
                await client.set_active_database(Mock(spec=AsyncDatabase))

            mock_hc.check_health.return_value = False

            with pytest.raises(
                NoValidDatabaseException,
                match="Cannot set active database, database is unhealthy",
            ):
                await client.set_active_database(mock_db1)
