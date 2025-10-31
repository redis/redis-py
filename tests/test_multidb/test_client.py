import threading
from time import sleep
from unittest.mock import patch, Mock

import pybreaker
import pytest

from redis.event import EventDispatcher, OnCommandsFailEvent
from redis.multidb.circuit import State as CBState, PBCircuitBreakerAdapter
from redis.multidb.database import SyncDatabase
from redis.multidb.client import MultiDBClient
from redis.multidb.exception import NoValidDatabaseException
from redis.multidb.failover import WeightBasedFailoverStrategy
from redis.multidb.failure_detector import FailureDetector
from redis.multidb.healthcheck import HealthCheck
from tests.test_multidb.conftest import create_weighted_list


@pytest.mark.onlynoncluster
class TestMultiDbClient:
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
    def test_execute_command_against_correct_db_on_successful_initialization(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_db1.client.execute_command.return_value = "OK1"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            assert mock_db.circuit.state == CBState.CLOSED
            assert mock_db1.circuit.state == CBState.CLOSED
            assert mock_db2.circuit.state == CBState.CLOSED

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
    def test_execute_command_against_correct_db_and_closed_circuit(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_db1.client.execute_command.return_value = "OK1"

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
            assert client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 7

            assert mock_db.circuit.state == CBState.CLOSED
            assert mock_db1.circuit.state == CBState.CLOSED
            assert mock_db2.circuit.state == CBState.OPEN

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
    def test_execute_command_against_correct_db_on_background_health_check_determine_active_db_unhealthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
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

        # Track health check runs across all databases
        health_check_run = 0

        # Create events for each failover scenario
        db1_became_unhealthy = threading.Event()
        db2_became_unhealthy = threading.Event()
        db_became_unhealthy = threading.Event()
        counter_lock = threading.Lock()

        def mock_check_health(database):
            nonlocal health_check_run

            # Increment run counter for each health check call
            with counter_lock:
                health_check_run += 1
                current_run = health_check_run

            # Run 1 (health_check_run 1-3): All databases healthy
            if current_run <= 3:
                return True

            # Run 2 (health_check_run 4-6): mock_db1 unhealthy, others healthy
            elif current_run <= 6:
                if database == mock_db1:
                    if current_run == 6:
                        db1_became_unhealthy.set()
                    return False

                # Signal that db1 has become unhealthy after all 3 checks
                if current_run == 6:
                    db1_became_unhealthy.set()
                return True

            # Run 3 (health_check_run 7-9): mock_db1 and mock_db2 unhealthy, mock_db healthy
            elif current_run <= 9:
                if database == mock_db1 or database == mock_db2:
                    if current_run == 9:
                        db2_became_unhealthy.set()
                    return False

                # Signal that db2 has become unhealthy after all 3 checks
                if current_run == 9:
                    db2_became_unhealthy.set()
                return True

            # Run 4 (health_check_run 10-12): mock_db unhealthy, others healthy
            else:
                if database == mock_db:
                    if current_run >= 12:
                        db_became_unhealthy.set()
                    return False

                # Signal that db has become unhealthy after all 3 checks
                if current_run >= 12:
                    db_became_unhealthy.set()
                return True

        mock_hc.check_health.side_effect = mock_check_health
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()
            mock_db.client.execute_command.return_value = "OK"
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"

            client = MultiDBClient(mock_multi_db_config)
            assert client.set("key", "value") == "OK1"

            # Wait for mock_db1 to become unhealthy
            assert db1_became_unhealthy.wait(timeout=1.0), (
                "Timeout waiting for mock_db1 to become unhealthy"
            )
            sleep(0.01)

            assert client.set("key", "value") == "OK2"

            # Wait for mock_db2 to become unhealthy
            assert db2_became_unhealthy.wait(timeout=1.0), (
                "Timeout waiting for mock_db2 to become unhealthy"
            )
            sleep(0.01)

            assert client.set("key", "value") == "OK"

            # Wait for mock_db to become unhealthy
            assert db_became_unhealthy.wait(timeout=1.0), (
                "Timeout waiting for mock_db to become unhealthy"
            )
            sleep(0.01)

            assert client.set("key", "value") == "OK1"

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
    def test_execute_command_auto_fallback_to_highest_weight_db(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        db1_counter = 0
        error_event = threading.Event()
        check = False

        def mock_check_health(database):
            nonlocal db1_counter, check

            if database == mock_db1 and not check:
                db1_counter += 1

                if db1_counter > 1:
                    error_event.set()
                    check = True
                    return False

            return True

        mock_hc.check_health.side_effect = mock_check_health
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_db.client.execute_command.return_value = "OK"
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"
            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.auto_fallback_interval = 0.2
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            client = MultiDBClient(mock_multi_db_config)
            assert client.set("key", "value") == "OK1"
            error_event.wait(timeout=0.5)
            assert client.set("key", "value") == "OK2"
            sleep(0.5)
            assert client.set("key", "value") == "OK1"

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
    def test_execute_command_do_not_auto_fallback_to_highest_weight_db(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        db1_counter = 0
        error_event = threading.Event()
        check = False

        def mock_check_health(database):
            nonlocal db1_counter, check

            if database == mock_db1 and not check:
                db1_counter += 1

                if db1_counter > 1:
                    error_event.set()
                    check = True
                    return False

            return True

        mock_hc.check_health.side_effect = mock_check_health
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_db.client.execute_command.return_value = "OK"
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"
            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.auto_fallback_interval = -1
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            client = MultiDBClient(mock_multi_db_config)
            assert client.set("key", "value") == "OK1"
            error_event.wait(timeout=0.5)
            assert client.set("key", "value") == "OK2"
            sleep(0.5)
            assert client.set("key", "value") == "OK2"

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
    def test_execute_command_throws_exception_on_failed_initialization(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            with pytest.raises(
                NoValidDatabaseException,
                match="Initial connection failed - no active database found",
            ):
                client.set("key", "value")

                assert mock_hc.check_health.call_count == 3

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
    def test_add_database_throws_exception_on_same_database(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            with pytest.raises(ValueError, match="Given database already exists"):
                client.add_database(mock_db)
                assert mock_hc.check_health.call_count == 3

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
    def test_add_database_makes_new_database_active(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert client.set("key", "value") == "OK2"
            assert mock_hc.check_health.call_count == 6

            client.add_database(mock_db1)
            assert mock_hc.check_health.call_count == 9

            assert client.set("key", "value") == "OK1"

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
    def test_remove_highest_weighted_database(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            client.remove_database(mock_db1)

            assert client.set("key", "value") == "OK2"

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
    def test_update_database_weight_to_be_highest(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db2.client.execute_command.return_value = "OK2"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            client.update_database_weight(mock_db2, 0.8)
            assert mock_db2.weight == 0.8

            assert client.set("key", "value") == "OK2"

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
    def test_add_new_failure_detector(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_multi_db_config.event_dispatcher = EventDispatcher()
            mock_fd = mock_multi_db_config.failure_detectors[0]

            # Event fired if command against mock_db1 would fail
            command_fail_event = OnCommandsFailEvent(
                commands=("SET", "key", "value"),
                exception=Exception(),
            )

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            # Simulate failing command events that lead to a failure detection
            for i in range(5):
                mock_multi_db_config.event_dispatcher.dispatch(command_fail_event)

            assert mock_fd.register_failure.call_count == 5

            another_fd = Mock(spec=FailureDetector)
            client.add_failure_detector(another_fd)

            # Simulate failing command events that lead to a failure detection
            for i in range(5):
                mock_multi_db_config.event_dispatcher.dispatch(command_fail_event)

            assert mock_fd.register_failure.call_count == 10
            assert another_fd.register_failure.call_count == 5

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
    def test_add_new_health_check(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_db1.client.execute_command.return_value = "OK1"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            another_hc = Mock(spec=HealthCheck)
            another_hc.check_health.return_value = True

            client.add_health_check(another_hc)
            client._check_db_health(mock_db1)

            assert mock_hc.check_health.call_count == 12
            assert another_hc.check_health.call_count == 3

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
    def test_set_active_database(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)
        mock_multi_db_config.health_checks = [mock_hc]

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
        ):
            mock_db1.client.execute_command.return_value = "OK1"
            mock_db.client.execute_command.return_value = "OK"

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set("key", "value") == "OK1"
            assert mock_hc.check_health.call_count == 9

            client.set_active_database(mock_db)
            assert client.set("key", "value") == "OK"

            with pytest.raises(
                ValueError, match="Given database is not a member of database list"
            ):
                client.set_active_database(Mock(spec=SyncDatabase))

            mock_hc.check_health.return_value = False

            with pytest.raises(
                NoValidDatabaseException,
                match="Cannot set active database, database is unhealthy",
            ):
                client.set_active_database(mock_db1)
