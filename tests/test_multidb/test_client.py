from time import sleep
from unittest.mock import patch, Mock

import pybreaker
import pytest

from redis.event import EventDispatcher, OnCommandsFailEvent
from redis.multidb.circuit import State as CBState, PBCircuitBreakerAdapter
from redis.multidb.config import DEFAULT_FAILOVER_RETRIES, \
    DEFAULT_FAILOVER_BACKOFF
from redis.multidb.database import State as DBState, AbstractDatabase
from redis.multidb.client import MultiDBClient
from redis.multidb.exception import NoValidDatabaseException
from redis.multidb.failover import WeightBasedFailoverStrategy
from redis.multidb.failure_detector import FailureDetector
from redis.multidb.healthcheck import HealthCheck, EchoHealthCheck, DEFAULT_HEALTH_CHECK_RETRIES, \
    DEFAULT_HEALTH_CHECK_BACKOFF
from redis.retry import Retry
from tests.test_multidb.conftest import create_weighted_list


class TestMultiDbClient:
    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_against_correct_db_on_successful_initialization(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set('key', 'value') == 'OK1'
            assert mock_hc.check_health.call_count == 3

            assert mock_db.circuit.state == CBState.CLOSED
            assert mock_db1.circuit.state == CBState.CLOSED
            assert mock_db2.circuit.state == CBState.CLOSED

    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_against_correct_db_and_closed_circuit(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_hc.check_health.side_effect = [False, True, True]

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set('key', 'value') == 'OK1'
            assert mock_hc.check_health.call_count == 3

            assert mock_db.circuit.state == CBState.CLOSED
            assert mock_db1.circuit.state == CBState.CLOSED
            assert mock_db2.circuit.state == CBState.OPEN

    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_against_correct_db_on_background_health_check_determine_active_db_unhealthy(
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

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[EchoHealthCheck(
                    retry=Retry(retries=DEFAULT_HEALTH_CHECK_RETRIES, backoff=DEFAULT_HEALTH_CHECK_BACKOFF)
                )]):
            mock_db.client.execute_command.side_effect = ['healthcheck', 'healthcheck', 'healthcheck', 'OK', 'error']
            mock_db1.client.execute_command.side_effect = ['healthcheck', 'OK1', 'error', 'error', 'healthcheck', 'OK1']
            mock_db2.client.execute_command.side_effect = ['healthcheck', 'healthcheck', 'OK2', 'error', 'error']
            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy(
                retry=Retry(retries=DEFAULT_FAILOVER_RETRIES, backoff=DEFAULT_FAILOVER_BACKOFF)
            )

            client = MultiDBClient(mock_multi_db_config)
            assert client.set('key', 'value') == 'OK1'
            sleep(0.15)
            assert client.set('key', 'value') == 'OK2'
            sleep(0.1)
            assert client.set('key', 'value') == 'OK'
            sleep(0.1)
            assert client.set('key', 'value') == 'OK1'

    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_auto_fallback_to_highest_weight_db(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[EchoHealthCheck(
                    retry=Retry(retries=DEFAULT_HEALTH_CHECK_RETRIES, backoff=DEFAULT_HEALTH_CHECK_BACKOFF)
                )]):
            mock_db.client.execute_command.side_effect = ['healthcheck', 'healthcheck', 'healthcheck', 'healthcheck', 'healthcheck']
            mock_db1.client.execute_command.side_effect = ['healthcheck', 'OK1', 'error', 'healthcheck', 'healthcheck', 'OK1']
            mock_db2.client.execute_command.side_effect = ['healthcheck', 'healthcheck', 'OK2', 'healthcheck', 'healthcheck', 'healthcheck']
            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.auto_fallback_interval = 0.2
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy(
                retry=Retry(retries=DEFAULT_FAILOVER_RETRIES, backoff=DEFAULT_FAILOVER_BACKOFF)
            )

            client = MultiDBClient(mock_multi_db_config)
            assert client.set('key', 'value') == 'OK1'

            assert mock_db.state == DBState.PASSIVE
            assert mock_db1.state == DBState.ACTIVE
            assert mock_db2.state == DBState.PASSIVE

            sleep(0.15)

            assert client.set('key', 'value') == 'OK2'

            assert mock_db.state == DBState.PASSIVE
            assert mock_db1.state == DBState.ACTIVE
            assert mock_db2.state == DBState.PASSIVE

            sleep(0.22)

            assert client.set('key', 'value') == 'OK1'

            assert mock_db.state == DBState.PASSIVE
            assert mock_db1.state == DBState.ACTIVE
            assert mock_db2.state == DBState.PASSIVE

    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.OPEN}},
                    {'weight': 0.7, 'circuit': {'state': CBState.OPEN}},
                    {'weight': 0.5, 'circuit': {'state': CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_throws_exception_on_failed_initialization(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            with pytest.raises(NoValidDatabaseException, match='Initial connection failed - no active database found'):
                client.set('key', 'value')
                assert mock_hc.check_health.call_count == 3

                assert mock_db.state == DBState.DISCONNECTED
                assert mock_db1.state == DBState.DISCONNECTED
                assert mock_db2.state == DBState.DISCONNECTED

    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_add_database_throws_exception_on_same_database(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            with pytest.raises(ValueError, match='Given database already exists'):
                client.add_database(mock_db)
                assert mock_hc.check_health.call_count == 3

    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_add_database_makes_new_database_active(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_db2.client.execute_command.return_value = 'OK2'
            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert client.set('key', 'value') == 'OK2'
            assert mock_hc.check_health.call_count == 2

            assert mock_db.state == DBState.PASSIVE
            assert mock_db2.state == DBState.ACTIVE

            client.add_database(mock_db1)

            assert mock_hc.check_health.call_count == 3

            assert client.set('key', 'value') == 'OK1'

            assert mock_db.state == DBState.PASSIVE
            assert mock_db1.state == DBState.ACTIVE
            assert mock_db2.state == DBState.PASSIVE

    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_remove_highest_weighted_database(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_db2.client.execute_command.return_value = 'OK2'
            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert client.set('key', 'value') == 'OK1'
            assert mock_hc.check_health.call_count == 3

            assert mock_db.state == DBState.PASSIVE
            assert mock_db1.state == DBState.ACTIVE
            assert mock_db2.state == DBState.PASSIVE

            client.remove_database(mock_db1)

            assert client.set('key', 'value') == 'OK2'

            assert mock_db.state == DBState.PASSIVE
            assert mock_db2.state == DBState.ACTIVE

    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_update_database_weight_to_be_highest(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_db2.client.execute_command.return_value = 'OK2'
            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert client.set('key', 'value') == 'OK1'
            assert mock_hc.check_health.call_count == 3

            assert mock_db.state == DBState.PASSIVE
            assert mock_db1.state == DBState.ACTIVE
            assert mock_db2.state == DBState.PASSIVE

            client.update_database_weight(mock_db2, 0.8)
            assert mock_db2.weight == 0.8

            assert client.set('key', 'value') == 'OK2'

            assert mock_db.state == DBState.PASSIVE
            assert mock_db1.state == DBState.PASSIVE
            assert mock_db2.state == DBState.ACTIVE

    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_add_new_failure_detector(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_multi_db_config.event_dispatcher = EventDispatcher()
            mock_fd = mock_multi_db_config.failure_detectors[0]

            # Event fired if command against mock_db1 would fail
            command_fail_event = OnCommandsFailEvent(
                commands=('SET', 'key', 'value'),
                exception=Exception(),
            )
            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set('key', 'value') == 'OK1'
            assert mock_hc.check_health.call_count == 3

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
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_add_new_health_check(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set('key', 'value') == 'OK1'
            assert mock_hc.check_health.call_count == 3

            another_hc = Mock(spec=HealthCheck)
            another_hc.check_health.return_value = True

            client.add_health_check(another_hc)
            client._check_db_health(mock_db1)

            assert another_hc.check_health.call_count == 1

    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_set_active_database(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_db.client.execute_command.return_value = 'OK'
            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set('key', 'value') == 'OK1'
            assert mock_hc.check_health.call_count == 3

            assert mock_db.state == DBState.PASSIVE
            assert mock_db1.state == DBState.ACTIVE
            assert mock_db2.state == DBState.PASSIVE

            client.set_active_database(mock_db)
            assert client.set('key', 'value') == 'OK'

            assert mock_db.state == DBState.ACTIVE
            assert mock_db1.state == DBState.PASSIVE
            assert mock_db2.state == DBState.PASSIVE

            with pytest.raises(ValueError, match='Given database is not a member of database list'):
                client.set_active_database(Mock(spec=AbstractDatabase))

            mock_hc.check_health.return_value = False

            with pytest.raises(NoValidDatabaseException, match='Cannot set active database, database is unhealthy'):
                client.set_active_database(mock_db1)