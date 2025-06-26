from unittest.mock import patch, Mock

import pytest

from redis.event import EventDispatcher, OnCommandFailEvent
from redis.multidb.circuit import State as CBState
from redis.multidb.database import State as DBState, AbstractDatabase
from redis.multidb.client import MultiDBClient
from redis.multidb.exception import NoValidDatabaseException
from redis.multidb.failure_detector import FailureDetector
from redis.multidb.healthcheck import HealthCheck
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
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.OPEN}},
            ),
        ],
        ids=['all closed - highest weight', 'highest weight - open'],
        indirect=True,
    )
    def test_execute_command_against_correct_db_on_successful_initialization(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(
                mock_multi_db_config,
                'databases',
                return_value=databases
        ):
            mock_db1.client.execute_command.return_value = 'OK1'

            for hc in mock_multi_db_config.health_checks:
                hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set('key', 'value') == 'OK1'

            for hc in mock_multi_db_config.health_checks:
                assert hc.check_health.call_count == 3

            assert mock_db.state == DBState.PASSIVE
            assert mock_db1.state == DBState.ACTIVE
            assert mock_db2.state == DBState.PASSIVE or mock_db2.state == DBState.DISCONNECTED

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
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(
                mock_multi_db_config,
                'databases',
                return_value=databases
        ):
            for hc in mock_multi_db_config.health_checks:
                hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            with pytest.raises(NoValidDatabaseException, match='Initial connection failed - no active database found'):
                client.set('key', 'value')

                for hc in mock_multi_db_config.health_checks:
                    assert hc.check_health.call_count == 3

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
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(
                mock_multi_db_config,
                'databases',
                return_value=databases
        ):
            for hc in mock_multi_db_config.health_checks:
                hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            with pytest.raises(ValueError, match='Given database already exists'):
                client.add_database(mock_db)

                for hc in mock_multi_db_config.health_checks:
                    assert hc.check_health.call_count == 3

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
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db2)

        with patch.object(
                mock_multi_db_config,
                'databases',
                return_value=databases
        ):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_db2.client.execute_command.return_value = 'OK2'

            for hc in mock_multi_db_config.health_checks:
                hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert client.set('key', 'value') == 'OK2'

            for hc in mock_multi_db_config.health_checks:
                assert hc.check_health.call_count == 2

            assert mock_db.state == DBState.PASSIVE
            assert mock_db2.state == DBState.ACTIVE

            client.add_database(mock_db1)

            for hc in mock_multi_db_config.health_checks:
                assert hc.check_health.call_count == 3

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
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(
                mock_multi_db_config,
                'databases',
                return_value=databases
        ):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_db2.client.execute_command.return_value = 'OK2'

            for hc in mock_multi_db_config.health_checks:
                hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert client.set('key', 'value') == 'OK1'

            for hc in mock_multi_db_config.health_checks:
                assert hc.check_health.call_count == 3

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
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(
                mock_multi_db_config,
                'databases',
                return_value=databases
        ):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_db2.client.execute_command.return_value = 'OK2'

            for hc in mock_multi_db_config.health_checks:
                hc.check_health.return_value = False

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            assert client.set('key', 'value') == 'OK1'

            for hc in mock_multi_db_config.health_checks:
                assert hc.check_health.call_count == 3

            assert mock_db.state == DBState.PASSIVE
            assert mock_db1.state == DBState.ACTIVE
            assert mock_db2.state == DBState.PASSIVE

            client.update_database_weight(mock_db2, 0.8)

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
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(
                mock_multi_db_config,
                'databases',
                return_value=databases
        ):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_multi_db_config.event_dispatcher = EventDispatcher()
            mock_fd = mock_multi_db_config.failure_detectors[0]

            # Event fired if command against mock_db1 would fail
            command_fail_event = OnCommandFailEvent(
                command=('SET', 'key', 'value'),
                exception=Exception(),
                client=mock_db1.client
            )

            for hc in mock_multi_db_config.health_checks:
                hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set('key', 'value') == 'OK1'

            for hc in mock_multi_db_config.health_checks:
                assert hc.check_health.call_count == 3

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
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(
                mock_multi_db_config,
                'databases',
                return_value=databases
        ):
            mock_db1.client.execute_command.return_value = 'OK1'

            for hc in mock_multi_db_config.health_checks:
                hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set('key', 'value') == 'OK1'

            for hc in mock_multi_db_config.health_checks:
                assert hc.check_health.call_count == 3

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
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(
                mock_multi_db_config,
                'databases',
                return_value=databases
        ):
            mock_db1.client.execute_command.return_value = 'OK1'
            mock_db.client.execute_command.return_value = 'OK'

            for hc in mock_multi_db_config.health_checks:
                hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1
            assert client.set('key', 'value') == 'OK1'

            for hc in mock_multi_db_config.health_checks:
                assert hc.check_health.call_count == 3

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

            mock_db1.circuit.state = CBState.OPEN

            with pytest.raises(NoValidDatabaseException, match='Cannot set active database, database is unhealthy'):
                client.set_active_database(mock_db1)