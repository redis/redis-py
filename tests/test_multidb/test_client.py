from unittest.mock import patch

import pytest

from redis.multidb.circuit import State as CBState
from redis.multidb.database import State as DBState
from redis.multidb.client import MultiDBClient
from redis.multidb.exception import NoValidDatabaseException
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