from time import sleep
from unittest.mock import PropertyMock

import pytest

from redis.event import EventDispatcher, OnCommandFailEvent
from redis.multidb.circuit import State as CBState
from redis.multidb.command_executor import DefaultCommandExecutor
from redis.multidb.failure_detector import CommandFailureDetector
from tests.test_multidb.conftest import create_weighted_list


class TestDefaultCommandExecutor:
    @pytest.mark.parametrize(
        'mock_db,mock_db1,mock_db2',
        [
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_on_active_database(self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed):
        mock_db1.client.execute_command.return_value = 'OK1'
        mock_db2.client.execute_command.return_value = 'OK2'
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed
        )

        executor.active_database = mock_db1
        assert executor.execute_command('SET', 'key', 'value') == 'OK1'

        executor.active_database = mock_db2
        assert executor.execute_command('SET', 'key', 'value') == 'OK2'
        assert mock_ed.register_listeners.call_count == 1

    @pytest.mark.parametrize(
        'mock_db,mock_db1,mock_db2',
        [
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_automatically_select_active_database(
            self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        mock_db1.client.execute_command.return_value = 'OK1'
        mock_db2.client.execute_command.return_value = 'OK2'
        mock_selector = PropertyMock(side_effect=[mock_db1, mock_db2])
        type(mock_fs).database = mock_selector
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed
        )

        assert executor.execute_command('SET', 'key', 'value') == 'OK1'
        mock_db1.circuit.state = CBState.OPEN

        assert executor.execute_command('SET', 'key', 'value') == 'OK2'
        assert mock_ed.register_listeners.call_count == 1
        assert mock_selector.call_count == 2

    @pytest.mark.parametrize(
        'mock_db,mock_db1,mock_db2',
        [
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_fallback_to_another_db_after_fallback_interval(
            self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        mock_db1.client.execute_command.return_value = 'OK1'
        mock_db2.client.execute_command.return_value = 'OK2'
        mock_selector = PropertyMock(side_effect=[mock_db1, mock_db2, mock_db1])
        type(mock_fs).database = mock_selector
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            auto_fallback_interval=0.1,
        )

        assert executor.execute_command('SET', 'key', 'value') == 'OK1'
        mock_db1.weight = 0.1
        sleep(0.15)

        assert executor.execute_command('SET', 'key', 'value') == 'OK2'
        mock_db1.weight = 0.7
        sleep(0.15)

        assert executor.execute_command('SET', 'key', 'value') == 'OK1'
        assert mock_ed.register_listeners.call_count == 1
        assert mock_selector.call_count == 3

    @pytest.mark.parametrize(
        'mock_db,mock_db1,mock_db2',
        [
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_fallback_to_another_db_after_failure_detection(
            self, mock_db, mock_db1, mock_db2, mock_fs
    ):
        mock_db1.client.execute_command.return_value = 'OK1'
        mock_db2.client.execute_command.return_value = 'OK2'
        mock_selector = PropertyMock(side_effect=[mock_db1, mock_db2, mock_db1])
        type(mock_fs).database = mock_selector
        threshold = 5
        fd = CommandFailureDetector(threshold, 1)
        ed = EventDispatcher()
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        # Event fired if command against mock_db1 would fail
        command_fail_event = OnCommandFailEvent(
            command=('SET', 'key', 'value'),
            exception=Exception(),
        )

        executor = DefaultCommandExecutor(
            failure_detectors=[fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=ed,
            auto_fallback_interval=0.1,
        )
        fd.set_command_executor(command_executor=executor)

        assert executor.execute_command('SET', 'key', 'value') == 'OK1'

        # Simulate failing command events that lead to a failure detection
        for i in range(threshold):
            ed.dispatch(command_fail_event)

        assert executor.execute_command('SET', 'key', 'value') == 'OK2'

        command_fail_event = OnCommandFailEvent(
            command=('SET', 'key', 'value'),
            exception=Exception(),
        )

        for i in range(threshold):
            ed.dispatch(command_fail_event)

        assert executor.execute_command('SET', 'key', 'value') == 'OK1'
        assert mock_selector.call_count == 3