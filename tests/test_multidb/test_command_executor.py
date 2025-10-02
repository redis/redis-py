from time import sleep
from unittest.mock import PropertyMock

import pytest

from redis.exceptions import ConnectionError
from redis.backoff import NoBackoff
from redis.event import EventDispatcher
from redis.multidb.circuit import State as CBState
from redis.multidb.command_executor import DefaultCommandExecutor
from redis.multidb.failure_detector import CommandFailureDetector
from redis.retry import Retry
from tests.test_multidb.conftest import create_weighted_list


class TestDefaultCommandExecutor:
    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_on_active_database(
        self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        mock_db1.client.execute_command.return_value = "OK1"
        mock_db2.client.execute_command.return_value = "OK2"
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            command_retry=Retry(NoBackoff(), 0),
        )

        executor.active_database = mock_db1
        assert executor.execute_command("SET", "key", "value") == "OK1"

        executor.active_database = mock_db2
        assert executor.execute_command("SET", "key", "value") == "OK2"
        assert mock_ed.register_listeners.call_count == 1
        assert mock_fd.register_command_execution.call_count == 2

    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_automatically_select_active_database(
        self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        mock_db1.client.execute_command.return_value = "OK1"
        mock_db2.client.execute_command.return_value = "OK2"
        mock_fs.database.side_effect = [mock_db1, mock_db2]
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            command_retry=Retry(NoBackoff(), 0),
        )

        assert executor.execute_command("SET", "key", "value") == "OK1"
        mock_db1.circuit.state = CBState.OPEN

        assert executor.execute_command("SET", "key", "value") == "OK2"
        assert mock_ed.register_listeners.call_count == 1
        assert mock_fs.database.call_count == 2
        assert mock_fd.register_command_execution.call_count == 2

    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_fallback_to_another_db_after_fallback_interval(
        self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        mock_db1.client.execute_command.return_value = "OK1"
        mock_db2.client.execute_command.return_value = "OK2"
        mock_fs.database.side_effect = [mock_db1, mock_db2, mock_db1]
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            auto_fallback_interval=0.1,
            command_retry=Retry(NoBackoff(), 0),
        )

        assert executor.execute_command("SET", "key", "value") == "OK1"
        mock_db1.weight = 0.1
        sleep(0.15)

        assert executor.execute_command("SET", "key", "value") == "OK2"
        mock_db1.weight = 0.7
        sleep(0.15)

        assert executor.execute_command("SET", "key", "value") == "OK1"
        assert mock_ed.register_listeners.call_count == 1
        assert mock_fs.database.call_count == 3
        assert mock_fd.register_command_execution.call_count == 3

    @pytest.mark.parametrize(
        "mock_db,mock_db1,mock_db2",
        [
            (
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_command_fallback_to_another_db_after_failure_detection(
        self, mock_db, mock_db1, mock_db2, mock_fs
    ):
        mock_db1.client.execute_command.side_effect = [
            "OK1",
            ConnectionError,
            ConnectionError,
            ConnectionError,
            "OK1",
        ]
        mock_db2.client.execute_command.side_effect = [
            "OK2",
            ConnectionError,
            ConnectionError,
            ConnectionError,
        ]
        mock_fs.database.side_effect = [mock_db1, mock_db2, mock_db1]
        threshold = 3
        fd = CommandFailureDetector(threshold, 0.0, 1)
        ed = EventDispatcher()
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=ed,
            auto_fallback_interval=0.1,
            command_retry=Retry(NoBackoff(), threshold),
        )
        fd.set_command_executor(command_executor=executor)

        assert executor.execute_command("SET", "key", "value") == "OK1"
        assert executor.execute_command("SET", "key", "value") == "OK2"
        assert executor.execute_command("SET", "key", "value") == "OK1"
        assert mock_fs.database.call_count == 3
