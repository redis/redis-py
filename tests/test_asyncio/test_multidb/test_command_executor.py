import asyncio
from unittest.mock import AsyncMock

import pytest

from redis.asyncio.multidb.failure_detector import FailureDetectorAsyncWrapper
from redis.event import EventDispatcher
from redis.exceptions import ConnectionError
from redis.asyncio.multidb.command_executor import DefaultCommandExecutor
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.multidb.circuit import State as CBState
from redis.multidb.failure_detector import CommandFailureDetector
from tests.test_asyncio.test_multidb.conftest import create_weighted_list


class TestDefaultCommandExecutor:
    @pytest.mark.asyncio
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
    async def test_execute_command_on_active_database(self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed):
        mock_db1.client.execute_command = AsyncMock(return_value='OK1')
        mock_db2.client.execute_command = AsyncMock(return_value='OK2')
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            command_retry=Retry(NoBackoff(), 0)
        )

        await executor.set_active_database(mock_db1)
        assert await executor.execute_command('SET', 'key', 'value') == 'OK1'

        await executor.set_active_database(mock_db2)
        assert await executor.execute_command('SET', 'key', 'value') == 'OK2'
        assert mock_ed.register_listeners.call_count == 1
        assert mock_fd.register_command_execution.call_count == 2

    @pytest.mark.asyncio
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
    async def test_execute_command_automatically_select_active_database(
            self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        mock_db1.client.execute_command = AsyncMock(return_value='OK1')
        mock_db2.client.execute_command = AsyncMock(return_value='OK2')
        mock_selector = AsyncMock(side_effect=[mock_db1, mock_db2])
        type(mock_fs).database = mock_selector
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            command_retry=Retry(NoBackoff(), 0)
        )

        assert await executor.execute_command('SET', 'key', 'value') == 'OK1'
        mock_db1.circuit.state = CBState.OPEN

        assert await executor.execute_command('SET', 'key', 'value') == 'OK2'
        assert mock_ed.register_listeners.call_count == 1
        assert mock_selector.call_count == 2
        assert mock_fd.register_command_execution.call_count == 2

    @pytest.mark.asyncio
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
    async def test_execute_command_fallback_to_another_db_after_fallback_interval(
            self, mock_db, mock_db1, mock_db2, mock_fd, mock_fs, mock_ed
    ):
        mock_db1.client.execute_command = AsyncMock(return_value='OK1')
        mock_db2.client.execute_command = AsyncMock(return_value='OK2')
        mock_selector = AsyncMock(side_effect=[mock_db1, mock_db2, mock_db1])
        type(mock_fs).database = mock_selector
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        executor = DefaultCommandExecutor(
            failure_detectors=[mock_fd],
            databases=databases,
            failover_strategy=mock_fs,
            event_dispatcher=mock_ed,
            auto_fallback_interval=0.1,
            command_retry=Retry(NoBackoff(), 0)
        )

        assert await executor.execute_command('SET', 'key', 'value') == 'OK1'
        mock_db1.weight = 0.1
        await asyncio.sleep(0.15)

        assert await executor.execute_command('SET', 'key', 'value') == 'OK2'
        mock_db1.weight = 0.7
        await asyncio.sleep(0.15)

        assert await executor.execute_command('SET', 'key', 'value') == 'OK1'
        assert mock_ed.register_listeners.call_count == 1
        assert mock_selector.call_count == 3
        assert mock_fd.register_command_execution.call_count == 3

    @pytest.mark.asyncio
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
    async def test_execute_command_fallback_to_another_db_after_failure_detection(
            self, mock_db, mock_db1, mock_db2, mock_fs
    ):
        mock_db1.client.execute_command = AsyncMock(side_effect=['OK1', ConnectionError, ConnectionError, ConnectionError, 'OK1'])
        mock_db2.client.execute_command = AsyncMock(side_effect=['OK2', ConnectionError, ConnectionError, ConnectionError])
        mock_selector = AsyncMock(side_effect=[mock_db1, mock_db2, mock_db1])
        type(mock_fs).database = mock_selector
        threshold = 3
        fd = FailureDetectorAsyncWrapper(CommandFailureDetector(threshold, 1))
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

        assert await executor.execute_command('SET', 'key', 'value') == 'OK1'
        assert await executor.execute_command('SET', 'key', 'value') == 'OK2'
        assert await executor.execute_command('SET', 'key', 'value') == 'OK1'
        assert mock_selector.call_count == 3