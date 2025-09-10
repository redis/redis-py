import asyncio
from unittest.mock import Mock

import pytest

from redis.asyncio.multidb.command_executor import AsyncCommandExecutor
from redis.asyncio.multidb.failure_detector import FailureDetectorAsyncWrapper
from redis.multidb.circuit import State as CBState
from redis.multidb.failure_detector import CommandFailureDetector


class TestFailureDetectorAsyncWrapper:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
        ],
        indirect=True,
    )
    async def test_failure_detector_open_circuit_on_threshold_exceed_and_interval_not_exceed(self, mock_db):
        fd = FailureDetectorAsyncWrapper(CommandFailureDetector(5, 1))
        mock_ce = Mock(spec=AsyncCommandExecutor)
        mock_ce.active_database = mock_db
        fd.set_command_executor(mock_ce)
        assert mock_db.circuit.state == CBState.CLOSED

        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.OPEN

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
        ],
        indirect=True,
    )
    async def test_failure_detector_do_not_open_circuit_if_threshold_not_exceed_and_interval_not_exceed(self, mock_db):
        fd = FailureDetectorAsyncWrapper(CommandFailureDetector(5, 1))
        mock_ce = Mock(spec=AsyncCommandExecutor)
        mock_ce.active_database = mock_db
        fd.set_command_executor(mock_ce)
        assert mock_db.circuit.state == CBState.CLOSED

        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.CLOSED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
        ],
        indirect=True,
    )
    async def test_failure_detector_do_not_open_circuit_on_threshold_exceed_and_interval_exceed(self, mock_db):
        fd = FailureDetectorAsyncWrapper(CommandFailureDetector(5, 0.3))
        mock_ce = Mock(spec=AsyncCommandExecutor)
        mock_ce.active_database = mock_db
        fd.set_command_executor(mock_ce)
        assert mock_db.circuit.state == CBState.CLOSED

        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await asyncio.sleep(0.1)
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await asyncio.sleep(0.1)
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await asyncio.sleep(0.1)
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await asyncio.sleep(0.1)
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.CLOSED

        # 4 more failures as the last one already refreshed timer
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.OPEN

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
        ],
        indirect=True,
    )
    async def test_failure_detector_refresh_timer_on_expired_duration(self, mock_db):
        fd = FailureDetectorAsyncWrapper(CommandFailureDetector(5, 0.3))
        mock_ce = Mock(spec=AsyncCommandExecutor)
        mock_ce.active_database = mock_db
        fd.set_command_executor(mock_ce)
        assert mock_db.circuit.state == CBState.CLOSED

        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await asyncio.sleep(0.4)

        assert mock_db.circuit.state == CBState.CLOSED

        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.CLOSED
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.OPEN

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
        ],
        indirect=True,
    )
    async def test_failure_detector_open_circuit_on_specific_exception_threshold_exceed(self, mock_db):
        fd = FailureDetectorAsyncWrapper(CommandFailureDetector(5, 1, error_types=[ConnectionError]))
        mock_ce = Mock(spec=AsyncCommandExecutor)
        mock_ce.active_database = mock_db
        fd.set_command_executor(mock_ce)
        assert mock_db.circuit.state == CBState.CLOSED

        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(ConnectionError(), ('SET', 'key1', 'value1'))
        await fd.register_failure(ConnectionError(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        await fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.CLOSED

        await fd.register_failure(ConnectionError(), ('SET', 'key1', 'value1'))
        await fd.register_failure(ConnectionError(), ('SET', 'key1', 'value1'))
        await fd.register_failure(ConnectionError(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.OPEN