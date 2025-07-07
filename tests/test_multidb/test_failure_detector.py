from time import sleep

import pytest

from redis.multidb.failure_detector import CommandFailureDetector
from redis.multidb.circuit import State as CBState
from redis.exceptions import ConnectionError


class TestCommandFailureDetector:
    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
        ],
        indirect=True,
    )
    def test_failure_detector_open_circuit_on_threshold_exceed_and_interval_not_exceed(self, mock_db):
        fd = CommandFailureDetector(5, 1)
        assert mock_db.circuit.state == CBState.CLOSED

        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.OPEN

    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
        ],
        indirect=True,
    )
    def test_failure_detector_do_not_open_circuit_if_threshold_not_exceed_and_interval_not_exceed(self, mock_db):
        fd = CommandFailureDetector(5, 1)
        assert mock_db.circuit.state == CBState.CLOSED

        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.CLOSED

    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
        ],
        indirect=True,
    )
    def test_failure_detector_do_not_open_circuit_on_threshold_exceed_and_interval_exceed(self, mock_db):
        fd = CommandFailureDetector(5, 0.3)
        assert mock_db.circuit.state == CBState.CLOSED

        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        sleep(0.1)
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        sleep(0.1)
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        sleep(0.1)
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        sleep(0.1)
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.CLOSED

        # 4 more failure as last one already refreshed timer
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.OPEN

    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
        ],
        indirect=True,
    )
    def test_failure_detector_refresh_timer_on_expired_duration(self, mock_db):
        fd = CommandFailureDetector(5, 0.3)
        assert mock_db.circuit.state == CBState.CLOSED

        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        sleep(0.4)

        assert mock_db.circuit.state == CBState.CLOSED

        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.CLOSED
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.OPEN

    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
        ],
        indirect=True,
    )
    def test_failure_detector_open_circuit_on_specific_exception_threshold_exceed(self, mock_db):
        fd = CommandFailureDetector(5, 1, error_types=[ConnectionError])
        assert mock_db.circuit.state == CBState.CLOSED

        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, ConnectionError(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, ConnectionError(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, Exception(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.CLOSED

        fd.register_failure(mock_db, ConnectionError(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, ConnectionError(), ('SET', 'key1', 'value1'))
        fd.register_failure(mock_db, ConnectionError(), ('SET', 'key1', 'value1'))

        assert mock_db.circuit.state == CBState.OPEN