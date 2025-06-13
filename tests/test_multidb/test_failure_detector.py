from time import sleep

from redis.multidb.failure_detector import CommandFailureDetector
from redis.multidb.circuit import State as CBState
from redis.exceptions import ConnectionError


class TestCommandFailureDetector:
    def test_failure_detector_open_circuit_on_threshold_exceed_and_interval_not_exceed(self, mock_db):
        fd = CommandFailureDetector(5, 1, mock_db)
        assert fd.database.circuit.state == CBState.CLOSED

        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert fd.database.circuit.state == CBState.OPEN

    def test_failure_detector_do_not_open_circuit_if_threshold_not_exceed_and_interval_not_exceed(self, mock_db):
        fd = CommandFailureDetector(5, 1, mock_db)
        assert fd.database.circuit.state == CBState.CLOSED

        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert fd.database.circuit.state == CBState.CLOSED

    def test_failure_detector_do_not_open_circuit_on_threshold_exceed_and_interval_exceed(self, mock_db):
        fd = CommandFailureDetector(5, 0.3, mock_db)
        assert fd.database.circuit.state == CBState.CLOSED

        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        sleep(0.1)
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        sleep(0.1)
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        sleep(0.1)
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        sleep(0.1)
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert fd.database.circuit.state == CBState.CLOSED

        # 4 more failure as last one already refreshed timer
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert fd.database.circuit.state == CBState.OPEN

    def test_failure_detector_refresh_timer_on_expired_duration(self, mock_db):
        fd = CommandFailureDetector(5, 0.3, mock_db)
        assert fd.database.circuit.state == CBState.CLOSED

        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        sleep(0.4)

        assert fd.database.circuit.state == CBState.CLOSED

        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert fd.database.circuit.state == CBState.CLOSED
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert fd.database.circuit.state == CBState.OPEN

    def test_failure_detector_open_circuit_on_specific_exception_threshold_exceed(self, mock_db):
        fd = CommandFailureDetector(5, 1, mock_db, error_types=[ConnectionError])
        assert fd.database.circuit.state == CBState.CLOSED

        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(ConnectionError(), ('SET', 'key1', 'value1'))
        fd.register_failure(ConnectionError(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))
        fd.register_failure(Exception(), ('SET', 'key1', 'value1'))

        assert fd.database.circuit.state == CBState.CLOSED

        fd.register_failure(ConnectionError(), ('SET', 'key1', 'value1'))
        fd.register_failure(ConnectionError(), ('SET', 'key1', 'value1'))
        fd.register_failure(ConnectionError(), ('SET', 'key1', 'value1'))

        assert fd.database.circuit.state == CBState.OPEN