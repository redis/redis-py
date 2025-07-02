import pybreaker
import pytest

from redis.multidb.circuit import PBCircuitBreakerAdapter, State as CbState, CircuitBreaker


class TestPBCircuitBreaker:
    @pytest.mark.parametrize(
        'mock_db',
        [
            {'weight': 0.7, 'circuit': {'state': CbState.CLOSED}},
        ],
        indirect=True,
    )
    def test_cb_correctly_configured(self, mock_db):
        pb_circuit = pybreaker.CircuitBreaker(reset_timeout=5)
        adapter = PBCircuitBreakerAdapter(cb=pb_circuit)
        assert adapter.state == CbState.CLOSED

        adapter.state = CbState.OPEN
        assert adapter.state == CbState.OPEN

        adapter.state = CbState.HALF_OPEN
        assert adapter.state == CbState.HALF_OPEN

        adapter.state = CbState.CLOSED
        assert adapter.state == CbState.CLOSED

        assert adapter.grace_period == 5
        adapter.grace_period = 10

        assert adapter.grace_period == 10

        adapter.database = mock_db
        assert adapter.database == mock_db

    def test_cb_executes_callback_on_state_changed(self):
        pb_circuit = pybreaker.CircuitBreaker(reset_timeout=5)
        adapter = PBCircuitBreakerAdapter(cb=pb_circuit)
        called_count = 0

        def callback(cb: CircuitBreaker, old_state: CbState, new_state: CbState):
            nonlocal called_count
            assert old_state == CbState.CLOSED
            assert new_state == CbState.HALF_OPEN
            assert isinstance(cb, PBCircuitBreakerAdapter)
            called_count += 1

        adapter.on_state_changed(callback)
        adapter.state = CbState.HALF_OPEN

        assert called_count == 1