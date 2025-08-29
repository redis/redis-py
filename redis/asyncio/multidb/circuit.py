from abc import abstractmethod
from typing import Callable

import pybreaker

from redis.multidb.circuit import CircuitBreaker, State, BaseCircuitBreaker, PBCircuitBreakerAdapter


class AsyncCircuitBreaker(CircuitBreaker):
    """Async implementation of Circuit Breaker interface."""

    @abstractmethod
    async def on_state_changed(self, cb: Callable[["CircuitBreaker", State, State], None]):
        """Callback called when the state of the circuit changes."""
        pass

class AsyncPBCircuitBreakerAdapter(BaseCircuitBreaker, AsyncCircuitBreaker):
    """
    Async adapter for pybreaker's CircuitBreaker implementation.
    """
    def __init__(self, cb: pybreaker.CircuitBreaker):
        super().__init__(cb)
        self._sync_cb = PBCircuitBreakerAdapter(cb)

    async def on_state_changed(self, cb: Callable[["CircuitBreaker", State, State], None]):
        self._sync_cb.on_state_changed(cb)