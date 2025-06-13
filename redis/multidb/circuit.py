from abc import abstractmethod, ABC
from enum import Enum
from typing import Callable

import pybreaker

class State(Enum):
    CLOSED = 'closed'
    OPEN = 'open'
    HALF_OPEN = 'half-open'

class CircuitBreaker(ABC):
    @property
    @abstractmethod
    def grace_period(self) -> float:
        """The grace period in seconds when the circle should be kept open."""
        pass

    @property
    @abstractmethod
    def state(self) -> State:
        """The current state of the circuit."""
        pass

    @state.setter
    @abstractmethod
    def state(self, state: State):
        """Set current state of the circuit."""
        pass

    @abstractmethod
    def on_state_changed(self, cb: Callable[["CircuitBreaker", State, State], None]):
        """Callback called when the state of the circuit changes."""
        pass

class PBListener(pybreaker.CircuitBreakerListener):
    def __init__(
            self,
            cb: Callable[[CircuitBreaker, State, State], None]
    ):
        """Wrapper for callback to be compatible with pybreaker implementation."""
        self._cb = cb

    def state_change(self, cb, old_state, new_state):
        cb = PBCircuitBreakerAdapter(cb)
        old_state = State(value=old_state.name)
        new_state = State(value=new_state.name)
        self._cb(cb, old_state, new_state)


class PBCircuitBreakerAdapter(CircuitBreaker):
    def __init__(self, cb: pybreaker.CircuitBreaker):
        """Adapter for pybreaker CircuitBreaker."""
        self._cb = cb
        self._state_pb_mapper = {
            State.CLOSED: self._cb.close,
            State.OPEN: self._cb.open,
            State.HALF_OPEN: self._cb.half_open,
        }

    @property
    def grace_period(self) -> float:
        return self._cb.reset_timeout

    @grace_period.setter
    def grace_period(self, grace_period: float):
        self._cb.reset_timeout = grace_period

    @property
    def state(self) -> State:
        return State(value=self._cb.state.name)

    @state.setter
    def state(self, state: State):
        self._state_pb_mapper[state]()

    def on_state_changed(self, cb: Callable[["CircuitBreaker", State, State], None]):
        listener = PBListener(cb)
        self._cb.add_listener(listener)