import threading
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Type

from typing_extensions import Optional

from redis.multidb.circuit import State as CBState


class FailureDetector(ABC):

    @abstractmethod
    def register_failure(self, exception: Exception, cmd: tuple) -> None:
        """Register a failure that occurred during command execution."""
        pass

    @abstractmethod
    def set_command_executor(self, command_executor) -> None:
        """Set the command executor for this failure."""
        pass

class CommandFailureDetector(FailureDetector):
    """
    Detects a failure based on a threshold of failed commands during a specific period of time.
    """

    def __init__(
            self,
            threshold: int,
            duration: float,
            error_types: Optional[List[Type[Exception]]] = None,
    ) -> None:
        """
        Initialize a new CommandFailureDetector instance.

        Args:
            threshold: The number of failures that must occur within the duration to trigger failure detection.
            duration: The time window in seconds during which failures are counted.
            error_types: Optional list of exception types to trigger failover. If None, all exceptions are counted.

        The detector tracks command failures within a sliding time window. When the number of failures
        exceeds the threshold within the specified duration, it triggers failure detection.
        """
        self._command_executor = None
        self._threshold = threshold
        self._duration = duration
        self._error_types = error_types
        self._start_time: datetime = datetime.now()
        self._end_time: datetime = self._start_time + timedelta(seconds=self._duration)
        self._failures_within_duration: List[tuple[datetime, tuple]] = []
        self._lock = threading.RLock()

    def register_failure(self, exception: Exception, cmd: tuple) -> None:
        failure_time = datetime.now()

        if not self._start_time < failure_time < self._end_time:
            self._reset()

        with self._lock:
            if self._error_types:
                if type(exception) in self._error_types:
                    self._failures_within_duration.append((datetime.now(), cmd))
            else:
                self._failures_within_duration.append((datetime.now(), cmd))

        self._check_threshold()

    def set_command_executor(self, command_executor) -> None:
        self._command_executor = command_executor

    def _check_threshold(self):
        with self._lock:
            if len(self._failures_within_duration) >= self._threshold:
                self._command_executor.active_database.circuit.state = CBState.OPEN
                self._reset()

    def _reset(self) -> None:
        with self._lock:
            self._start_time = datetime.now()
            self._end_time = self._start_time + timedelta(seconds=self._duration)
            self._failures_within_duration = []