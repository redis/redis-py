import threading
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Type

from typing_extensions import Optional

from redis.multidb.circuit import State as CBState

class FailureDetector(ABC):

    @abstractmethod
    def register_failure(self, database, exception: Exception, cmd: tuple) -> None:
        """Register a failure that occurred during command execution."""
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
        :param threshold: Threshold of failed commands over the duration after which database will be marked as failed.
        :param duration: Interval in seconds after which database will be marked as failed if threshold was exceeded.
        :param error_types: List of exception that has to be registered. By default, all exceptions are registered.
        """
        self._threshold = threshold
        self._duration = duration
        self._error_types = error_types
        self._start_time: datetime = datetime.now()
        self._end_time: datetime = self._start_time + timedelta(seconds=self._duration)
        self._failures_within_duration: List[tuple[datetime, tuple]] = []
        self._lock = threading.RLock()

    def register_failure(self, database, exception: Exception, cmd: tuple) -> None:
        failure_time = datetime.now()

        if not self._start_time < failure_time < self._end_time:
            self._reset()

        with self._lock:
            if self._error_types:
                if type(exception) in self._error_types:
                    self._failures_within_duration.append((datetime.now(), cmd))
            else:
                self._failures_within_duration.append((datetime.now(), cmd))

        self._check_threshold(database)

    def _check_threshold(self, database):
        if len(self._failures_within_duration) >= self._threshold:
            database.circuit.state = CBState.OPEN
            self._reset()

    def _reset(self) -> None:
        self._start_time = datetime.now()
        self._end_time = self._start_time + timedelta(seconds=self._duration)
        self._failures_within_duration = []