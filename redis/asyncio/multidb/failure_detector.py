from abc import ABC, abstractmethod

from redis.multidb.failure_detector import FailureDetector

DEFAULT_FAILURES_THRESHOLD = 1000
DEFAULT_FAILURES_DURATION = 2

class AsyncFailureDetector(ABC):

    @abstractmethod
    async def register_failure(self, exception: Exception, cmd: tuple) -> None:
        """Register a failure that occurred during command execution."""
        pass

    @abstractmethod
    def set_command_executor(self, command_executor) -> None:
        """Set the command executor for this failure."""
        pass

class FailureDetectorAsyncWrapper(AsyncFailureDetector):
    """
    Async wrapper for the failure detector.
    """
    def __init__(self, failure_detector: FailureDetector) -> None:
        self._failure_detector = failure_detector

    async def register_failure(self, exception: Exception, cmd: tuple) -> None:
        self._failure_detector.register_failure(exception, cmd)

    def set_command_executor(self, command_executor) -> None:
        self._failure_detector.set_command_executor(command_executor)