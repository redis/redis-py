from abc import abstractmethod, ABC

from redis.backoff import AbstractBackoff
from redis.retry import Retry
from redis.multidb.circuit import State as CBState


class HealthCheck(ABC):
    @property
    @abstractmethod
    def check_interval(self) -> float:
        """The health check interval in seconds."""
        pass

    @property
    @abstractmethod
    def num_retries(self) -> int:
        """The number of times to retry the health check."""
        pass

    @property
    @abstractmethod
    def backoff(self) -> AbstractBackoff:
        """The backoff strategy for the health check."""
        pass

    @abstractmethod
    def check_health(self, database) -> bool:
        """Function to determine the health status."""
        pass

class AbstractHealthCheck(HealthCheck):
    def __init__(
            self,
            check_interval: float,
            num_retries: int,
            backoff: AbstractBackoff
    ) -> None:
        self._check_interval = check_interval
        self._num_retries = num_retries
        self._backoff = backoff
        self._retry = Retry(self._backoff, self._num_retries)

    @property
    def check_interval(self) -> float:
        return self._check_interval

    @property
    def num_retries(self) -> int:
        return self._num_retries

    @property
    def backoff(self) -> AbstractBackoff:
        return self._backoff

    @abstractmethod
    def check_health(self, database) -> bool:
        pass


class EchoHealthCheck(AbstractHealthCheck):
    def __init__(
            self,
            check_interval: float,
            num_retries: int,
            backoff: AbstractBackoff,
    ) -> None:
        """
        Check database healthiness by sending an echo request.
        """
        super().__init__(
            check_interval=check_interval,
            num_retries=num_retries,
            backoff=backoff,
        )
    def check_health(self, database) -> bool:
        try:
            return self._retry.call_with_retry(
                lambda : self._returns_echoed_message(database),
                lambda _: self.dummy_fail()
            )
        except Exception:
            database.circuit.state = CBState.OPEN
            return False

    def _returns_echoed_message(self, database) -> bool:
        expected_message = "healthcheck"
        actual_message = database.client.execute_command('ECHO', expected_message)
        return actual_message == expected_message

    def dummy_fail(self):
        pass