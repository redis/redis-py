import socket
from abc import abstractmethod, ABC
from redis.exceptions import ConnectionError, TimeoutError

from redis.retry import Retry
from redis.multidb.circuit import State as CBState


class HealthCheck(ABC):

    @property
    @abstractmethod
    def retry(self) -> Retry:
        """The retry object to use for health checks."""
        pass

    @abstractmethod
    def check_health(self, database) -> bool:
        """Function to determine the health status."""
        pass

class AbstractHealthCheck(HealthCheck):
    def __init__(
            self,
            retry: Retry,
    ) -> None:
        self._retry = retry

    @property
    def retry(self) -> Retry:
        return self._retry

    @abstractmethod
    def check_health(self, database) -> bool:
        pass


class EchoHealthCheck(AbstractHealthCheck):
    def __init__(
            self,
            retry: Retry,
    ) -> None:
        """
        Check database healthiness by sending an echo request.
        """
        super().__init__(
            retry=retry,
        )
    def check_health(self, database) -> bool:
        try:
            is_healthy = self._retry.call_with_retry(
                lambda : self._returns_echoed_message(database),
                lambda _: self._dummy_fail()
            )

            if not is_healthy:
                database.circuit.state = CBState.OPEN
            elif is_healthy and database.circuit.state != CBState.CLOSED:
                database.circuit.state = CBState.CLOSED

            return is_healthy
        except (ConnectionError, TimeoutError, socket.timeout):
            database.circuit.state = CBState.OPEN
            return False

    def _returns_echoed_message(self, database) -> bool:
        expected_message = "healthcheck"
        actual_message = database.client.execute_command('ECHO', expected_message)
        return actual_message == expected_message

    def _dummy_fail(self):
        pass