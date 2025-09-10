import logging
from abc import ABC, abstractmethod

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialWithJitterBackoff
from redis.utils import dummy_fail_async

DEFAULT_HEALTH_CHECK_RETRIES = 3
DEFAULT_HEALTH_CHECK_BACKOFF = ExponentialWithJitterBackoff(cap=10)

logger = logging.getLogger(__name__)

class HealthCheck(ABC):

    @property
    @abstractmethod
    def retry(self) -> Retry:
        """The retry object to use for health checks."""
        pass

    @abstractmethod
    async def check_health(self, database) -> bool:
        """Function to determine the health status."""
        pass

class AbstractHealthCheck(HealthCheck):
    def __init__(
            self,
            retry: Retry = Retry(retries=DEFAULT_HEALTH_CHECK_RETRIES, backoff=DEFAULT_HEALTH_CHECK_BACKOFF)
    ) -> None:
        self._retry = retry
        self._retry.update_supported_errors([ConnectionRefusedError])

    @property
    def retry(self) -> Retry:
        return self._retry

    @abstractmethod
    async def check_health(self, database) -> bool:
        pass

class EchoHealthCheck(AbstractHealthCheck):
    def __init__(
        self,
        retry: Retry = Retry(retries=DEFAULT_HEALTH_CHECK_RETRIES, backoff=DEFAULT_HEALTH_CHECK_BACKOFF)
    ) -> None:
        """
        Check database healthiness by sending an echo request.
        """
        super().__init__(
            retry=retry,
        )
    async def check_health(self, database) -> bool:
        return await self._retry.call_with_retry(
            lambda: self._returns_echoed_message(database),
            lambda _: dummy_fail_async()
        )

    async def _returns_echoed_message(self, database) -> bool:
        expected_message = ["healthcheck", b"healthcheck"]

        if isinstance(database.client, Redis):
            actual_message = await database.client.execute_command("ECHO", "healthcheck")
            return actual_message in expected_message
        else:
            # For a cluster checks if all nodes are healthy.
            all_nodes = database.client.get_nodes()
            for node in all_nodes:
                actual_message = await node.redis_connection.execute_command("ECHO", "healthcheck")

                if actual_message not in expected_message:
                    return False

            return True