from abc import abstractmethod, ABC

from redis.asyncio.multidb.database import AsyncDatabase, Databases
from redis.multidb.circuit import State as CBState
from redis.asyncio.retry import Retry
from redis.data_structure import WeightedList
from redis.multidb.exception import NoValidDatabaseException
from redis.utils import dummy_fail_async


class AsyncFailoverStrategy(ABC):

    @property
    @abstractmethod
    async def database(self) -> AsyncDatabase:
        """Select the database according to the strategy."""
        pass

    @abstractmethod
    def set_databases(self, databases: Databases) -> None:
        """Set the database strategy operates on."""
        pass

class WeightBasedFailoverStrategy(AsyncFailoverStrategy):
    """
    Failover strategy based on database weights.
    """
    def __init__(
            self,
            retry: Retry
    ):
        self._retry = retry
        self._retry.update_supported_errors([NoValidDatabaseException])
        self._databases = WeightedList()

    @property
    async def database(self) -> AsyncDatabase:
        return await self._retry.call_with_retry(
            lambda: self._get_active_database(),
            lambda _: dummy_fail_async()
        )

    def set_databases(self, databases: Databases) -> None:
        self._databases = databases

    async def _get_active_database(self) -> AsyncDatabase:
        for database, _ in self._databases:
            if database.circuit.state == CBState.CLOSED:
                return database

        raise NoValidDatabaseException('No valid database available for communication')