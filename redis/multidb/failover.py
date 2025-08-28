from abc import ABC, abstractmethod

from redis.data_structure import WeightedList
from redis.multidb.database import Databases, SyncDatabase
from redis.multidb.circuit import State as CBState
from redis.multidb.exception import NoValidDatabaseException
from redis.retry import Retry
from redis.utils import dummy_fail


class FailoverStrategy(ABC):

    @property
    @abstractmethod
    def database(self) -> SyncDatabase:
        """Select the database according to the strategy."""
        pass

    @abstractmethod
    def set_databases(self, databases: Databases) -> None:
        """Set the database strategy operates on."""
        pass

class WeightBasedFailoverStrategy(FailoverStrategy):
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
    def database(self) -> SyncDatabase:
        return self._retry.call_with_retry(
            lambda: self._get_active_database(),
            lambda _: dummy_fail()
        )

    def set_databases(self, databases: Databases) -> None:
        self._databases = databases

    def _get_active_database(self) -> SyncDatabase:
        for database, _ in self._databases:
            if database.circuit.state == CBState.CLOSED:
                return database

        raise NoValidDatabaseException('No valid database available for communication')
