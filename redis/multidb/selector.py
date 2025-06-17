from abc import ABC, abstractmethod

from redis.data_structure import WeightedList
from redis.multidb.database import AbstractDatabase
from redis.multidb.circuit import State as CBState
from redis.multidb.exception import NoValidDatabaseException
from redis.retry import Retry


class DatabaseSelector(ABC):

    @property
    @abstractmethod
    def database(self) -> AbstractDatabase:
        """Select the database."""
        pass

    @abstractmethod
    def add_database(self, database: AbstractDatabase) -> None:
        """Add the database."""
        pass


class WeightBasedDatabaseSelector(DatabaseSelector):
    """
    Choose the active database with the highest weight.
    """
    def __init__(
            self,
            retry: Retry,
    ):
        self._retry = retry
        self._retry.update_supported_errors([NoValidDatabaseException])
        self._databases = WeightedList()

    @property
    def database(self) -> AbstractDatabase:
        return self._retry.call_with_retry(
            lambda: self._get_active_database(),
            lambda _: self._dummy_fail()
        )

    def add_database(self, database: AbstractDatabase) -> None:
        self._databases.add(database, database.weight)

    def _get_active_database(self) -> AbstractDatabase:
        for database, _ in self._databases:
            if database.circuit.state == CBState.CLOSED:
                return database
            else:
                continue

        raise NoValidDatabaseException('No valid database available for communication')

    def _dummy_fail(self):
        pass
