import redis
from abc import ABC, abstractmethod
from enum import Enum
from typing import Union, Optional

from redis import RedisCluster
from redis.data_structure import WeightedList
from redis.multidb.circuit import SyncCircuitBreaker
from redis.typing import Number

class AbstractDatabase(ABC):
    @property
    @abstractmethod
    def weight(self) -> float:
        """The weight of this database in compare to others. Used to determine the database failover to."""
        pass

    @weight.setter
    @abstractmethod
    def weight(self, weight: float):
        """Set the weight of this database in compare to others."""
        pass

    @property
    @abstractmethod
    def health_check_url(self) -> Optional[str]:
        """Health check URL associated with the current database."""
        pass

    @health_check_url.setter
    @abstractmethod
    def health_check_url(self, health_check_url: Optional[str]):
        """Set the health check URL associated with the current database."""
        pass

class BaseDatabase(AbstractDatabase):
    def __init__(
            self,
            weight: float,
            health_check_url: Optional[str] = None,
    ):
        self._weight = weight
        self._health_check_url = health_check_url

    @property
    def weight(self) -> float:
        return self._weight

    @weight.setter
    def weight(self, weight: float):
        self._weight = weight

    @property
    def health_check_url(self) -> Optional[str]:
        return self._health_check_url

    @health_check_url.setter
    def health_check_url(self, health_check_url: Optional[str]):
        self._health_check_url = health_check_url

class SyncDatabase(AbstractDatabase):
    """Database with an underlying synchronous redis client."""
    @property
    @abstractmethod
    def client(self) -> Union[redis.Redis, RedisCluster]:
        """The underlying redis client."""
        pass

    @client.setter
    @abstractmethod
    def client(self, client: Union[redis.Redis, RedisCluster]):
        """Set the underlying redis client."""
        pass

    @property
    @abstractmethod
    def circuit(self) -> SyncCircuitBreaker:
        """Circuit breaker for the current database."""
        pass

    @circuit.setter
    @abstractmethod
    def circuit(self, circuit: SyncCircuitBreaker):
        """Set the circuit breaker for the current database."""
        pass

Databases = WeightedList[tuple[SyncDatabase, Number]]

class Database(BaseDatabase, SyncDatabase):
    def __init__(
            self,
            client: Union[redis.Redis, RedisCluster],
            circuit: SyncCircuitBreaker,
            weight: float,
            health_check_url: Optional[str] = None,
    ):
        """
        Initialize a new Database instance.

        Args:
            client: Underlying Redis client instance for database operations
            circuit: Circuit breaker for handling database failures
            weight: Weight value used for database failover prioritization
            health_check_url: Health check URL associated with the current database
        """
        self._client = client
        self._cb = circuit
        self._cb.database = self
        super().__init__(weight, health_check_url)

    @property
    def client(self) -> Union[redis.Redis, RedisCluster]:
        return self._client

    @client.setter
    def client(self, client: Union[redis.Redis, RedisCluster]):
        self._client = client

    @property
    def circuit(self) -> SyncCircuitBreaker:
        return self._cb

    @circuit.setter
    def circuit(self, circuit: SyncCircuitBreaker):
        self._cb = circuit