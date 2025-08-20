import redis
from abc import ABC, abstractmethod
from enum import Enum
from typing import Union

from redis import RedisCluster
from redis.data_structure import WeightedList
from redis.multidb.circuit import CircuitBreaker
from redis.typing import Number

class AbstractDatabase(ABC):
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
    def circuit(self) -> CircuitBreaker:
        """Circuit breaker for the current database."""
        pass

    @circuit.setter
    @abstractmethod
    def circuit(self, circuit: CircuitBreaker):
        """Set the circuit breaker for the current database."""
        pass

Databases = WeightedList[tuple[AbstractDatabase, Number]]

class Database(AbstractDatabase):
    def __init__(
            self,
            client: Union[redis.Redis, RedisCluster],
            circuit: CircuitBreaker,
            weight: float
    ):
        """
        Initialize a new Database instance.

        Args:
            client: Underlying Redis client instance for database operations
            circuit: Circuit breaker for handling database failures
            weight: Weight value used for database failover prioritization
            state: Initial database state, defaults to DISCONNECTED
        """
        self._client = client
        self._cb = circuit
        self._cb.database = self
        self._weight = weight

    @property
    def client(self) -> Union[redis.Redis, RedisCluster]:
        return self._client

    @client.setter
    def client(self, client: Union[redis.Redis, RedisCluster]):
        self._client = client

    @property
    def weight(self) -> float:
        return self._weight

    @weight.setter
    def weight(self, weight: float):
        self._weight = weight

    @property
    def circuit(self) -> CircuitBreaker:
        return self._cb

    @circuit.setter
    def circuit(self, circuit: CircuitBreaker):
        self._cb = circuit
