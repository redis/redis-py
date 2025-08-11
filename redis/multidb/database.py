import redis
from abc import ABC, abstractmethod
from enum import Enum
from typing import Union

from redis import RedisCluster
from redis.data_structure import WeightedList
from redis.multidb.circuit import CircuitBreaker
from redis.typing import Number


class State(Enum):
    ACTIVE = 0
    PASSIVE = 1
    DISCONNECTED = 2

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
    def state(self) -> State:
        """The state of the current database."""
        pass

    @state.setter
    @abstractmethod
    def state(self, state: State):
        """Set the state of the current database."""
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
            weight: float,
            state: State = State.DISCONNECTED,
    ):
        """
        param: client: Client instance for communication with the database.
        param: circuit: Circuit breaker for the current database.
        param: weight: Weight of current database. Database with the highest weight becomes Active.
        param: state: State of the current database.
        """
        self._client = client
        self._cb = circuit
        self._cb.database = self
        self._weight = weight
        self._state = state

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
    def state(self) -> State:
        return self._state

    @state.setter
    def state(self, state: State):
        self._state = state

    @property
    def circuit(self) -> CircuitBreaker:
        return self._cb

    @circuit.setter
    def circuit(self, circuit: CircuitBreaker):
        self._cb = circuit
