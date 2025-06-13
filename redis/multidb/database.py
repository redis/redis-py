import redis
from abc import ABC, abstractmethod
from enum import Enum
from typing import Union, List

from typing_extensions import Optional

from redis import RedisCluster, Sentinel
from redis.multidb.circuit import CircuitBreaker, State as CBState
from redis.multidb.healthcheck import HealthCheck, AbstractHealthCheck


class State(Enum):
    ACTIVE = 0
    PASSIVE = 1
    DISCONNECTED = 2

class AbstractDatabase(ABC):
    @property
    @abstractmethod
    def client(self) -> Union[redis.Redis, RedisCluster, Sentinel]:
        """The underlying redis client."""
        pass

    @property
    @abstractmethod
    def weight(self) -> float:
        """The weight of this database in compare to others. Used to determine the database failover to."""
        pass

    @property
    @abstractmethod
    def state(self) -> State:
        """The state of the current database."""
        pass

    @property
    @abstractmethod
    def circuit(self) -> CircuitBreaker:
        """Circuit breaker for the current database."""
        pass

    @abstractmethod
    def add_health_check(self, health_check: HealthCheck) -> None:
        """Adds a new healthcheck to the current database."""
        pass

    @abstractmethod
    def is_healthy(self) -> bool:
        """Checks if the current database is healthy."""
        pass

class Database(AbstractDatabase):
    def __init__(
            self,
            client: Union[redis.Redis, RedisCluster, Sentinel],
            cb: CircuitBreaker,
            weight: float,
            state: State,
            health_checks: Optional[List[HealthCheck]] = None,
    ):
        """
        param: client: Client instance for communication with the database.
        param: cb: Circuit breaker for the current database.
        param: weight: Weight of current database. Database with the highest weight becomes Active.
        param: state: State of the current database.
        param: health_checks: List of health cheks to determine if the current database is healthy.
        """
        self._client = client
        self._cb = cb
        self._weight = weight
        self._state = state
        self._health_checks = health_checks or []

    @property
    def client(self) -> Union[redis.Redis, RedisCluster, Sentinel]:
        return self._client

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

    def add_health_check(self, health_check: HealthCheck) -> None:
        self._health_checks.append(health_check)

    def is_healthy(self) -> bool:
        is_healthy = True

        for health_check in self._health_checks:
            is_healthy = health_check.check_health(self)

            if not is_healthy:
                self._cb.state = CBState.OPEN
                break

        return is_healthy
