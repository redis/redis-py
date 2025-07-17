from dataclasses import dataclass, field
from typing import List, Type, Union

import pybreaker
from typing_extensions import Optional

from redis import Redis, Sentinel
from redis.asyncio import RedisCluster
from redis.backoff import ExponentialWithJitterBackoff, AbstractBackoff
from redis.data_structure import WeightedList
from redis.event import EventDispatcher, EventDispatcherInterface
from redis.multidb.circuit import CircuitBreaker, PBCircuitBreakerAdapter
from redis.multidb.database import Database, Databases
from redis.multidb.failure_detector import FailureDetector, CommandFailureDetector
from redis.multidb.healthcheck import HealthCheck, EchoHealthCheck
from redis.multidb.failover import FailoverStrategy, WeightBasedFailoverStrategy
from redis.retry import Retry

DEFAULT_GRACE_PERIOD = 5.0
DEFAULT_HEALTH_CHECK_INTERVAL = 5
DEFAULT_HEALTH_CHECK_RETRIES = 3
DEFAULT_HEALTH_CHECK_BACKOFF = ExponentialWithJitterBackoff(cap=10)
DEFAULT_FAILURES_THRESHOLD = 100
DEFAULT_FAILURES_DURATION = 2
DEFAULT_FAILOVER_RETRIES = 3
DEFAULT_FAILOVER_BACKOFF = ExponentialWithJitterBackoff(cap=3)
DEFAULT_AUTO_FALLBACK_INTERVAL = -1

def default_event_dispatcher() -> EventDispatcherInterface:
    return EventDispatcher()

@dataclass
class DatabaseConfig:
    weight: float
    client_kwargs: dict = field(default_factory=dict)
    circuit: Optional[CircuitBreaker] = None
    grace_period: float = DEFAULT_GRACE_PERIOD

    def default_circuit_breaker(self) -> CircuitBreaker:
        circuit_breaker = pybreaker.CircuitBreaker(reset_timeout=self.grace_period)
        return PBCircuitBreakerAdapter(circuit_breaker)

@dataclass
class MultiDbConfig:
    databases_config: List[DatabaseConfig]
    client_class: Type[Union[Redis, RedisCluster, Sentinel]] = Redis
    failure_detectors: Optional[List[FailureDetector]] = None
    failure_threshold: int = DEFAULT_FAILURES_THRESHOLD
    failures_interval: float = DEFAULT_FAILURES_DURATION
    health_checks: Optional[List[HealthCheck]] = None
    health_check_interval: float = DEFAULT_HEALTH_CHECK_INTERVAL
    health_check_retries: int = DEFAULT_HEALTH_CHECK_RETRIES
    health_check_backoff: AbstractBackoff = DEFAULT_HEALTH_CHECK_BACKOFF
    failover_strategy: Optional[FailoverStrategy] = None
    failover_retries: int = DEFAULT_FAILOVER_RETRIES
    failover_backoff: AbstractBackoff = DEFAULT_FAILOVER_BACKOFF
    auto_fallback_interval: float = DEFAULT_AUTO_FALLBACK_INTERVAL
    event_dispatcher: EventDispatcherInterface = field(default_factory=default_event_dispatcher)

    def databases(self) -> Databases:
        databases = WeightedList()

        for database_config in self.databases_config:
            client = self.client_class(**database_config.client_kwargs)
            circuit = database_config.default_circuit_breaker() \
                if database_config.circuit is None else database_config.circuit
            databases.add(
                Database(client=client, circuit=circuit, weight=database_config.weight),
                database_config.weight
            )

        return databases

    def default_failure_detectors(self) -> List[FailureDetector]:
        return [
            CommandFailureDetector(threshold=self.failure_threshold, duration=self.failures_interval),
        ]

    def default_health_checks(self) -> List[HealthCheck]:
        return [
            EchoHealthCheck(retry=Retry(retries=self.health_check_retries, backoff=self.health_check_backoff)),
        ]

    def default_failover_strategy(self) -> FailoverStrategy:
        return WeightBasedFailoverStrategy(
            retry=Retry(retries=self.failover_retries, backoff=self.failover_backoff),
        )
