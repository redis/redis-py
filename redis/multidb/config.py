from dataclasses import dataclass, field
from typing import List, Type, Union

import pybreaker
from typing_extensions import Optional

from redis import Redis, ConnectionPool
from redis.asyncio import RedisCluster
from redis.backoff import ExponentialWithJitterBackoff, AbstractBackoff, NoBackoff
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
DEFAULT_FAILURES_THRESHOLD = 3
DEFAULT_FAILURES_DURATION = 2
DEFAULT_FAILOVER_RETRIES = 3
DEFAULT_FAILOVER_BACKOFF = ExponentialWithJitterBackoff(cap=3)
DEFAULT_AUTO_FALLBACK_INTERVAL = -1

def default_event_dispatcher() -> EventDispatcherInterface:
    return EventDispatcher()

@dataclass
class DatabaseConfig:
    weight: float = 1.0
    client_kwargs: dict = field(default_factory=dict)
    from_url: Optional[str] = None
    from_pool: Optional[ConnectionPool] = None
    circuit: Optional[CircuitBreaker] = None
    grace_period: float = DEFAULT_GRACE_PERIOD

    def default_circuit_breaker(self) -> CircuitBreaker:
        circuit_breaker = pybreaker.CircuitBreaker(reset_timeout=self.grace_period)
        return PBCircuitBreakerAdapter(circuit_breaker)

@dataclass
class MultiDbConfig:
    """
    Configuration class for managing multiple database connections in a resilient and fail-safe manner.

    Attributes:
        databases_config: A list of database configurations.
        client_class: The client class used to manage database connections.
        command_retry: Retry strategy for executing database commands.
        failure_detectors: Optional list of failure detectors for monitoring database failures.
        failure_threshold: Threshold for determining database failure.
        failures_interval: Time interval for tracking database failures.
        health_checks: Optional list of health checks performed on databases.
        health_check_interval: Time interval for executing health checks.
        health_check_retries: Number of retry attempts for performing health checks.
        health_check_backoff: Backoff strategy for health check retries.
        failover_strategy: Optional strategy for handling database failover scenarios.
        failover_retries: Number of retries allowed for failover operations.
        failover_backoff: Backoff strategy for failover retries.
        auto_fallback_interval: Time interval to trigger automatic fallback.
        event_dispatcher: Interface for dispatching events related to database operations.

    Methods:
        databases:
            Retrieves a collection of database clients managed by weighted configurations.
            Initializes database clients based on the provided configuration and removes
            redundant retry objects for lower-level clients to rely on global retry logic.

        default_failure_detectors:
            Returns the default list of failure detectors used to monitor database failures.

        default_health_checks:
            Returns the default list of health checks used to monitor database health
            with specific retry and backoff strategies.

        default_failover_strategy:
            Provides the default failover strategy used for handling failover scenarios
            with defined retry and backoff configurations.
    """
    databases_config: List[DatabaseConfig]
    client_class: Type[Union[Redis, RedisCluster]] = Redis
    command_retry: Retry = Retry(
        backoff=ExponentialWithJitterBackoff(base=1, cap=10), retries=3
    )
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
            # The retry object is not used in the lower level clients, so we can safely remove it.
            # We rely on command_retry in terms of global retries.
            database_config.client_kwargs.update({"retry": Retry(retries=0, backoff=NoBackoff())})

            if database_config.from_url:
                client = self.client_class.from_url(database_config.from_url, **database_config.client_kwargs)
            elif database_config.from_pool:
                database_config.from_pool.set_retry(Retry(retries=0, backoff=NoBackoff()))
                client = self.client_class.from_pool(connection_pool=database_config.from_pool)
            else:
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
