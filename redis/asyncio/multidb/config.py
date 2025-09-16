from dataclasses import dataclass, field
from typing import Optional, List, Type, Union

import pybreaker

from redis.asyncio import ConnectionPool, Redis, RedisCluster
from redis.asyncio.multidb.database import Databases, Database
from redis.asyncio.multidb.failover import AsyncFailoverStrategy, WeightBasedFailoverStrategy, DEFAULT_FAILOVER_DELAY, \
    DEFAULT_FAILOVER_ATTEMPTS
from redis.asyncio.multidb.failure_detector import AsyncFailureDetector, FailureDetectorAsyncWrapper, \
    DEFAULT_FAILURES_THRESHOLD, DEFAULT_FAILURES_DURATION
from redis.asyncio.multidb.healthcheck import HealthCheck, EchoHealthCheck, DEFAULT_HEALTH_CHECK_INTERVAL, \
    DEFAULT_HEALTH_CHECK_PROBES, DEFAULT_HEALTH_CHECK_DELAY, HealthCheckPolicies, DEFAULT_HEALTH_CHECK_POLICY
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialWithJitterBackoff, NoBackoff
from redis.data_structure import WeightedList
from redis.event import EventDispatcherInterface, EventDispatcher
from redis.multidb.circuit import CircuitBreaker, PBCircuitBreakerAdapter, DEFAULT_GRACE_PERIOD
from redis.multidb.failure_detector import CommandFailureDetector

DEFAULT_AUTO_FALLBACK_INTERVAL = 120

def default_event_dispatcher() -> EventDispatcherInterface:
    return EventDispatcher()

@dataclass
class DatabaseConfig:
    """
    Dataclass representing the configuration for a database connection.

    This class is used to store configuration settings for a database connection,
    including client options, connection sourcing details, circuit breaker settings,
    and cluster-specific properties. It provides a structure for defining these
    attributes and allows for the creation of customized configurations for various
    database setups.

    Attributes:
        weight (float): Weight of the database to define the active one.
        client_kwargs (dict): Additional parameters for the database client connection.
        from_url (Optional[str]): Redis URL way of connecting to the database.
        from_pool (Optional[ConnectionPool]): A pre-configured connection pool to use.
        circuit (Optional[CircuitBreaker]): Custom circuit breaker implementation.
        grace_period (float): Grace period after which we need to check if the circuit could be closed again.
        health_check_url (Optional[str]): URL for health checks. Cluster FQDN is typically used
            on public Redis Enterprise endpoints.

    Methods:
        default_circuit_breaker:
            Generates and returns a default CircuitBreaker instance adapted for use.
    """
    weight: float = 1.0
    client_kwargs: dict = field(default_factory=dict)
    from_url: Optional[str] = None
    from_pool: Optional[ConnectionPool] = None
    circuit: Optional[CircuitBreaker] = None
    grace_period: float = DEFAULT_GRACE_PERIOD
    health_check_url: Optional[str] = None

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
        failure_detectors: Optional list of additional failure detectors for monitoring database failures.
        failure_threshold: Threshold for determining database failure.
        failures_interval: Time interval for tracking database failures.
        health_checks: Optional list of additional health checks performed on databases.
        health_check_interval: Time interval for executing health checks.
        health_check_probes: Number of attempts to evaluate the health of a database.
        health_check_delay: Delay between health check attempts.
        failover_strategy: Optional strategy for handling database failover scenarios.
        failover_attempts: Number of retries allowed for failover operations.
        failover_delay: Delay between failover attempts.
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
    failure_detectors: Optional[List[AsyncFailureDetector]] = None
    failure_threshold: int = DEFAULT_FAILURES_THRESHOLD
    failures_interval: float = DEFAULT_FAILURES_DURATION
    health_checks: Optional[List[HealthCheck]] = None
    health_check_interval: float = DEFAULT_HEALTH_CHECK_INTERVAL
    health_check_probes: int = DEFAULT_HEALTH_CHECK_PROBES
    health_check_delay: float = DEFAULT_HEALTH_CHECK_DELAY
    health_check_policy: HealthCheckPolicies = DEFAULT_HEALTH_CHECK_POLICY
    failover_strategy: Optional[AsyncFailoverStrategy] = None
    failover_attempts: int = DEFAULT_FAILOVER_ATTEMPTS
    failover_delay: float = DEFAULT_FAILOVER_DELAY
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
                Database(
                    client=client,
                    circuit=circuit,
                    weight=database_config.weight,
                    health_check_url=database_config.health_check_url
                ),
                database_config.weight
            )

        return databases

    def default_failure_detectors(self) -> List[AsyncFailureDetector]:
        return [
            FailureDetectorAsyncWrapper(
                CommandFailureDetector(threshold=self.failure_threshold, duration=self.failures_interval)
            ),
        ]

    def default_health_checks(self) -> List[HealthCheck]:
        return [
            EchoHealthCheck(),
        ]

    def default_failover_strategy(self) -> AsyncFailoverStrategy:
        return WeightBasedFailoverStrategy()