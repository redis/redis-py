from dataclasses import dataclass, field
from typing import List, Type, Union

import pybreaker

from redis import Redis, Sentinel
from redis.asyncio import RedisCluster
from redis.backoff import ExponentialWithJitterBackoff
from redis.multidb.circuit import CircuitBreaker, PBCircuitBreakerAdapter
from redis.multidb.database import Database, AbstractDatabase
from redis.multidb.failure_detector import FailureDetector, CommandFailureDetector
from redis.multidb.healthcheck import HealthCheck, EchoHealthCheck
from redis.multidb.selector import DatabaseSelector, WeightBasedDatabaseSelector
from redis.retry import Retry

DEFAULT_GRACE_PERIOD = 1
DEFAULT_HEALTH_CHECK_INTERVAL = 5
DEFAULT_HEALTH_CHECK_RETRIES = 3
DEFAULT_HEALTH_CHECK_BACKOFF = ExponentialWithJitterBackoff(cap=10)
DEFAULT_FAILURES_THRESHOLD = 100
DEFAULT_FAILURES_DURATION = 2
DEFAULT_DATABASE_SELECTOR_RETRIES = 3
DEFAULT_DATABASE_SELECTOR_BACKOFF = ExponentialWithJitterBackoff(cap=3)
DEFAULT_AUTO_FALLBACK_INTERVAL = -1

def default_health_checks() -> List[HealthCheck]:
    return [
        EchoHealthCheck(retry=Retry(retries=DEFAULT_HEALTH_CHECK_RETRIES, backoff=DEFAULT_HEALTH_CHECK_BACKOFF)),
    ]

def default_failure_detectors() -> List[FailureDetector]:
    return [
        CommandFailureDetector(threshold=DEFAULT_FAILURES_THRESHOLD, duration=DEFAULT_FAILURES_DURATION),
    ]

def default_database_selector() -> DatabaseSelector:
    return WeightBasedDatabaseSelector(
        retry=Retry(retries=DEFAULT_DATABASE_SELECTOR_RETRIES, backoff=DEFAULT_DATABASE_SELECTOR_BACKOFF)
    )

def default_circuit_breaker() -> CircuitBreaker:
    circuit_breaker = pybreaker.CircuitBreaker(reset_timeout=DEFAULT_GRACE_PERIOD)
    return PBCircuitBreakerAdapter(circuit_breaker)

@dataclass
class DatabaseConfig:
    client_kwargs: dict
    weight: float
    circuit: CircuitBreaker = field(default_factory=default_circuit_breaker)

@dataclass
class MultiDbConfig:
    databases_config: List[DatabaseConfig]
    client_class: Type[Union[Redis, RedisCluster, Sentinel]] = Redis
    failure_detectors: List[FailureDetector] = field(default_factory=default_failure_detectors)
    health_checks: List[HealthCheck] = field(default_factory=default_health_checks)
    health_check_interval: int = DEFAULT_HEALTH_CHECK_INTERVAL
    database_selector: DatabaseSelector = field(default_factory=default_database_selector)
    auto_fallback_interval: int = DEFAULT_AUTO_FALLBACK_INTERVAL

    def databases(self) -> List[AbstractDatabase]:
        databases = []

        for database_config in self.databases_config:
            client = self.client_class(**database_config.client_kwargs)
            databases.append(
                Database(client=client, circuit=database_config.circuit, weight=database_config.weight)
            )

        return databases

