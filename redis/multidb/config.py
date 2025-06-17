from dataclasses import dataclass, field
from typing import List, Type, Union

from redis import Redis, Sentinel
from redis.asyncio import RedisCluster
from redis.backoff import ExponentialWithJitterBackoff
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

@dataclass
class MultiDbConfig:
    client_class: Type[Union[Redis, RedisCluster, Sentinel]] = Redis
    client_kwargs: dict = field(default_factory=dict)
    grace_period: int = DEFAULT_GRACE_PERIOD
    failure_detectors: List[FailureDetector] = field(default_factory=default_failure_detectors)
    health_checks: List[HealthCheck] = field(default_factory=default_health_checks)
    health_check_interval: int = DEFAULT_HEALTH_CHECK_INTERVAL
    database_selector: DatabaseSelector = field(default_factory=default_database_selector)
    auto_fallback_interval: int = DEFAULT_AUTO_FALLBACK_INTERVAL

    def client(self) -> Union[Redis, RedisCluster, Sentinel]:
        if len(self.client_kwargs) > 0:
            return self.client_class(**self.client_kwargs)

        return self.client_class()


