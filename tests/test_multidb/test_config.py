from unittest.mock import Mock

from redis.connection import ConnectionPool

from redis import Redis
from redis.multidb.config import MultiDbConfig, DEFAULT_GRACE_PERIOD, DEFAULT_HEALTH_CHECK_INTERVAL, \
    DEFAULT_AUTO_FALLBACK_INTERVAL
from redis.multidb.failure_detector import CommandFailureDetector, FailureDetector
from redis.multidb.healthcheck import EchoHealthCheck, HealthCheck
from redis.multidb.selector import WeightBasedDatabaseSelector, DatabaseSelector


class TestMultiDbConfig:
    def test_default_config(self):
        config = MultiDbConfig()

        assert isinstance(config.client(), Redis)
        assert config.grace_period == DEFAULT_GRACE_PERIOD
        assert len(config.failure_detectors) == 1
        assert isinstance(config.failure_detectors[0], CommandFailureDetector)
        assert len(config.health_checks) == 1
        assert isinstance(config.health_checks[0], EchoHealthCheck)
        assert config.health_check_interval == DEFAULT_HEALTH_CHECK_INTERVAL
        assert isinstance(config.database_selector, WeightBasedDatabaseSelector)
        assert config.auto_fallback_interval == DEFAULT_AUTO_FALLBACK_INTERVAL

    def test_overridden_config(self):
        mock_connection_pool = Mock(spec=ConnectionPool)
        mock_connection_pool.connection_kwargs = {}
        grace_period = 2
        mock_failure_detectors = [Mock(spec=FailureDetector), Mock(spec=FailureDetector)]
        mock_health_checks = [Mock(spec=HealthCheck), Mock(spec=HealthCheck)]
        health_check_interval = 10
        mock_database_selector = Mock(spec=DatabaseSelector)
        auto_fallback_interval = 10

        config = MultiDbConfig(
            client_kwargs={"connection_pool": mock_connection_pool},
            grace_period=grace_period,
            failure_detectors=mock_failure_detectors,
            health_checks=mock_health_checks,
            health_check_interval=health_check_interval,
            database_selector=mock_database_selector,
            auto_fallback_interval=auto_fallback_interval,
        )

        client = config.client()
        assert isinstance(client, Redis)
        assert client.connection_pool == mock_connection_pool
        assert config.grace_period == grace_period
        assert len(config.failure_detectors) == 2
        assert config.failure_detectors[0] == mock_failure_detectors[0]
        assert config.failure_detectors[1] == mock_failure_detectors[1]
        assert len(config.health_checks) == 2
        assert config.health_checks[0] == mock_health_checks[0]
        assert config.health_checks[1] == mock_health_checks[1]
        assert config.health_check_interval == health_check_interval
        assert config.database_selector == mock_database_selector
        assert config.auto_fallback_interval == auto_fallback_interval