from unittest.mock import Mock

import pytest

from redis._defaults import DEFAULT_RETRY_COUNT
from redis.backoff import NoBackoff
from redis.connection import ConnectionPool
from redis.maint_notifications import MaintNotificationsConfig
from redis.multidb.circuit import (
    PBCircuitBreakerAdapter,
    CircuitBreaker,
    DEFAULT_GRACE_PERIOD,
)
from redis.multidb.config import (
    MultiDbConfig,
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_AUTO_FALLBACK_INTERVAL,
    DatabaseConfig,
)
from redis.multidb.database import Database
from redis.multidb.failure_detector import CommandFailureDetector, FailureDetector
from redis.asyncio.multidb.healthcheck import (
    DEFAULT_HEALTH_CHECK_DELAY,
    DEFAULT_HEALTH_CHECK_TIMEOUT,
    HealthCheck,
    PingHealthCheck,
)
from redis.multidb.failover import WeightBasedFailoverStrategy, FailoverStrategy
from redis.retry import Retry


@pytest.mark.fixed_client
class TestMultiDbConfig:
    def test_default_config(self):
        db_configs = [
            DatabaseConfig(
                client_kwargs={"host": "host1", "port": "port1"}, weight=1.0
            ),
            DatabaseConfig(
                client_kwargs={"host": "host2", "port": "port2"}, weight=0.9
            ),
            DatabaseConfig(
                client_kwargs={"host": "host3", "port": "port3"}, weight=0.8
            ),
        ]

        config = MultiDbConfig(databases_config=db_configs)

        assert config.databases_config == db_configs
        databases = config.databases()
        assert len(databases) == 3

        i = 0
        for db, weight in databases:
            assert isinstance(db, Database)
            assert weight == db_configs[i].weight
            assert db.circuit.grace_period == DEFAULT_GRACE_PERIOD
            assert db.client.get_retry() is not config.command_retry
            i += 1

        assert len(config.default_failure_detectors()) == 1
        assert isinstance(config.default_failure_detectors()[0], CommandFailureDetector)
        for db, _ in databases:
            assert len(db.health_checks) == 1
            assert isinstance(db.health_checks[0], PingHealthCheck)
        # Every database owns its own health check instance
        assert len({id(db.health_checks[0]) for db, _ in databases}) == 3
        assert config.health_check_interval == DEFAULT_HEALTH_CHECK_INTERVAL
        assert isinstance(
            config.default_failover_strategy(), WeightBasedFailoverStrategy
        )
        assert config.auto_fallback_interval == DEFAULT_AUTO_FALLBACK_INTERVAL
        assert isinstance(config.command_retry, Retry)

    def test_default_command_retry_is_isolated(self):
        first = MultiDbConfig(databases_config=[])
        second = MultiDbConfig(databases_config=[])

        assert first.command_retry is not second.command_retry
        assert first.command_retry._backoff is not second.command_retry._backoff
        assert first.command_retry == second.command_retry

        first.command_retry.update_retries(0)
        first.command_retry.update_supported_errors((ValueError,))

        for config in (second, MultiDbConfig(databases_config=[])):
            assert config.command_retry.get_retries() == DEFAULT_RETRY_COUNT
            assert ValueError not in config.command_retry._supported_errors

    def test_explicit_command_retry_is_preserved(self):
        retry = Retry(backoff=NoBackoff(), retries=2)
        config = MultiDbConfig(databases_config=[], command_retry=retry)

        assert config.command_retry is retry
        assert config.command_retry.get_retries() == 2

    def test_overridden_config(self):
        grace_period = 2
        mock_connection_pools = [
            Mock(spec=ConnectionPool),
            Mock(spec=ConnectionPool),
            Mock(spec=ConnectionPool),
        ]
        mock_connection_pools[0].connection_kwargs = {}
        mock_connection_pools[1].connection_kwargs = {}
        mock_connection_pools[2].connection_kwargs = {}
        mock_cb1 = Mock(spec=CircuitBreaker)
        mock_cb1.grace_period = grace_period
        mock_cb2 = Mock(spec=CircuitBreaker)
        mock_cb2.grace_period = grace_period
        mock_cb3 = Mock(spec=CircuitBreaker)
        mock_cb3.grace_period = grace_period
        mock_failure_detectors = [
            Mock(spec=FailureDetector),
            Mock(spec=FailureDetector),
        ]
        mock_health_checks = [Mock(spec=HealthCheck), Mock(spec=HealthCheck)]
        factory_health_check = Mock(spec=HealthCheck)
        mock_health_checks_factory = Mock(return_value=[factory_health_check])
        health_check_interval = 10
        mock_failover_strategy = Mock(spec=FailoverStrategy)
        auto_fallback_interval = 10
        db_configs = [
            DatabaseConfig(
                client_kwargs={"connection_pool": mock_connection_pools[0]},
                weight=1.0,
                circuit=mock_cb1,
                health_checks=mock_health_checks,
            ),
            DatabaseConfig(
                client_kwargs={"connection_pool": mock_connection_pools[1]},
                weight=0.9,
                circuit=mock_cb2,
            ),
            DatabaseConfig(
                client_kwargs={"connection_pool": mock_connection_pools[2]},
                weight=0.8,
                circuit=mock_cb3,
            ),
        ]

        config = MultiDbConfig(
            databases_config=db_configs,
            failure_detectors=mock_failure_detectors,
            health_checks_factory=mock_health_checks_factory,
            health_check_interval=health_check_interval,
            failover_strategy=mock_failover_strategy,
            auto_fallback_interval=auto_fallback_interval,
        )

        assert config.databases_config == db_configs
        databases = config.databases()
        assert len(databases) == 3

        i = 0
        for db, weight in databases:
            assert isinstance(db, Database)
            assert weight == db_configs[i].weight
            assert db.client.connection_pool == mock_connection_pools[i]
            assert db.circuit.grace_period == grace_period
            i += 1

        assert len(config.failure_detectors) == 2
        assert config.failure_detectors[0] == mock_failure_detectors[0]
        assert config.failure_detectors[1] == mock_failure_detectors[1]
        # Health checks of a database's own config replace the factory ones,
        # which are built for every other database from its config
        dbs = [db for db, _ in databases]
        assert dbs[0].health_checks == mock_health_checks
        assert dbs[1].health_checks == [factory_health_check]
        assert dbs[2].health_checks == [factory_health_check]
        assert [call.args for call in mock_health_checks_factory.call_args_list] == [
            (db_configs[1],),
            (db_configs[2],),
        ]
        assert config.health_check_interval == health_check_interval
        assert config.failover_strategy == mock_failover_strategy
        assert config.auto_fallback_interval == auto_fallback_interval

    def test_global_values_tune_default_ping_of_every_database(self):
        config = MultiDbConfig(
            databases_config=[
                DatabaseConfig(client_kwargs={"host": "host1", "port": "port1"}),
                DatabaseConfig(client_kwargs={"host": "host2", "port": "port2"}),
            ],
            health_check_probes=7,
            health_check_delay=0.2,
            health_check_timeout=9,
        )

        health_checks = [db.health_checks for db, _ in config.databases()]

        assert len(health_checks) == 2
        for (health_check,) in health_checks:
            assert isinstance(health_check, PingHealthCheck)
            assert health_check.health_check_probes == 7
            assert health_check.health_check_delay == 0.2
            assert health_check.health_check_timeout == 9
        assert health_checks[0][0] is not health_checks[1][0]

    def test_factory_builds_health_checks_of_every_database(self):
        db_configs = [
            DatabaseConfig(client_kwargs={"host": "host1", "port": "port1"}),
            DatabaseConfig(client_kwargs={"host": "host2", "port": "port2"}),
        ]
        built = {}

        def factory(database_config):
            built[id(database_config)] = [PingHealthCheck(health_check_probes=2)]
            return built[id(database_config)]

        config = MultiDbConfig(
            databases_config=db_configs,
            health_checks_factory=factory,
            # Global values belong to the default ping the factory replaces
            health_check_probes=7,
            health_check_delay=0.2,
            health_check_timeout=9,
        )

        health_checks = [db.health_checks for db, _ in config.databases()]

        assert health_checks == [built[id(db_config)] for db_config in db_configs]
        assert health_checks[0][0] is not health_checks[1][0]
        for (health_check,) in health_checks:
            assert health_check.health_check_probes == 2
            assert health_check.health_check_delay == DEFAULT_HEALTH_CHECK_DELAY
            assert health_check.health_check_timeout == DEFAULT_HEALTH_CHECK_TIMEOUT

        # A database added later is built by the factory as well
        new_db_config = DatabaseConfig(client_kwargs={"host": "host3"})
        assert config.database_health_checks(new_db_config) == built[id(new_db_config)]

    def test_database_health_checks_are_not_combined_with_factory(self):
        own_health_checks = [Mock(spec=HealthCheck)]
        factory_health_check = Mock(spec=HealthCheck)
        factory = Mock(return_value=[factory_health_check])
        db_configs = [
            DatabaseConfig(
                client_kwargs={"host": "host1", "port": "port1"},
                health_checks=own_health_checks,
            ),
            DatabaseConfig(client_kwargs={"host": "host2", "port": "port2"}),
        ]
        config = MultiDbConfig(
            databases_config=db_configs, health_checks_factory=factory
        )

        health_checks = [db.health_checks for db, _ in config.databases()]

        assert health_checks == [own_health_checks, [factory_health_check]]
        factory.assert_called_once_with(db_configs[1])

    def test_database_health_checks_replace_default_ping(self):
        own_health_checks = [Mock(spec=HealthCheck)]
        config = MultiDbConfig(
            databases_config=[
                DatabaseConfig(
                    client_kwargs={"host": "host1", "port": "port1"},
                    health_checks=own_health_checks,
                ),
                DatabaseConfig(client_kwargs={"host": "host2", "port": "port2"}),
            ],
            health_check_probes=7,
        )

        own, (default,) = [db.health_checks for db, _ in config.databases()]

        # Only its own health checks, no ping added to them
        assert own == own_health_checks
        # The other database stays on the default ping with the global values
        assert isinstance(default, PingHealthCheck)
        assert default.health_check_probes == 7

    def test_empty_database_health_checks_disable_health_checks(self):
        factory = Mock(return_value=[Mock(spec=HealthCheck)])
        db_configs = [
            DatabaseConfig(
                client_kwargs={"host": "host1", "port": "port1"}, health_checks=[]
            ),
            DatabaseConfig(client_kwargs={"host": "host2", "port": "port2"}),
        ]

        for config in (
            MultiDbConfig(databases_config=db_configs),
            MultiDbConfig(databases_config=db_configs, health_checks_factory=factory),
        ):
            disabled, other = [db.health_checks for db, _ in config.databases()]

            # Neither the factory nor the default ping fills an empty list
            assert disabled == []
            assert len(other) == 1
        factory.assert_called_once_with(db_configs[1])

    def test_database_add_health_check(self):
        health_checks = [Mock(spec=HealthCheck)]
        db = Database(Mock(), Mock(spec=CircuitBreaker), 1.0, health_checks)
        added = Mock(spec=HealthCheck)

        db.add_health_check(added)

        assert db.health_checks == [health_checks[0], added]
        # The list given to the database is not shared with it
        assert health_checks == [health_checks[0]]

    def test_underlying_clients_have_disabled_retry_and_maint_notifications(self):
        """
        Test that underlying clients have retry disabled (0 retries)
        and maintenance notifications disabled.
        """
        db_configs = [
            DatabaseConfig(
                client_kwargs={"host": "host1", "port": "port1"},
                weight=1.0,
            ),
            DatabaseConfig(
                client_kwargs={"host": "host2", "port": "port2"},
                weight=0.9,
            ),
        ]

        config = MultiDbConfig(databases_config=db_configs)
        databases = config.databases()

        assert len(databases) == 2

        for db, weight in databases:
            # Verify retry is disabled (0 retries)
            retry = db.client.get_retry()
            assert retry is not None
            assert retry.get_retries() == 0

            # Verify maint_notifications_config is disabled
            # When maint_notifications_config.enabled is False, the pool handler is None
            pool = db.client.connection_pool
            assert pool._maint_notifications_pool_handler is None

    def test_user_provided_maint_notifications_config_is_respected(self):
        """
        Test that user-provided maint_notifications_config is not overwritten.
        """
        user_maint_config = MaintNotificationsConfig(enabled=True)
        db_configs = [
            DatabaseConfig(
                client_kwargs={
                    "host": "host1",
                    "port": "port1",
                    "protocol": 3,  # Required for maint notifications
                    "maint_notifications_config": user_maint_config,
                },
                weight=1.0,
            ),
        ]

        config = MultiDbConfig(databases_config=db_configs)
        databases = config.databases()

        assert len(databases) == 1

        db, weight = databases[0]
        # Verify user-provided maint_notifications_config is respected
        pool = db.client.connection_pool
        assert pool._maint_notifications_pool_handler is not None
        assert pool._maint_notifications_pool_handler.config.enabled is True

    def test_supplied_pool_keeps_its_own_maint_notifications_config(self):
        """
        The disabled default only applies to clients built from client_kwargs; a
        pool the user supplies is used as it is, and its kwargs are not touched.
        """
        pool = ConnectionPool(host="host1", port=6379, protocol=3)
        db_config = DatabaseConfig(from_pool=pool, weight=1.0)

        config = MultiDbConfig(databases_config=[db_config])
        databases = config.databases()

        db, weight = databases[0]
        assert db.client.connection_pool is pool
        assert "maint_notifications_config" not in db_config.client_kwargs
        # The pool's own RESP3 default ("auto") is left in place
        assert pool.maint_notifications_enabled()


@pytest.mark.fixed_client
class TestDatabaseConfig:
    def test_default_config(self):
        config = DatabaseConfig(
            client_kwargs={"host": "host1", "port": "port1"}, weight=1.0
        )

        assert config.client_kwargs == {"host": "host1", "port": "port1"}
        assert config.weight == 1.0
        assert isinstance(config.default_circuit_breaker(), PBCircuitBreakerAdapter)

    def test_overridden_config(self):
        mock_connection_pool = Mock(spec=ConnectionPool)
        mock_circuit = Mock(spec=CircuitBreaker)

        config = DatabaseConfig(
            client_kwargs={"connection_pool": mock_connection_pool},
            weight=1.0,
            circuit=mock_circuit,
        )

        assert config.client_kwargs == {"connection_pool": mock_connection_pool}
        assert config.weight == 1.0
        assert config.circuit == mock_circuit
