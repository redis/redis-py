import threading
from time import sleep
from unittest.mock import patch, Mock

import pybreaker
import pytest

from redis.client import Pipeline
from redis.multidb.circuit import State as CBState, PBCircuitBreakerAdapter
from redis.multidb.client import MultiDBClient
from redis.multidb.failover import (
    WeightBasedFailoverStrategy,
)
from redis.multidb.healthcheck import EchoHealthCheck
from tests.test_multidb.conftest import create_weighted_list


def mock_pipe() -> Pipeline:
    mock_pipe = Mock(spec=Pipeline)
    mock_pipe.__enter__ = Mock(return_value=mock_pipe)
    mock_pipe.__exit__ = Mock(return_value=None)
    return mock_pipe


@pytest.mark.onlynoncluster
class TestPipeline:
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_executes_pipeline_against_correct_db(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            pipe = mock_pipe()
            pipe.execute.return_value = ["OK1", "value1"]
            mock_db1.client.pipeline.return_value = pipe

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            pipe = client.pipeline()
            pipe.set("key1", "value1")
            pipe.get("key1")

            assert pipe.execute() == ["OK1", "value1"]
            assert mock_hc.check_health.call_count == 9

    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    def test_execute_pipeline_against_correct_db_and_closed_circuit(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            pipe = mock_pipe()
            pipe.execute.return_value = ["OK1", "value1"]
            mock_db1.client.pipeline.return_value = pipe

            mock_hc.check_health.side_effect = [
                False,
                True,
                True,
                True,
                True,
                True,
                True,
            ]

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            with client.pipeline() as pipe:
                pipe.set("key1", "value1")
                pipe.get("key1")

            assert pipe.execute() == ["OK1", "value1"]
            assert mock_hc.check_health.call_count == 7

            assert mock_db.circuit.state == CBState.CLOSED
            assert mock_db1.circuit.state == CBState.CLOSED
            assert mock_db2.circuit.state == CBState.OPEN

    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"health_check_probes": 1},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_pipeline_against_correct_db_on_background_health_check_determine_active_db_unhealthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        cb = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=5))
        cb.database = mock_db
        mock_db.circuit = cb

        cb1 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=5))
        cb1.database = mock_db1
        mock_db1.circuit = cb1

        cb2 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=5))
        cb2.database = mock_db2
        mock_db2.circuit = cb2

        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        # Track health check runs across all databases
        health_check_run = 0

        # Create events for each failover scenario
        db1_became_unhealthy = threading.Event()
        db2_became_unhealthy = threading.Event()
        db_became_unhealthy = threading.Event()
        counter_lock = threading.Lock()

        def mock_check_health(database):
            nonlocal health_check_run

            # Increment run counter for each health check call
            with counter_lock:
                health_check_run += 1
                current_run = health_check_run

            # Run 1 (health_check_run 1-3): All databases healthy
            if current_run <= 3:
                return True

            # Run 2 (health_check_run 4-6): mock_db1 unhealthy, others healthy
            elif current_run <= 6:
                if database == mock_db1:
                    if current_run == 6:
                        db1_became_unhealthy.set()
                    return False

                # Signal that db1 has become unhealthy after all 3 checks
                if current_run == 6:
                    db1_became_unhealthy.set()
                return True

            # Run 3 (health_check_run 7-9): mock_db1 and mock_db2 unhealthy, mock_db healthy
            elif current_run <= 9:
                if database == mock_db1 or database == mock_db2:
                    if current_run == 9:
                        db2_became_unhealthy.set()
                    return False

                # Signal that db2 has become unhealthy after all 3 checks
                if current_run == 9:
                    db2_became_unhealthy.set()
                return True

            # Run 4 (health_check_run 10-12): mock_db unhealthy, others healthy
            else:
                if database == mock_db:
                    if current_run >= 12:
                        db_became_unhealthy.set()
                    return False

                # Signal that db has become unhealthy after all 3 checks
                if current_run >= 12:
                    db_became_unhealthy.set()
                return True

        mock_hc.check_health.side_effect = mock_check_health

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config,
                "default_health_checks",
                return_value=[mock_hc],
            ),
        ):
            pipe = mock_pipe()
            pipe.execute.return_value = ["OK", "value"]
            mock_db.client.pipeline.return_value = pipe

            pipe1 = mock_pipe()
            pipe1.execute.return_value = ["OK1", "value"]
            mock_db1.client.pipeline.return_value = pipe1

            pipe2 = mock_pipe()
            pipe2.execute.return_value = ["OK2", "value"]
            mock_db2.client.pipeline.return_value = pipe2

            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            client = MultiDBClient(mock_multi_db_config)

            with client.pipeline() as pipe:
                pipe.set("key1", "value")
                pipe.get("key1")

                # Run 1: All databases healthy - should use mock_db1 (highest weight 0.7)
                assert pipe.execute() == ["OK1", "value"]

                # Wait for mock_db1 to become unhealthy
                assert db1_became_unhealthy.wait(timeout=1.0), "Timeout waiting for mock_db1 to become unhealthy"
                sleep(0.01)

                # Run 2: mock_db1 unhealthy - should failover to mock_db2 (weight 0.5)
                assert pipe.execute() == ["OK2", "value"]

                # Wait for mock_db2 to become unhealthy
                assert db2_became_unhealthy.wait(timeout=1.0), "Timeout waiting for mock_db2 to become unhealthy"
                sleep(0.01)

                # Run 3: mock_db1 and mock_db2 unhealthy - should use mock_db (weight 0.2)
                assert pipe.execute() == ["OK", "value"]

                # Wait for mock_db to become unhealthy
                assert db_became_unhealthy.wait(timeout=1.0), "Timeout waiting for mock_db to become unhealthy"
                sleep(0.01)

                # Run 4: mock_db unhealthy, others healthy - should use mock_db1 (highest weight)
                assert pipe.execute() == ["OK1", "value"]

@pytest.mark.onlynoncluster
class TestTransaction:
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_executes_transaction_against_correct_db(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_db1.client.transaction.return_value = ["OK1", "value1"]

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            def callback(pipe: Pipeline):
                pipe.set("key1", "value1")
                pipe.get("key1")

            assert client.transaction(callback) == ["OK1", "value1"]
            assert mock_hc.check_health.call_count == 9

    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    def test_execute_transaction_against_correct_db_and_closed_circuit(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config, "default_health_checks", return_value=[mock_hc]
            ),
        ):
            mock_db1.client.transaction.return_value = ["OK1", "value1"]

            mock_hc.check_health.side_effect = [
                False,
                True,
                True,
                True,
                True,
                True,
                True,
            ]

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            def callback(pipe: Pipeline):
                pipe.set("key1", "value1")
                pipe.get("key1")

            assert client.transaction(callback) == ["OK1", "value1"]
            assert mock_hc.check_health.call_count == 7

            assert mock_db.circuit.state == CBState.CLOSED
            assert mock_db1.circuit.state == CBState.CLOSED
            assert mock_db2.circuit.state == CBState.OPEN

    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"health_check_probes": 1},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_execute_transaction_against_correct_db_on_background_health_check_determine_active_db_unhealthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        cb = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=5))
        cb.database = mock_db
        mock_db.circuit = cb

        cb1 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=5))
        cb1.database = mock_db1
        mock_db1.circuit = cb1

        cb2 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=5))
        cb2.database = mock_db2
        mock_db2.circuit = cb2

        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        # Track health check runs across all databases
        health_check_run = 0
        
        # Create events for each failover scenario
        db1_became_unhealthy = threading.Event()
        db2_became_unhealthy = threading.Event()
        db_became_unhealthy = threading.Event()
        counter_lock = threading.Lock()

        def mock_check_health(database):
            nonlocal health_check_run
            
            # Increment run counter for each health check call
            with counter_lock:
                health_check_run += 1
                current_run = health_check_run
            
            # Run 1 (health_check_run 1-3): All databases healthy
            if current_run <= 3:
                return True
            
            # Run 2 (health_check_run 4-6): mock_db1 unhealthy, others healthy
            elif current_run <= 6:
                if database == mock_db1:
                    if current_run == 6:
                        db1_became_unhealthy.set()
                    return False

                # Signal that db1 has become unhealthy after all 3 checks
                if current_run == 6:
                    db1_became_unhealthy.set()
                return True
            
            # Run 3 (health_check_run 7-9): mock_db1 and mock_db2 unhealthy, mock_db healthy
            elif current_run <= 9:
                if database == mock_db1 or database == mock_db2:
                    if current_run == 9:
                        db2_became_unhealthy.set()
                    return False

                # Signal that db2 has become unhealthy after all 3 checks
                if current_run == 9:
                    db2_became_unhealthy.set()
                return True
            
            # Run 4 (health_check_run 10-12): mock_db unhealthy, others healthy
            else:
                if database == mock_db:
                    if current_run >= 12:
                        db_became_unhealthy.set()
                    return False

                # Signal that db has become unhealthy after all 3 checks
                if current_run >= 12:
                    db_became_unhealthy.set()
                return True

        mock_hc.check_health.side_effect = mock_check_health

        with (
            patch.object(mock_multi_db_config, "databases", return_value=databases),
            patch.object(
                mock_multi_db_config,
                "default_health_checks",
                return_value=[mock_hc],
            ),
        ):
            mock_db.client.transaction.return_value = ["OK", "value"]
            mock_db1.client.transaction.return_value = ["OK1", "value"]
            mock_db2.client.transaction.return_value = ["OK2", "value"]

            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            client = MultiDBClient(mock_multi_db_config)

            def callback(pipe: Pipeline):
                pipe.set("key1", "value1")
                pipe.get("key1")

            # Run 1: All databases healthy - should use mock_db1 (highest weight 0.7)
            assert client.transaction(callback) == ["OK1", "value"]
            
            # Wait for mock_db1 to become unhealthy
            assert db1_became_unhealthy.wait(timeout=1.0), "Timeout waiting for mock_db1 to become unhealthy"
            sleep(0.01)
            
            # Run 2: mock_db1 unhealthy - should failover to mock_db2 (weight 0.5)
            assert client.transaction(callback) == ["OK2", "value"]
            
            # Wait for mock_db2 to become unhealthy
            assert db2_became_unhealthy.wait(timeout=1.0), "Timeout waiting for mock_db2 to become unhealthy"
            sleep(0.01)

            # Run 3: mock_db1 and mock_db2 unhealthy - should use mock_db (weight 0.2)
            assert client.transaction(callback) == ["OK", "value"]
            
            # Wait for mock_db to become unhealthy
            assert db_became_unhealthy.wait(timeout=1.0), "Timeout waiting for mock_db to become unhealthy"
            sleep(0.01)
            
            # Run 4: mock_db unhealthy, others healthy - should use mock_db1 (highest weight)
            assert client.transaction(callback) == ["OK1", "value"]
