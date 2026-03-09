import asyncio
from unittest.mock import Mock, AsyncMock, patch

import pybreaker
import pytest

from redis.asyncio.client import Pipeline
from redis.asyncio.multidb.client import MultiDBClient
from redis.asyncio.multidb.config import InitialHealthCheck
from redis.asyncio.multidb.failover import WeightBasedFailoverStrategy
from redis.multidb.circuit import State as CBState, PBCircuitBreakerAdapter
from tests.test_asyncio.helpers import wait_for_condition
from tests.test_asyncio.test_multidb.conftest import create_weighted_list


def mock_pipe() -> Pipeline:
    mock_pipe = Mock(spec=Pipeline)
    mock_pipe.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipe.__aexit__ = AsyncMock(return_value=None)
    return mock_pipe


@pytest.mark.onlynoncluster
class TestPipeline:
    @pytest.mark.asyncio
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
    async def test_executes_pipeline_against_correct_db(
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

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )

                pipe = client.pipeline()
                pipe.set("key1", "value1")
                pipe.get("key1")

                assert await pipe.execute() == ["OK1", "value1"]
                assert len(mock_hc.check_health.call_args_list) >= 9

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"initial_health_check_policy": InitialHealthCheck.MAJORITY_AVAILABLE},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_pipeline_against_correct_db_and_closed_circuit(
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

            async def mock_check_health(database):
                if database == mock_db2:
                    return False
                else:
                    return True

            mock_hc.check_health.side_effect = mock_check_health
            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )

                async with client.pipeline() as pipe:
                    pipe.set("key1", "value1")
                    pipe.get("key1")

                assert await pipe.execute() == ["OK1", "value1"]
                assert len(mock_hc.check_health.call_args_list) >= 7

                assert mock_db.circuit.state == CBState.CLOSED
                assert mock_db1.circuit.state == CBState.CLOSED
                assert mock_db2.circuit.state == CBState.OPEN

    @pytest.mark.asyncio
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
    async def test_execute_pipeline_against_correct_db_on_background_health_check_determine_active_db_unhealthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        cb = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=0.1))
        cb.database = mock_db
        mock_db.circuit = cb

        cb1 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=0.1))
        cb1.database = mock_db1
        mock_db1.circuit = cb1

        cb2 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=0.1))
        cb2.database = mock_db2
        mock_db2.circuit = cb2

        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        # Track health check rounds per database
        db_rounds = {id(mock_db): 0, id(mock_db1): 0, id(mock_db2): 0}
        db_probes_in_round = {id(mock_db): 0, id(mock_db1): 0, id(mock_db2): 0}

        # Create events for each failover scenario
        db1_became_unhealthy = asyncio.Event()
        db2_became_unhealthy = asyncio.Event()
        db_became_unhealthy = asyncio.Event()
        counter_lock = asyncio.Lock()

        async def mock_check_health(database):
            async with counter_lock:
                db_probes_in_round[id(database)] += 1
                # After 3 probes, increment the round counter
                if db_probes_in_round[id(database)] > 3:
                    db_rounds[id(database)] += 1
                    db_probes_in_round[id(database)] = 1
                current_round = db_rounds[id(database)]

            # Round 0 (initial health check): All databases healthy
            if current_round == 0:
                return True

            # Round 1: mock_db1 becomes unhealthy, others healthy
            if current_round == 1:
                if database == mock_db1:
                    db1_became_unhealthy.set()
                    return False
                return True

            # Round 2: mock_db2 also becomes unhealthy, mock_db healthy
            if current_round == 2:
                if database == mock_db1:
                    return False
                if database == mock_db2:
                    db2_became_unhealthy.set()
                    return False
                return True

            # Round 3: mock_db also becomes unhealthy, but mock_db1 recovers
            if current_round == 3:
                if database == mock_db:
                    db_became_unhealthy.set()
                    return False
                if database == mock_db2:
                    return False
                return True

            # Round 4+: mock_db1 is healthy, others unhealthy
            if database == mock_db1:
                return True
            return False

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

            mock_multi_db_config.health_check_interval = 0.05
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            client = MultiDBClient(mock_multi_db_config)

            async with client.pipeline() as pipe:
                pipe.set("key1", "value")
                pipe.get("key1")

            # Run 1: All databases healthy - should use mock_db1 (highest weight 0.7)
            assert await pipe.execute() == ["OK1", "value"]

            # Wait for mock_db1 to become unhealthy
            await wait_for_condition(
                lambda: cb1.state == CBState.OPEN,
                timeout=0.5,
                error_message="Timeout waiting for cb1 to open",
            )

            # Run 2: mock_db1 unhealthy - should failover to mock_db2 (weight 0.5)
            assert await pipe.execute() == ["OK2", "value"]

            # Wait for mock_db2 to become unhealthy
            await wait_for_condition(
                lambda: cb2.state == CBState.OPEN,
                timeout=0.5,
                error_message="Timeout waiting for cb2 to open",
            )

            # Run 3: mock_db1 and mock_db2 unhealthy - should use mock_db (weight 0.2)
            assert await pipe.execute() == ["OK", "value"]

            # Wait for mock_db to become unhealthy
            assert await asyncio.wait_for(db_became_unhealthy.wait(), timeout=1.0), (
                "Timeout waiting for mock_db to become unhealthy"
            )
            await wait_for_condition(
                lambda: cb.state == CBState.OPEN,
                timeout=0.5,
                error_message="Timeout waiting for cb to open",
            )

            # Wait for mock_db1 to recover (circuit breaker to close)
            await wait_for_condition(
                lambda: cb1.state == CBState.CLOSED,
                timeout=1.0,
                error_message="Timeout waiting for cb1 to close (mock_db1 to recover)",
            )

            # Run 4: mock_db unhealthy, others healthy - should use mock_db1 (highest weight)
            assert await pipe.execute() == ["OK1", "value"]

            await client.aclose()


@pytest.mark.onlynoncluster
class TestTransaction:
    @pytest.mark.asyncio
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
    async def test_executes_transaction_against_correct_db(
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

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )

                async def callback(pipe: Pipeline):
                    pipe.set("key1", "value1")
                    pipe.get("key1")

                assert await client.transaction(callback) == ["OK1", "value1"]
                # if we assume at least 3 health checks have run per each database
                # we should have at least 9 total calls
                assert len(mock_hc.check_health.call_args_list) >= 9

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_multi_db_config,mock_db, mock_db1, mock_db2",
        [
            (
                {"initial_health_check_policy": InitialHealthCheck.MAJORITY_AVAILABLE},
                {"weight": 0.2, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.5, "circuit": {"state": CBState.CLOSED}},
                {"weight": 0.7, "circuit": {"state": CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_transaction_against_correct_db_and_closed_circuit(
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

            async def mock_check_health(database):
                if database == mock_db2:
                    return False
                else:
                    return True

            mock_hc.check_health.side_effect = mock_check_health

            async with MultiDBClient(mock_multi_db_config) as client:
                assert (
                    mock_multi_db_config.failover_strategy.set_databases.call_count == 1
                )

                async def callback(pipe: Pipeline):
                    pipe.set("key1", "value1")
                    pipe.get("key1")

                assert await client.transaction(callback) == ["OK1", "value1"]
                assert len(mock_hc.check_health.call_args_list) >= 7

                assert mock_db.circuit.state == CBState.CLOSED
                assert mock_db1.circuit.state == CBState.CLOSED
                assert mock_db2.circuit.state == CBState.OPEN

    @pytest.mark.asyncio
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
    async def test_execute_transaction_against_correct_db_on_background_health_check_determine_active_db_unhealthy(
        self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        cb = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=0.1))
        cb.database = mock_db
        mock_db.circuit = cb

        cb1 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=0.1))
        cb1.database = mock_db1
        mock_db1.circuit = cb1

        cb2 = PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=0.1))
        cb2.database = mock_db2
        mock_db2.circuit = cb2

        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        # Track health check rounds per database
        db_rounds = {id(mock_db): 0, id(mock_db1): 0, id(mock_db2): 0}
        db_probes_in_round = {id(mock_db): 0, id(mock_db1): 0, id(mock_db2): 0}

        # Create events for each failover scenario
        db1_became_unhealthy = asyncio.Event()
        db2_became_unhealthy = asyncio.Event()
        db_became_unhealthy = asyncio.Event()
        counter_lock = asyncio.Lock()

        async def mock_check_health(database):
            async with counter_lock:
                db_probes_in_round[id(database)] += 1
                # After 3 probes, increment the round counter
                if db_probes_in_round[id(database)] > 3:
                    db_rounds[id(database)] += 1
                    db_probes_in_round[id(database)] = 1
                current_round = db_rounds[id(database)]

            # Round 0 (initial health check): All databases healthy
            if current_round == 0:
                return True

            # Round 1: mock_db1 becomes unhealthy, others healthy
            if current_round == 1:
                if database == mock_db1:
                    db1_became_unhealthy.set()
                    return False
                return True

            # Round 2: mock_db2 also becomes unhealthy, mock_db healthy
            if current_round == 2:
                if database == mock_db1:
                    return False
                if database == mock_db2:
                    db2_became_unhealthy.set()
                    return False
                return True

            # Round 3: mock_db also becomes unhealthy, but mock_db1 recovers
            if current_round == 3:
                if database == mock_db:
                    db_became_unhealthy.set()
                    return False
                if database == mock_db2:
                    return False
                return True

            # Round 4+: mock_db1 is healthy, others unhealthy
            if database == mock_db1:
                return True
            return False

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

            mock_multi_db_config.health_check_interval = 0.05
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy()

            async with MultiDBClient(mock_multi_db_config) as client:

                async def callback(pipe: Pipeline):
                    pipe.set("key1", "value1")
                    pipe.get("key1")

                assert await client.transaction(callback) == ["OK1", "value"]

                # Wait for mock_db1 to become unhealthy
                await wait_for_condition(
                    lambda: cb1.state == CBState.OPEN,
                    timeout=0.5,
                    error_message="Timeout waiting for cb1 to open",
                )

                assert await client.transaction(callback) == ["OK2", "value"]

                # Wait for mock_db2 to become unhealthy
                await wait_for_condition(
                    lambda: cb2.state == CBState.OPEN,
                    timeout=0.5,
                    error_message="Timeout waiting for cb2 to open",
                )

                assert await client.transaction(callback) == ["OK", "value"]

                # Wait for mock_db to become unhealthy
                assert await asyncio.wait_for(
                    db_became_unhealthy.wait(), timeout=1.0
                ), "Timeout waiting for mock_db to become unhealthy"
                await wait_for_condition(
                    lambda: cb.state == CBState.OPEN,
                    timeout=0.5,
                    error_message="Timeout waiting for cb to open",
                )

                # Wait for mock_db1 to recover (circuit breaker to close)
                await wait_for_condition(
                    lambda: cb1.state == CBState.CLOSED,
                    timeout=1.0,
                    error_message="Timeout waiting for cb1 to close (mock_db1 to recover)",
                )

                assert await client.transaction(callback) == ["OK1", "value"]
