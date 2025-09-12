import asyncio
from unittest.mock import Mock, AsyncMock, patch

import pybreaker
import pytest

from redis.asyncio.client import Pipeline
from redis.asyncio.multidb.client import MultiDBClient
from redis.asyncio.multidb.config import DEFAULT_FAILOVER_RETRIES
from redis.asyncio.multidb.failover import WeightBasedFailoverStrategy
from redis.asyncio.multidb.healthcheck import EchoHealthCheck
from redis.asyncio.retry import Retry
from redis.multidb.circuit import State as CBState, PBCircuitBreakerAdapter
from redis.multidb.config import DEFAULT_FAILOVER_BACKOFF
from tests.test_asyncio.test_multidb.conftest import create_weighted_list


def mock_pipe() -> Pipeline:
    mock_pipe = Mock(spec=Pipeline)
    mock_pipe.__aenter__ = AsyncMock(return_value=mock_pipe)
    mock_pipe.__aexit__ = AsyncMock(return_value=None)
    return mock_pipe

class TestPipeline:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_executes_pipeline_against_correct_db(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            pipe = mock_pipe()
            pipe.execute.return_value = ['OK1', 'value1']
            mock_db1.client.pipeline.return_value = pipe

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            pipe = client.pipeline()
            pipe.set('key1', 'value1')
            pipe.get('key1')

            assert await pipe.execute() == ['OK1', 'value1']
            assert mock_hc.check_health.call_count == 9

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_pipeline_against_correct_db_and_closed_circuit(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            pipe = mock_pipe()
            pipe.execute.return_value = ['OK1', 'value1']
            mock_db1.client.pipeline.return_value = pipe

            mock_hc.check_health.side_effect = [False, True, True, True, True, True, True]

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            async with client.pipeline() as pipe:
                pipe.set('key1', 'value1')
                pipe.get('key1')

            assert await pipe.execute() == ['OK1', 'value1']
            assert mock_hc.check_health.call_count == 7

            assert mock_db.circuit.state == CBState.CLOSED
            assert mock_db1.circuit.state == CBState.CLOSED
            assert mock_db2.circuit.state == CBState.OPEN

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {"health_check_probes" : 1},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_pipeline_against_correct_db_on_background_health_check_determine_active_db_unhealthy(
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

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[EchoHealthCheck()]):
            mock_db.client.execute_command.side_effect = ['healthcheck', 'healthcheck', 'healthcheck', 'error']
            mock_db1.client.execute_command.side_effect = ['healthcheck', 'error', 'error', 'healthcheck']
            mock_db2.client.execute_command.side_effect = ['healthcheck', 'healthcheck', 'error', 'error']

            pipe = mock_pipe()
            pipe.execute.return_value = ['OK', 'value']
            mock_db.client.pipeline.return_value = pipe

            pipe1 = mock_pipe()
            pipe1.execute.return_value = ['OK1', 'value']
            mock_db1.client.pipeline.return_value = pipe1

            pipe2 = mock_pipe()
            pipe2.execute.return_value = ['OK2', 'value']
            mock_db2.client.pipeline.return_value = pipe2

            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy(
                retry=Retry(retries=DEFAULT_FAILOVER_RETRIES, backoff=DEFAULT_FAILOVER_BACKOFF)
            )

            client = MultiDBClient(mock_multi_db_config)

            async with client.pipeline() as pipe:
                pipe.set('key1', 'value')
                pipe.get('key1')

            assert await pipe.execute() == ['OK1', 'value']

            await asyncio.sleep(0.15)

            async with client.pipeline() as pipe:
                pipe.set('key1', 'value')
                pipe.get('key1')

            assert await pipe.execute() == ['OK2', 'value']

            await asyncio.sleep(0.1)

            async with client.pipeline() as pipe:
                pipe.set('key1', 'value')
                pipe.get('key1')

            assert await pipe.execute() == ['OK', 'value']

            await asyncio.sleep(0.1)

            async with client.pipeline() as pipe:
                pipe.set('key1', 'value')
                pipe.get('key1')

            assert await pipe.execute() == ['OK1', 'value']

class TestTransaction:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_executes_transaction_against_correct_db(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_db1.client.transaction.return_value = ['OK1', 'value1']

            mock_hc.check_health.return_value = True

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            async def callback(pipe: Pipeline):
                pipe.set('key1', 'value1')
                pipe.get('key1')

            assert await client.transaction(callback) == ['OK1', 'value1']
            assert mock_hc.check_health.call_count == 9

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_transaction_against_correct_db_and_closed_circuit(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2, mock_hc
    ):
        databases = create_weighted_list(mock_db, mock_db1, mock_db2)

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[mock_hc]):
            mock_db1.client.transaction.return_value = ['OK1', 'value1']

            mock_hc.check_health.side_effect = [False, True, True, True, True, True, True]

            client = MultiDBClient(mock_multi_db_config)
            assert mock_multi_db_config.failover_strategy.set_databases.call_count == 1

            async def callback(pipe: Pipeline):
                pipe.set('key1', 'value1')
                pipe.get('key1')

            assert await client.transaction(callback) == ['OK1', 'value1']
            assert mock_hc.check_health.call_count == 7

            assert mock_db.circuit.state == CBState.CLOSED
            assert mock_db1.circuit.state == CBState.CLOSED
            assert mock_db2.circuit.state == CBState.OPEN

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_multi_db_config,mock_db, mock_db1, mock_db2',
        [
            (
                    {"health_check_probes" : 1},
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    async def test_execute_transaction_against_correct_db_on_background_health_check_determine_active_db_unhealthy(
            self, mock_multi_db_config, mock_db, mock_db1, mock_db2
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

        with patch.object(mock_multi_db_config,'databases',return_value=databases), \
             patch.object(mock_multi_db_config,'default_health_checks', return_value=[EchoHealthCheck()]):
            mock_db.client.execute_command.side_effect = ['healthcheck', 'healthcheck', 'healthcheck', 'error']
            mock_db1.client.execute_command.side_effect = ['healthcheck', 'error', 'error', 'healthcheck']
            mock_db2.client.execute_command.side_effect = ['healthcheck', 'healthcheck', 'error', 'error']

            mock_db.client.transaction.return_value =  ['OK', 'value']
            mock_db1.client.transaction.return_value = ['OK1', 'value']
            mock_db2.client.transaction.return_value = ['OK2', 'value']

            mock_multi_db_config.health_check_interval = 0.1
            mock_multi_db_config.failover_strategy = WeightBasedFailoverStrategy(
                retry=Retry(retries=DEFAULT_FAILOVER_RETRIES, backoff=DEFAULT_FAILOVER_BACKOFF)
            )

            client = MultiDBClient(mock_multi_db_config)

            async def callback(pipe: Pipeline):
                pipe.set('key1', 'value1')
                pipe.get('key1')

            assert await client.transaction(callback) == ['OK1', 'value']
            await asyncio.sleep(0.15)
            assert await client.transaction(callback) == ['OK2', 'value']
            await asyncio.sleep(0.1)
            assert await client.transaction(callback) == ['OK', 'value']
            await asyncio.sleep(0.1)
            assert await client.transaction(callback) == ['OK1', 'value']