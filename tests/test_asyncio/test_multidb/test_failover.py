import asyncio
from unittest.mock import PropertyMock

import pytest

from redis.backoff import NoBackoff, ExponentialBackoff
from redis.data_structure import WeightedList
from redis.multidb.circuit import State as CBState
from redis.multidb.exception import NoValidDatabaseException, TemporaryUnavailableException
from redis.asyncio.multidb.failover import WeightBasedFailoverStrategy
from redis.asyncio.retry import Retry


class TestAsyncWeightBasedFailoverStrategy:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_db,mock_db1,mock_db2',
        [
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.OPEN}},
            ),
        ],
        ids=['all closed - highest weight', 'highest weight - open'],
        indirect=True,
    )
    async def test_get_valid_database(self, mock_db, mock_db1, mock_db2):
        databases = WeightedList()
        databases.add(mock_db, mock_db.weight)
        databases.add(mock_db1, mock_db1.weight)
        databases.add(mock_db2, mock_db2.weight)

        strategy = WeightBasedFailoverStrategy()
        strategy.set_databases(databases)

        assert await strategy.database() == mock_db1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_db,mock_db1,mock_db2',
        [
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.OPEN}},
                    {'weight': 0.7, 'circuit': {'state': CBState.OPEN}},
                    {'weight': 0.5, 'circuit': {'state': CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_get_valid_database_with_failover_attempts(self, mock_db, mock_db1, mock_db2):
        state_mock = PropertyMock(
            side_effect=[CBState.OPEN, CBState.OPEN, CBState.OPEN, CBState.CLOSED]
        )
        type(mock_db.circuit).state = state_mock
        failover_attempts = 3

        databases = WeightedList()
        databases.add(mock_db, mock_db.weight)
        databases.add(mock_db1, mock_db1.weight)
        databases.add(mock_db2, mock_db2.weight)
        failover_strategy = WeightBasedFailoverStrategy(
            failover_attempts=failover_attempts,
            failover_delay=0.1
        )
        failover_strategy.set_databases(databases)

        for i in range(failover_attempts + 1):
            try:
                database = await failover_strategy.database()
                assert database == mock_db
            except TemporaryUnavailableException as e:
                assert e.args[0] == (
                    "No database connections currently available. "
                    "This is a temporary condition - please retry the operation."
                )
                await asyncio.sleep(0.11)
                pass

        assert state_mock.call_count == 4

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_db,mock_db1,mock_db2',
        [
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.OPEN}},
                    {'weight': 0.7, 'circuit': {'state': CBState.OPEN}},
                    {'weight': 0.5, 'circuit': {'state': CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_get_valid_database_throws_exception_on_attempts_exceed(self, mock_db, mock_db1, mock_db2):
        state_mock = PropertyMock(
            side_effect=[CBState.OPEN, CBState.OPEN, CBState.OPEN, CBState.OPEN]
        )
        type(mock_db.circuit).state = state_mock
        failover_attempts = 3

        databases = WeightedList()
        databases.add(mock_db, mock_db.weight)
        databases.add(mock_db1, mock_db1.weight)
        databases.add(mock_db2, mock_db2.weight)
        failover_strategy = WeightBasedFailoverStrategy(
            failover_attempts=failover_attempts,
            failover_delay=0.1
        )
        failover_strategy.set_databases(databases)

        with pytest.raises(NoValidDatabaseException, match='No valid database available for communication'):
            for i in range(failover_attempts + 1):
                try:
                    database = await failover_strategy.database()
                except TemporaryUnavailableException as e:
                    assert e.args[0] == (
                        "No database connections currently available. "
                        "This is a temporary condition - please retry the operation."
                    )
                    await asyncio.sleep(0.11)
                    pass

        assert state_mock.call_count == 4

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_db,mock_db1,mock_db2',
        [
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.OPEN}},
                    {'weight': 0.7, 'circuit': {'state': CBState.OPEN}},
                    {'weight': 0.5, 'circuit': {'state': CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_get_valid_database_throws_exception_on_attempts_does_not_exceed_delay(self, mock_db, mock_db1, mock_db2):
        state_mock = PropertyMock(
            side_effect=[CBState.OPEN, CBState.OPEN, CBState.OPEN, CBState.OPEN]
        )
        type(mock_db.circuit).state = state_mock
        failover_attempts = 3

        databases = WeightedList()
        databases.add(mock_db, mock_db.weight)
        databases.add(mock_db1, mock_db1.weight)
        databases.add(mock_db2, mock_db2.weight)
        failover_strategy = WeightBasedFailoverStrategy(
            failover_attempts=failover_attempts,
            failover_delay=0.1
        )
        failover_strategy.set_databases(databases)

        with pytest.raises(TemporaryUnavailableException, match=(
                        "No database connections currently available. "
                        "This is a temporary condition - please retry the operation."
                    )):
            for i in range(failover_attempts + 1):
                try:
                    database = await failover_strategy.database()
                except TemporaryUnavailableException as e:
                    assert e.args[0] == (
                        "No database connections currently available. "
                        "This is a temporary condition - please retry the operation."
                    )
                    if i == failover_attempts:
                        raise e

        assert state_mock.call_count == 4

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'mock_db,mock_db1,mock_db2',
        [
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.OPEN}},
                    {'weight': 0.7, 'circuit': {'state': CBState.OPEN}},
                    {'weight': 0.5, 'circuit': {'state': CBState.OPEN}},
            ),
        ],
        indirect=True,
    )
    async def test_throws_exception_on_empty_databases(self, mock_db, mock_db1, mock_db2):
        failover_strategy = WeightBasedFailoverStrategy(failover_attempts=0, failover_delay=0)

        with pytest.raises(NoValidDatabaseException, match='No valid database available for communication'):
            assert await failover_strategy.database()