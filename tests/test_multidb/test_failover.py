from unittest.mock import PropertyMock

import pytest

from redis.backoff import NoBackoff, ExponentialBackoff
from redis.multidb.circuit import State as CBState
from redis.multidb.exception import NoValidDatabaseException
from redis.multidb.failover import WeightBasedFailoverStrategy
from redis.retry import Retry


class TestWeightBasedFailoverStrategy:
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
    def test_get_valid_database(self, mock_db, mock_db1, mock_db2):
        retry = Retry(NoBackoff(), 0)
        failover_strategy = WeightBasedFailoverStrategy(retry=retry)
        failover_strategy.add_database(mock_db)
        failover_strategy.add_database(mock_db1)
        failover_strategy.add_database(mock_db2)

        assert failover_strategy.database == mock_db1

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
    def test_get_valid_database_with_retries(self, mock_db, mock_db1, mock_db2):
        state_mock = PropertyMock(
            side_effect=[CBState.OPEN, CBState.OPEN, CBState.OPEN, CBState.CLOSED]
        )
        type(mock_db.circuit).state = state_mock

        retry = Retry(ExponentialBackoff(cap=1), 3)
        failover_strategy = WeightBasedFailoverStrategy(retry=retry)
        failover_strategy.add_database(mock_db)
        failover_strategy.add_database(mock_db1)
        failover_strategy.add_database(mock_db2)

        assert failover_strategy.database == mock_db
        assert state_mock.call_count == 4

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
    def test_get_valid_database_throws_exception_with_retries(self, mock_db, mock_db1, mock_db2):
        state_mock = PropertyMock(
            side_effect=[CBState.OPEN, CBState.OPEN, CBState.OPEN, CBState.OPEN]
        )
        type(mock_db.circuit).state = state_mock

        retry = Retry(ExponentialBackoff(cap=1), 3)
        failover_strategy = WeightBasedFailoverStrategy(retry=retry)
        failover_strategy.add_database(mock_db)
        failover_strategy.add_database(mock_db1)
        failover_strategy.add_database(mock_db2)

        with pytest.raises(NoValidDatabaseException, match='No valid database available for communication'):
            assert failover_strategy.database

        assert state_mock.call_count == 4

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
    def test_throws_exception_on_empty_databases(self, mock_db, mock_db1, mock_db2):
        retry = Retry(NoBackoff(), 0)
        failover_strategy = WeightBasedFailoverStrategy(retry=retry)

        with pytest.raises(NoValidDatabaseException, match='No valid database available for communication'):
            assert failover_strategy.database

    @pytest.mark.parametrize(
        'mock_db,mock_db1,mock_db2',
        [
            (
                    {'weight': 0.2, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.7, 'circuit': {'state': CBState.CLOSED}},
                    {'weight': 0.5, 'circuit': {'state': CBState.CLOSED}},
            ),
        ],
        indirect=True,
    )
    def test_add_database_return_valid_database(self, mock_db, mock_db1, mock_db2):
        retry = Retry(ExponentialBackoff(cap=1), 3)
        failover_strategy = WeightBasedFailoverStrategy(retry=retry)
        failover_strategy.add_database(mock_db)
        failover_strategy.add_database(mock_db2)
        assert failover_strategy.database == mock_db2

        failover_strategy.add_database(mock_db1)
        assert failover_strategy.database == mock_db1