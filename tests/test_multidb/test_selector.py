from unittest.mock import PropertyMock

import pytest

from redis.backoff import NoBackoff, ExponentialBackoff
from redis.multidb.circuit import State as CBState
from redis.multidb.exception import NoValidDatabaseException
from redis.multidb.selector import WeightBasedDatabaseSelector
from redis.retry import Retry


class TestWeightBasedDatabaseSelector:
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
        selector = WeightBasedDatabaseSelector(retry=retry)
        selector.add_database(mock_db)
        selector.add_database(mock_db1)
        selector.add_database(mock_db2)

        assert selector.database == mock_db1

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
        selector = WeightBasedDatabaseSelector(retry=retry)
        selector.add_database(mock_db)
        selector.add_database(mock_db1)
        selector.add_database(mock_db2)

        assert selector.database == mock_db
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
        selector = WeightBasedDatabaseSelector(retry=retry)
        selector.add_database(mock_db)
        selector.add_database(mock_db1)
        selector.add_database(mock_db2)

        with pytest.raises(NoValidDatabaseException, match='No valid database available for communication'):
            assert selector.database

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
        selector = WeightBasedDatabaseSelector(retry=retry)

        with pytest.raises(NoValidDatabaseException, match='No valid database available for communication'):
            assert selector.database

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
        selector = WeightBasedDatabaseSelector(retry=retry)
        selector.add_database(mock_db)
        selector.add_database(mock_db2)
        assert selector.database == mock_db2

        selector.add_database(mock_db1)
        assert selector.database == mock_db1