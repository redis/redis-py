from unittest.mock import Mock

import pytest

from redis import Redis
from redis.multidb.circuit import CircuitBreaker, State as CBState
from redis.multidb.database import Database, State


@pytest.fixture()
def mock_client() -> Redis:
    return Mock(spec=Redis)

@pytest.fixture()
def mock_cb() -> CircuitBreaker:
    return Mock(spec=CircuitBreaker)

@pytest.fixture()
def mock_db() -> Database:
     db = Mock(spec=Database)
     db.circuit.state = CBState.CLOSED
     return db