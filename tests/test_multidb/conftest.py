from unittest.mock import Mock

import pytest

from redis import Redis
from redis.multidb.circuit import CircuitBreaker, State as CBState
from redis.multidb.database import Database, State


@pytest.fixture()
def mock_client() -> Redis:
    return Mock(spec=Redis)

@pytest.fixture(scope='function')
def mock_cb() -> CircuitBreaker:
    return Mock(spec=CircuitBreaker)

@pytest.fixture()
def mock_db(request) -> Database:
     db = Mock(spec=Database)
     db.weight = request.param.get("weight", 1.0)
     db.state = request.param.get("state", State.ACTIVE)
     db.client = Mock(spec=Redis)

     cb = request.param.get("circuit", {})
     mock_cb = Mock(spec=CircuitBreaker)
     mock_cb.grace_period = cb.get("grace_period", 1.0)
     mock_cb.state = cb.get("state", CBState.CLOSED)

     db.circuit = mock_cb
     return db

@pytest.fixture()
def mock_db1(request) -> Database:
     db = Mock(spec=Database)
     db.weight = request.param.get("weight", 1.0)
     db.state = request.param.get("state", State.ACTIVE)
     db.client = Mock(spec=Redis)

     cb = request.param.get("circuit", {})
     mock_cb = Mock(spec=CircuitBreaker)
     mock_cb.grace_period = cb.get("grace_period", 1.0)
     mock_cb.state = cb.get("state", CBState.CLOSED)

     db.circuit = mock_cb
     return db

@pytest.fixture()
def mock_db2(request) -> Database:
     db = Mock(spec=Database)
     db.weight = request.param.get("weight", 1.0)
     db.state = request.param.get("state", State.ACTIVE)
     db.client = Mock(spec=Redis)

     cb = request.param.get("circuit", {})
     mock_cb = Mock(spec=CircuitBreaker)
     mock_cb.grace_period = cb.get("grace_period", 1.0)
     mock_cb.state = cb.get("state", CBState.CLOSED)

     db.circuit = mock_cb
     return db