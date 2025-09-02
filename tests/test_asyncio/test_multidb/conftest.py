from unittest.mock import Mock

import pytest

from redis.asyncio.multidb.failover import AsyncFailoverStrategy
from redis.asyncio.multidb.failure_detector import AsyncFailureDetector
from redis.asyncio.multidb.healthcheck import HealthCheck
from redis.data_structure import WeightedList
from redis.multidb.circuit import State as CBState
from redis.asyncio import Redis
from redis.asyncio.multidb.circuit import AsyncCircuitBreaker
from redis.asyncio.multidb.database import Database, Databases


@pytest.fixture()
def mock_client() -> Redis:
    return Mock(spec=Redis)

@pytest.fixture()
def mock_cb() -> AsyncCircuitBreaker:
    return Mock(spec=AsyncCircuitBreaker)

@pytest.fixture()
def mock_fd() -> AsyncFailureDetector:
     return Mock(spec=AsyncFailureDetector)

@pytest.fixture()
def mock_fs() -> AsyncFailoverStrategy:
     return Mock(spec=AsyncFailoverStrategy)

@pytest.fixture()
def mock_hc() -> HealthCheck:
     return Mock(spec=HealthCheck)

@pytest.fixture()
def mock_db(request) -> Database:
     db = Mock(spec=Database)
     db.weight = request.param.get("weight", 1.0)
     db.client = Mock(spec=Redis)

     cb = request.param.get("circuit", {})
     mock_cb = Mock(spec=AsyncCircuitBreaker)
     mock_cb.grace_period = cb.get("grace_period", 1.0)
     mock_cb.state = cb.get("state", CBState.CLOSED)

     db.circuit = mock_cb
     return db

@pytest.fixture()
def mock_db1(request) -> Database:
     db = Mock(spec=Database)
     db.weight = request.param.get("weight", 1.0)
     db.client = Mock(spec=Redis)

     cb = request.param.get("circuit", {})
     mock_cb = Mock(spec=AsyncCircuitBreaker)
     mock_cb.grace_period = cb.get("grace_period", 1.0)
     mock_cb.state = cb.get("state", CBState.CLOSED)

     db.circuit = mock_cb
     return db

@pytest.fixture()
def mock_db2(request) -> Database:
     db = Mock(spec=Database)
     db.weight = request.param.get("weight", 1.0)
     db.client = Mock(spec=Redis)

     cb = request.param.get("circuit", {})
     mock_cb = Mock(spec=AsyncCircuitBreaker)
     mock_cb.grace_period = cb.get("grace_period", 1.0)
     mock_cb.state = cb.get("state", CBState.CLOSED)

     db.circuit = mock_cb
     return db


def create_weighted_list(*databases) -> Databases:
    dbs = WeightedList()

    for db in databases:
        dbs.add(db, db.weight)

    return dbs