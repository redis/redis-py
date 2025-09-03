from unittest.mock import Mock

import pytest

from redis.asyncio.multidb.config import MultiDbConfig, DEFAULT_HEALTH_CHECK_INTERVAL, DEFAULT_AUTO_FALLBACK_INTERVAL, \
    DatabaseConfig
from redis.asyncio.multidb.failover import AsyncFailoverStrategy
from redis.asyncio.multidb.failure_detector import AsyncFailureDetector
from redis.asyncio.multidb.healthcheck import HealthCheck
from redis.data_structure import WeightedList
from redis.multidb.circuit import State as CBState, CircuitBreaker
from redis.asyncio import Redis
from redis.asyncio.multidb.database import Database, Databases


@pytest.fixture()
def mock_client() -> Redis:
    return Mock(spec=Redis)

@pytest.fixture()
def mock_cb() -> CircuitBreaker:
    return Mock(spec=CircuitBreaker)

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
     mock_cb = Mock(spec=CircuitBreaker)
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
     mock_cb = Mock(spec=CircuitBreaker)
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
     mock_cb = Mock(spec=CircuitBreaker)
     mock_cb.grace_period = cb.get("grace_period", 1.0)
     mock_cb.state = cb.get("state", CBState.CLOSED)

     db.circuit = mock_cb
     return db

@pytest.fixture()
def mock_multi_db_config(
        request, mock_fd, mock_fs, mock_hc, mock_ed
) -> MultiDbConfig:
     hc_interval = request.param.get('hc_interval', None)
     if hc_interval is None:
          hc_interval = DEFAULT_HEALTH_CHECK_INTERVAL

     auto_fallback_interval = request.param.get('auto_fallback_interval', None)
     if auto_fallback_interval is None:
          auto_fallback_interval = DEFAULT_AUTO_FALLBACK_INTERVAL

     config = MultiDbConfig(
          databases_config=[Mock(spec=DatabaseConfig)],
          failure_detectors=[mock_fd],
          health_check_interval=hc_interval,
          failover_strategy=mock_fs,
          auto_fallback_interval=auto_fallback_interval,
          event_dispatcher=mock_ed
     )

     return config


def create_weighted_list(*databases) -> Databases:
    dbs = WeightedList()

    for db in databases:
        dbs.add(db, db.weight)

    return dbs