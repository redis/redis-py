from unittest.mock import Mock

import pytest

from redis import Redis
from redis.data_structure import WeightedList
from redis.multidb.circuit import State as CBState, CircuitBreaker
from redis.multidb.config import MultiDbConfig, DatabaseConfig, DEFAULT_HEALTH_CHECK_INTERVAL, \
    DEFAULT_AUTO_FALLBACK_INTERVAL
from redis.multidb.database import Database, Databases
from redis.multidb.failover import FailoverStrategy
from redis.multidb.failure_detector import FailureDetector
from redis.multidb.healthcheck import HealthCheck, DEFAULT_HEALTH_CHECK_PROBES, DEFAULT_HEALTH_CHECK_POLICY
from tests.conftest import mock_ed


@pytest.fixture()
def mock_client() -> Redis:
    return Mock(spec=Redis)

@pytest.fixture()
def mock_cb() -> CircuitBreaker:
    return Mock(spec=CircuitBreaker)

@pytest.fixture()
def mock_fd() -> FailureDetector:
     return Mock(spec=FailureDetector)

@pytest.fixture()
def mock_fs() -> FailoverStrategy:
     return Mock(spec=FailoverStrategy)

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
     hc_interval = request.param.get('hc_interval', DEFAULT_HEALTH_CHECK_INTERVAL)
     auto_fallback_interval = request.param.get('auto_fallback_interval', DEFAULT_AUTO_FALLBACK_INTERVAL)
     health_check_policy = request.param.get('health_check_policy', DEFAULT_HEALTH_CHECK_POLICY)
     health_check_probes = request.param.get('health_check_probes', DEFAULT_HEALTH_CHECK_PROBES)

     config = MultiDbConfig(
          databases_config=[Mock(spec=DatabaseConfig)],
          failure_detectors=[mock_fd],
          health_check_interval=hc_interval,
          health_check_delay=0.05,
          health_check_policy=health_check_policy,
          health_check_probes=health_check_probes,
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