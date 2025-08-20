import json
import os

import pytest

from redis.backoff import NoBackoff, ExponentialBackoff
from redis.event import EventDispatcher, EventListenerInterface
from redis.multidb.client import MultiDBClient
from redis.multidb.config import DatabaseConfig, MultiDbConfig, DEFAULT_HEALTH_CHECK_INTERVAL, \
    DEFAULT_FAILURES_THRESHOLD
from redis.multidb.event import ActiveDatabaseChanged
from redis.multidb.healthcheck import EchoHealthCheck
from redis.retry import Retry
from tests.test_scenario.fault_injector_client import FaultInjectorClient

class CheckActiveDatabaseChangedListener(EventListenerInterface):
    def __init__(self):
        self.is_changed_flag = False

    def listen(self, event: ActiveDatabaseChanged):
        self.is_changed_flag = True

def get_endpoint_config(endpoint_name: str):
    endpoints_config = os.getenv("REDIS_ENDPOINTS_CONFIG_PATH", None)

    if not (endpoints_config and os.path.exists(endpoints_config)):
        raise FileNotFoundError(f"Endpoints config file not found: {endpoints_config}")

    try:
        with open(endpoints_config, "r") as f:
            data = json.load(f)
            db = data[endpoint_name]
            return db
    except Exception as e:
        raise ValueError(
            f"Failed to load endpoints config file: {endpoints_config}"
        ) from e

@pytest.fixture()
def fault_injector_client():
     url = os.getenv("FAULT_INJECTION_API_URL", "http://127.0.0.1:20324")
     return FaultInjectorClient(url)

@pytest.fixture()
def r_multi_db(request) -> tuple[MultiDBClient, CheckActiveDatabaseChangedListener]:
     endpoint_config = get_endpoint_config('re-active-active')
     username = endpoint_config.get('username', None)
     password = endpoint_config.get('password', None)
     failure_threshold = request.param.get('failure_threshold', DEFAULT_FAILURES_THRESHOLD)
     command_retry = request.param.get('command_retry', Retry(ExponentialBackoff(cap=0.5, base=0.05), retries=3))

     # Retry configuration different for health checks as initial health check require more time in case
     # if infrastructure wasn't restored from the previous test.
     health_check_interval = request.param.get('health_check_interval', DEFAULT_HEALTH_CHECK_INTERVAL)
     event_dispatcher = EventDispatcher()
     listener = CheckActiveDatabaseChangedListener()
     event_dispatcher.register_listeners({
         ActiveDatabaseChanged: [listener],
     })
     db_configs = []

     db_config = DatabaseConfig(
         weight=1.0,
         from_url=endpoint_config['endpoints'][0],
         client_kwargs={
             'username': username,
             'password': password,
             'decode_responses': True,
         }
     )
     db_configs.append(db_config)

     db_config1 = DatabaseConfig(
         weight=0.9,
         from_url=endpoint_config['endpoints'][1],
         client_kwargs={
             'username': username,
             'password': password,
             'decode_responses': True,
         }
     )
     db_configs.append(db_config1)

     config = MultiDbConfig(
         databases_config=db_configs,
         command_retry=command_retry,
         failure_threshold=failure_threshold,
         health_check_interval=health_check_interval,
         health_check_backoff=ExponentialBackoff(cap=0.5, base=0.05),
         health_check_retries=3,
         event_dispatcher=event_dispatcher,
     )

     return MultiDBClient(config), listener