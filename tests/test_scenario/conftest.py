import json
import os

import pytest

from redis.backoff import NoBackoff
from redis.multidb.client import MultiDBClient
from redis.multidb.config import DatabaseConfig, MultiDbConfig, DEFAULT_HEALTH_CHECK_INTERVAL, \
    DEFAULT_FAILURES_THRESHOLD
from redis.retry import Retry
from tests.test_scenario.fault_injector_client import FaultInjectorClient


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
def r_multi_db(request) -> MultiDBClient:
     endpoint_config = get_endpoint_config('re-active-active')
     username = endpoint_config.get('username', None)
     password = endpoint_config.get('password', None)
     failure_threshold = request.param.get('failure_threshold', DEFAULT_FAILURES_THRESHOLD)
     command_retry = request.param.get('command_retry', Retry(NoBackoff(), retries=3))
     health_check_interval = request.param.get('health_check_interval', DEFAULT_HEALTH_CHECK_INTERVAL)
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
     )

     return MultiDBClient(config)