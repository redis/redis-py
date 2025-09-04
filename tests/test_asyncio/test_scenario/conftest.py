import os

import pytest

from redis.asyncio import Redis
from redis.asyncio.multidb.client import MultiDBClient
from redis.asyncio.multidb.config import DEFAULT_FAILURES_THRESHOLD, DEFAULT_HEALTH_CHECK_INTERVAL, DatabaseConfig, \
    MultiDbConfig
from redis.asyncio.multidb.event import AsyncActiveDatabaseChanged
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.event import AsyncEventListenerInterface, EventDispatcher
from tests.test_scenario.conftest import get_endpoint_config, extract_cluster_fqdn
from tests.test_scenario.fault_injector_client import FaultInjectorClient


class CheckActiveDatabaseChangedListener(AsyncEventListenerInterface):
    def __init__(self):
        self.is_changed_flag = False

    async def listen(self, event: AsyncActiveDatabaseChanged):
        self.is_changed_flag = True

@pytest.fixture()
def fault_injector_client():
     url = os.getenv("FAULT_INJECTION_API_URL", "http://127.0.0.1:20324")
     return FaultInjectorClient(url)

@pytest.fixture()
def r_multi_db(request) -> tuple[MultiDBClient, CheckActiveDatabaseChangedListener, dict]:
     client_class = request.param.get('client_class', Redis)

     if client_class == Redis:
        endpoint_config = get_endpoint_config('re-active-active')
     else:
        endpoint_config = get_endpoint_config('re-active-active-oss-cluster')

     username = endpoint_config.get('username', None)
     password = endpoint_config.get('password', None)
     failure_threshold = request.param.get('failure_threshold', DEFAULT_FAILURES_THRESHOLD)
     command_retry = request.param.get('command_retry', Retry(ExponentialBackoff(cap=2, base=0.05), retries=10))

     # Retry configuration different for health checks as initial health check require more time in case
     # if infrastructure wasn't restored from the previous test.
     health_check_interval = request.param.get('health_check_interval', DEFAULT_HEALTH_CHECK_INTERVAL)
     event_dispatcher = EventDispatcher()
     listener = CheckActiveDatabaseChangedListener()
     event_dispatcher.register_listeners({
         AsyncActiveDatabaseChanged: [listener],
     })
     db_configs = []

     db_config = DatabaseConfig(
         weight=1.0,
         from_url=endpoint_config['endpoints'][0],
         client_kwargs={
             'username': username,
             'password': password,
             'decode_responses': True,
         },
         health_check_url=extract_cluster_fqdn(endpoint_config['endpoints'][0])
     )
     db_configs.append(db_config)

     db_config1 = DatabaseConfig(
         weight=0.9,
         from_url=endpoint_config['endpoints'][1],
         client_kwargs={
             'username': username,
             'password': password,
             'decode_responses': True,
         },
         health_check_url=extract_cluster_fqdn(endpoint_config['endpoints'][1])
     )
     db_configs.append(db_config1)

     config = MultiDbConfig(
         client_class=client_class,
         databases_config=db_configs,
         command_retry=command_retry,
         failure_threshold=failure_threshold,
         health_check_retries=3,
         health_check_interval=health_check_interval,
         event_dispatcher=event_dispatcher,
         health_check_backoff=ExponentialBackoff(cap=5, base=0.5),
     )

     return MultiDBClient(config), listener, endpoint_config