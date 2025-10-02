import asyncio
import os
from typing import Any, AsyncGenerator

import pytest
import pytest_asyncio

from redis.asyncio import Redis, RedisCluster
from redis.asyncio.multidb.client import MultiDBClient
from redis.asyncio.multidb.config import (
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DatabaseConfig,
    MultiDbConfig,
)
from redis.asyncio.multidb.event import AsyncActiveDatabaseChanged
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.event import AsyncEventListenerInterface, EventDispatcher
from redis.multidb.failure_detector import DEFAULT_MIN_NUM_FAILURES
from tests.test_scenario.conftest import get_endpoints_config, extract_cluster_fqdn
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


@pytest_asyncio.fixture()
async def r_multi_db(
    request,
) -> AsyncGenerator[tuple[MultiDBClient, CheckActiveDatabaseChangedListener, Any], Any]:
    client_class = request.param.get("client_class", Redis)

    if client_class == Redis:
        endpoint_config = get_endpoints_config("re-active-active")
    else:
        endpoint_config = get_endpoints_config("re-active-active-oss-cluster")

    username = endpoint_config.get("username", None)
    password = endpoint_config.get("password", None)
    min_num_failures = request.param.get("min_num_failures", DEFAULT_MIN_NUM_FAILURES)
    command_retry = request.param.get(
        "command_retry", Retry(ExponentialBackoff(cap=0.1, base=0.01), retries=10)
    )

    # Retry configuration different for health checks as initial health check require more time in case
    # if infrastructure wasn't restored from the previous test.
    health_check_interval = request.param.get("health_check_interval", 10)
    health_checks = request.param.get("health_checks", [])
    event_dispatcher = EventDispatcher()
    listener = CheckActiveDatabaseChangedListener()
    event_dispatcher.register_listeners(
        {
            AsyncActiveDatabaseChanged: [listener],
        }
    )
    db_configs = []

    db_config = DatabaseConfig(
        weight=1.0,
        from_url=endpoint_config["endpoints"][0],
        client_kwargs={
            "username": username,
            "password": password,
            "decode_responses": True,
        },
        health_check_url=extract_cluster_fqdn(endpoint_config["endpoints"][0]),
    )
    db_configs.append(db_config)

    db_config1 = DatabaseConfig(
        weight=0.9,
        from_url=endpoint_config["endpoints"][1],
        client_kwargs={
            "username": username,
            "password": password,
            "decode_responses": True,
        },
        health_check_url=extract_cluster_fqdn(endpoint_config["endpoints"][1]),
    )
    db_configs.append(db_config1)

    config = MultiDbConfig(
        client_class=client_class,
        databases_config=db_configs,
        command_retry=command_retry,
        min_num_failures=min_num_failures,
        health_checks=health_checks,
        health_check_probes=3,
        health_check_interval=health_check_interval,
        event_dispatcher=event_dispatcher,
    )

    client = MultiDBClient(config)

    async def teardown():
        await client.aclose()

        if client.command_executor.active_database and isinstance(
            client.command_executor.active_database.client, Redis
        ):
            await client.command_executor.active_database.client.connection_pool.disconnect()

        await asyncio.sleep(10)

    yield client, listener, endpoint_config
    await teardown()
