import asyncio
import os
from typing import Any, AsyncGenerator, Optional
from urllib.parse import urlparse

import pytest
import pytest_asyncio

from redis.asyncio import Redis
from redis.asyncio.multidb.client import MultiDBClient
from redis.asyncio.multidb.config import (
    DatabaseConfig,
    MultiDbConfig,
)
from redis.asyncio.multidb.event import AsyncActiveDatabaseChanged
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff, ExponentialWithJitterBackoff, NoBackoff
from redis.event import AsyncEventListenerInterface, EventDispatcher
from redis.maint_notifications import EndpointType, MaintNotificationsConfig
from redis.multidb.failure_detector import DEFAULT_MIN_NUM_FAILURES
from tests.test_scenario.conftest import (
    CLIENT_TIMEOUT,
    RELAXED_TIMEOUT,
    _prepare_ssl_certificates,
    extract_cluster_fqdn,
    get_endpoints_config,
    use_mock_proxy,
)
from tests.test_asyncio.test_scenario.async_fault_injector_client import (
    AsyncProxyServerFaultInjector,
    AsyncREFaultInjector,
)


class CheckActiveDatabaseChangedListener(AsyncEventListenerInterface):
    def __init__(self):
        self.is_changed_flag = False

    async def listen(self, event: AsyncActiveDatabaseChanged):
        self.is_changed_flag = True


@pytest.fixture()
def fault_injector_client():
    if use_mock_proxy():
        return AsyncProxyServerFaultInjector(oss_cluster=False)
    else:
        url = os.getenv("FAULT_INJECTION_API_URL", "http://127.0.0.1:20324")
        return AsyncREFaultInjector(url)


def _get_async_client_maint_notifications(
    endpoints_config,
    protocol: int = 3,
    enable_maintenance_notifications: bool = True,
    endpoint_type: Optional[EndpointType] = None,
    enable_relaxed_timeout: bool = True,
    enable_proactive_reconnect: bool = True,
    disable_retries: bool = False,
    auth_ssl_client_certs: bool = False,
    socket_timeout: Optional[float] = None,
    host_config: Optional[str] = None,
) -> Redis:
    """Create async Redis client with maintenance notifications enabled."""
    username = endpoints_config.get("username")
    password = endpoints_config.get("password")

    endpoints = endpoints_config.get("endpoints", [])
    if not endpoints:
        raise ValueError("No endpoints found in configuration")

    parsed = urlparse(endpoints[0])
    host = parsed.hostname if host_config is None else host_config
    port = parsed.port

    if not host:
        raise ValueError(f"Could not parse host from endpoint URL: {endpoints[0]}")

    if port is None:
        raise ValueError(f"Could not parse port from endpoint URL: {endpoints[0]}")

    maintenance_config = MaintNotificationsConfig(
        enabled=enable_maintenance_notifications,
        proactive_reconnect=enable_proactive_reconnect,
        relaxed_timeout=RELAXED_TIMEOUT if enable_relaxed_timeout else -1,
        endpoint_type=endpoint_type,
    )

    if disable_retries:
        retry = Retry(NoBackoff(), 0)
    else:
        retry = Retry(
            backoff=ExponentialWithJitterBackoff(base=0.01, cap=1), retries=10
        )

    tls_enabled = parsed.scheme == "rediss"
    tls_kwargs = {"ssl": tls_enabled}
    if tls_enabled:
        ssl_config = _prepare_ssl_certificates(auth_ssl_client_certs)
        tls_kwargs.update(ssl_config)

    return Redis(
        host=host,
        port=port,
        socket_timeout=CLIENT_TIMEOUT if socket_timeout is None else socket_timeout,
        username=username,
        password=password,
        protocol=protocol,
        maint_notifications_config=maintenance_config,
        retry=retry,
        **tls_kwargs,
    )


def get_async_standalone_client_maint_notifications(
    endpoints_config,
    protocol: int = 3,
    enable_maintenance_notifications: bool = True,
    endpoint_type: Optional[EndpointType] = None,
    enable_relaxed_timeout: bool = True,
    enable_proactive_reconnect: bool = True,
    disable_retries: bool = False,
    auth_ssl_client_certs: bool = False,
    socket_timeout: Optional[float] = None,
) -> Redis:
    """Create async Redis standalone client with maintenance notifications enabled."""
    return _get_async_client_maint_notifications(
        endpoints_config=endpoints_config,
        protocol=protocol,
        enable_maintenance_notifications=enable_maintenance_notifications,
        endpoint_type=endpoint_type,
        enable_relaxed_timeout=enable_relaxed_timeout,
        enable_proactive_reconnect=enable_proactive_reconnect,
        disable_retries=disable_retries,
        auth_ssl_client_certs=auth_ssl_client_certs,
        socket_timeout=socket_timeout,
    )


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
