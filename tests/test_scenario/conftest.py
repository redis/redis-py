import json
import logging
import os
from typing import Optional
from urllib.parse import urlparse
import pytest

from redis.backoff import ExponentialWithJitterBackoff, NoBackoff
from redis.client import Redis
from redis.maintenance_events import EndpointType, MaintNotificationsConfig
from redis.retry import Retry
from tests.test_scenario.fault_injector_client import FaultInjectorClient

RELAXED_TIMEOUT = 30
CLIENT_TIMEOUT = 5

DEFAULT_ENDPOINT_NAME = "m-standard"


@pytest.fixture()
def endpoint_name(request):
    return request.config.getoption("--endpoint-name") or os.getenv(
        "REDIS_ENDPOINT_NAME", DEFAULT_ENDPOINT_NAME
    )


@pytest.fixture()
def endpoints_config(endpoint_name: str):
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
def client_maint_events(endpoints_config):
    return _get_client_maint_events(endpoints_config)


def _get_client_maint_events(
    endpoints_config,
    protocol: int = 3,
    enable_maintenance_events: bool = True,
    endpoint_type: Optional[EndpointType] = None,
    enable_relaxed_timeout: bool = True,
    enable_proactive_reconnect: bool = True,
    disable_retries: bool = False,
    socket_timeout: Optional[float] = None,
    host_config: Optional[str] = None,
):
    """Create Redis client with maintenance events enabled."""

    # Get credentials from the configuration
    username = endpoints_config.get("username")
    password = endpoints_config.get("password")

    # Parse host and port from endpoints URL
    endpoints = endpoints_config.get("endpoints", [])
    if not endpoints:
        raise ValueError("No endpoints found in configuration")

    parsed = urlparse(endpoints[0])
    host = parsed.hostname if host_config is None else host_config
    port = parsed.port

    if not host:
        raise ValueError(f"Could not parse host from endpoint URL: {endpoints[0]}")

    logging.info(f"Connecting to Redis Enterprise: {host}:{port} with user: {username}")

    # Configure maintenance events
    maintenance_config = MaintNotificationsConfig(
        enabled=enable_maintenance_events,
        proactive_reconnect=enable_proactive_reconnect,
        relaxed_timeout=RELAXED_TIMEOUT if enable_relaxed_timeout else -1,
        endpoint_type=endpoint_type,
    )

    # Create Redis client with maintenance notifications config
    # This will automatically create the MaintNotificationsPoolHandler
    if disable_retries:
        retry = Retry(NoBackoff(), 0)
    else:
        retry = Retry(backoff=ExponentialWithJitterBackoff(base=1, cap=10), retries=3)

    tls_enabled = True if parsed.scheme == "rediss" else False
    logging.info(f"TLS enabled: {tls_enabled}")

    client = Redis(
        host=host,
        port=port,
        socket_timeout=CLIENT_TIMEOUT if socket_timeout is None else socket_timeout,
        username=username,
        password=password,
        ssl=tls_enabled,
        ssl_cert_reqs="none",
        ssl_check_hostname=False,
        protocol=protocol,  # RESP3 required for push notifications
        maint_notifications_config=maintenance_config,
        retry=retry,
    )
    logging.info("Redis client created with maintenance events enabled")
    logging.info(f"Client uses Protocol: {client.connection_pool.get_protocol()}")
    maintenance_handler_exists = client.maint_notifications_pool_handler is not None
    logging.info(f"Maintenance events pool handler: {maintenance_handler_exists}")

    return client
