import json
import logging
import os
import re
from typing import Optional
from urllib.parse import urlparse

import pytest

from redis import Redis
from redis.backoff import NoBackoff, ExponentialBackoff
from redis.event import EventDispatcher, EventListenerInterface
from redis.multidb.client import MultiDBClient
from redis.multidb.config import DatabaseConfig, MultiDbConfig, DEFAULT_HEALTH_CHECK_INTERVAL
from redis.multidb.event import ActiveDatabaseChanged
from redis.multidb.failure_detector import DEFAULT_FAILURES_THRESHOLD
from redis.multidb.healthcheck import EchoHealthCheck, DEFAULT_HEALTH_CHECK_DELAY
from redis.backoff import ExponentialWithJitterBackoff, NoBackoff
from redis.client import Redis
from redis.maintenance_events import EndpointType, MaintenanceEventsConfig
from redis.retry import Retry
from tests.test_scenario.fault_injector_client import FaultInjectorClient

RELAX_TIMEOUT = 30
CLIENT_TIMEOUT = 5

DEFAULT_ENDPOINT_NAME = "m-standard"

class CheckActiveDatabaseChangedListener(EventListenerInterface):
    def __init__(self):
        self.is_changed_flag = False

    def listen(self, event: ActiveDatabaseChanged):
        self.is_changed_flag = True

@pytest.fixture()
def endpoint_name(request):
    return request.config.getoption("--endpoint-name") or os.getenv(
        "REDIS_ENDPOINT_NAME", DEFAULT_ENDPOINT_NAME
    )


def get_endpoints_config(endpoint_name: str):
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
def r_multi_db(request) -> tuple[MultiDBClient, CheckActiveDatabaseChangedListener, dict]:
     client_class = request.param.get('client_class', Redis)

     if client_class == Redis:
        endpoint_config = get_endpoints_config('re-active-active')
     else:
        endpoint_config = get_endpoints_config('re-active-active-oss-cluster')

     username = endpoint_config.get('username', None)
     password = endpoint_config.get('password', None)
     failure_threshold = request.param.get('failure_threshold', DEFAULT_FAILURES_THRESHOLD)
     command_retry = request.param.get('command_retry', Retry(ExponentialBackoff(cap=2, base=0.05), retries=10))

     # Retry configuration different for health checks as initial health check require more time in case
     # if infrastructure wasn't restored from the previous test.
     health_check_interval = request.param.get('health_check_interval', DEFAULT_HEALTH_CHECK_INTERVAL)
     health_check_delay = request.param.get('health_check_delay', DEFAULT_HEALTH_CHECK_DELAY)
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
         health_check_probes=3,
         health_check_interval=health_check_interval,
         event_dispatcher=event_dispatcher,
         health_check_delay=health_check_delay,
     )

     return MultiDBClient(config), listener, endpoint_config


def extract_cluster_fqdn(url):
    """
    Extract Cluster FQDN from Redis URL
    """
    # Parse the URL
    parsed = urlparse(url)

    # Extract hostname and port
    hostname = parsed.hostname
    port = parsed.port

    # Remove the 'redis-XXXX.' prefix using regex
    # This pattern matches 'redis-' followed by digits and a dot
    cleaned_hostname = re.sub(r'^redis-\d+\.', '', hostname)

    # Reconstruct the URL
    return f"https://{cleaned_hostname}"

@pytest.fixture()
def client_maint_events(endpoints_config):
    return _get_client_maint_events(endpoints_config)

def _get_client_maint_events(
    endpoints_config,
    enable_maintenance_events: bool = True,
    endpoint_type: Optional[EndpointType] = None,
    enable_relax_timeout: bool = True,
    enable_proactive_reconnect: bool = True,
    disable_retries: bool = False,
    socket_timeout: Optional[float] = None,
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
    host = parsed.hostname
    port = parsed.port

    tls_enabled = True if parsed.scheme == "rediss" else False

    if not host:
        raise ValueError(f"Could not parse host from endpoint URL: {endpoints[0]}")

    logging.info(f"Connecting to Redis Enterprise: {host}:{port} with user: {username}")

    # Configure maintenance events
    maintenance_config = MaintenanceEventsConfig(
        enabled=enable_maintenance_events,
        proactive_reconnect=enable_proactive_reconnect,
        relax_timeout=RELAX_TIMEOUT if enable_relax_timeout else -1,
        endpoint_type=endpoint_type,
    )

    # Create Redis client with maintenance events config
    # This will automatically create the MaintenanceEventPoolHandler
    if disable_retries:
        retry = Retry(NoBackoff(), 0)
    else:
        retry = Retry(backoff=ExponentialWithJitterBackoff(base=1, cap=10), retries=3)

    client = Redis(
        host=host,
        port=port,
        socket_timeout=CLIENT_TIMEOUT if socket_timeout is None else socket_timeout,
        username=username,
        password=password,
        ssl=tls_enabled,
        ssl_cert_reqs="none",
        ssl_check_hostname=False,
        protocol=3,  # RESP3 required for push notifications
        maintenance_events_config=maintenance_config,
        retry=retry,
    )
    logging.info("Redis client created with maintenance events enabled")
    logging.info(f"Client uses Protocol: {client.connection_pool.get_protocol()}")
    maintenance_handler_exists = client.maintenance_events_pool_handler is not None
    logging.info(f"Maintenance events pool handler: {maintenance_handler_exists}")

    return client