import os
import random
from contextlib import asynccontextmanager as _asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import Union

import jwt
import pytest
import pytest_asyncio
import redis.asyncio as redis
from mock.mock import Mock
from packaging.version import Version
from redis.asyncio import Sentinel
from redis.asyncio.client import Monitor
from redis.asyncio.connection import Connection, parse_url
from redis.asyncio.retry import Retry
from redis.auth.idp import IdentityProviderInterface
from redis.auth.token import JWToken
from redis.backoff import NoBackoff
from redis.credentials import CredentialProvider
from redis_entraid.cred_provider import EntraIdCredentialsProvider, TokenAuthConfig
from redis_entraid.identity_provider import (
    ManagedIdentityIdType,
    ManagedIdentityType,
    create_provider_from_managed_identity,
    create_provider_from_service_principal,
)
from tests.conftest import REDIS_INFO

from .compat import mock


class AuthType(Enum):
    MANAGED_IDENTITY = "managed_identity"
    SERVICE_PRINCIPAL = "service_principal"


async def _get_info(redis_url):
    client = redis.Redis.from_url(redis_url)
    info = await client.info()
    await client.connection_pool.disconnect()
    return info


@pytest_asyncio.fixture(
    params=[
        pytest.param(
            (True,),
            marks=pytest.mark.skipif(
                'config.REDIS_INFO["cluster_enabled"]', reason="cluster mode enabled"
            ),
        ),
        (False,),
    ],
    ids=[
        "single",
        "pool",
    ],
)
async def create_redis(request):
    """Wrapper around redis.create_redis."""
    (single_connection,) = request.param

    teardown_clients = []

    async def client_factory(
        url: str = request.config.getoption("--redis-url"),
        cls=redis.Redis,
        flushdb=True,
        **kwargs,
    ):
        if "protocol" not in url and kwargs.get("protocol") is None:
            kwargs["protocol"] = request.config.getoption("--protocol")

        cluster_mode = REDIS_INFO["cluster_enabled"]
        if not cluster_mode:
            single = kwargs.pop("single_connection_client", False) or single_connection
            url_options = parse_url(url)
            url_options.update(kwargs)
            pool = redis.ConnectionPool(**url_options)
            client = cls(connection_pool=pool)
        else:
            client = redis.RedisCluster.from_url(url, **kwargs)
            await client.initialize()
            single = False
        if single:
            client = client.client()
            await client.initialize()

        async def teardown():
            if not cluster_mode:
                if flushdb and "username" not in kwargs:
                    try:
                        await client.flushdb()
                    except redis.ConnectionError:
                        # handle cases where a test disconnected a client
                        # just manually retry the flushdb
                        await client.flushdb()
                await client.aclose()
                await client.connection_pool.disconnect()
            else:
                if flushdb:
                    try:
                        await client.flushdb(target_nodes="primaries")
                    except redis.ConnectionError:
                        # handle cases where a test disconnected a client
                        # just manually retry the flushdb
                        await client.flushdb(target_nodes="primaries")
                await client.aclose()

        teardown_clients.append(teardown)
        return client

    yield client_factory

    for teardown in teardown_clients:
        await teardown()


@pytest_asyncio.fixture()
async def r(create_redis):
    return await create_redis()


@pytest_asyncio.fixture()
async def r2(create_redis):
    """A second client for tests that need multiple"""
    return await create_redis()


@pytest_asyncio.fixture()
async def decoded_r(create_redis):
    return await create_redis(decode_responses=True)


@pytest_asyncio.fixture()
async def sentinel_setup(local_cache, request):
    sentinel_ips = request.config.getoption("--sentinels")
    sentinel_endpoints = [
        (ip.strip(), int(port.strip()))
        for ip, port in (endpoint.split(":") for endpoint in sentinel_ips.split(","))
    ]
    kwargs = request.param.get("kwargs", {}) if hasattr(request, "param") else {}
    sentinel = Sentinel(
        sentinel_endpoints,
        socket_timeout=0.1,
        client_cache=local_cache,
        protocol=3,
        **kwargs,
    )
    yield sentinel
    for s in sentinel.sentinels:
        await s.aclose()


@pytest_asyncio.fixture()
async def master(request, sentinel_setup):
    master_service = request.config.getoption("--master-service")
    master = sentinel_setup.master_for(master_service)
    yield master
    await master.aclose()


def _gen_cluster_mock_resp(r, response):
    connection = mock.AsyncMock(spec=Connection)
    connection.retry = Retry(NoBackoff(), 0)
    connection.read_response.return_value = response
    with mock.patch.object(r, "connection", connection):
        yield r


@pytest_asyncio.fixture()
async def mock_cluster_resp_ok(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    for mocked in _gen_cluster_mock_resp(r, "OK"):
        yield mocked


@pytest_asyncio.fixture()
async def mock_cluster_resp_int(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    for mocked in _gen_cluster_mock_resp(r, 2):
        yield mocked


@pytest_asyncio.fixture()
async def mock_cluster_resp_info(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    response = (
        "cluster_state:ok\r\ncluster_slots_assigned:16384\r\n"
        "cluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\n"
        "cluster_slots_fail:0\r\ncluster_known_nodes:7\r\n"
        "cluster_size:3\r\ncluster_current_epoch:7\r\n"
        "cluster_my_epoch:2\r\ncluster_stats_messages_sent:170262\r\n"
        "cluster_stats_messages_received:105653\r\n"
    )
    for mocked in _gen_cluster_mock_resp(r, response):
        yield mocked


@pytest_asyncio.fixture()
async def mock_cluster_resp_nodes(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    response = (
        "c8253bae761cb1ecb2b61857d85dfe455a0fec8b 172.17.0.7:7006 "
        "slave aa90da731f673a99617dfe930306549a09f83a6b 0 "
        "1447836263059 5 connected\n"
        "9bd595fe4821a0e8d6b99d70faa660638a7612b3 172.17.0.7:7008 "
        "master - 0 1447836264065 0 connected\n"
        "aa90da731f673a99617dfe930306549a09f83a6b 172.17.0.7:7003 "
        "myself,master - 0 0 2 connected 5461-10922\n"
        "1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
        "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
        "1447836262556 3 connected\n"
        "4ad9a12e63e8f0207025eeba2354bcf4c85e5b22 172.17.0.7:7005 "
        "master - 0 1447836262555 7 connected 0-5460\n"
        "19efe5a631f3296fdf21a5441680f893e8cc96ec 172.17.0.7:7004 "
        "master - 0 1447836263562 3 connected 10923-16383\n"
        "fbb23ed8cfa23f17eaf27ff7d0c410492a1093d6 172.17.0.7:7002 "
        "master,fail - 1447829446956 1447829444948 1 disconnected\n"
    )
    for mocked in _gen_cluster_mock_resp(r, response):
        yield mocked


@pytest_asyncio.fixture()
async def mock_cluster_resp_slaves(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    response = (
        "['1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
        "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
        "1447836789290 3 connected']"
    )
    for mocked in _gen_cluster_mock_resp(r, response):
        yield mocked


def mock_identity_provider() -> IdentityProviderInterface:
    mock_provider = Mock(spec=IdentityProviderInterface)
    token = {"exp": datetime.now(timezone.utc).timestamp() + 3600, "oid": "username"}
    encoded = jwt.encode(token, "secret", algorithm="HS256")
    jwt_token = JWToken(encoded)
    mock_provider.request_token.return_value = jwt_token
    return mock_provider


def identity_provider(request) -> IdentityProviderInterface:
    if hasattr(request, "param"):
        kwargs = request.param.get("idp_kwargs", {})
    else:
        kwargs = {}

    if request.param.get("mock_idp", None) is not None:
        return mock_identity_provider()

    auth_type = kwargs.pop("auth_type", AuthType.SERVICE_PRINCIPAL)

    if auth_type == "MANAGED_IDENTITY":
        return _get_managed_identity_provider(request)

    return _get_service_principal_provider(request)


def _get_managed_identity_provider(request):
    authority = os.getenv("AZURE_AUTHORITY")
    resource = os.getenv("AZURE_RESOURCE")
    id_value = os.getenv("AZURE_ID_VALUE", None)

    if hasattr(request, "param"):
        kwargs = request.param.get("idp_kwargs", {})
    else:
        kwargs = {}

    identity_type = kwargs.pop("identity_type", ManagedIdentityType.SYSTEM_ASSIGNED)
    id_type = kwargs.pop("id_type", ManagedIdentityIdType.CLIENT_ID)

    return create_provider_from_managed_identity(
        identity_type=identity_type,
        resource=resource,
        id_type=id_type,
        id_value=id_value,
        authority=authority,
        **kwargs,
    )


def _get_service_principal_provider(request):
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_credential = os.getenv("AZURE_CLIENT_SECRET")
    authority = os.getenv("AZURE_AUTHORITY")
    scopes = os.getenv("AZURE_REDIS_SCOPES", [])

    if hasattr(request, "param"):
        kwargs = request.param.get("idp_kwargs", {})
        token_kwargs = request.param.get("token_kwargs", {})
        timeout = request.param.get("timeout", None)
    else:
        kwargs = {}
        token_kwargs = {}
        timeout = None

    if isinstance(scopes, str):
        scopes = scopes.split(",")

    return create_provider_from_service_principal(
        client_id=client_id,
        client_credential=client_credential,
        scopes=scopes,
        timeout=timeout,
        token_kwargs=token_kwargs,
        authority=authority,
        **kwargs,
    )


def get_credential_provider(request) -> CredentialProvider:
    cred_provider_class = request.param.get("cred_provider_class")
    cred_provider_kwargs = request.param.get("cred_provider_kwargs", {})

    if cred_provider_class != EntraIdCredentialsProvider:
        return cred_provider_class(**cred_provider_kwargs)

    idp = identity_provider(request)
    initial_delay_in_ms = cred_provider_kwargs.get("initial_delay_in_ms", 0)
    block_for_initial = cred_provider_kwargs.get("block_for_initial", False)
    expiration_refresh_ratio = cred_provider_kwargs.get(
        "expiration_refresh_ratio", TokenAuthConfig.DEFAULT_EXPIRATION_REFRESH_RATIO
    )
    lower_refresh_bound_millis = cred_provider_kwargs.get(
        "lower_refresh_bound_millis", TokenAuthConfig.DEFAULT_LOWER_REFRESH_BOUND_MILLIS
    )
    max_attempts = cred_provider_kwargs.get(
        "max_attempts", TokenAuthConfig.DEFAULT_MAX_ATTEMPTS
    )
    delay_in_ms = cred_provider_kwargs.get(
        "delay_in_ms", TokenAuthConfig.DEFAULT_DELAY_IN_MS
    )

    auth_config = TokenAuthConfig(idp)
    auth_config.expiration_refresh_ratio = expiration_refresh_ratio
    auth_config.lower_refresh_bound_millis = lower_refresh_bound_millis
    auth_config.max_attempts = max_attempts
    auth_config.delay_in_ms = delay_in_ms

    return EntraIdCredentialsProvider(
        config=auth_config,
        initial_delay_in_ms=initial_delay_in_ms,
        block_for_initial=block_for_initial,
    )


@pytest_asyncio.fixture()
async def credential_provider(request) -> CredentialProvider:
    return get_credential_provider(request)


async def wait_for_command(
    client: redis.Redis, monitor: Monitor, command: str, key: Union[str, None] = None
):
    # issue a command with a key name that's local to this process.
    # if we find a command with our key before the command we're waiting
    # for, something went wrong
    if key is None:
        # generate key
        redis_version = REDIS_INFO["version"]
        if Version(redis_version) >= Version("5.0.0"):
            id_str = str(await client.client_id())
        else:
            id_str = f"{random.randrange(2 ** 32):08x}"
        key = f"__REDIS-PY-{id_str}__"
    await client.get(key)
    while True:
        monitor_response = await monitor.next_command()
        if command in monitor_response["command"]:
            return monitor_response
        if key in monitor_response["command"]:
            return None


# python 3.6 doesn't have the asynccontextmanager decorator.  Provide it here.
class AsyncContextManager:
    def __init__(self, async_generator):
        self.gen = async_generator

    async def __aenter__(self):
        try:
            return await self.gen.__anext__()
        except StopAsyncIteration as err:
            raise RuntimeError("Pickles") from err

    async def __aexit__(self, exc_type, exc_inst, tb):
        if exc_type:
            await self.gen.athrow(exc_type, exc_inst, tb)
            return True
        try:
            await self.gen.__anext__()
        except StopAsyncIteration:
            return
        raise RuntimeError("More pickles")


def asynccontextmanager(func):
    return _asynccontextmanager(func)


# helpers to get the connection arguments for this run
@pytest.fixture()
def redis_url(request):
    return request.config.getoption("--redis-url")


@pytest.fixture()
def connect_args(request):
    url = request.config.getoption("--redis-url")
    return parse_url(url)
