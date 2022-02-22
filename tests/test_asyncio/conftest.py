import asyncio
import random
import sys
from typing import Union
from urllib.parse import urlparse

if sys.version_info[0:2] == (3, 6):
    import pytest as pytest_asyncio
else:
    import pytest_asyncio

import pytest
from packaging.version import Version

import redis.asyncio as redis
from redis.asyncio.client import Monitor
from redis.asyncio.connection import (
    HIREDIS_AVAILABLE,
    HiredisParser,
    PythonParser,
    parse_url,
)
from tests.conftest import REDIS_INFO

from .compat import mock


async def _get_info(redis_url):
    client = redis.Redis.from_url(redis_url)
    info = await client.info()
    await client.connection_pool.disconnect()
    return info


@pytest_asyncio.fixture(
    params=[
        (True, PythonParser),
        (False, PythonParser),
        pytest.param(
            (True, HiredisParser),
            marks=pytest.mark.skipif(
                not HIREDIS_AVAILABLE, reason="hiredis is not installed"
            ),
        ),
        pytest.param(
            (False, HiredisParser),
            marks=pytest.mark.skipif(
                not HIREDIS_AVAILABLE, reason="hiredis is not installed"
            ),
        ),
    ],
    ids=[
        "single-python-parser",
        "pool-python-parser",
        "single-hiredis",
        "pool-hiredis",
    ],
)
def create_redis(request, event_loop: asyncio.BaseEventLoop):
    """Wrapper around redis.create_redis."""
    single_connection, parser_cls = request.param

    async def f(url: str = request.config.getoption("--redis-url"), **kwargs):
        single = kwargs.pop("single_connection_client", False) or single_connection
        parser_class = kwargs.pop("parser_class", None) or parser_cls
        url_options = parse_url(url)
        url_options.update(kwargs)
        pool = redis.ConnectionPool(parser_class=parser_class, **url_options)
        client: redis.Redis = redis.Redis(connection_pool=pool)
        if single:
            client = client.client()
            await client.initialize()

        def teardown():
            async def ateardown():
                if "username" in kwargs:
                    return
                try:
                    await client.flushdb()
                except redis.ConnectionError:
                    # handle cases where a test disconnected a client
                    # just manually retry the flushdb
                    await client.flushdb()
                await client.close()
                await client.connection_pool.disconnect()

            if event_loop.is_running():
                event_loop.create_task(ateardown())
            else:
                event_loop.run_until_complete(ateardown())

        request.addfinalizer(teardown)

        return client

    return f


@pytest_asyncio.fixture()
async def r(create_redis):
    yield await create_redis()


@pytest_asyncio.fixture()
async def r2(create_redis):
    """A second client for tests that need multiple"""
    yield await create_redis()


def _gen_cluster_mock_resp(r, response):
    connection = mock.AsyncMock()
    connection.read_response.return_value = response
    r.connection = connection
    return r


@pytest_asyncio.fixture()
async def mock_cluster_resp_ok(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    return _gen_cluster_mock_resp(r, "OK")


@pytest_asyncio.fixture()
async def mock_cluster_resp_int(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    return _gen_cluster_mock_resp(r, "2")


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
    return _gen_cluster_mock_resp(r, response)


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
    return _gen_cluster_mock_resp(r, response)


@pytest_asyncio.fixture()
async def mock_cluster_resp_slaves(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    response = (
        "['1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
        "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
        "1447836789290 3 connected']"
    )
    return _gen_cluster_mock_resp(r, response)


@pytest_asyncio.fixture(scope="session")
def master_host(request):
    url = request.config.getoption("--redis-url")
    parts = urlparse(url)
    yield parts.hostname


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
            id_str = str(client.client_id())
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
