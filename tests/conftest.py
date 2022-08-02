import argparse
import random
import time
from typing import Callable, TypeVar
from unittest.mock import Mock
from urllib.parse import urlparse

import pytest
from packaging.version import Version

import redis
from redis.backoff import NoBackoff
from redis.connection import parse_url
from redis.exceptions import RedisClusterException
from redis.retry import Retry

REDIS_INFO = {}
default_redis_url = "redis://localhost:6379/9"
default_redismod_url = "redis://localhost:36379"
default_redis_unstable_url = "redis://localhost:6378"

# default ssl client ignores verification for the purpose of testing
default_redis_ssl_url = "rediss://localhost:6666"
default_cluster_nodes = 6

_DecoratedTest = TypeVar("_DecoratedTest", bound="Callable")
_TestDecorator = Callable[[_DecoratedTest], _DecoratedTest]


# Taken from python3.9
class BooleanOptionalAction(argparse.Action):
    def __init__(
        self,
        option_strings,
        dest,
        default=None,
        type=None,
        choices=None,
        required=False,
        help=None,
        metavar=None,
    ):

        _option_strings = []
        for option_string in option_strings:
            _option_strings.append(option_string)

            if option_string.startswith("--"):
                option_string = "--no-" + option_string[2:]
                _option_strings.append(option_string)

        if help is not None and default is not None:
            help += f" (default: {default})"

        super().__init__(
            option_strings=_option_strings,
            dest=dest,
            nargs=0,
            default=default,
            type=type,
            choices=choices,
            required=required,
            help=help,
            metavar=metavar,
        )

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string in self.option_strings:
            setattr(namespace, self.dest, not option_string.startswith("--no-"))

    def format_usage(self):
        return " | ".join(self.option_strings)


def pytest_addoption(parser):
    parser.addoption(
        "--redis-url",
        default=default_redis_url,
        action="store",
        help="Redis connection string," " defaults to `%(default)s`",
    )

    parser.addoption(
        "--redismod-url",
        default=default_redismod_url,
        action="store",
        help="Connection string to redis server"
        " with loaded modules,"
        " defaults to `%(default)s`",
    )

    parser.addoption(
        "--redis-ssl-url",
        default=default_redis_ssl_url,
        action="store",
        help="Redis SSL connection string," " defaults to `%(default)s`",
    )

    parser.addoption(
        "--redis-cluster-nodes",
        default=default_cluster_nodes,
        action="store",
        help="The number of cluster nodes that need to be "
        "available before the test can start,"
        " defaults to `%(default)s`",
    )

    parser.addoption(
        "--redis-unstable-url",
        default=default_redis_unstable_url,
        action="store",
        help="Redis unstable (latest version) connection string "
        "defaults to %(default)s`",
    )
    parser.addoption(
        "--uvloop", action=BooleanOptionalAction, help="Run tests with uvloop"
    )


def _get_info(redis_url):
    client = redis.Redis.from_url(redis_url)
    info = client.info()
    try:
        client.execute_command("DPING")
        info["enterprise"] = True
    except redis.ResponseError:
        info["enterprise"] = False
    client.connection_pool.disconnect()
    return info


def pytest_sessionstart(session):
    # during test discovery, e.g. with VS Code, we may not
    # have a server running.
    redis_url = session.config.getoption("--redis-url")
    try:
        info = _get_info(redis_url)
        version = info["redis_version"]
        arch_bits = info["arch_bits"]
        cluster_enabled = info["cluster_enabled"]
        enterprise = info["enterprise"]
    except redis.ConnectionError:
        # provide optimistic defaults
        version = "10.0.0"
        arch_bits = 64
        cluster_enabled = False
        enterprise = False
    REDIS_INFO["version"] = version
    REDIS_INFO["arch_bits"] = arch_bits
    REDIS_INFO["cluster_enabled"] = cluster_enabled
    REDIS_INFO["enterprise"] = enterprise
    # store REDIS_INFO in config so that it is available from "condition strings"
    session.config.REDIS_INFO = REDIS_INFO

    # module info, if the second redis is running
    try:
        redismod_url = session.config.getoption("--redismod-url")
        info = _get_info(redismod_url)
        REDIS_INFO["modules"] = info["modules"]
    except redis.exceptions.ConnectionError:
        pass
    except KeyError:
        pass

    if cluster_enabled:
        cluster_nodes = session.config.getoption("--redis-cluster-nodes")
        wait_for_cluster_creation(redis_url, cluster_nodes)

    use_uvloop = session.config.getoption("--uvloop")

    if use_uvloop:
        try:
            import uvloop

            uvloop.install()
        except ImportError as e:
            raise RuntimeError(
                "Can not import uvloop, make sure it is installed"
            ) from e


def wait_for_cluster_creation(redis_url, cluster_nodes, timeout=60):
    """
    Waits for the cluster creation to complete.
    As soon as all :cluster_nodes: nodes become available, the cluster will be
    considered ready.
    :param redis_url: the cluster's url, e.g. redis://localhost:16379/0
    :param cluster_nodes: The number of nodes in the cluster
    :param timeout: the amount of time to wait (in seconds)
    """
    now = time.time()
    end_time = now + timeout
    client = None
    print(f"Waiting for {cluster_nodes} cluster nodes to become available")
    while now < end_time:
        try:
            client = redis.RedisCluster.from_url(redis_url)
            if len(client.get_nodes()) == int(cluster_nodes):
                print("All nodes are available!")
                break
        except RedisClusterException:
            pass
        time.sleep(1)
        now = time.time()
    if now >= end_time:
        available_nodes = 0 if client is None else len(client.get_nodes())
        raise RedisClusterException(
            f"The cluster did not become available after {timeout} seconds. "
            f"Only {available_nodes} nodes out of {cluster_nodes} are available"
        )


def skip_if_server_version_lt(min_version: str) -> _TestDecorator:
    redis_version = REDIS_INFO.get("version", "0")
    check = Version(redis_version) < Version(min_version)
    return pytest.mark.skipif(check, reason=f"Redis version required >= {min_version}")


def skip_if_server_version_gte(min_version: str) -> _TestDecorator:
    redis_version = REDIS_INFO.get("version", "0")
    check = Version(redis_version) >= Version(min_version)
    return pytest.mark.skipif(check, reason=f"Redis version required < {min_version}")


def skip_unless_arch_bits(arch_bits: int) -> _TestDecorator:
    return pytest.mark.skipif(
        REDIS_INFO.get("arch_bits", "") != arch_bits,
        reason=f"server is not {arch_bits}-bit",
    )


def skip_ifmodversion_lt(min_version: str, module_name: str):
    try:
        modules = REDIS_INFO["modules"]
    except KeyError:
        return pytest.mark.skipif(True, reason="Redis server does not have modules")
    if modules == []:
        return pytest.mark.skipif(True, reason="No redis modules found")

    for j in modules:
        if module_name == j.get("name"):
            version = j.get("ver")
            mv = int(min_version.replace(".", ""))
            check = version < mv
            return pytest.mark.skipif(check, reason="Redis module version")

    raise AttributeError(f"No redis module named {module_name}")


def skip_if_redis_enterprise() -> _TestDecorator:
    check = REDIS_INFO.get("enterprise", False) is True
    return pytest.mark.skipif(check, reason="Redis enterprise")


def skip_ifnot_redis_enterprise() -> _TestDecorator:
    check = REDIS_INFO.get("enterprise", False) is False
    return pytest.mark.skipif(check, reason="Not running in redis enterprise")


def skip_if_nocryptography() -> _TestDecorator:
    try:
        import cryptography  # noqa

        return pytest.mark.skipif(False, reason="Cryptography dependency found")
    except ImportError:
        return pytest.mark.skipif(True, reason="No cryptography dependency")


def skip_if_cryptography() -> _TestDecorator:
    try:
        import cryptography  # noqa

        return pytest.mark.skipif(True, reason="Cryptography dependency found")
    except ImportError:
        return pytest.mark.skipif(False, reason="No cryptography dependency")


def _get_client(
    cls, request, single_connection_client=True, flushdb=True, from_url=None, **kwargs
):
    """
    Helper for fixtures or tests that need a Redis client

    Uses the "--redis-url" command line argument for connection info. Unlike
    ConnectionPool.from_url, keyword arguments to this function override
    values specified in the URL.
    """
    if from_url is None:
        redis_url = request.config.getoption("--redis-url")
    else:
        redis_url = from_url
    cluster_mode = REDIS_INFO["cluster_enabled"]
    if not cluster_mode:
        url_options = parse_url(redis_url)
        url_options.update(kwargs)
        pool = redis.ConnectionPool(**url_options)
        client = cls(connection_pool=pool)
    else:
        client = redis.RedisCluster.from_url(redis_url, **kwargs)
        single_connection_client = False
    if single_connection_client:
        client = client.client()
    if request:

        def teardown():
            if not cluster_mode:
                if flushdb:
                    try:
                        client.flushdb()
                    except redis.ConnectionError:
                        # handle cases where a test disconnected a client
                        # just manually retry the flushdb
                        client.flushdb()
                client.close()
                client.connection_pool.disconnect()
            else:
                cluster_teardown(client, flushdb)

        request.addfinalizer(teardown)
    return client


def cluster_teardown(client, flushdb):
    if flushdb:
        try:
            client.flushdb(target_nodes="primaries")
        except redis.ConnectionError:
            # handle cases where a test disconnected a client
            # just manually retry the flushdb
            client.flushdb(target_nodes="primaries")
    client.close()
    client.disconnect_connection_pools()


# specifically set to the zero database, because creating
# an index on db != 0 raises a ResponseError in redis
@pytest.fixture()
def modclient(request, **kwargs):
    rmurl = request.config.getoption("--redismod-url")
    with _get_client(
        redis.Redis, request, from_url=rmurl, decode_responses=True, **kwargs
    ) as client:
        yield client


@pytest.fixture()
def r(request):
    with _get_client(redis.Redis, request) as client:
        yield client


@pytest.fixture()
def r_timeout(request):
    with _get_client(redis.Redis, request, socket_timeout=1) as client:
        yield client


@pytest.fixture()
def r2(request):
    "A second client for tests that need multiple"
    with _get_client(redis.Redis, request) as client:
        yield client


@pytest.fixture()
def sslclient(request):
    with _get_client(redis.Redis, request, ssl=True) as client:
        yield client


def _gen_cluster_mock_resp(r, response):
    connection = Mock()
    connection.retry = Retry(NoBackoff(), 0)
    connection.read_response.return_value = response
    r.connection = connection
    return r


@pytest.fixture()
def mock_cluster_resp_ok(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    return _gen_cluster_mock_resp(r, "OK")


@pytest.fixture()
def mock_cluster_resp_int(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    return _gen_cluster_mock_resp(r, "2")


@pytest.fixture()
def mock_cluster_resp_info(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    response = (
        "cluster_state:ok\r\ncluster_slots_assigned:16384\r\n"
        "cluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\n"
        "cluster_slots_fail:0\r\ncluster_known_nodes:7\r\n"
        "cluster_size:3\r\ncluster_current_epoch:7\r\n"
        "cluster_my_epoch:2\r\ncluster_stats_messages_sent:170262\r\n"
        "cluster_stats_messages_received:105653\r\n"
    )
    return _gen_cluster_mock_resp(r, response)


@pytest.fixture()
def mock_cluster_resp_nodes(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
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


@pytest.fixture()
def mock_cluster_resp_slaves(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    response = (
        "['1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
        "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
        "1447836789290 3 connected']"
    )
    return _gen_cluster_mock_resp(r, response)


@pytest.fixture(scope="session")
def master_host(request):
    url = request.config.getoption("--redis-url")
    parts = urlparse(url)
    yield parts.hostname, parts.port


@pytest.fixture()
def unstable_r(request):
    url = request.config.getoption("--redis-unstable-url")
    with _get_client(
        redis.Redis, request, from_url=url, decode_responses=True
    ) as client:
        yield client


def wait_for_command(client, monitor, command, key=None):
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
    client.get(key)
    while True:
        monitor_response = monitor.next_command()
        if command in monitor_response["command"]:
            return monitor_response
        if key in monitor_response["command"]:
            return None
