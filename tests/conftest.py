import pytest
import redis
from mock import Mock

from distutils.version import StrictVersion


REDIS_INFO = {}
default_redis_host = "localhost"
default_redis_port = "6379"
default_cluster_master_host = "127.0.0.1"
default_cluster_master_port = "6379"


def pytest_addoption(parser):
    parser.addoption('--redis-host', default=default_redis_host,
                     action="store",
                     help="Redis hostname,"
                          " defaults to `%(default)s`")
    parser.addoption('--redis-port', default=default_redis_port,
                     action="store",
                     help="Redis port,"
                          " defaults to `%(default)s`")
    parser.addoption('--cluster-master-host',
                     default=default_cluster_master_host,
                     action="store",
                     help="Hostname of cluster master,"
                          " defaults to `%(default)s`")
    parser.addoption('--cluster-master-port',
                     default=default_cluster_master_port,
                     action="store",
                     help="Port of cluster master,"
                          " defaults to `%(default)s`")


def _build_redis_url(redis_host, redis_port):
    return "redis://{}:{}/9".format(redis_host, redis_port)


def _get_info(redis_url):
    client = redis.Redis.from_url(redis_url)
    info = client.info()
    client.connection_pool.disconnect()
    return info


def pytest_sessionstart(session):
    redis_host = session.config.getoption("--redis-host")
    redis_port = session.config.getoption("--redis-port")
    redis_url = _build_redis_url(redis_host, redis_port)
    info = _get_info(redis_url)
    version = info["redis_version"]
    arch_bits = info["arch_bits"]
    REDIS_INFO["version"] = version
    REDIS_INFO["arch_bits"] = arch_bits


def skip_if_server_version_lt(min_version):
    redis_version = REDIS_INFO["version"]
    check = StrictVersion(redis_version) < StrictVersion(min_version)
    return pytest.mark.skipif(
        check,
        reason="Redis version required >= {}".format(min_version))


def skip_if_server_version_gte(min_version):
    redis_version = REDIS_INFO["version"]
    check = StrictVersion(redis_version) >= StrictVersion(min_version)
    return pytest.mark.skipif(
        check,
        reason="Redis version required < {}".format(min_version))


def skip_unless_arch_bits(arch_bits):
    return pytest.mark.skipif(REDIS_INFO["arch_bits"] != arch_bits,
                              reason="server is not {}-bit".format(arch_bits))


def _get_client(cls, request, single_connection_client=True, **kwargs):
    redis_host = request.config.getoption("--redis-host")
    redis_port = request.config.getoption("--redis-port")
    redis_url = _build_redis_url(redis_host, redis_port)
    client = cls.from_url(redis_url, **kwargs)
    if single_connection_client:
        client = client.client()
    if request:
        def teardown():
            try:
                client.flushdb()
            except redis.ConnectionError:
                # handle cases where a test disconnected a client
                # just manually retry the flushdb
                client.flushdb()
            client.close()
            client.connection_pool.disconnect()
        request.addfinalizer(teardown)
    return client


@pytest.fixture()
def r(request):
    with _get_client(redis.Redis, request) as client:
        yield client


@pytest.fixture()
def r2(request):
    "A second client for tests that need multiple"
    with _get_client(redis.Redis, request) as client:
        yield client


@pytest.fixture()
def redis_host(request):
    return request.config.getoption("--redis-host")


@pytest.fixture()
def redis_port(request):
    return request.config.getoption("--redis-port")


def _gen_cluster_mock_resp(r, response):
    connection = Mock()
    connection.read_response.return_value = response
    r.connection = connection
    return r


@pytest.fixture()
def mock_cluster_resp_ok(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    return _gen_cluster_mock_resp(r, 'OK')


@pytest.fixture()
def mock_cluster_resp_int(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    return _gen_cluster_mock_resp(r, '2')


@pytest.fixture()
def mock_cluster_resp_info(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    response = ('cluster_state:ok\r\ncluster_slots_assigned:16384\r\n'
                'cluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\n'
                'cluster_slots_fail:0\r\ncluster_known_nodes:7\r\n'
                'cluster_size:3\r\ncluster_current_epoch:7\r\n'
                'cluster_my_epoch:2\r\ncluster_stats_messages_sent:170262\r\n'
                'cluster_stats_messages_received:105653\r\n')
    return _gen_cluster_mock_resp(r, response)


@pytest.fixture()
def mock_cluster_resp_nodes(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    response = ('c8253bae761cb1ecb2b61857d85dfe455a0fec8b 172.17.0.7:7006 '
                'slave aa90da731f673a99617dfe930306549a09f83a6b 0 '
                '1447836263059 5 connected\n'
                '9bd595fe4821a0e8d6b99d70faa660638a7612b3 172.17.0.7:7008 '
                'master - 0 1447836264065 0 connected\n'
                'aa90da731f673a99617dfe930306549a09f83a6b 172.17.0.7:7003 '
                'myself,master - 0 0 2 connected 5461-10922\n'
                '1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 '
                'slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 '
                '1447836262556 3 connected\n'
                '4ad9a12e63e8f0207025eeba2354bcf4c85e5b22 172.17.0.7:7005 '
                'master - 0 1447836262555 7 connected 0-5460\n'
                '19efe5a631f3296fdf21a5441680f893e8cc96ec 172.17.0.7:7004 '
                'master - 0 1447836263562 3 connected 10923-16383\n'
                'fbb23ed8cfa23f17eaf27ff7d0c410492a1093d6 172.17.0.7:7002 '
                'master,fail - 1447829446956 1447829444948 1 disconnected\n'
                )
    return _gen_cluster_mock_resp(r, response)


@pytest.fixture()
def mock_cluster_resp_slaves(request, **kwargs):
    r = _get_client(redis.Redis, request, **kwargs)
    response = ("['1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
                "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
                "1447836789290 3 connected']")
    return _gen_cluster_mock_resp(r, response)
