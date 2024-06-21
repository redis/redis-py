import socket
from unittest import mock

import pytest
import redis.sentinel
from redis import exceptions
from redis.sentinel import (
    MasterNotFoundError,
    Sentinel,
    SentinelConnectionPool,
    SlaveNotFoundError,
)


@pytest.fixture(scope="module")
def master_ip(master_host):
    yield socket.gethostbyname(master_host[0])


class SentinelTestClient:
    def __init__(self, cluster, id):
        self.cluster = cluster
        self.id = id

    def sentinel_masters(self):
        self.cluster.connection_error_if_down(self)
        self.cluster.timeout_if_down(self)
        return {self.cluster.service_name: self.cluster.master}

    def sentinel_slaves(self, master_name):
        self.cluster.connection_error_if_down(self)
        self.cluster.timeout_if_down(self)
        if master_name != self.cluster.service_name:
            return []
        return self.cluster.slaves

    def execute_command(self, *args, **kwargs):
        # wrapper  purely to validate the calls don't explode
        from redis.client import bool_ok

        return bool_ok


class SentinelTestCluster:
    def __init__(self, servisentinel_ce_name="mymaster", ip="127.0.0.1", port=6379):
        self.clients = {}
        self.master = {
            "ip": ip,
            "port": port,
            "is_master": True,
            "is_sdown": False,
            "is_odown": False,
            "num-other-sentinels": 0,
        }
        self.service_name = servisentinel_ce_name
        self.slaves = []
        self.nodes_down = set()
        self.nodes_timeout = set()

    def connection_error_if_down(self, node):
        if node.id in self.nodes_down:
            raise exceptions.ConnectionError

    def timeout_if_down(self, node):
        if node.id in self.nodes_timeout:
            raise exceptions.TimeoutError

    def client(self, host, port, **kwargs):
        return SentinelTestClient(self, (host, port))


@pytest.fixture()
def cluster(request, master_ip):
    def teardown():
        redis.sentinel.Redis = saved_Redis

    cluster = SentinelTestCluster(ip=master_ip)
    saved_Redis = redis.sentinel.Redis
    redis.sentinel.Redis = cluster.client
    request.addfinalizer(teardown)
    return cluster


@pytest.fixture()
def sentinel(request, cluster):
    return Sentinel([("foo", 26379), ("bar", 26379)])


@pytest.mark.onlynoncluster
def test_discover_master(sentinel, master_ip):
    address = sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)


@pytest.mark.onlynoncluster
def test_discover_master_error(sentinel):
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master("xxx")


@pytest.mark.onlynoncluster
def test_dead_pool(sentinel):
    master = sentinel.master_for("mymaster", db=9)
    conn = master.connection_pool.get_connection("_")
    conn.disconnect()
    del master
    conn.connect()


@pytest.mark.onlynoncluster
def test_discover_master_sentinel_down(cluster, sentinel, master_ip):
    # Put first sentinel 'foo' down
    cluster.nodes_down.add(("foo", 26379))
    address = sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


@pytest.mark.onlynoncluster
def test_discover_master_sentinel_timeout(cluster, sentinel, master_ip):
    # Put first sentinel 'foo' down
    cluster.nodes_timeout.add(("foo", 26379))
    address = sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


@pytest.mark.onlynoncluster
def test_master_min_other_sentinels(cluster, master_ip):
    sentinel = Sentinel([("foo", 26379)], min_other_sentinels=1)
    # min_other_sentinels
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master("mymaster")
    cluster.master["num-other-sentinels"] = 2
    address = sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)


@pytest.mark.onlynoncluster
def test_master_odown(cluster, sentinel):
    cluster.master["is_odown"] = True
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master("mymaster")


@pytest.mark.onlynoncluster
def test_master_sdown(cluster, sentinel):
    cluster.master["is_sdown"] = True
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master("mymaster")


@pytest.mark.onlynoncluster
def test_discover_slaves(cluster, sentinel):
    assert sentinel.discover_slaves("mymaster") == []

    cluster.slaves = [
        {"ip": "slave0", "port": 1234, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 1234, "is_odown": False, "is_sdown": False},
    ]
    assert sentinel.discover_slaves("mymaster") == [("slave0", 1234), ("slave1", 1234)]

    # slave0 -> ODOWN
    cluster.slaves[0]["is_odown"] = True
    assert sentinel.discover_slaves("mymaster") == [("slave1", 1234)]

    # slave1 -> SDOWN
    cluster.slaves[1]["is_sdown"] = True
    assert sentinel.discover_slaves("mymaster") == []

    cluster.slaves[0]["is_odown"] = False
    cluster.slaves[1]["is_sdown"] = False

    # node0 -> DOWN
    cluster.nodes_down.add(("foo", 26379))
    assert sentinel.discover_slaves("mymaster") == [("slave0", 1234), ("slave1", 1234)]
    cluster.nodes_down.clear()

    # node0 -> TIMEOUT
    cluster.nodes_timeout.add(("foo", 26379))
    assert sentinel.discover_slaves("mymaster") == [("slave0", 1234), ("slave1", 1234)]


@pytest.mark.onlynoncluster
def test_master_for(cluster, sentinel, master_ip):
    master = sentinel.master_for("mymaster", db=9)
    assert master.ping()
    assert master.connection_pool.master_address == (master_ip, 6379)

    # Use internal connection check
    master = sentinel.master_for("mymaster", db=9, check_connection=True)
    assert master.ping()


@pytest.mark.onlynoncluster
def test_slave_for(cluster, sentinel):
    cluster.slaves = [
        {"ip": "127.0.0.1", "port": 6379, "is_odown": False, "is_sdown": False}
    ]
    slave = sentinel.slave_for("mymaster", db=9)
    assert slave.ping()


@pytest.mark.onlynoncluster
def test_slave_for_slave_not_found_error(cluster, sentinel):
    cluster.master["is_odown"] = True
    slave = sentinel.slave_for("mymaster", db=9)
    with pytest.raises(SlaveNotFoundError):
        slave.ping()


@pytest.mark.onlynoncluster
def test_slave_round_robin(cluster, sentinel, master_ip):
    cluster.slaves = [
        {"ip": "slave0", "port": 6379, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 6379, "is_odown": False, "is_sdown": False},
    ]
    pool = SentinelConnectionPool("mymaster", sentinel)
    rotator = pool.rotate_slaves()
    assert next(rotator) in (("slave0", 6379), ("slave1", 6379))
    assert next(rotator) in (("slave0", 6379), ("slave1", 6379))
    # Fallback to master
    assert next(rotator) == (master_ip, 6379)
    with pytest.raises(SlaveNotFoundError):
        next(rotator)


@pytest.mark.onlynoncluster
def test_ckquorum(cluster, sentinel):
    assert sentinel.sentinel_ckquorum("mymaster")


@pytest.mark.onlynoncluster
def test_flushconfig(cluster, sentinel):
    assert sentinel.sentinel_flushconfig()


@pytest.mark.onlynoncluster
def test_reset(cluster, sentinel):
    cluster.master["is_odown"] = True
    assert sentinel.sentinel_reset("mymaster")


@pytest.mark.onlynoncluster
@pytest.mark.parametrize("method_name", ["master_for", "slave_for"])
def test_auto_close_pool(cluster, sentinel, method_name):
    """
    Check that the connection pool created by the sentinel client is
    automatically closed
    """

    method = getattr(sentinel, method_name)
    client = method("mymaster", db=9)
    pool = client.connection_pool
    assert client.auto_close_connection_pool is True
    calls = 0

    def mock_disconnect():
        nonlocal calls
        calls += 1

    with mock.patch.object(pool, "disconnect", mock_disconnect):
        client.close()

    assert calls == 1
    pool.disconnect()
