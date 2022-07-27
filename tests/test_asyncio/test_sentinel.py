import socket

import pytest
import pytest_asyncio

import redis.asyncio.sentinel
from redis import exceptions
from redis.asyncio.sentinel import (
    MasterNotFoundError,
    Sentinel,
    SentinelConnectionPool,
    SlaveNotFoundError,
)


@pytest_asyncio.fixture(scope="module")
def master_ip(master_host):
    yield socket.gethostbyname(master_host)


class SentinelTestClient:
    def __init__(self, cluster, id):
        self.cluster = cluster
        self.id = id

    async def sentinel_masters(self):
        self.cluster.connection_error_if_down(self)
        self.cluster.timeout_if_down(self)
        return {self.cluster.service_name: self.cluster.master}

    async def sentinel_slaves(self, master_name):
        self.cluster.connection_error_if_down(self)
        self.cluster.timeout_if_down(self)
        if master_name != self.cluster.service_name:
            return []
        return self.cluster.slaves

    async def execute_command(self, *args, **kwargs):
        # wrapper purely to validate the calls don't explode
        from redis.asyncio.client import bool_ok

        return bool_ok


class SentinelTestCluster:
    def __init__(self, service_name="mymaster", ip="127.0.0.1", port=6379):
        self.clients = {}
        self.master = {
            "ip": ip,
            "port": port,
            "is_master": True,
            "is_sdown": False,
            "is_odown": False,
            "num-other-sentinels": 0,
        }
        self.service_name = service_name
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


@pytest_asyncio.fixture()
async def cluster(master_ip):

    cluster = SentinelTestCluster(ip=master_ip)
    saved_Redis = redis.asyncio.sentinel.Redis
    redis.asyncio.sentinel.Redis = cluster.client
    yield cluster
    redis.asyncio.sentinel.Redis = saved_Redis


@pytest_asyncio.fixture()
def sentinel(request, cluster):
    return Sentinel([("foo", 26379), ("bar", 26379)])


@pytest.mark.onlynoncluster
async def test_discover_master(sentinel, master_ip):
    address = await sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)


@pytest.mark.onlynoncluster
async def test_discover_master_error(sentinel):
    with pytest.raises(MasterNotFoundError):
        await sentinel.discover_master("xxx")


@pytest.mark.onlynoncluster
async def test_discover_master_sentinel_down(cluster, sentinel, master_ip):
    # Put first sentinel 'foo' down
    cluster.nodes_down.add(("foo", 26379))
    address = await sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


@pytest.mark.onlynoncluster
async def test_discover_master_sentinel_timeout(cluster, sentinel, master_ip):
    # Put first sentinel 'foo' down
    cluster.nodes_timeout.add(("foo", 26379))
    address = await sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ("bar", 26379)


@pytest.mark.onlynoncluster
async def test_master_min_other_sentinels(cluster, master_ip):
    sentinel = Sentinel([("foo", 26379)], min_other_sentinels=1)
    # min_other_sentinels
    with pytest.raises(MasterNotFoundError):
        await sentinel.discover_master("mymaster")
    cluster.master["num-other-sentinels"] = 2
    address = await sentinel.discover_master("mymaster")
    assert address == (master_ip, 6379)


@pytest.mark.onlynoncluster
async def test_master_odown(cluster, sentinel):
    cluster.master["is_odown"] = True
    with pytest.raises(MasterNotFoundError):
        await sentinel.discover_master("mymaster")


@pytest.mark.onlynoncluster
async def test_master_sdown(cluster, sentinel):
    cluster.master["is_sdown"] = True
    with pytest.raises(MasterNotFoundError):
        await sentinel.discover_master("mymaster")


@pytest.mark.onlynoncluster
async def test_discover_slaves(cluster, sentinel):
    assert await sentinel.discover_slaves("mymaster") == []

    cluster.slaves = [
        {"ip": "slave0", "port": 1234, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 1234, "is_odown": False, "is_sdown": False},
    ]
    assert await sentinel.discover_slaves("mymaster") == [
        ("slave0", 1234),
        ("slave1", 1234),
    ]

    # slave0 -> ODOWN
    cluster.slaves[0]["is_odown"] = True
    assert await sentinel.discover_slaves("mymaster") == [("slave1", 1234)]

    # slave1 -> SDOWN
    cluster.slaves[1]["is_sdown"] = True
    assert await sentinel.discover_slaves("mymaster") == []

    cluster.slaves[0]["is_odown"] = False
    cluster.slaves[1]["is_sdown"] = False

    # node0 -> DOWN
    cluster.nodes_down.add(("foo", 26379))
    assert await sentinel.discover_slaves("mymaster") == [
        ("slave0", 1234),
        ("slave1", 1234),
    ]
    cluster.nodes_down.clear()

    # node0 -> TIMEOUT
    cluster.nodes_timeout.add(("foo", 26379))
    assert await sentinel.discover_slaves("mymaster") == [
        ("slave0", 1234),
        ("slave1", 1234),
    ]


@pytest.mark.onlynoncluster
async def test_master_for(cluster, sentinel, master_ip):
    master = sentinel.master_for("mymaster", db=9)
    assert await master.ping()
    assert master.connection_pool.master_address == (master_ip, 6379)

    # Use internal connection check
    master = sentinel.master_for("mymaster", db=9, check_connection=True)
    assert await master.ping()


@pytest.mark.onlynoncluster
async def test_slave_for(cluster, sentinel):
    cluster.slaves = [
        {"ip": "127.0.0.1", "port": 6379, "is_odown": False, "is_sdown": False}
    ]
    slave = sentinel.slave_for("mymaster", db=9)
    assert await slave.ping()


@pytest.mark.onlynoncluster
async def test_slave_for_slave_not_found_error(cluster, sentinel):
    cluster.master["is_odown"] = True
    slave = sentinel.slave_for("mymaster", db=9)
    with pytest.raises(SlaveNotFoundError):
        await slave.ping()


@pytest.mark.onlynoncluster
async def test_slave_round_robin(cluster, sentinel, master_ip):
    cluster.slaves = [
        {"ip": "slave0", "port": 6379, "is_odown": False, "is_sdown": False},
        {"ip": "slave1", "port": 6379, "is_odown": False, "is_sdown": False},
    ]
    pool = SentinelConnectionPool("mymaster", sentinel)
    rotator = pool.rotate_slaves()
    assert await rotator.__anext__() in (("slave0", 6379), ("slave1", 6379))
    assert await rotator.__anext__() in (("slave0", 6379), ("slave1", 6379))
    # Fallback to master
    assert await rotator.__anext__() == (master_ip, 6379)
    with pytest.raises(SlaveNotFoundError):
        await rotator.__anext__()


@pytest.mark.onlynoncluster
async def test_ckquorum(cluster, sentinel):
    assert await sentinel.sentinel_ckquorum("mymaster")


@pytest.mark.onlynoncluster
async def test_flushconfig(cluster, sentinel):
    assert await sentinel.sentinel_flushconfig()


@pytest.mark.onlynoncluster
async def test_reset(cluster, sentinel):
    cluster.master["is_odown"] = True
    assert await sentinel.sentinel_reset("mymaster")
