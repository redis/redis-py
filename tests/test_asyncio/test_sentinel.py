import socket
from unittest import mock

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


@pytest_asyncio.fixture(scope="module", loop_scope="module")
def master_ip(master_host):
    yield socket.gethostbyname(master_host[0])


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


@pytest.fixture()
async def deployed_sentinel(request):
    sentinel_ips = request.config.getoption("--sentinels")
    sentinel_endpoints = [
        (ip.strip(), int(port.strip()))
        for ip, port in (endpoint.split(":") for endpoint in sentinel_ips.split(","))
    ]
    kwargs = {}
    decode_responses = True

    sentinel_kwargs = {"decode_responses": decode_responses}
    force_master_ip = "localhost"

    protocol = request.config.getoption("--protocol", 2)

    sentinel = Sentinel(
        sentinel_endpoints,
        force_master_ip=force_master_ip,
        sentinel_kwargs=sentinel_kwargs,
        socket_timeout=0.1,
        protocol=protocol,
        decode_responses=decode_responses,
        **kwargs,
    )
    yield sentinel
    for s in sentinel.sentinels:
        await s.close()


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
    async with sentinel.master_for("mymaster", db=9) as master:
        assert await master.ping()
        assert master.connection_pool.master_address == (master_ip, 6379)

    # Use internal connection check
    async with sentinel.master_for("mymaster", db=9, check_connection=True) as master:
        assert await master.ping()


@pytest.mark.onlynoncluster
async def test_slave_for(cluster, sentinel):
    cluster.slaves = [
        {"ip": "127.0.0.1", "port": 6379, "is_odown": False, "is_sdown": False}
    ]
    async with sentinel.slave_for("mymaster", db=9) as slave:
        assert await slave.ping()


@pytest.mark.onlynoncluster
async def test_slave_for_slave_not_found_error(cluster, sentinel):
    cluster.master["is_odown"] = True
    async with sentinel.slave_for("mymaster", db=9) as slave:
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
async def test_ckquorum(sentinel):
    resp = await sentinel.sentinel_ckquorum("mymaster")
    assert resp is True


@pytest.mark.onlynoncluster
async def test_flushconfig(sentinel):
    resp = await sentinel.sentinel_flushconfig()
    assert resp is True


@pytest.mark.onlynoncluster
async def test_reset(cluster, sentinel):
    cluster.master["is_odown"] = True
    resp = await sentinel.sentinel_reset("mymaster")
    assert resp is True


@pytest.mark.onlynoncluster
@pytest.mark.parametrize("method_name", ["master_for", "slave_for"])
async def test_auto_close_pool(cluster, sentinel, method_name):
    """
    Check that the connection pool created by the sentinel client is
    automatically closed
    """

    method = getattr(sentinel, method_name)
    client = method("mymaster", db=9)
    pool = client.connection_pool
    assert client.auto_close_connection_pool is True
    calls = 0

    async def mock_disconnect():
        nonlocal calls
        calls += 1

    with mock.patch.object(pool, "disconnect", mock_disconnect):
        await client.aclose()

    assert calls == 1
    await pool.disconnect()


@pytest.mark.onlynoncluster
async def test_repr_correctly_represents_connection_object(sentinel):
    pool = SentinelConnectionPool("mymaster", sentinel)
    connection = await pool.get_connection()

    assert (
        str(connection)
        == "<redis.asyncio.sentinel.SentinelManagedConnection,host=127.0.0.1,port=6379)>"  # noqa: E501
    )
    assert connection.connection_pool == pool
    await pool.release(connection)

    del pool

    assert (
        str(connection)
        == "<redis.asyncio.sentinel.SentinelManagedConnection,host=127.0.0.1,port=6379)>"  # noqa: E501
    )


# Tests against real sentinel instances
@pytest.mark.onlynoncluster
async def test_get_sentinels(deployed_sentinel):
    resps = await deployed_sentinel.sentinel_sentinels(
        "redis-py-test", return_responses=True
    )

    # validate that the original command response is returned
    assert isinstance(resps, list)

    # validate that the command has been executed against all sentinels
    # each response from each sentinel is returned
    assert len(resps) > 1

    # validate default behavior
    resps = await deployed_sentinel.sentinel_sentinels("redis-py-test")
    assert isinstance(resps, bool)


@pytest.mark.onlynoncluster
async def test_get_master_addr_by_name(deployed_sentinel):
    resps = await deployed_sentinel.sentinel_get_master_addr_by_name(
        "redis-py-test",
        return_responses=True,
    )

    # validate that the original command response is returned
    assert isinstance(resps, list)

    # validate that the command has been executed just once
    # when executed once, only one response element is returned
    assert len(resps) == 1

    assert isinstance(resps[0], tuple)

    # validate default behavior
    resps = await deployed_sentinel.sentinel_get_master_addr_by_name("redis-py-test")
    assert isinstance(resps, bool)


@pytest.mark.onlynoncluster
async def test_redis_master_usage(deployed_sentinel):
    r = await deployed_sentinel.master_for("redis-py-test", db=0)
    await r.set("foo", "bar")
    assert (await r.get("foo")) == "bar"
