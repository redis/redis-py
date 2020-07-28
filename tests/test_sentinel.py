import socket

import pytest

from redis import exceptions, ConnectionPool
from redis.sentinel import (Sentinel, SentinelConnectionPool,
                            MasterNotFoundError, SlaveNotFoundError)
from redis._compat import next
from redis.utils import HIREDIS_AVAILABLE
import redis.sentinel


@pytest.fixture(scope="module")
def master_ip(master_host):
    yield socket.gethostbyname(master_host)


@pytest.fixture(scope="module")
def slave_ip(slave_host):
    yield socket.gethostbyname(slave_host)


class MockReadonlySocketBuffer(object):
    def readline(self):
        return b"-READONLY"

    def close(self):
        pass


class SentinelTestClient(object):
    def __init__(self, cluster, id):
        self.cluster = cluster
        self.id = id
        self.connection_pool = ConnectionPool(host=id[0], port=id[1])

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


class SentinelTestCluster(object):
    def __init__(self, service_name='mymaster', ip='127.0.0.1', port=6379):
        self.clients = {}
        self.master = {
            'ip': ip,
            'port': port,
            'is_master': True,
            'is_sdown': False,
            'is_odown': False,
            'num-other-sentinels': 0,
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
    return Sentinel([('foo', 26379), ('bar', 26379)])


@pytest.fixture()
def live_sentinel(is_docker):
    if is_docker:
        hosts = ['sentinel_1', 'sentinel_2', 'sentinel_3']
    else:
        hosts = ['localhost'] * 3
    ports = [26379, 26380, 26381]
    return Sentinel(zip(hosts, ports))


def test_discover_master(sentinel, master_ip):
    address = sentinel.discover_master('mymaster')
    assert address == (master_ip, 6379)


def test_discover_master_error(sentinel):
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master('xxx')


def test_discover_master_sentinel_down(cluster, sentinel, master_ip):
    # Put first sentinel 'foo' down
    cluster.nodes_down.add(('foo', 26379))
    address = sentinel.discover_master('mymaster')
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ('bar', 26379)


def test_discover_master_sentinel_timeout(cluster, sentinel, master_ip):
    # Put first sentinel 'foo' down
    cluster.nodes_timeout.add(('foo', 26379))
    address = sentinel.discover_master('mymaster')
    assert address == (master_ip, 6379)
    # 'bar' is now first sentinel
    assert sentinel.sentinels[0].id == ('bar', 26379)


def test_master_min_other_sentinels(cluster, master_ip):
    sentinel = Sentinel([('foo', 26379)], min_other_sentinels=1)
    # min_other_sentinels
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master('mymaster')
    cluster.master['num-other-sentinels'] = 2
    address = sentinel.discover_master('mymaster')
    assert address == (master_ip, 6379)


def test_master_odown(cluster, sentinel):
    cluster.master['is_odown'] = True
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master('mymaster')


def test_master_sdown(cluster, sentinel):
    cluster.master['is_sdown'] = True
    with pytest.raises(MasterNotFoundError):
        sentinel.discover_master('mymaster')


def test_discover_slaves(cluster, sentinel):
    assert sentinel.discover_slaves('mymaster') == []

    cluster.slaves = [
        {'ip': 'slave0', 'port': 1234, 'is_odown': False, 'is_sdown': False},
        {'ip': 'slave1', 'port': 1234, 'is_odown': False, 'is_sdown': False},
    ]
    assert sentinel.discover_slaves('mymaster') == [
        ('slave0', 1234), ('slave1', 1234)]

    # slave0 -> ODOWN
    cluster.slaves[0]['is_odown'] = True
    assert sentinel.discover_slaves('mymaster') == [
        ('slave1', 1234)]

    # slave1 -> SDOWN
    cluster.slaves[1]['is_sdown'] = True
    assert sentinel.discover_slaves('mymaster') == []

    cluster.slaves[0]['is_odown'] = False
    cluster.slaves[1]['is_sdown'] = False

    # node0 -> DOWN
    cluster.nodes_down.add(('foo', 26379))
    assert sentinel.discover_slaves('mymaster') == [
        ('slave0', 1234), ('slave1', 1234)]
    cluster.nodes_down.clear()

    # node0 -> TIMEOUT
    cluster.nodes_timeout.add(('foo', 26379))
    assert sentinel.discover_slaves('mymaster') == [
        ('slave0', 1234), ('slave1', 1234)]


def test_master_for(cluster, sentinel, master_ip):
    master = sentinel.master_for('mymaster', db=9)
    assert master.ping()
    assert master.connection_pool.master_address == (master_ip, 6379)

    # Use internal connection check
    master = sentinel.master_for('mymaster', db=9, check_connection=True)
    assert master.ping()


def test_slave_for(cluster, sentinel):
    cluster.slaves = [
        {'ip': '127.0.0.1', 'port': 6379,
         'is_odown': False, 'is_sdown': False},
    ]
    slave = sentinel.slave_for('mymaster', db=9)
    assert slave.ping()


def test_slave_for_slave_not_found_error(cluster, sentinel):
    cluster.master['is_odown'] = True
    slave = sentinel.slave_for('mymaster', db=9)
    with pytest.raises(SlaveNotFoundError):
        slave.ping()


def test_slave_round_robin(cluster, sentinel, master_ip):
    cluster.slaves = [
        {'ip': 'slave0', 'port': 6379, 'is_odown': False, 'is_sdown': False},
        {'ip': 'slave1', 'port': 6379, 'is_odown': False, 'is_sdown': False},
    ]
    pool = SentinelConnectionPool('mymaster', sentinel)
    rotator = pool.rotate_slaves()
    assert next(rotator) in (('slave0', 6379), ('slave1', 6379))
    assert next(rotator) in (('slave0', 6379), ('slave1', 6379))
    # Fallback to master
    assert next(rotator) == (master_ip, 6379)
    with pytest.raises(SlaveNotFoundError):
        next(rotator)


def test_managed_connection_repr(sentinel):
    pool = SentinelConnectionPool('mymaster', sentinel)
    conn = pool.make_connection()
    assert str(conn) == 'SentinelManagedConnection' \
                        '<service=mymaster,host=localhost,port=6379>'


def test_managed_connection_failed_connection():
    sentinel = Sentinel([('foo', 99999), ('bar', 99991)])
    pool = SentinelConnectionPool('mymaster', sentinel)
    conn = pool.make_connection()

    with pytest.raises(exceptions.ConnectionError):
        conn.connect()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason='PythonParser only')
def test_master_readonly_connection_error(cluster, sentinel):
    pool = SentinelConnectionPool('mymaster', sentinel)
    conn = pool.make_connection()
    conn._parser._buffer = MockReadonlySocketBuffer()

    with pytest.raises(exceptions.ConnectionError):
        conn.read_response()


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason='PythonParser only')
def test_slave_readonly_error(cluster, sentinel):
    pool = SentinelConnectionPool('mymaster', sentinel, is_master=False)
    conn = pool.make_connection()
    conn._parser._buffer = MockReadonlySocketBuffer()

    with pytest.raises(exceptions.ReadOnlyError):
        conn.read_response()


def test_sentinel_repr(sentinel):
    sentinel = Sentinel([('foo', 99999), ('bar', 99991)])
    assert str(sentinel) == "Sentinel<sentinels=[foo:99999,bar:99991]>"


def test_sentinel_connection_pool_repr():
    sentinel = Sentinel([('foo', 99999), ('bar', 99991)])
    pool = SentinelConnectionPool('mymaster', sentinel)
    assert str(pool) == "SentinelConnectionPool<service=mymaster(master)"


def test_sentinel_live_discover_master(live_sentinel, master_ip):
    assert live_sentinel.discover_master('redis-py-test') == (master_ip, 6379)


def test_sentinel_live_discover_slave(live_sentinel, slave_ip):
    assert live_sentinel.discover_slaves('redis-py-test') == [(slave_ip, 6380)]
