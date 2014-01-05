# coding=utf-8
import os
import itertools

from hash_ring import HashRing
import pytest
import minimock

from redis import StrictRedis
from redis.connection import (
    ShardedMasterSlaveConnectionPool,
    Shard,
    Queue,
    InvalidCommandException)


# noinspection PyProtectedMember,PyDocstring
class TestUnitShardedConnectionPool(object):
    @pytest.fixture(autouse=True)
    def get_pool(self):
        self.tracker = minimock.TraceTracker()
        self.pool = ShardedMasterSlaveConnectionPool(
            [('master1:6379', 'slave1:6379'), ('master2:6379',)],
            socket_timeout=1000
        )

    def test_hash_ring(self):
        assert isinstance(self.pool._hash_ring, HashRing)
        assert isinstance(self.pool._hash_ring.get_node(''), Shard)

    def test_create_shard(self):
        shard = self.pool._create_shard(('master1:6379', 'slave1:6379'))
        assert shard.master['host'] == 'master1:6379'
        assert isinstance(shard.master['queue'], Queue)

        assert isinstance(shard.slaves, itertools.cycle)
        slave = shard.slaves.next()
        assert slave['host'] == 'slave1:6379'
        assert isinstance(slave['queue'], Queue)

    def test_merge_connection_options(self):
        options = self.pool._merge_connection_options('master1:6379')
        assert options == {
            'host': 'master1',
            'port': 6379,
            'socket_timeout': 1000
        }

    def test_create_queue(self):
        queue = self.pool._create_queue()
        assert isinstance(queue, Queue)
        assert queue.qsize() == self.pool.max_connections
        assert queue.get() is None

    @pytest.mark.parametrize("pid", [None, 1])
    def test_check_pid(self, pid):
        if pid:
            self.pool.pid = 1

        self.pool.disconnect = minimock.Mock('disconnect',
                                             tracker=self.tracker)
        self.pool._checkpid()

        tracker = []
        if pid:
            tracker = ['Called disconnect()']
        minimock.assert_same_trace(self.tracker, '\n'.join(tracker))

    @pytest.mark.parametrize("key,is_slave,expected", [
        ('gotoshard1', False, 'master1:6379'),
        ('gotoshard1', True, 'slave1:6379'),
        ('gotoshard2', False, 'master2:6379'),
        ('gotoshard2', True, 'master2:6379'),
    ])
    def test_get_server(self, key, is_slave, expected):
        server = self.pool._get_server(key, is_slave)
        assert server['host'] == expected

    def test_get_connection_fail_if_no_shard_key(self):
        with pytest.raises(InvalidCommandException) as excinfo:
            self.pool.get_connection('KEYS')
        assert excinfo.value.message == 'KEYS'

    @pytest.mark.parametrize("arguments,default,expected", [
        # default
        (('GET', ('1',), {}), False, "Called _get_server('1', False)"),
        (('GET', ('1',), {}), True, "Called _get_server('1', True)"),
        # override default
        (('GET', ('1',), {'slave_ok': True}), False,
         "Called _get_server('1', True)"),
        (('GET', ('1',), {'slave_ok': False}), True,
         "Called _get_server('1', False)"),
        # master only command
        (('SET', ('1',), {}), True, "Called _get_server('1', False)"),
    ])
    def test_get_connection_slaves(self, arguments, default, expected):
        self.pool.slave_ok = default

        queue = Queue()
        queue.put('test')
        self.pool._get_server = minimock.Mock('_get_server',
                                              returns={'queue': queue},
                                              tracker=self.tracker)

        res = self.pool.get_connection(arguments[0],
                                       *arguments[1],
                                       **arguments[2])

        assert res == 'test'
        minimock.assert_same_trace(self.tracker, expected)

    def test_get_connection_new_connection(self):
        queue = Queue()
        queue.put(None)
        self.pool._get_server = minimock.Mock('_get_server',
                                              returns={
                                                  'queue': queue,
                                                  'host': 'test_host'},
                                              tracker=self.tracker)
        self.pool.make_connection = minimock.Mock('make_connection',
                                                  returns='connection_object',
                                                  tracker=self.tracker)

        connection = self.pool.get_connection('GET', [1])
        assert connection == 'connection_object'
        assert len(self.pool._all_connections) == 1
        assert self.pool._all_connections[0] == 'connection_object'

        minimock.assert_same_trace(self.tracker, '\n'.join([
            "Called _get_server([1], True)",
            "Called make_connection(",
            "   {'queue': <Queue.Queue instance at ...>, 'host': 'test_host'})"
        ]))

    def test_get_connection_already_exists(self):
        queue = Queue()
        queue.put('connection_object')
        self.pool._get_server = minimock.Mock('_get_server',
                                              returns={
                                                  'queue': queue, 'host':
                                                  'test_host'})
        self.pool.make_connection = minimock.Mock('make_connection',
                                                  returns='never_called',
                                                  tracker=self.tracker)

        connection = self.pool.get_connection('GET', [1])
        assert connection == 'connection_object'

        # make sure this block is not called
        assert len(self.pool._all_connections) == 0
        minimock.assert_same_trace(self.tracker, '')
        minimock.assert_same_trace(self.tracker, '')

    @pytest.mark.parametrize("checkpid,expected", [
        (True, [
            'Called checkpid()',
            'Called connection.return_to_queue()'
        ]),
        (False, [
            'Called checkpid()',
        ]),
    ])
    def test_make_connection(self, checkpid, expected):
        self.pool._checkpid = minimock.Mock('checkpid',
                                            returns=checkpid,
                                            tracker=self.tracker)

        mocked_connection = minimock.Mock('connection',
                                          tracker=self.tracker)

        self.pool.release(mocked_connection)

        minimock.assert_same_trace(self.tracker, '\n'.join(expected))


# noinspection PyDocstring
@pytest.mark.skipif(
    os.getenv('RUN_SHARD_FUNCTIONAL_TEST', False) != '1',
    reason="Set RUN_SHARD_FUNCTIONAL_TEST environment "
           "variable to '1' to run these test")
class TestFunctionalShardedConnectionPool(object):
    @pytest.fixture(autouse=True)
    def get_pool(self):
        self.tracker = minimock.TraceTracker()
        self.pool = ShardedMasterSlaveConnectionPool(
            [('localhost:6379', 'localhost:6381'), ('localhost:6380',)]
        )

    def test_basic(self):
        redis = StrictRedis(connection_pool=self.pool)

        for i in xrange(50):
            redis.delete('shard%s' % i)

        for i in xrange(50):
            redis.set('shard%s' % i, i)

        for i in xrange(50):
            assert redis.get('shard%s' % i) == str(i)

    def test_pipeline(self):
        redis = StrictRedis(connection_pool=self.pool)
        pipeline = redis.pipeline(shard_hint='1')
        pipeline.set('pipeline1', '1')
        pipeline.set('pipeline2', '2')
        pipeline.get('pipeline1')
        pipeline.get('pipeline2')
        res = pipeline.execute()
        assert res == [True, True, '1', '2']
