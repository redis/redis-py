from __future__ import with_statement
import os
import pytest
import redis
import time
import re

from threading import Thread
from redis._compat import Queue


class DummyConnection(object):
    description_format = "DummyConnection<>"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()


class TestConnectionPoolCase(object):
    def get_pool(self, connection_info=None, max_connections=None):
        connection_info = connection_info or {'a': 1, 'b': 2, 'c': 3}
        pool = redis.ConnectionPool(
            connection_class=DummyConnection, max_connections=max_connections,
            **connection_info)
        return pool

    def test_connection_creation(self):
        connection_info = {'foo': 'bar', 'biz': 'baz'}
        pool = self.get_pool(connection_info=connection_info)
        connection = pool.get_connection('_')
        assert connection.kwargs == connection_info

    def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        c2 = pool.get_connection('_')
        assert c1 != c2

    def test_max_connections(self):
        pool = self.get_pool(max_connections=2)
        pool.get_connection('_')
        pool.get_connection('_')
        with pytest.raises(redis.ConnectionError):
            pool.get_connection('_')

    def test_reuse_previously_released_connection(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        pool.release(c1)
        c2 = pool.get_connection('_')
        assert c1 == c2

    def test_repr_contains_db_info_tcp(self):
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)

        assert re.match('(.*)<(.*)<(.*)>>', repr(pool)).groups() == (
            'ConnectionPool',
            'Connection',
            'host=localhost,port=6379,db=0',
        )

    def test_repr_contains_db_info_unix(self):
        pool = redis.ConnectionPool(
            connection_class=redis.UnixDomainSocketConnection,
            path='abc',
            db=0,
        )

        assert re.match('(.*)<(.*)<(.*)>>', repr(pool)).groups() == (
            'ConnectionPool',
            'UnixDomainSocketConnection',
            'path=abc,db=0',
        )


class TestBlockingConnectionPool(object):
    def get_pool(self, connection_info=None, max_connections=10, timeout=20):
        connection_info = connection_info or {'a': 1, 'b': 2, 'c': 3}
        pool = redis.BlockingConnectionPool(connection_class=DummyConnection,
                                            max_connections=max_connections,
                                            timeout=timeout, **connection_info)
        return pool

    def test_connection_creation(self):
        connection_info = {'foo': 'bar', 'biz': 'baz'}
        pool = self.get_pool(connection_info=connection_info)
        connection = pool.get_connection('_')
        assert connection.kwargs == connection_info

    def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        c2 = pool.get_connection('_')
        assert c1 != c2

    def test_max_connections_blocks(self):
        """Getting a connection should block for until available."""
        q = Queue()
        q.put_nowait('Not yet got')
        pool = self.get_pool(max_connections=2, timeout=5)
        c1 = pool.get_connection('_')
        pool.get_connection('_')

        target = lambda: q.put_nowait(pool.get_connection('_'))
        Thread(target=target).start()

        # Blocks while non available.
        time.sleep(0.05)
        c3 = q.get_nowait()
        assert c3 == 'Not yet got'

        # Then got when available.
        pool.release(c1)
        time.sleep(0.05)
        c3 = q.get_nowait()
        assert c1 == c3

    def test_max_connections_timeout(self):
        """Getting a connection raises ``ConnectionError`` after timeout."""

        pool = self.get_pool(max_connections=2, timeout=0.1)
        pool.get_connection('_')
        pool.get_connection('_')
        with pytest.raises(redis.ConnectionError):
            pool.get_connection('_')

    def test_reuse_previously_released_connection(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        pool.release(c1)
        c2 = pool.get_connection('_')
        assert c1 == c2

    def test_repr_contains_db_info_tcp(self):
        pool = redis.BlockingConnectionPool(
            host='localhost',
            port=6379,
            db=0,
        )

        assert re.match('(.*)<(.*)<(.*)>>', repr(pool)).groups() == (
            'BlockingConnectionPool',
            'Connection',
            'host=localhost,port=6379,db=0',
        )

    def test_repr_contains_db_info_unix(self):
        pool = redis.BlockingConnectionPool(
            connection_class=redis.UnixDomainSocketConnection,
            path='abc',
            db=0
        )

        assert re.match('(.*)<(.*)<(.*)>>', repr(pool)).groups() == (
            'BlockingConnectionPool',
            'UnixDomainSocketConnection',
            'path=abc,db=0',
        )


class TestConnection(object):
    def test_on_connect_error(self):
        """
        An error in Connection.on_connect should disconnect from the server
        see for details: https://github.com/andymccurdy/redis-py/issues/368
        """
        # this assumed the Redis server being tested against doesn't have
        # 9999 databases ;)
        bad_connection = redis.Redis(db=9999)
        # an error should be raised on connect
        with pytest.raises(redis.RedisError):
            bad_connection.info()
        pool = bad_connection.connection_pool
        assert len(pool._available_connections) == 1
        assert not pool._available_connections[0]._sock
