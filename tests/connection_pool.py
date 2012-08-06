import os
import unittest

import redis


class DummyConnection(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()


class ConnectionPoolTestCase(unittest.TestCase):
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
        self.assertEquals(connection.kwargs, connection_info)

    def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        c2 = pool.get_connection('_')
        self.assert_(c1 != c2)

    def test_max_connections(self):
        pool = self.get_pool(max_connections=2)
        c1 = pool.get_connection('_')
        c2 = pool.get_connection('_')
        self.assertRaises(redis.ConnectionError, pool.get_connection, '_')

    def test_release(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        pool.release(c1)
        c2 = pool.get_connection('_')
        self.assertEquals(c1, c2)
