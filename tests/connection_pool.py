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
        pool.get_connection('_')
        pool.get_connection('_')
        self.assertRaises(redis.ConnectionError, pool.get_connection, '_')

    def test_blocking_max_connections(self):
        pool = self.get_pool(max_connections=2)
        pool.get_connection('_')
        pool.get_connection('_')
        self.assertRaises(redis.ConnectionError, pool.get_connection, '_')

    def test_release(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        pool.release(c1)
        c2 = pool.get_connection('_')
        self.assertEquals(c1, c2)


class BlockingConnectionPoolTestCase(unittest.TestCase):
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
        self.assertEquals(connection.kwargs, connection_info)

    def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        c2 = pool.get_connection('_')
        self.assert_(c1 != c2)

    def test_max_connections_blocks(self):
        """Getting a connection should block for until available."""

        import time
        from threading import Thread

        # We use a queue for cross thread communication within the unit test.
        try:  # Python 3
            from queue import Queue
        except ImportError:
            from Queue import Queue

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
        self.assertEquals(c3, 'Not yet got')

        # Then got when available.
        pool.release(c1)
        time.sleep(0.05)
        c3 = q.get_nowait()
        self.assertEquals(c1, c3)

    def test_max_connections_timeout(self):
        """Getting a connection raises ``ConnectionError`` after timeout."""

        pool = self.get_pool(max_connections=2, timeout=0.1)
        pool.get_connection('_')
        pool.get_connection('_')
        self.assertRaises(redis.ConnectionError, pool.get_connection, '_')

    def test_release(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        pool.release(c1)
        c2 = pool.get_connection('_')
        self.assertEquals(c1, c2)
