import redis
import threading
import time
import unittest

class ConnectionPoolTestCase(unittest.TestCase):
    def test_multiple_connections(self):
        # 2 clients to the same host/port/db/pool should use the same connection
        pool = redis.ConnectionPool()
        r1 = redis.Redis(host='localhost', port=6379, db=9, connection_pool=pool)
        r2 = redis.Redis(host='localhost', port=6379, db=9, connection_pool=pool)
        self.assertEquals(r1.connection, r2.connection)

        # if one of them switches, they should have
        # separate conncetion objects
        r2.select(db=10, host='localhost', port=6379)
        self.assertNotEqual(r1.connection, r2.connection)

        conns = [r1.connection, r2.connection]
        conns.sort()

        # but returning to the original state shares the object again
        r2.select(db=9, host='localhost', port=6379)
        self.assertEquals(r1.connection, r2.connection)

        # the connection manager should still have just 2 connections
        mgr_conns = pool.get_all_connections()
        mgr_conns.sort()
        self.assertEquals(conns, mgr_conns)

    def test_threaded_workers(self):
        r = redis.Redis(host='localhost', port=6379, db=9)
        r.set('a', 'foo')
        r.set('b', 'bar')

        def _info_worker():
            for i in range(50):
                _ = r.info()
                time.sleep(0.01)

        def _keys_worker():
            for i in range(50):
                _ = r.keys()
                time.sleep(0.01)

        t1 = threading.Thread(target=_info_worker)
        t2 = threading.Thread(target=_keys_worker)
        t1.start()
        t2.start()

        for i in [t1, t2]:
            i.join()

