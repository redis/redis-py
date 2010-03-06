import redis
import threading
import time
import unittest

class ConnectionPoolTestCase(unittest.TestCase):
    def test_multiple_connections(self):
        # 2 clients to the same host/port/db should use the same connection
        r1 = redis.Redis(host='localhost', port=6379, db=9)
        r2 = redis.Redis(host='localhost', port=6379, db=9)
        self.assertEquals(r1.connection, r2.connection)
        
        # if one o them switches, they should have
        # separate conncetion objects
        r2.select('localhost', 6379, db=10)
        self.assertNotEqual(r1.connection, r2.connection)
        
        # but returning to the original state shares the object again
        r2.select('localhost', 6379, db=9)
        self.assertEquals(r1.connection, r2.connection)
        
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
        
        