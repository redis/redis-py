import redis
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
        
    