import redis
import unittest

class MonitorTestCase(unittest.TestCase):
    def setUp(self):
        self.connection_pool = redis.ConnectionPool()
        self.client = redis.Redis(connection_pool=self.connection_pool)
        self.monitor = self.client.monitor()

    def tearDown(self):
        self.connection_pool.disconnect()

    def test_monitor(self):
        self.assertEquals(self.monitor.monitor(), 'OK')

    def test_listen(self):
        self.monitor.monitor()
        self.assertEquals(self.monitor.listen().next()['command'], '"MONITOR"')
        self.client.set('foo', 'bar')
        self.assertEquals(self.monitor.listen().next()['command'], '"SET" "foo" "bar"')
