import redis
import unittest

from redis.exceptions import ConnectionError

class PubSubTestCase(unittest.TestCase):
    def setUp(self):
        self.connection_pool = redis.ConnectionPool()
        self.client = redis.Redis(connection_pool=self.connection_pool)
        self.pubsub = self.client.pubsub()

    def tearDown(self):
        self.connection_pool.disconnect()

    def test_channel_subscribe(self):
        self.assertEquals(
            self.pubsub.subscribe('foo'),
            ['subscribe', 'foo', 1]
            )
        self.assertEquals(self.client.publish('foo', 'hello foo'), 1)
        self.assertEquals(
            self.pubsub.listen().next(),
            {
                'type': 'message',
                'pattern': None,
                'channel': 'foo',
                'data': 'hello foo'
            }
            )
        self.assertEquals(
            self.pubsub.unsubscribe('foo'),
            ['unsubscribe', 'foo', 0]
            )

    def test_pattern_subscribe(self):
        self.assertEquals(
            self.pubsub.psubscribe('fo*'),
            ['psubscribe', 'fo*', 1]
            )
        self.assertEquals(self.client.publish('foo', 'hello foo'), 1)
        self.assertEquals(
            self.pubsub.listen().next(),
            {
                'type': 'pmessage',
                'pattern': 'fo*',
                'channel': 'foo',
                'data': 'hello foo'
            }
            )
        self.assertEquals(
            self.pubsub.punsubscribe('fo*'),
            ['punsubscribe', 'fo*', 0]
            )

class PubSubRedisDownTestCase(unittest.TestCase):
    def setUp(self):
        self.connection_pool = redis.ConnectionPool(port=6390)
        self.client = redis.Redis(connection_pool=self.connection_pool)
        self.pubsub = self.client.pubsub()

    def tearDown(self):
        self.connection_pool.disconnect()

    def test_channel_subscribe(self):
        got_exception = False
        try:
            self.pubsub.subscribe('foo')
        except ConnectionError:
            got_exception = True
        self.assertTrue(got_exception)
