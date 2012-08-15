import unittest

from redis._compat import b, next
from redis.exceptions import ConnectionError
import redis


class PubSubTestCase(unittest.TestCase):
    def setUp(self):
        self.connection_pool = redis.ConnectionPool()
        self.client = redis.Redis(connection_pool=self.connection_pool)
        self.pubsub = self.client.pubsub()

    def tearDown(self):
        self.connection_pool.disconnect()

    def test_channel_subscribe(self):
        # subscribe doesn't return anything
        self.assertEquals(
            self.pubsub.subscribe('foo'),
            None
        )
        # send a message
        self.assertEquals(self.client.publish('foo', 'hello foo'), 1)
        # there should be now 2 messages in the buffer, a subscribe and the
        # one we just published
        self.assertEquals(
            next(self.pubsub.listen()),
            {
                'type': 'subscribe',
                'pattern': None,
                'channel': 'foo',
                'data': 1
            }
        )
        self.assertEquals(
            next(self.pubsub.listen()),
            {
                'type': 'message',
                'pattern': None,
                'channel': 'foo',
                'data': b('hello foo')
            }
        )

        # unsubscribe
        self.assertEquals(
            self.pubsub.unsubscribe('foo'),
            None
        )
        # unsubscribe message should be in the buffer
        self.assertEquals(
            next(self.pubsub.listen()),
            {
                'type': 'unsubscribe',
                'pattern': None,
                'channel': 'foo',
                'data': 0
            }
        )

    def test_pattern_subscribe(self):
        # psubscribe doesn't return anything
        self.assertEquals(
            self.pubsub.psubscribe('f*'),
            None
        )
        # send a message
        self.assertEquals(self.client.publish('foo', 'hello foo'), 1)
        # there should be now 2 messages in the buffer, a subscribe and the
        # one we just published
        self.assertEquals(
            next(self.pubsub.listen()),
            {
                'type': 'psubscribe',
                'pattern': None,
                'channel': 'f*',
                'data': 1
            }
        )
        self.assertEquals(
            next(self.pubsub.listen()),
            {
                'type': 'pmessage',
                'pattern': 'f*',
                'channel': 'foo',
                'data': b('hello foo')
            }
        )

        # unsubscribe
        self.assertEquals(
            self.pubsub.punsubscribe('f*'),
            None
        )
        # unsubscribe message should be in the buffer
        self.assertEquals(
            next(self.pubsub.listen()),
            {
                'type': 'punsubscribe',
                'pattern': None,
                'channel': 'f*',
                'data': 0
            }
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
