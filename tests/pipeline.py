import redis
import unittest

class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.client = redis.Redis(host='localhost', port=6379, db=9)
        self.client.flushdb()

    def tearDown(self):
        self.client.flushdb()

    def test_pipeline(self):
        pipe = self.client.pipeline()
        pipe.set('a', 'a1').get('a').zadd('z', z1=1).zadd('z', z2=4)
        pipe.zincrby('z', 'z1').zrange('z', 0, 5, withscores=True)
        self.assertEquals(pipe.execute(),
            [
                True,
                'a1',
                True,
                True,
                2.0,
                [('z1', 2.0), ('z2', 4)],
            ]
            )

    def test_invalid_command_in_pipeline(self):
        # all commands but the invalid one should be excuted correctly
        self.client['c'] = 'a'
        pipe = self.client.pipeline()
        pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4)
        result = pipe.execute()

        self.assertEquals(result[0], True)
        self.assertEquals(self.client['a'], '1')
        self.assertEquals(result[1], True)
        self.assertEquals(self.client['b'], '2')
        # we can't lpush to a key that's a string value, so this should
        # be a ResponseError exception
        self.assert_(isinstance(result[2], redis.ResponseError))
        self.assertEquals(self.client['c'], 'a')
        self.assertEquals(result[3], True)
        self.assertEquals(self.client['d'], '4')

        # make sure the pipe was restored to a working state
        self.assertEquals(pipe.set('z', 'zzz').execute(), [True])
        self.assertEquals(self.client['z'], 'zzz')

    def test_pipeline_no_transaction(self):
        pipe = self.client.pipeline(transaction=False)
        pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
        self.assertEquals(pipe.execute(), [True, True, True])
        self.assertEquals(self.client['a'], 'a1')
        self.assertEquals(self.client['b'], 'b1')
        self.assertEquals(self.client['c'], 'c1')

