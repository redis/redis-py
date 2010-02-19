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
        pipe.set('a', 'a1').get('a').zadd('z', 'z1', 1).zadd('z', 'z2', 4)
        pipe.zincrby('z', 'z1').zrange('z', 0, 5, withscores=True)
        self.assertEquals(pipe.execute(),
            [
                True,
                'a1',
                True,
                True,
                2.0,
                [('z1', 2.0), ('z2', 4)]
            ]
            )
