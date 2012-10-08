from __future__ import with_statement
import unittest

import redis
from redis._compat import b

class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.client = redis.Redis(host='localhost', port=6379, db=9)
        self.client.flushdb()

    def tearDown(self):
        self.client.flushdb()

    def test_pipeline(self):
        with self.client.pipeline() as pipe:
            pipe.set('a', 'a1').get('a').zadd('z', z1=1).zadd('z', z2=4)
            pipe.zincrby('z', 'z1').zrange('z', 0, 5, withscores=True)
            self.assertEquals(
                pipe.execute(),
                [
                    True,
                    b('a1'),
                    True,
                    True,
                    2.0,
                    [(b('z1'), 2.0), (b('z2'), 4)],
                ]
            )

    def test_pipeline_no_transaction(self):
        with self.client.pipeline(transaction=False) as pipe:
            pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
            self.assertEquals(pipe.execute(), [True, True, True])
            self.assertEquals(self.client['a'], b('a1'))
            self.assertEquals(self.client['b'], b('b1'))
            self.assertEquals(self.client['c'], b('c1'))

    def test_pipeline_no_transaction_watch(self):
        self.client.set('a', 0)

        with self.client.pipeline(transaction=False) as pipe:
            pipe.watch('a')
            a = pipe.get('a')

            pipe.multi()
            pipe.set('a', int(a) + 1)
            result = pipe.execute()
            self.assertEquals(result, [True])

    def test_pipeline_no_transaction_watch_failure(self):
        self.client.set('a', 0)

        with self.client.pipeline(transaction=False) as pipe:
            pipe.watch('a')
            a = pipe.get('a')

            self.client.set('a', 'bad')

            pipe.multi()
            pipe.set('a', int(a) + 1)
            self.assertRaises(redis.WatchError, pipe.execute)

    def test_exec_error_in_response(self):
        # an invalid pipeline command at exec time adds the exception instance
        # to the list of returned values
        self.client['c'] = 'a'
        with self.client.pipeline() as pipe:
            pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4)
            result = pipe.execute(raise_on_error=False)

            self.assertEquals(result[0], True)
            self.assertEquals(self.client['a'], b('1'))
            self.assertEquals(result[1], True)
            self.assertEquals(self.client['b'], b('2'))
            # we can't lpush to a key that's a string value, so this should
            # be a ResponseError exception
            self.assert_(isinstance(result[2], redis.ResponseError))
            self.assertEquals(self.client['c'], b('a'))
            self.assertEquals(result[3], True)
            self.assertEquals(self.client['d'], b('4'))

            # make sure the pipe was restored to a working state
            self.assertEquals(pipe.set('z', 'zzz').execute(), [True])
            self.assertEquals(self.client['z'], b('zzz'))

    def test_exec_error_raised(self):
        self.client['c'] = 'a'
        with self.client.pipeline() as pipe:
            pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4)
            self.assertRaises(redis.ResponseError, pipe.execute)

            # make sure the pipe was restored to a working state
            self.assertEquals(pipe.set('z', 'zzz').execute(), [True])
            self.assertEquals(self.client['z'], b('zzz'))

    def test_parse_error_in_response(self):
        with self.client.pipeline() as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set('a', 1).zrem('b').set('b', 2)
            result = pipe.execute(raise_on_error=False)

            self.assertEquals(result[0], True)
            self.assertEquals(self.client['a'], b('1'))
            self.assert_(isinstance(result[1], redis.ResponseError))
            self.assertEquals(result[2], True)
            self.assertEquals(self.client['b'], b('2'))

            # make sure the pipe was restored to a working state
            self.assertEquals(pipe.set('z', 'zzz').execute(), [True])
            self.assertEquals(self.client['z'], b('zzz'))

    def test_parse_error_raised(self):
        with self.client.pipeline() as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set('a', 1).zrem('b').set('b', 2)
            self.assertRaises(redis.ResponseError, pipe.execute)

            # make sure the pipe was restored to a working state
            self.assertEquals(pipe.set('z', 'zzz').execute(), [True])
            self.assertEquals(self.client['z'], b('zzz'))

    def test_watch_succeed(self):
        self.client.set('a', 1)
        self.client.set('b', 2)

        with self.client.pipeline() as pipe:
            pipe.watch('a', 'b')
            self.assertEquals(pipe.watching, True)
            a_value = pipe.get('a')
            b_value = pipe.get('b')
            self.assertEquals(a_value, b('1'))
            self.assertEquals(b_value, b('2'))
            pipe.multi()

            pipe.set('c', 3)
            self.assertEquals(pipe.execute(), [True])
            self.assertEquals(pipe.watching, False)

    def test_watch_failure(self):
        self.client.set('a', 1)
        self.client.set('b', 2)

        with self.client.pipeline() as pipe:
            pipe.watch('a', 'b')
            self.client.set('b', 3)
            pipe.multi()
            pipe.get('a')
            self.assertRaises(redis.WatchError, pipe.execute)
            self.assertEquals(pipe.watching, False)

    def test_unwatch(self):
        self.client.set('a', 1)
        self.client.set('b', 2)

        with self.client.pipeline() as pipe:
            pipe.watch('a', 'b')
            self.client.set('b', 3)
            pipe.unwatch()
            self.assertEquals(pipe.watching, False)
            pipe.get('a')
            self.assertEquals(pipe.execute(), [b('1')])

    def test_transaction_callable(self):
        self.client.set('a', 1)
        self.client.set('b', 2)
        has_run = []

        def my_transaction(pipe):
            a_value = pipe.get('a')
            self.assert_(a_value in (b('1'), b('2')))
            b_value = pipe.get('b')
            self.assertEquals(b_value, b('2'))

            # silly run-once code... incr's a so WatchError should be raised
            # forcing this all to run again
            if not has_run:
                self.client.incr('a')
                has_run.append('it has')

            pipe.multi()
            pipe.set('c', int(a_value) + int(b_value))

        result = self.client.transaction(my_transaction, 'a', 'b')
        self.assertEquals(result, [True])
        self.assertEquals(self.client.get('c'), b('4'))
