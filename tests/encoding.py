from __future__ import with_statement
import unittest

from redis._compat import unichr, u, unicode
from redis.connection import ConnectionPool, PythonParser, HiredisParser
import redis


class EncodingTestCase(unittest.TestCase):
    def setUp(self):
        self.client = redis.Redis(
            host='localhost', port=6379, db=9, charset='utf-8')
        self.client.flushdb()

    def tearDown(self):
        self.client.flushdb()

    def test_simple_encoding(self):
        unicode_string = unichr(3456) + u('abcd') + unichr(3421)
        self.client.set('unicode-string', unicode_string)
        cached_val = self.client.get('unicode-string')
        self.assertEquals(
            unicode.__name__, type(cached_val).__name__,
            'Cache returned value with type "%s", expected "%s"' %
            (type(cached_val).__name__, unicode.__name__))
        self.assertEqual(unicode_string, cached_val)

    def test_list_encoding(self):
        unicode_string = unichr(3456) + u('abcd') + unichr(3421)
        result = [unicode_string, unicode_string, unicode_string]
        for i in range(len(result)):
            self.client.rpush('a', unicode_string)
        self.assertEquals(self.client.lrange('a', 0, -1), result)


class PythonParserEncodingTestCase(EncodingTestCase):
    def setUp(self):
        pool = ConnectionPool(
            host='localhost', port=6379, db=9,
            encoding='utf-8', decode_responses=True, parser_class=PythonParser)
        self.client = redis.Redis(connection_pool=pool)
        self.client.flushdb()


class HiredisEncodingTestCase(EncodingTestCase):
    def setUp(self):
        pool = ConnectionPool(
            host='localhost', port=6379, db=9,
            encoding='utf-8', decode_responses=True,
            parser_class=HiredisParser)
        self.client = redis.Redis(connection_pool=pool)
        self.client.flushdb()
