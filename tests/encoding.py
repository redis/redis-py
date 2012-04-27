from __future__ import with_statement
import redis
from redis.connection import PythonParser, HiredisParser
import unittest

class EncodingTestCase(unittest.TestCase):
    def setUp(self):
        self.client = redis.Redis(host='localhost', port=6379, db=9,
            charset='utf-8')
        self.client.flushdb()

    def tearDown(self):
        self.client.flushdb()

    def test_encoding(self):
        unicode_string = unichr(3456) + u'abcd' + unichr(3421)
        self.client.set('unicode-string', unicode_string)
        cached_val = self.client.get('unicode-string')
        self.assertEqual('unicode', type(cached_val).__name__,
            'Cache returned value with type "%s", expected "unicode"' \
            % type(cached_val).__name__
        )
        self.assertEqual(unicode_string, cached_val)

class PythonParserEncodingTestCase(EncodingTestCase):
    def setUp(self):
        self.client = redis.Redis(host='localhost', port=6379, db=9,
            charset='utf-8', parser_class=PythonParser)
        self.client.flushdb()

class HiredisEncodingTestCase(EncodingTestCase):
    def setUp(self):
        self.client = redis.Redis(host='localhost', port=6379, db=9,
            charset='utf-8', parser_class=HiredisParser)
        self.client.flushdb()