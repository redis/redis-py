from __future__ import unicode_literals
import pytest
import redis

from redis._compat import unichr, unicode
from .conftest import _get_client


class TestEncoding(object):
    @pytest.fixture()
    def r(self, request):
        return _get_client(redis.Redis, request=request, decode_responses=True)

    def test_simple_encoding(self, r):
        unicode_string = unichr(3456) + 'abcd' + unichr(3421)
        r['unicode-string'] = unicode_string
        cached_val = r['unicode-string']
        assert isinstance(cached_val, unicode)
        assert unicode_string == cached_val

    def test_list_encoding(self, r):
        unicode_string = unichr(3456) + 'abcd' + unichr(3421)
        result = [unicode_string, unicode_string, unicode_string]
        r.rpush('a', *result)
        assert r.lrange('a', 0, -1) == result

    def test_object_value(self, r):
        unicode_string = unichr(3456) + 'abcd' + unichr(3421)
        r['unicode-string'] = Exception(unicode_string)
        cached_val = r['unicode-string']
        assert isinstance(cached_val, unicode)
        assert unicode_string == cached_val


class TestCommandsAndTokensArentEncoded(object):
    @pytest.fixture()
    def r(self, request):
        return _get_client(redis.Redis, request=request, encoding='utf-16')

    def test_basic_command(self, r):
        r.set('hello', 'world')
