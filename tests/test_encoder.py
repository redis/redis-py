# -*- coding: utf-8 -*-
from __future__ import with_statement
import pytest
import sys

from redis.connection import Encoder, Token
from redis._compat import b, long, unicode


@pytest.fixture()
def encoder():
    return Encoder('utf-8', 'strict', False)


def test_encode_token(encoder):
    token = Token.get_token('foo')
    assert encoder.encode(token) == b('foo')


def test_encode_bytes(encoder):
    assert encoder.encode('Hello World') == b('Hello World')


def test_encode_bool(encoder):
    assert encoder.encode(True) == b('1')


def test_encode_int(encoder):
    assert encoder.encode(123) == b('123')
    assert encoder.encode(-42) == b('-42')


def test_encode_long(encoder):
    assert encoder.encode(long(123)) == b('123')
    assert encoder.encode(long(-42)) == b('-42')


def test_encode_float(encoder):
    assert encoder.encode(1e-2) == b('0.01')
    if sys.version_info[0] == 2 and sys.version_info[1] < 7:
        assert encoder.encode(3.14) == b('3.1400000000000001')
    else:
        assert encoder.encode(3.14) == b('3.14')


def test_encode_unicode(encoder):
    assert encoder.encode(unicode('hello')) == b('hello')


def test_decode_string(encoder):
    assert encoder.decode(b('hello')) == unicode('hello')
