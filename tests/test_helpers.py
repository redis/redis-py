import string

import pytest

from redis.commands.helpers import (
    delist,
    list_or_args,
    nativestr,
    parse_to_list,
    random_string,
)
from redis.utils import str_if_bytes


@pytest.mark.fixed_client
def test_list_or_args():
    k = ["hello, world"]
    a = ["some", "argument", "list"]
    assert list_or_args(k, a) == k + a

    for i in ["banana", b"banana", bytearray(b"banana"), memoryview(b"banana")]:
        assert list_or_args(i, a) == [i] + a


@pytest.mark.fixed_client
def test_parse_to_list():
    assert parse_to_list(None) == []
    r = ["hello", b"my name", "45", "555.55", "is simon!", None]
    assert parse_to_list(r) == ["hello", "my name", 45, 555.55, "is simon!", None]


@pytest.mark.fixed_client
def test_nativestr():
    assert nativestr("teststr") == "teststr"
    assert nativestr(b"teststr") == "teststr"
    assert nativestr("null") is None


@pytest.mark.fixed_client
def test_parse_to_list_with_str_if_bytes_decoder():
    # A key literally named "null" stays "null"; numeric coercion is kept.
    assert parse_to_list([b"null", b"cpu"], decoder=str_if_bytes) == ["null", "cpu"]
    assert parse_to_list([b"2"], decoder=str_if_bytes) == [2]


@pytest.mark.fixed_client
def test_delist():
    assert delist(None) is None
    assert delist([b"hello", "world", b"banana"]) == ["hello", "world", "banana"]


@pytest.mark.fixed_client
def test_random_string():
    assert len(random_string()) == 10
    assert len(random_string(15)) == 15
    for a in random_string():
        assert a in string.ascii_lowercase
