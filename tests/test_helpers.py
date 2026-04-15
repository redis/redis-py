import string

import pytest

from redis.commands.helpers import (
    delist,
    list_or_args,
    nativestr,
    parse_to_list,
    random_string,
)


@pytest.mark.fixed_client
def test_list_or_args():
    k = ["hello, world"]
    a = ["some", "argument", "list"]
    assert list_or_args(k, a) == k + a

    for i in ["banana", b"banana"]:
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
def test_delist():
    assert delist(None) is None
    assert delist([b"hello", "world", b"banana"]) == ["hello", "world", "banana"]


@pytest.mark.fixed_client
def test_random_string():
    assert len(random_string()) == 10
    assert len(random_string(15)) == 15
    for a in random_string():
        assert a in string.ascii_lowercase
