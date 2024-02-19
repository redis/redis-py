import string

from redis.commands.helpers import (
    delist,
    list_or_args,
    nativestr,
    parse_to_dict,
    parse_to_list,
    quote_string,
    random_string,
)


def test_list_or_args():
    k = ["hello, world"]
    a = ["some", "argument", "list"]
    assert list_or_args(k, a) == k + a

    for i in ["banana", b"banana"]:
        assert list_or_args(i, a) == [i] + a


def test_parse_to_list():
    assert parse_to_list(None) == []
    r = ["hello", b"my name", "45", "555.55", "is simon!", None]
    assert parse_to_list(r) == ["hello", "my name", 45, 555.55, "is simon!", None]


def test_parse_to_dict():
    assert parse_to_dict(None) == {}
    r = [
        ["Some number", "1.0345"],
        ["Some string", "hello"],
        [
            "Child iterators",
            [
                "Time",
                "0.2089",
                "Counter",
                3,
                "Child iterators",
                ["Type", "bar", "Time", "0.0729", "Counter", 3],
                ["Type", "barbar", "Time", "0.058", "Counter", 3],
                ["Type", "barbarbar", "Time", "0.0234", "Counter", 3],
            ],
        ],
    ]
    assert parse_to_dict(r) == {
        "Child iterators": {
            "Child iterators": [
                {"Counter": 3.0, "Time": 0.0729, "Type": "bar"},
                {"Counter": 3.0, "Time": 0.058, "Type": "barbar"},
                {"Counter": 3.0, "Time": 0.0234, "Type": "barbarbar"},
            ],
            "Counter": 3.0,
            "Time": 0.2089,
        },
        "Some number": 1.0345,
        "Some string": "hello",
    }


def test_nativestr():
    assert nativestr("teststr") == "teststr"
    assert nativestr(b"teststr") == "teststr"
    assert nativestr("null") is None


def test_delist():
    assert delist(None) is None
    assert delist([b"hello", "world", b"banana"]) == ["hello", "world", "banana"]


def test_random_string():
    assert len(random_string()) == 10
    assert len(random_string(15)) == 15
    for a in random_string():
        assert a in string.ascii_lowercase


def test_quote_string():
    assert quote_string("hello world!") == '"hello world!"'
    assert quote_string("") == '""'
    assert quote_string("hello world!") == '"hello world!"'
    assert quote_string("abc") == '"abc"'
    assert quote_string("") == '""'
    assert quote_string('"') == r'"\""'
    assert quote_string(r"foo \ bar") == r'"foo \\ bar"'
    assert quote_string(r"foo \" bar") == r'"foo \\\" bar"'
    assert quote_string('a"a') == r'"a\"a"'
