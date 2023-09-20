import pytest
import redis
from redis import Redis, exceptions
from redis.commands.json.decoders import decode_list, unstring
from redis.commands.json.path import Path

from .conftest import _get_client, assert_resp_response, skip_ifmodversion_lt


@pytest.fixture
def client(request):
    r = _get_client(Redis, request, decode_responses=True)
    r.flushdb()
    return r


def test_json_setbinarykey(client):
    d = {"hello": "world", b"some": "value"}
    with pytest.raises(TypeError):
        client.json().set("somekey", Path.root_path(), d)
    assert client.json().set("somekey", Path.root_path(), d, decode_keys=True)


def test_json_setgetdeleteforget(client):
    assert client.json().set("foo", Path.root_path(), "bar")
    assert_resp_response(client, client.json().get("foo"), "bar", [["bar"]])
    assert client.json().get("baz") is None
    assert client.json().delete("foo") == 1
    assert client.json().forget("foo") == 0  # second delete
    assert client.exists("foo") == 0


def test_jsonget(client):
    client.json().set("foo", Path.root_path(), "bar")
    assert_resp_response(client, client.json().get("foo"), "bar", [["bar"]])


def test_json_get_jset(client):
    assert client.json().set("foo", Path.root_path(), "bar")
    assert_resp_response(client, client.json().get("foo"), "bar", [["bar"]])
    assert client.json().get("baz") is None
    assert 1 == client.json().delete("foo")
    assert client.exists("foo") == 0


@skip_ifmodversion_lt("2.06.00", "ReJSON")  # todo: update after the release
def test_json_merge(client):
    # Test with root path $
    assert client.json().set(
        "person_data",
        "$",
        {"person1": {"personal_data": {"name": "John"}}},
    )
    assert client.json().merge(
        "person_data", "$", {"person1": {"personal_data": {"hobbies": "reading"}}}
    )
    assert client.json().get("person_data") == {
        "person1": {"personal_data": {"name": "John", "hobbies": "reading"}}
    }

    # Test with root path path $.person1.personal_data
    assert client.json().merge(
        "person_data", "$.person1.personal_data", {"country": "Israel"}
    )
    assert client.json().get("person_data") == {
        "person1": {
            "personal_data": {"name": "John", "hobbies": "reading", "country": "Israel"}
        }
    }

    # Test with null value to delete a value
    assert client.json().merge("person_data", "$.person1.personal_data", {"name": None})
    assert client.json().get("person_data") == {
        "person1": {"personal_data": {"country": "Israel", "hobbies": "reading"}}
    }


def test_nonascii_setgetdelete(client):
    assert client.json().set("notascii", Path.root_path(), "hyvää-élève")
    res = "hyvää-élève"
    assert_resp_response(
        client, client.json().get("notascii", no_escape=True), res, [[res]]
    )
    assert 1 == client.json().delete("notascii")
    assert client.exists("notascii") == 0


def test_jsonsetexistentialmodifiersshouldsucceed(client):
    obj = {"foo": "bar"}
    assert client.json().set("obj", Path.root_path(), obj)

    # Test that flags prevent updates when conditions are unmet
    assert client.json().set("obj", Path("foo"), "baz", nx=True) is None
    assert client.json().set("obj", Path("qaz"), "baz", xx=True) is None

    # Test that flags allow updates when conditions are met
    assert client.json().set("obj", Path("foo"), "baz", xx=True)
    assert client.json().set("obj", Path("qaz"), "baz", nx=True)

    # Test that flags are mutually exlusive
    with pytest.raises(Exception):
        client.json().set("obj", Path("foo"), "baz", nx=True, xx=True)


def test_mgetshouldsucceed(client):
    client.json().set("1", Path.root_path(), 1)
    client.json().set("2", Path.root_path(), 2)
    assert client.json().mget(["1"], Path.root_path()) == [1]

    assert client.json().mget([1, 2], Path.root_path()) == [1, 2]


@pytest.mark.onlynoncluster
@skip_ifmodversion_lt("2.06.00", "ReJSON")
def test_mset(client):
    client.json().mset([("1", Path.root_path(), 1), ("2", Path.root_path(), 2)])

    assert client.json().mget(["1"], Path.root_path()) == [1]
    assert client.json().mget(["1", "2"], Path.root_path()) == [1, 2]


@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
def test_clear(client):
    client.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 1 == client.json().clear("arr", Path.root_path())
    assert_resp_response(client, client.json().get("arr"), [], [[[]]])


def test_type(client):
    client.json().set("1", Path.root_path(), 1)
    assert_resp_response(
        client, client.json().type("1", Path.root_path()), "integer", ["integer"]
    )
    assert_resp_response(client, client.json().type("1"), "integer", ["integer"])


def test_numincrby(client):
    client.json().set("num", Path.root_path(), 1)
    assert_resp_response(
        client, client.json().numincrby("num", Path.root_path(), 1), 2, [2]
    )
    assert_resp_response(
        client, client.json().numincrby("num", Path.root_path(), 0.5), 2.5, [2.5]
    )
    assert_resp_response(
        client, client.json().numincrby("num", Path.root_path(), -1.25), 1.25, [1.25]
    )


def test_nummultby(client):
    client.json().set("num", Path.root_path(), 1)

    with pytest.deprecated_call():
        assert_resp_response(
            client, client.json().nummultby("num", Path.root_path(), 2), 2, [2]
        )
        assert_resp_response(
            client, client.json().nummultby("num", Path.root_path(), 2.5), 5, [5]
        )
        assert_resp_response(
            client, client.json().nummultby("num", Path.root_path(), 0.5), 2.5, [2.5]
        )


@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
def test_toggle(client):
    client.json().set("bool", Path.root_path(), False)
    assert client.json().toggle("bool", Path.root_path())
    assert client.json().toggle("bool", Path.root_path()) is False
    # check non-boolean value
    client.json().set("num", Path.root_path(), 1)
    with pytest.raises(redis.exceptions.ResponseError):
        client.json().toggle("num", Path.root_path())


def test_strappend(client):
    client.json().set("jsonkey", Path.root_path(), "foo")
    assert 6 == client.json().strappend("jsonkey", "bar")
    assert_resp_response(
        client, client.json().get("jsonkey", Path.root_path()), "foobar", [["foobar"]]
    )


# # def test_debug(client):
#    client.json().set("str", Path.root_path(), "foo")
#    assert 24 == client.json().debug("MEMORY", "str", Path.root_path())
#    assert 24 == client.json().debug("MEMORY", "str")
#
#    # technically help is valid
#    assert isinstance(client.json().debug("HELP"), list)


def test_strlen(client):
    client.json().set("str", Path.root_path(), "foo")
    assert 3 == client.json().strlen("str", Path.root_path())
    client.json().strappend("str", "bar", Path.root_path())
    assert 6 == client.json().strlen("str", Path.root_path())
    assert 6 == client.json().strlen("str")


def test_arrappend(client):
    client.json().set("arr", Path.root_path(), [1])
    assert 2 == client.json().arrappend("arr", Path.root_path(), 2)
    assert 4 == client.json().arrappend("arr", Path.root_path(), 3, 4)
    assert 7 == client.json().arrappend("arr", Path.root_path(), *[5, 6, 7])


def test_arrindex(client):
    client.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 1 == client.json().arrindex("arr", Path.root_path(), 1)
    assert -1 == client.json().arrindex("arr", Path.root_path(), 1, 2)
    assert 4 == client.json().arrindex("arr", Path.root_path(), 4)
    assert 4 == client.json().arrindex("arr", Path.root_path(), 4, start=0)
    assert 4 == client.json().arrindex("arr", Path.root_path(), 4, start=0, stop=5000)
    assert -1 == client.json().arrindex("arr", Path.root_path(), 4, start=0, stop=-1)
    assert -1 == client.json().arrindex("arr", Path.root_path(), 4, start=1, stop=3)


def test_arrinsert(client):
    client.json().set("arr", Path.root_path(), [0, 4])
    assert 5 - -client.json().arrinsert("arr", Path.root_path(), 1, *[1, 2, 3])
    res = [0, 1, 2, 3, 4]
    assert_resp_response(client, client.json().get("arr"), res, [[res]])

    # test prepends
    client.json().set("val2", Path.root_path(), [5, 6, 7, 8, 9])
    client.json().arrinsert("val2", Path.root_path(), 0, ["some", "thing"])
    res = [["some", "thing"], 5, 6, 7, 8, 9]
    assert_resp_response(client, client.json().get("val2"), res, [[res]])


def test_arrlen(client):
    client.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 5 == client.json().arrlen("arr", Path.root_path())
    assert 5 == client.json().arrlen("arr")
    assert client.json().arrlen("fakekey") is None


def test_arrpop(client):
    client.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 4 == client.json().arrpop("arr", Path.root_path(), 4)
    assert 3 == client.json().arrpop("arr", Path.root_path(), -1)
    assert 2 == client.json().arrpop("arr", Path.root_path())
    assert 0 == client.json().arrpop("arr", Path.root_path(), 0)
    assert_resp_response(client, client.json().get("arr"), [1], [[[1]]])

    # test out of bounds
    client.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 4 == client.json().arrpop("arr", Path.root_path(), 99)

    # none test
    client.json().set("arr", Path.root_path(), [])
    assert client.json().arrpop("arr") is None


def test_arrtrim(client):
    client.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 3 == client.json().arrtrim("arr", Path.root_path(), 1, 3)
    assert_resp_response(client, client.json().get("arr"), [1, 2, 3], [[[1, 2, 3]]])

    # <0 test, should be 0 equivalent
    client.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == client.json().arrtrim("arr", Path.root_path(), -1, 3)

    # testing stop > end
    client.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 2 == client.json().arrtrim("arr", Path.root_path(), 3, 99)

    # start > array size and stop
    client.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == client.json().arrtrim("arr", Path.root_path(), 9, 1)

    # all larger
    client.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == client.json().arrtrim("arr", Path.root_path(), 9, 11)


def test_resp(client):
    obj = {"foo": "bar", "baz": 1, "qaz": True}
    client.json().set("obj", Path.root_path(), obj)
    assert "bar" == client.json().resp("obj", Path("foo"))
    assert 1 == client.json().resp("obj", Path("baz"))
    assert client.json().resp("obj", Path("qaz"))
    assert isinstance(client.json().resp("obj"), list)


def test_objkeys(client):
    obj = {"foo": "bar", "baz": "qaz"}
    client.json().set("obj", Path.root_path(), obj)
    keys = client.json().objkeys("obj", Path.root_path())
    keys.sort()
    exp = list(obj.keys())
    exp.sort()
    assert exp == keys

    client.json().set("obj", Path.root_path(), obj)
    keys = client.json().objkeys("obj")
    assert keys == list(obj.keys())

    assert client.json().objkeys("fakekey") is None


def test_objlen(client):
    obj = {"foo": "bar", "baz": "qaz"}
    client.json().set("obj", Path.root_path(), obj)
    assert len(obj) == client.json().objlen("obj", Path.root_path())

    client.json().set("obj", Path.root_path(), obj)
    assert len(obj) == client.json().objlen("obj")


def test_json_commands_in_pipeline(client):
    p = client.json().pipeline()
    p.set("foo", Path.root_path(), "bar")
    p.get("foo")
    p.delete("foo")
    assert_resp_response(client, p.execute(), [True, "bar", 1], [True, [["bar"]], 1])
    assert client.keys() == []
    assert client.get("foo") is None

    # now with a true, json object
    client.flushdb()
    p = client.json().pipeline()
    d = {"hello": "world", "oh": "snap"}
    with pytest.deprecated_call():
        p.jsonset("foo", Path.root_path(), d)
        p.jsonget("foo")
    p.exists("notarealkey")
    p.delete("foo")
    assert_resp_response(client, p.execute(), [True, d, 0, 1], [True, [[d]], 0, 1])
    assert client.keys() == []
    assert client.get("foo") is None


def test_json_delete_with_dollar(client):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert client.json().set("doc1", "$", doc1)
    assert client.json().delete("doc1", "$..a") == 2
    res = [{"nested": {"b": 3}}]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    assert client.json().set("doc2", "$", doc2)
    assert client.json().delete("doc2", "$..a") == 1
    res = [{"nested": {"b": [True, "a", "b"]}, "b": ["a", "b"]}]
    assert_resp_response(client, client.json().get("doc2", "$"), res, [res])

    doc3 = [
        {
            "ciao": ["non ancora"],
            "nested": [
                {"ciao": [1, "a"]},
                {"ciao": [2, "a"]},
                {"ciaoc": [3, "non", "ciao"]},
                {"ciao": [4, "a"]},
                {"e": [5, "non", "ciao"]},
            ],
        }
    ]
    assert client.json().set("doc3", "$", doc3)
    assert client.json().delete("doc3", '$.[0]["nested"]..ciao') == 3

    doc3val = [
        [
            {
                "ciao": ["non ancora"],
                "nested": [
                    {},
                    {},
                    {"ciaoc": [3, "non", "ciao"]},
                    {},
                    {"e": [5, "non", "ciao"]},
                ],
            }
        ]
    ]
    assert_resp_response(client, client.json().get("doc3", "$"), doc3val, [doc3val])

    # Test default path
    assert client.json().delete("doc3") == 1
    assert client.json().get("doc3", "$") is None

    client.json().delete("not_a_document", "..a")


def test_json_forget_with_dollar(client):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert client.json().set("doc1", "$", doc1)
    assert client.json().forget("doc1", "$..a") == 2
    res = [{"nested": {"b": 3}}]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    assert client.json().set("doc2", "$", doc2)
    assert client.json().forget("doc2", "$..a") == 1
    res = [{"nested": {"b": [True, "a", "b"]}, "b": ["a", "b"]}]
    assert_resp_response(client, client.json().get("doc2", "$"), res, [res])

    doc3 = [
        {
            "ciao": ["non ancora"],
            "nested": [
                {"ciao": [1, "a"]},
                {"ciao": [2, "a"]},
                {"ciaoc": [3, "non", "ciao"]},
                {"ciao": [4, "a"]},
                {"e": [5, "non", "ciao"]},
            ],
        }
    ]
    assert client.json().set("doc3", "$", doc3)
    assert client.json().forget("doc3", '$.[0]["nested"]..ciao') == 3

    doc3val = [
        [
            {
                "ciao": ["non ancora"],
                "nested": [
                    {},
                    {},
                    {"ciaoc": [3, "non", "ciao"]},
                    {},
                    {"e": [5, "non", "ciao"]},
                ],
            }
        ]
    ]
    assert_resp_response(client, client.json().get("doc3", "$"), doc3val, [doc3val])

    # Test default path
    assert client.json().forget("doc3") == 1
    assert client.json().get("doc3", "$") is None

    client.json().forget("not_a_document", "..a")


def test_json_mget_dollar(client):
    # Test mget with multi paths
    client.json().set(
        "doc1",
        "$",
        {"a": 1, "b": 2, "nested": {"a": 3}, "c": None, "nested2": {"a": None}},
    )
    client.json().set(
        "doc2",
        "$",
        {"a": 4, "b": 5, "nested": {"a": 6}, "c": None, "nested2": {"a": [None]}},
    )
    # Compare also to single JSON.GET
    res = [1, 3, None]
    assert_resp_response(client, client.json().get("doc1", "$..a"), res, [res])
    res = [4, 6, [None]]
    assert_resp_response(client, client.json().get("doc2", "$..a"), res, [res])

    # Test mget with single path
    client.json().mget("doc1", "$..a") == [1, 3, None]
    # Test mget with multi path
    client.json().mget(["doc1", "doc2"], "$..a") == [[1, 3, None], [4, 6, [None]]]

    # Test missing key
    client.json().mget(["doc1", "missing_doc"], "$..a") == [[1, 3, None], None]
    res = client.json().mget(["missing_doc1", "missing_doc2"], "$..a")
    assert res == [None, None]


def test_numby_commands_dollar(client):
    # Test NUMINCRBY
    client.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})
    # Test multi
    assert client.json().numincrby("doc1", "$..a", 2) == [None, 4, 7.0, None]

    assert client.json().numincrby("doc1", "$..a", 2.5) == [None, 6.5, 9.5, None]
    # Test single
    assert client.json().numincrby("doc1", "$.b[1].a", 2) == [11.5]

    assert client.json().numincrby("doc1", "$.b[2].a", 2) == [None]
    assert client.json().numincrby("doc1", "$.b[1].a", 3.5) == [15.0]

    # Test NUMMULTBY
    client.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})

    # test list
    with pytest.deprecated_call():
        assert client.json().nummultby("doc1", "$..a", 2) == [None, 4, 10, None]
        assert client.json().nummultby("doc1", "$..a", 2.5) == [None, 10.0, 25.0, None]

    # Test single
    with pytest.deprecated_call():
        assert client.json().nummultby("doc1", "$.b[1].a", 2) == [50.0]
        assert client.json().nummultby("doc1", "$.b[2].a", 2) == [None]
        assert client.json().nummultby("doc1", "$.b[1].a", 3) == [150.0]

    # test missing keys
    with pytest.raises(exceptions.ResponseError):
        client.json().numincrby("non_existing_doc", "$..a", 2)
        client.json().nummultby("non_existing_doc", "$..a", 2)

    # Test legacy NUMINCRBY
    client.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})
    client.json().numincrby("doc1", ".b[0].a", 3) == 5

    # Test legacy NUMMULTBY
    client.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})

    with pytest.deprecated_call():
        client.json().nummultby("doc1", ".b[0].a", 3) == 6


def test_strappend_dollar(client):
    client.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    # Test multi
    client.json().strappend("doc1", "bar", "$..a") == [6, 8, None]

    # res = [{"a": "foobar", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}]
    # assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test single
    client.json().strappend("doc1", "baz", "$.nested1.a") == [11]

    # res = [{"a": "foobar", "nested1": {"a": "hellobarbaz"}, "nested2": {"a": 31}}]
    # assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().strappend("non_existing_doc", "$..a", "err")

    # Test multi
    client.json().strappend("doc1", "bar", ".*.a") == 8
    # res = [{"a": "foo", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}]
    # assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test missing path
    with pytest.raises(exceptions.ResponseError):
        client.json().strappend("doc1", "piu")


def test_strlen_dollar(client):
    # Test multi
    client.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    assert client.json().strlen("doc1", "$..a") == [3, 5, None]

    res2 = client.json().strappend("doc1", "bar", "$..a")
    res1 = client.json().strlen("doc1", "$..a")
    assert res1 == res2

    # Test single
    client.json().strlen("doc1", "$.nested1.a") == [8]
    client.json().strlen("doc1", "$.nested2.a") == [None]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().strlen("non_existing_doc", "$..a")


def test_arrappend_dollar(client):
    client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    client.json().arrappend("doc1", "$..a", "bar", "racuda") == [3, 5, None]
    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test single
    assert client.json().arrappend("doc1", "$.nested1.a", "baz") == [6]
    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().arrappend("non_existing_doc", "$..a")

    # Test legacy
    client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (all paths are updated, but return result of last path)
    assert client.json().arrappend("doc1", "..a", "bar", "racuda") == 5

    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test single
    assert client.json().arrappend("doc1", ".nested1.a", "baz") == 6
    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().arrappend("non_existing_doc", "$..a")


def test_arrinsert_dollar(client):
    client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    assert client.json().arrinsert("doc1", "$..a", "1", "bar", "racuda") == [3, 5, None]

    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", "bar", "racuda", None, "world"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test single
    assert client.json().arrinsert("doc1", "$.nested1.a", -2, "baz") == [6]
    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", "bar", "racuda", "baz", None, "world"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().arrappend("non_existing_doc", "$..a")


def test_arrlen_dollar(client):
    client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi
    assert client.json().arrlen("doc1", "$..a") == [1, 3, None]
    assert client.json().arrappend("doc1", "$..a", "non", "abba", "stanza") == [
        4,
        6,
        None,
    ]

    client.json().clear("doc1", "$.a")
    assert client.json().arrlen("doc1", "$..a") == [0, 6, None]
    # Test single
    assert client.json().arrlen("doc1", "$.nested1.a") == [6]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().arrappend("non_existing_doc", "$..a")

    client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (return result of last path)
    assert client.json().arrlen("doc1", "$..a") == [1, 3, None]
    assert client.json().arrappend("doc1", "..a", "non", "abba", "stanza") == 6

    # Test single
    assert client.json().arrlen("doc1", ".nested1.a") == 6

    # Test missing key
    assert client.json().arrlen("non_existing_doc", "..a") is None


def test_arrpop_dollar(client):
    client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # # # Test multi
    assert client.json().arrpop("doc1", "$..a", 1) == ['"foo"', None, None]

    res = [{"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().arrpop("non_existing_doc", "..a")

    # # Test legacy
    client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (all paths are updated, but return result of last path)
    client.json().arrpop("doc1", "..a", "1") is None
    res = [{"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().arrpop("non_existing_doc", "..a")


def test_arrtrim_dollar(client):
    client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    assert client.json().arrtrim("doc1", "$..a", "1", -1) == [0, 2, None]
    res = [{"a": [], "nested1": {"a": [None, "world"]}, "nested2": {"a": 31}}]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    assert client.json().arrtrim("doc1", "$..a", "1", "1") == [0, 1, None]
    res = [{"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test single
    assert client.json().arrtrim("doc1", "$.nested1.a", 1, 0) == [0]
    res = [{"a": [], "nested1": {"a": []}, "nested2": {"a": 31}}]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().arrtrim("non_existing_doc", "..a", "0", 1)

    # Test legacy
    client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi (all paths are updated, but return result of last path)
    assert client.json().arrtrim("doc1", "..a", "1", "-1") == 2

    # Test single
    assert client.json().arrtrim("doc1", ".nested1.a", "1", "1") == 1
    res = [{"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().arrtrim("non_existing_doc", "..a", 1, 1)


def test_objkeys_dollar(client):
    client.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )

    # Test single
    assert client.json().objkeys("doc1", "$.nested1.a") == [["foo", "bar"]]

    # Test legacy
    assert client.json().objkeys("doc1", ".*.a") == ["foo", "bar"]
    # Test single
    assert client.json().objkeys("doc1", ".nested2.a") == ["baz"]

    # Test missing key
    assert client.json().objkeys("non_existing_doc", "..a") is None

    # Test non existing doc
    with pytest.raises(exceptions.ResponseError):
        assert client.json().objkeys("non_existing_doc", "$..a") == []

    assert client.json().objkeys("doc1", "$..nowhere") == []


def test_objlen_dollar(client):
    client.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )
    # Test multi
    assert client.json().objlen("doc1", "$..a") == [None, 2, 1]
    # Test single
    assert client.json().objlen("doc1", "$.nested1.a") == [2]

    # Test missing key, and path
    with pytest.raises(exceptions.ResponseError):
        client.json().objlen("non_existing_doc", "$..a")

    assert client.json().objlen("doc1", "$.nowhere") == []

    # Test legacy
    assert client.json().objlen("doc1", ".*.a") == 2

    # Test single
    assert client.json().objlen("doc1", ".nested2.a") == 1

    # Test missing key
    assert client.json().objlen("non_existing_doc", "..a") is None

    # Test missing path
    # with pytest.raises(exceptions.ResponseError):
    client.json().objlen("doc1", ".nowhere")


def load_types_data(nested_key_name):
    td = {
        "object": {},
        "array": [],
        "string": "str",
        "integer": 42,
        "number": 1.2,
        "boolean": False,
        "null": None,
    }
    jdata = {}
    types = []
    for i, (k, v) in zip(range(1, len(td) + 1), iter(td.items())):
        jdata["nested" + str(i)] = {nested_key_name: v}
        types.append(k)

    return jdata, types


def test_type_dollar(client):
    jdata, jtypes = load_types_data("a")
    client.json().set("doc1", "$", jdata)
    # Test multi
    assert_resp_response(client, client.json().type("doc1", "$..a"), jtypes, [jtypes])

    # Test single
    assert_resp_response(
        client, client.json().type("doc1", "$.nested2.a"), [jtypes[1]], [[jtypes[1]]]
    )

    # Test missing key
    assert_resp_response(
        client, client.json().type("non_existing_doc", "..a"), None, [None]
    )


def test_clear_dollar(client):
    client.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        },
    )
    # Test multi
    assert client.json().clear("doc1", "$..a") == 3

    res = [
        {"nested1": {"a": {}}, "a": [], "nested2": {"a": "claro"}, "nested3": {"a": {}}}
    ]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test single
    client.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        },
    )
    assert client.json().clear("doc1", "$.nested1.a") == 1
    res = [
        {
            "nested1": {"a": {}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        }
    ]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test missing path (defaults to root)
    assert client.json().clear("doc1") == 1
    assert_resp_response(client, client.json().get("doc1", "$"), [{}], [[{}]])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().clear("non_existing_doc", "$..a")


def test_toggle_dollar(client):
    client.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": False},
            "nested2": {"a": 31},
            "nested3": {"a": True},
        },
    )
    # Test multi
    assert client.json().toggle("doc1", "$..a") == [None, 1, None, 0]
    res = [
        {
            "a": ["foo"],
            "nested1": {"a": True},
            "nested2": {"a": 31},
            "nested3": {"a": False},
        }
    ]
    assert_resp_response(client, client.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        client.json().toggle("non_existing_doc", "$..a")


# # def test_debug_dollar(client):
#
#    jdata, jtypes = load_types_data("a")
#
#    client.json().set("doc1", "$", jdata)
#
#    # Test multi
#    assert client.json().debug("MEMORY", "doc1", "$..a") == [72, 24, 24, 16, 16, 1, 0]
#
#    # Test single
#    assert client.json().debug("MEMORY", "doc1", "$.nested2.a") == [24]
#
#    # Test legacy
#    assert client.json().debug("MEMORY", "doc1", "..a") == 72
#
#    # Test missing path (defaults to root)
#    assert client.json().debug("MEMORY", "doc1") == 72
#
#    # Test missing key
#    assert client.json().debug("MEMORY", "non_existing_doc", "$..a") == []


def test_resp_dollar(client):
    data = {
        "L1": {
            "a": {
                "A1_B1": 10,
                "A1_B2": False,
                "A1_B3": {
                    "A1_B3_C1": None,
                    "A1_B3_C2": [
                        "A1_B3_C2_D1_1",
                        "A1_B3_C2_D1_2",
                        -19.5,
                        "A1_B3_C2_D1_4",
                        "A1_B3_C2_D1_5",
                        {"A1_B3_C2_D1_6_E1": True},
                    ],
                    "A1_B3_C3": [1],
                },
                "A1_B4": {"A1_B4_C1": "foo"},
            }
        },
        "L2": {
            "a": {
                "A2_B1": 20,
                "A2_B2": False,
                "A2_B3": {
                    "A2_B3_C1": None,
                    "A2_B3_C2": [
                        "A2_B3_C2_D1_1",
                        "A2_B3_C2_D1_2",
                        -37.5,
                        "A2_B3_C2_D1_4",
                        "A2_B3_C2_D1_5",
                        {"A2_B3_C2_D1_6_E1": False},
                    ],
                    "A2_B3_C3": [2],
                },
                "A2_B4": {"A2_B4_C1": "bar"},
            }
        },
    }
    client.json().set("doc1", "$", data)
    # Test multi
    res = client.json().resp("doc1", "$..a")
    resp2 = [
        [
            "{",
            "A1_B1",
            10,
            "A1_B2",
            "false",
            "A1_B3",
            [
                "{",
                "A1_B3_C1",
                None,
                "A1_B3_C2",
                [
                    "[",
                    "A1_B3_C2_D1_1",
                    "A1_B3_C2_D1_2",
                    "-19.5",
                    "A1_B3_C2_D1_4",
                    "A1_B3_C2_D1_5",
                    ["{", "A1_B3_C2_D1_6_E1", "true"],
                ],
                "A1_B3_C3",
                ["[", 1],
            ],
            "A1_B4",
            ["{", "A1_B4_C1", "foo"],
        ],
        [
            "{",
            "A2_B1",
            20,
            "A2_B2",
            "false",
            "A2_B3",
            [
                "{",
                "A2_B3_C1",
                None,
                "A2_B3_C2",
                [
                    "[",
                    "A2_B3_C2_D1_1",
                    "A2_B3_C2_D1_2",
                    "-37.5",
                    "A2_B3_C2_D1_4",
                    "A2_B3_C2_D1_5",
                    ["{", "A2_B3_C2_D1_6_E1", "false"],
                ],
                "A2_B3_C3",
                ["[", 2],
            ],
            "A2_B4",
            ["{", "A2_B4_C1", "bar"],
        ],
    ]
    resp3 = [
        [
            "{",
            "A1_B1",
            10,
            "A1_B2",
            "false",
            "A1_B3",
            [
                "{",
                "A1_B3_C1",
                None,
                "A1_B3_C2",
                [
                    "[",
                    "A1_B3_C2_D1_1",
                    "A1_B3_C2_D1_2",
                    -19.5,
                    "A1_B3_C2_D1_4",
                    "A1_B3_C2_D1_5",
                    ["{", "A1_B3_C2_D1_6_E1", "true"],
                ],
                "A1_B3_C3",
                ["[", 1],
            ],
            "A1_B4",
            ["{", "A1_B4_C1", "foo"],
        ],
        [
            "{",
            "A2_B1",
            20,
            "A2_B2",
            "false",
            "A2_B3",
            [
                "{",
                "A2_B3_C1",
                None,
                "A2_B3_C2",
                [
                    "[",
                    "A2_B3_C2_D1_1",
                    "A2_B3_C2_D1_2",
                    -37.5,
                    "A2_B3_C2_D1_4",
                    "A2_B3_C2_D1_5",
                    ["{", "A2_B3_C2_D1_6_E1", "false"],
                ],
                "A2_B3_C3",
                ["[", 2],
            ],
            "A2_B4",
            ["{", "A2_B4_C1", "bar"],
        ],
    ]
    assert_resp_response(client, res, resp2, resp3)

    # Test single
    res = client.json().resp("doc1", "$.L1.a")
    resp2 = [
        [
            "{",
            "A1_B1",
            10,
            "A1_B2",
            "false",
            "A1_B3",
            [
                "{",
                "A1_B3_C1",
                None,
                "A1_B3_C2",
                [
                    "[",
                    "A1_B3_C2_D1_1",
                    "A1_B3_C2_D1_2",
                    "-19.5",
                    "A1_B3_C2_D1_4",
                    "A1_B3_C2_D1_5",
                    ["{", "A1_B3_C2_D1_6_E1", "true"],
                ],
                "A1_B3_C3",
                ["[", 1],
            ],
            "A1_B4",
            ["{", "A1_B4_C1", "foo"],
        ]
    ]
    resp3 = [
        [
            "{",
            "A1_B1",
            10,
            "A1_B2",
            "false",
            "A1_B3",
            [
                "{",
                "A1_B3_C1",
                None,
                "A1_B3_C2",
                [
                    "[",
                    "A1_B3_C2_D1_1",
                    "A1_B3_C2_D1_2",
                    -19.5,
                    "A1_B3_C2_D1_4",
                    "A1_B3_C2_D1_5",
                    ["{", "A1_B3_C2_D1_6_E1", "true"],
                ],
                "A1_B3_C3",
                ["[", 1],
            ],
            "A1_B4",
            ["{", "A1_B4_C1", "foo"],
        ]
    ]
    assert_resp_response(client, res, resp2, resp3)

    # Test missing path
    client.json().resp("doc1", "$.nowhere")

    # Test missing key
    # with pytest.raises(exceptions.ResponseError):
    client.json().resp("non_existing_doc", "$..a")


def test_arrindex_dollar(client):
    client.json().set(
        "store",
        "$",
        {
            "store": {
                "book": [
                    {
                        "category": "reference",
                        "author": "Nigel Rees",
                        "title": "Sayings of the Century",
                        "price": 8.95,
                        "size": [10, 20, 30, 40],
                    },
                    {
                        "category": "fiction",
                        "author": "Evelyn Waugh",
                        "title": "Sword of Honour",
                        "price": 12.99,
                        "size": [50, 60, 70, 80],
                    },
                    {
                        "category": "fiction",
                        "author": "Herman Melville",
                        "title": "Moby Dick",
                        "isbn": "0-553-21311-3",
                        "price": 8.99,
                        "size": [5, 10, 20, 30],
                    },
                    {
                        "category": "fiction",
                        "author": "J. R. R. Tolkien",
                        "title": "The Lord of the Rings",
                        "isbn": "0-395-19395-8",
                        "price": 22.99,
                        "size": [5, 6, 7, 8],
                    },
                ],
                "bicycle": {"color": "red", "price": 19.95},
            }
        },
    )

    assert_resp_response(
        client,
        client.json().get("store", "$.store.book[?(@.price<10)].size"),
        [[10, 20, 30, 40], [5, 10, 20, 30]],
        [[[10, 20, 30, 40], [5, 10, 20, 30]]],
    )

    assert client.json().arrindex(
        "store", "$.store.book[?(@.price<10)].size", "20"
    ) == [-1, -1]

    # Test index of int scalar in multi values
    client.json().set(
        "test_num",
        ".",
        [
            {"arr": [0, 1, 3.0, 3, 2, 1, 0, 3]},
            {"nested1_found": {"arr": [5, 4, 3, 2, 1, 0, 1, 2, 3.0, 2, 4, 5]}},
            {"nested2_not_found": {"arr": [2, 4, 6]}},
            {"nested3_scalar": {"arr": "3"}},
            [
                {"nested41_not_arr": {"arr_renamed": [1, 2, 3]}},
                {"nested42_empty_arr": {"arr": []}},
            ],
        ],
    )

    res = [
        [0, 1, 3.0, 3, 2, 1, 0, 3],
        [5, 4, 3, 2, 1, 0, 1, 2, 3.0, 2, 4, 5],
        [2, 4, 6],
        "3",
        [],
    ]
    assert_resp_response(client, client.json().get("test_num", "$..arr"), res, [res])

    assert client.json().arrindex("test_num", "$..arr", 3) == [3, 2, -1, None, -1]

    # Test index of double scalar in multi values
    assert client.json().arrindex("test_num", "$..arr", 3.0) == [2, 8, -1, None, -1]

    # Test index of string scalar in multi values
    client.json().set(
        "test_string",
        ".",
        [
            {"arr": ["bazzz", "bar", 2, "baz", 2, "ba", "baz", 3]},
            {
                "nested1_found": {
                    "arr": [None, "baz2", "buzz", 2, 1, 0, 1, "2", "baz", 2, 4, 5]
                }
            },
            {"nested2_not_found": {"arr": ["baz2", 4, 6]}},
            {"nested3_scalar": {"arr": "3"}},
            [
                {"nested41_arr": {"arr_renamed": [1, "baz", 3]}},
                {"nested42_empty_arr": {"arr": []}},
            ],
        ],
    )
    res = [
        ["bazzz", "bar", 2, "baz", 2, "ba", "baz", 3],
        [None, "baz2", "buzz", 2, 1, 0, 1, "2", "baz", 2, 4, 5],
        ["baz2", 4, 6],
        "3",
        [],
    ]
    assert_resp_response(client, client.json().get("test_string", "$..arr"), res, [res])

    assert client.json().arrindex("test_string", "$..arr", "baz") == [
        3,
        8,
        -1,
        None,
        -1,
    ]

    assert client.json().arrindex("test_string", "$..arr", "baz", 2) == [
        3,
        8,
        -1,
        None,
        -1,
    ]
    assert client.json().arrindex("test_string", "$..arr", "baz", 4) == [
        6,
        8,
        -1,
        None,
        -1,
    ]
    assert client.json().arrindex("test_string", "$..arr", "baz", -5) == [
        3,
        8,
        -1,
        None,
        -1,
    ]
    assert client.json().arrindex("test_string", "$..arr", "baz", 4, 7) == [
        6,
        -1,
        -1,
        None,
        -1,
    ]
    assert client.json().arrindex("test_string", "$..arr", "baz", 4, -1) == [
        6,
        8,
        -1,
        None,
        -1,
    ]
    assert client.json().arrindex("test_string", "$..arr", "baz", 4, 0) == [
        6,
        8,
        -1,
        None,
        -1,
    ]
    assert client.json().arrindex("test_string", "$..arr", "5", 7, -1) == [
        -1,
        -1,
        -1,
        None,
        -1,
    ]
    assert client.json().arrindex("test_string", "$..arr", "5", 7, 0) == [
        -1,
        -1,
        -1,
        None,
        -1,
    ]

    # Test index of None scalar in multi values
    client.json().set(
        "test_None",
        ".",
        [
            {"arr": ["bazzz", "None", 2, None, 2, "ba", "baz", 3]},
            {
                "nested1_found": {
                    "arr": ["zaz", "baz2", "buzz", 2, 1, 0, 1, "2", None, 2, 4, 5]
                }
            },
            {"nested2_not_found": {"arr": ["None", 4, 6]}},
            {"nested3_scalar": {"arr": None}},
            [
                {"nested41_arr": {"arr_renamed": [1, None, 3]}},
                {"nested42_empty_arr": {"arr": []}},
            ],
        ],
    )
    res = [
        ["bazzz", "None", 2, None, 2, "ba", "baz", 3],
        ["zaz", "baz2", "buzz", 2, 1, 0, 1, "2", None, 2, 4, 5],
        ["None", 4, 6],
        None,
        [],
    ]
    assert_resp_response(client, client.json().get("test_None", "$..arr"), res, [res])

    # Test with none-scalar value
    assert client.json().arrindex(
        "test_None", "$..nested42_empty_arr.arr", {"arr": []}
    ) == [-1]

    # Test legacy (path begins with dot)
    # Test index of int scalar in single value
    assert client.json().arrindex("test_num", ".[0].arr", 3) == 3
    assert client.json().arrindex("test_num", ".[0].arr", 9) == -1

    with pytest.raises(exceptions.ResponseError):
        client.json().arrindex("test_num", ".[0].arr_not", 3)
    # Test index of string scalar in single value
    assert client.json().arrindex("test_string", ".[0].arr", "baz") == 3
    assert client.json().arrindex("test_string", ".[0].arr", "faz") == -1
    # Test index of None scalar in single value
    assert client.json().arrindex("test_None", ".[0].arr", "None") == 1
    assert client.json().arrindex("test_None", "..nested2_not_found.arr", "None") == 0


def test_decoders_and_unstring():
    assert unstring("4") == 4
    assert unstring("45.55") == 45.55
    assert unstring("hello world") == "hello world"

    assert decode_list(b"45.55") == 45.55
    assert decode_list("45.55") == 45.55
    assert decode_list(["hello", b"world"]) == ["hello", "world"]


def test_custom_decoder(client):
    import json

    import ujson

    cj = client.json(encoder=ujson, decoder=ujson)
    assert cj.set("foo", Path.root_path(), "bar")
    assert_resp_response(client, cj.get("foo"), "bar", [["bar"]])
    assert cj.get("baz") is None
    assert 1 == cj.delete("foo")
    assert client.exists("foo") == 0
    assert not isinstance(cj.__encoder__, json.JSONEncoder)
    assert not isinstance(cj.__decoder__, json.JSONDecoder)


def test_set_file(client):
    import json
    import tempfile

    obj = {"hello": "world"}
    jsonfile = tempfile.NamedTemporaryFile(suffix=".json")
    with open(jsonfile.name, "w+") as fp:
        fp.write(json.dumps(obj))

    nojsonfile = tempfile.NamedTemporaryFile()
    nojsonfile.write(b"Hello World")

    assert client.json().set_file("test", Path.root_path(), jsonfile.name)
    assert_resp_response(client, client.json().get("test"), obj, [[obj]])
    with pytest.raises(json.JSONDecodeError):
        client.json().set_file("test2", Path.root_path(), nojsonfile.name)


def test_set_path(client):
    import json
    import tempfile

    root = tempfile.mkdtemp()
    sub = tempfile.mkdtemp(dir=root)
    jsonfile = tempfile.mktemp(suffix=".json", dir=sub)
    nojsonfile = tempfile.mktemp(dir=root)

    with open(jsonfile, "w+") as fp:
        fp.write(json.dumps({"hello": "world"}))
    with open(nojsonfile, "a+") as fp:
        fp.write("hello")

    result = {jsonfile: True, nojsonfile: False}
    assert client.json().set_path(Path.root_path(), root) == result
    res = {"hello": "world"}
    assert_resp_response(
        client, client.json().get(jsonfile.rsplit(".")[0]), res, [[res]]
    )
