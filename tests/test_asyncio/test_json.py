import pytest

import redis.asyncio as redis
from redis import exceptions
from redis.commands.json.path import Path
from tests.conftest import skip_ifmodversion_lt


@pytest.mark.redismod
async def test_json_setbinarykey(modclient: redis.Redis):
    d = {"hello": "world", b"some": "value"}
    with pytest.raises(TypeError):
        modclient.json().set("somekey", Path.root_path(), d)
    assert await modclient.json().set("somekey", Path.root_path(), d, decode_keys=True)


@pytest.mark.redismod
async def test_json_setgetdeleteforget(modclient: redis.Redis):
    assert await modclient.json().set("foo", Path.root_path(), "bar")
    assert await modclient.json().get("foo") == "bar"
    assert await modclient.json().get("baz") is None
    assert await modclient.json().delete("foo") == 1
    assert await modclient.json().forget("foo") == 0  # second delete
    assert await modclient.exists("foo") == 0


@pytest.mark.redismod
async def test_jsonget(modclient: redis.Redis):
    await modclient.json().set("foo", Path.root_path(), "bar")
    assert await modclient.json().get("foo") == "bar"


@pytest.mark.redismod
async def test_json_get_jset(modclient: redis.Redis):
    assert await modclient.json().set("foo", Path.root_path(), "bar")
    assert "bar" == await modclient.json().get("foo")
    assert await modclient.json().get("baz") is None
    assert 1 == await modclient.json().delete("foo")
    assert await modclient.exists("foo") == 0


@pytest.mark.redismod
async def test_nonascii_setgetdelete(modclient: redis.Redis):
    assert await modclient.json().set("notascii", Path.root_path(), "hyvää-élève")
    assert "hyvää-élève" == await modclient.json().get("notascii", no_escape=True)
    assert 1 == await modclient.json().delete("notascii")
    assert await modclient.exists("notascii") == 0


@pytest.mark.redismod
async def test_jsonsetexistentialmodifiersshouldsucceed(modclient: redis.Redis):
    obj = {"foo": "bar"}
    assert await modclient.json().set("obj", Path.root_path(), obj)

    # Test that flags prevent updates when conditions are unmet
    assert await modclient.json().set("obj", Path("foo"), "baz", nx=True) is None
    assert await modclient.json().set("obj", Path("qaz"), "baz", xx=True) is None

    # Test that flags allow updates when conditions are met
    assert await modclient.json().set("obj", Path("foo"), "baz", xx=True)
    assert await modclient.json().set("obj", Path("qaz"), "baz", nx=True)

    # Test that flags are mutually exlusive
    with pytest.raises(Exception):
        await modclient.json().set("obj", Path("foo"), "baz", nx=True, xx=True)


@pytest.mark.redismod
async def test_mgetshouldsucceed(modclient: redis.Redis):
    await modclient.json().set("1", Path.root_path(), 1)
    await modclient.json().set("2", Path.root_path(), 2)
    assert await modclient.json().mget(["1"], Path.root_path()) == [1]

    assert await modclient.json().mget([1, 2], Path.root_path()) == [1, 2]


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
async def test_clear(modclient: redis.Redis):
    await modclient.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 1 == await modclient.json().clear("arr", Path.root_path())
    assert [] == await modclient.json().get("arr")


@pytest.mark.redismod
async def test_type(modclient: redis.Redis):
    await modclient.json().set("1", Path.root_path(), 1)
    assert "integer" == await modclient.json().type("1", Path.root_path())
    assert "integer" == await modclient.json().type("1")


@pytest.mark.redismod
async def test_numincrby(modclient):
    await modclient.json().set("num", Path.root_path(), 1)
    assert 2 == await modclient.json().numincrby("num", Path.root_path(), 1)
    assert 2.5 == await modclient.json().numincrby("num", Path.root_path(), 0.5)
    assert 1.25 == await modclient.json().numincrby("num", Path.root_path(), -1.25)


@pytest.mark.redismod
async def test_nummultby(modclient: redis.Redis):
    await modclient.json().set("num", Path.root_path(), 1)

    with pytest.deprecated_call():
        assert 2 == await modclient.json().nummultby("num", Path.root_path(), 2)
        assert 5 == await modclient.json().nummultby("num", Path.root_path(), 2.5)
        assert 2.5 == await modclient.json().nummultby("num", Path.root_path(), 0.5)


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
async def test_toggle(modclient: redis.Redis):
    await modclient.json().set("bool", Path.root_path(), False)
    assert await modclient.json().toggle("bool", Path.root_path())
    assert await modclient.json().toggle("bool", Path.root_path()) is False
    # check non-boolean value
    await modclient.json().set("num", Path.root_path(), 1)
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().toggle("num", Path.root_path())


@pytest.mark.redismod
async def test_strappend(modclient: redis.Redis):
    await modclient.json().set("jsonkey", Path.root_path(), "foo")
    assert 6 == await modclient.json().strappend("jsonkey", "bar")
    assert "foobar" == await modclient.json().get("jsonkey", Path.root_path())


@pytest.mark.redismod
async def test_strlen(modclient: redis.Redis):
    await modclient.json().set("str", Path.root_path(), "foo")
    assert 3 == await modclient.json().strlen("str", Path.root_path())
    await modclient.json().strappend("str", "bar", Path.root_path())
    assert 6 == await modclient.json().strlen("str", Path.root_path())
    assert 6 == await modclient.json().strlen("str")


@pytest.mark.redismod
async def test_arrappend(modclient: redis.Redis):
    await modclient.json().set("arr", Path.root_path(), [1])
    assert 2 == await modclient.json().arrappend("arr", Path.root_path(), 2)
    assert 4 == await modclient.json().arrappend("arr", Path.root_path(), 3, 4)
    assert 7 == await modclient.json().arrappend("arr", Path.root_path(), *[5, 6, 7])


@pytest.mark.redismod
async def test_arrindex(modclient: redis.Redis):
    await modclient.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 1 == await modclient.json().arrindex("arr", Path.root_path(), 1)
    assert -1 == await modclient.json().arrindex("arr", Path.root_path(), 1, 2)


@pytest.mark.redismod
async def test_arrinsert(modclient: redis.Redis):
    await modclient.json().set("arr", Path.root_path(), [0, 4])
    assert 5 - -await modclient.json().arrinsert("arr", Path.root_path(), 1, *[1, 2, 3])
    assert [0, 1, 2, 3, 4] == await modclient.json().get("arr")

    # test prepends
    await modclient.json().set("val2", Path.root_path(), [5, 6, 7, 8, 9])
    await modclient.json().arrinsert("val2", Path.root_path(), 0, ["some", "thing"])
    assert await modclient.json().get("val2") == [["some", "thing"], 5, 6, 7, 8, 9]


@pytest.mark.redismod
async def test_arrlen(modclient: redis.Redis):
    await modclient.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 5 == await modclient.json().arrlen("arr", Path.root_path())
    assert 5 == await modclient.json().arrlen("arr")
    assert await modclient.json().arrlen("fakekey") is None


@pytest.mark.redismod
async def test_arrpop(modclient: redis.Redis):
    await modclient.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 4 == await modclient.json().arrpop("arr", Path.root_path(), 4)
    assert 3 == await modclient.json().arrpop("arr", Path.root_path(), -1)
    assert 2 == await modclient.json().arrpop("arr", Path.root_path())
    assert 0 == await modclient.json().arrpop("arr", Path.root_path(), 0)
    assert [1] == await modclient.json().get("arr")

    # test out of bounds
    await modclient.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 4 == await modclient.json().arrpop("arr", Path.root_path(), 99)

    # none test
    await modclient.json().set("arr", Path.root_path(), [])
    assert await modclient.json().arrpop("arr") is None


@pytest.mark.redismod
async def test_arrtrim(modclient: redis.Redis):
    await modclient.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 3 == await modclient.json().arrtrim("arr", Path.root_path(), 1, 3)
    assert [1, 2, 3] == await modclient.json().get("arr")

    # <0 test, should be 0 equivalent
    await modclient.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == await modclient.json().arrtrim("arr", Path.root_path(), -1, 3)

    # testing stop > end
    await modclient.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 2 == await modclient.json().arrtrim("arr", Path.root_path(), 3, 99)

    # start > array size and stop
    await modclient.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == await modclient.json().arrtrim("arr", Path.root_path(), 9, 1)

    # all larger
    await modclient.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == await modclient.json().arrtrim("arr", Path.root_path(), 9, 11)


@pytest.mark.redismod
async def test_resp(modclient: redis.Redis):
    obj = {"foo": "bar", "baz": 1, "qaz": True}
    await modclient.json().set("obj", Path.root_path(), obj)
    assert "bar" == await modclient.json().resp("obj", Path("foo"))
    assert 1 == await modclient.json().resp("obj", Path("baz"))
    assert await modclient.json().resp("obj", Path("qaz"))
    assert isinstance(await modclient.json().resp("obj"), list)


@pytest.mark.redismod
async def test_objkeys(modclient: redis.Redis):
    obj = {"foo": "bar", "baz": "qaz"}
    await modclient.json().set("obj", Path.root_path(), obj)
    keys = await modclient.json().objkeys("obj", Path.root_path())
    keys.sort()
    exp = list(obj.keys())
    exp.sort()
    assert exp == keys

    await modclient.json().set("obj", Path.root_path(), obj)
    keys = await modclient.json().objkeys("obj")
    assert keys == list(obj.keys())

    assert await modclient.json().objkeys("fakekey") is None


@pytest.mark.redismod
async def test_objlen(modclient: redis.Redis):
    obj = {"foo": "bar", "baz": "qaz"}
    await modclient.json().set("obj", Path.root_path(), obj)
    assert len(obj) == await modclient.json().objlen("obj", Path.root_path())

    await modclient.json().set("obj", Path.root_path(), obj)
    assert len(obj) == await modclient.json().objlen("obj")


# @pytest.mark.redismod
# async def test_json_commands_in_pipeline(modclient: redis.Redis):
#     async with modclient.json().pipeline() as p:
#         p.set("foo", Path.root_path(), "bar")
#         p.get("foo")
#         p.delete("foo")
#         assert [True, "bar", 1] == await p.execute()
#     assert await modclient.keys() == []
#     assert await modclient.get("foo") is None

#     # now with a true, json object
#     await modclient.flushdb()
#     p = await modclient.json().pipeline()
#     d = {"hello": "world", "oh": "snap"}
#     with pytest.deprecated_call():
#         p.jsonset("foo", Path.root_path(), d)
#         p.jsonget("foo")
#     p.exists("notarealkey")
#     p.delete("foo")
#     assert [True, d, 0, 1] == p.execute()
#     assert await modclient.keys() == []
#     assert await modclient.get("foo") is None


@pytest.mark.redismod
async def test_json_delete_with_dollar(modclient: redis.Redis):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert await modclient.json().set("doc1", "$", doc1)
    assert await modclient.json().delete("doc1", "$..a") == 2
    r = await modclient.json().get("doc1", "$")
    assert r == [{"nested": {"b": 3}}]

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    assert await modclient.json().set("doc2", "$", doc2)
    assert await modclient.json().delete("doc2", "$..a") == 1
    res = await modclient.json().get("doc2", "$")
    assert res == [{"nested": {"b": [True, "a", "b"]}, "b": ["a", "b"]}]

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
    assert await modclient.json().set("doc3", "$", doc3)
    assert await modclient.json().delete("doc3", '$.[0]["nested"]..ciao') == 3

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
    res = await modclient.json().get("doc3", "$")
    assert res == doc3val

    # Test async default path
    assert await modclient.json().delete("doc3") == 1
    assert await modclient.json().get("doc3", "$") is None

    await modclient.json().delete("not_a_document", "..a")


@pytest.mark.redismod
async def test_json_forget_with_dollar(modclient: redis.Redis):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert await modclient.json().set("doc1", "$", doc1)
    assert await modclient.json().forget("doc1", "$..a") == 2
    r = await modclient.json().get("doc1", "$")
    assert r == [{"nested": {"b": 3}}]

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    assert await modclient.json().set("doc2", "$", doc2)
    assert await modclient.json().forget("doc2", "$..a") == 1
    res = await modclient.json().get("doc2", "$")
    assert res == [{"nested": {"b": [True, "a", "b"]}, "b": ["a", "b"]}]

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
    assert await modclient.json().set("doc3", "$", doc3)
    assert await modclient.json().forget("doc3", '$.[0]["nested"]..ciao') == 3

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
    res = await modclient.json().get("doc3", "$")
    assert res == doc3val

    # Test async default path
    assert await modclient.json().forget("doc3") == 1
    assert await modclient.json().get("doc3", "$") is None

    await modclient.json().forget("not_a_document", "..a")


@pytest.mark.redismod
async def test_json_mget_dollar(modclient: redis.Redis):
    # Test mget with multi paths
    await modclient.json().set(
        "doc1",
        "$",
        {"a": 1, "b": 2, "nested": {"a": 3}, "c": None, "nested2": {"a": None}},
    )
    await modclient.json().set(
        "doc2",
        "$",
        {"a": 4, "b": 5, "nested": {"a": 6}, "c": None, "nested2": {"a": [None]}},
    )
    # Compare also to single JSON.GET
    assert await modclient.json().get("doc1", "$..a") == [1, 3, None]
    assert await modclient.json().get("doc2", "$..a") == [4, 6, [None]]

    # Test mget with single path
    await modclient.json().mget("doc1", "$..a") == [1, 3, None]
    # Test mget with multi path
    res = await modclient.json().mget(["doc1", "doc2"], "$..a")
    assert res == [[1, 3, None], [4, 6, [None]]]

    # Test missing key
    res = await modclient.json().mget(["doc1", "missing_doc"], "$..a")
    assert res == [[1, 3, None], None]
    res = await modclient.json().mget(["missing_doc1", "missing_doc2"], "$..a")
    assert res == [None, None]


@pytest.mark.redismod
async def test_numby_commands_dollar(modclient: redis.Redis):

    # Test NUMINCRBY
    await modclient.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )
    # Test multi
    assert await modclient.json().numincrby("doc1", "$..a", 2) == [None, 4, 7.0, None]

    res = await modclient.json().numincrby("doc1", "$..a", 2.5)
    assert res == [None, 6.5, 9.5, None]
    # Test single
    assert await modclient.json().numincrby("doc1", "$.b[1].a", 2) == [11.5]

    assert await modclient.json().numincrby("doc1", "$.b[2].a", 2) == [None]
    assert await modclient.json().numincrby("doc1", "$.b[1].a", 3.5) == [15.0]

    # Test NUMMULTBY
    await modclient.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )

    # test list
    with pytest.deprecated_call():
        res = await modclient.json().nummultby("doc1", "$..a", 2)
        assert res == [None, 4, 10, None]
        res = await modclient.json().nummultby("doc1", "$..a", 2.5)
        assert res == [None, 10.0, 25.0, None]

    # Test single
    with pytest.deprecated_call():
        assert await modclient.json().nummultby("doc1", "$.b[1].a", 2) == [50.0]
        assert await modclient.json().nummultby("doc1", "$.b[2].a", 2) == [None]
        assert await modclient.json().nummultby("doc1", "$.b[1].a", 3) == [150.0]

    # test missing keys
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().numincrby("non_existing_doc", "$..a", 2)
        await modclient.json().nummultby("non_existing_doc", "$..a", 2)

    # Test legacy NUMINCRBY
    await modclient.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )
    await modclient.json().numincrby("doc1", ".b[0].a", 3) == 5

    # Test legacy NUMMULTBY
    await modclient.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )

    with pytest.deprecated_call():
        await modclient.json().nummultby("doc1", ".b[0].a", 3) == 6


@pytest.mark.redismod
async def test_strappend_dollar(modclient: redis.Redis):

    await modclient.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    # Test multi
    await modclient.json().strappend("doc1", "bar", "$..a") == [6, 8, None]

    await modclient.json().get("doc1", "$") == [
        {"a": "foobar", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}
    ]
    # Test single
    await modclient.json().strappend("doc1", "baz", "$.nested1.a") == [11]

    await modclient.json().get("doc1", "$") == [
        {"a": "foobar", "nested1": {"a": "hellobarbaz"}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().strappend("non_existing_doc", "$..a", "err")

    # Test multi
    await modclient.json().strappend("doc1", "bar", ".*.a") == 8
    await modclient.json().get("doc1", "$") == [
        {"a": "foo", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}
    ]

    # Test missing path
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().strappend("doc1", "piu")


@pytest.mark.redismod
async def test_strlen_dollar(modclient: redis.Redis):

    # Test multi
    await modclient.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    assert await modclient.json().strlen("doc1", "$..a") == [3, 5, None]

    res2 = await modclient.json().strappend("doc1", "bar", "$..a")
    res1 = await modclient.json().strlen("doc1", "$..a")
    assert res1 == res2

    # Test single
    await modclient.json().strlen("doc1", "$.nested1.a") == [8]
    await modclient.json().strlen("doc1", "$.nested2.a") == [None]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().strlen("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrappend_dollar(modclient: redis.Redis):
    await modclient.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    await modclient.json().arrappend("doc1", "$..a", "bar", "racuda") == [3, 5, None]
    assert await modclient.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
            "nested2": {"a": 31},
        }
    ]

    # Test single
    assert await modclient.json().arrappend("doc1", "$.nested1.a", "baz") == [6]
    assert await modclient.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
            "nested2": {"a": 31},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().arrappend("non_existing_doc", "$..a")

    # Test legacy
    await modclient.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (all paths are updated, but return result of last path)
    assert await modclient.json().arrappend("doc1", "..a", "bar", "racuda") == 5

    assert await modclient.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
            "nested2": {"a": 31},
        }
    ]
    # Test single
    assert await modclient.json().arrappend("doc1", ".nested1.a", "baz") == 6
    assert await modclient.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
            "nested2": {"a": 31},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().arrappend("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrinsert_dollar(modclient: redis.Redis):
    await modclient.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    res = await modclient.json().arrinsert("doc1", "$..a", "1", "bar", "racuda")
    assert res == [3, 5, None]

    assert await modclient.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", "bar", "racuda", None, "world"]},
            "nested2": {"a": 31},
        }
    ]
    # Test single
    assert await modclient.json().arrinsert("doc1", "$.nested1.a", -2, "baz") == [6]
    assert await modclient.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", "bar", "racuda", "baz", None, "world"]},
            "nested2": {"a": 31},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().arrappend("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrlen_dollar(modclient: redis.Redis):

    await modclient.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi
    assert await modclient.json().arrlen("doc1", "$..a") == [1, 3, None]
    res = await modclient.json().arrappend("doc1", "$..a", "non", "abba", "stanza")
    assert res == [4, 6, None]

    await modclient.json().clear("doc1", "$.a")
    assert await modclient.json().arrlen("doc1", "$..a") == [0, 6, None]
    # Test single
    assert await modclient.json().arrlen("doc1", "$.nested1.a") == [6]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().arrappend("non_existing_doc", "$..a")

    await modclient.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (return result of last path)
    assert await modclient.json().arrlen("doc1", "$..a") == [1, 3, None]
    assert await modclient.json().arrappend("doc1", "..a", "non", "abba", "stanza") == 6

    # Test single
    assert await modclient.json().arrlen("doc1", ".nested1.a") == 6

    # Test missing key
    assert await modclient.json().arrlen("non_existing_doc", "..a") is None


@pytest.mark.redismod
async def test_arrpop_dollar(modclient: redis.Redis):
    await modclient.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # # # Test multi
    assert await modclient.json().arrpop("doc1", "$..a", 1) == ['"foo"', None, None]

    assert await modclient.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().arrpop("non_existing_doc", "..a")

    # # Test legacy
    await modclient.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (all paths are updated, but return result of last path)
    await modclient.json().arrpop("doc1", "..a", "1") is None
    assert await modclient.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}
    ]

    # # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().arrpop("non_existing_doc", "..a")


@pytest.mark.redismod
async def test_arrtrim_dollar(modclient: redis.Redis):

    await modclient.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    assert await modclient.json().arrtrim("doc1", "$..a", "1", -1) == [0, 2, None]
    assert await modclient.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": [None, "world"]}, "nested2": {"a": 31}}
    ]

    assert await modclient.json().arrtrim("doc1", "$..a", "1", "1") == [0, 1, None]
    assert await modclient.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}
    ]
    # Test single
    assert await modclient.json().arrtrim("doc1", "$.nested1.a", 1, 0) == [0]
    assert await modclient.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": []}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().arrtrim("non_existing_doc", "..a", "0", 1)

    # Test legacy
    await modclient.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi (all paths are updated, but return result of last path)
    assert await modclient.json().arrtrim("doc1", "..a", "1", "-1") == 2

    # Test single
    assert await modclient.json().arrtrim("doc1", ".nested1.a", "1", "1") == 1
    assert await modclient.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().arrtrim("non_existing_doc", "..a", 1, 1)


@pytest.mark.redismod
async def test_objkeys_dollar(modclient: redis.Redis):
    await modclient.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )

    # Test single
    assert await modclient.json().objkeys("doc1", "$.nested1.a") == [["foo", "bar"]]

    # Test legacy
    assert await modclient.json().objkeys("doc1", ".*.a") == ["foo", "bar"]
    # Test single
    assert await modclient.json().objkeys("doc1", ".nested2.a") == ["baz"]

    # Test missing key
    assert await modclient.json().objkeys("non_existing_doc", "..a") is None

    # Test non existing doc
    with pytest.raises(exceptions.ResponseError):
        assert await modclient.json().objkeys("non_existing_doc", "$..a") == []

    assert await modclient.json().objkeys("doc1", "$..nowhere") == []


@pytest.mark.redismod
async def test_objlen_dollar(modclient: redis.Redis):
    await modclient.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )
    # Test multi
    assert await modclient.json().objlen("doc1", "$..a") == [None, 2, 1]
    # Test single
    assert await modclient.json().objlen("doc1", "$.nested1.a") == [2]

    # Test missing key, and path
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().objlen("non_existing_doc", "$..a")

    assert await modclient.json().objlen("doc1", "$.nowhere") == []

    # Test legacy
    assert await modclient.json().objlen("doc1", ".*.a") == 2

    # Test single
    assert await modclient.json().objlen("doc1", ".nested2.a") == 1

    # Test missing key
    assert await modclient.json().objlen("non_existing_doc", "..a") is None

    # Test missing path
    # with pytest.raises(exceptions.ResponseError):
    await modclient.json().objlen("doc1", ".nowhere")


@pytest.mark.redismod
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


@pytest.mark.redismod
async def test_type_dollar(modclient: redis.Redis):
    jdata, jtypes = load_types_data("a")
    await modclient.json().set("doc1", "$", jdata)
    # Test multi
    assert await modclient.json().type("doc1", "$..a") == jtypes

    # Test single
    assert await modclient.json().type("doc1", "$.nested2.a") == [jtypes[1]]

    # Test missing key
    assert await modclient.json().type("non_existing_doc", "..a") is None


@pytest.mark.redismod
async def test_clear_dollar(modclient: redis.Redis):

    await modclient.json().set(
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
    assert await modclient.json().clear("doc1", "$..a") == 3

    assert await modclient.json().get("doc1", "$") == [
        {"nested1": {"a": {}}, "a": [], "nested2": {"a": "claro"}, "nested3": {"a": {}}}
    ]

    # Test single
    await modclient.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        },
    )
    assert await modclient.json().clear("doc1", "$.nested1.a") == 1
    assert await modclient.json().get("doc1", "$") == [
        {
            "nested1": {"a": {}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        }
    ]

    # Test missing path (async defaults to root)
    assert await modclient.json().clear("doc1") == 1
    assert await modclient.json().get("doc1", "$") == [{}]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().clear("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_toggle_dollar(modclient: redis.Redis):
    await modclient.json().set(
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
    assert await modclient.json().toggle("doc1", "$..a") == [None, 1, None, 0]
    assert await modclient.json().get("doc1", "$") == [
        {
            "a": ["foo"],
            "nested1": {"a": True},
            "nested2": {"a": 31},
            "nested3": {"a": False},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await modclient.json().toggle("non_existing_doc", "$..a")
