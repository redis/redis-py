import pytest

import redis.asyncio as redis
from redis import exceptions
from redis.commands.json.path import Path
from tests.conftest import skip_ifmodversion_lt


@pytest.mark.redismod
async def test_json_setbinarykey(r: redis.Redis):
    d = {"hello": "world", b"some": "value"}
    with pytest.raises(TypeError):
        r.json().set("somekey", Path.root_path(), d)
    assert await r.json().set("somekey", Path.root_path(), d, decode_keys=True)


@pytest.mark.redismod
async def test_json_setgetdeleteforget(r: redis.Redis):
    assert await r.json().set("foo", Path.root_path(), "bar")
    assert await r.json().get("foo") == "bar"
    assert await r.json().get("baz") is None
    assert await r.json().delete("foo") == 1
    assert await r.json().forget("foo") == 0  # second delete
    assert await r.exists("foo") == 0


@pytest.mark.redismod
async def test_jsonget(r: redis.Redis):
    await r.json().set("foo", Path.root_path(), "bar")
    assert await r.json().get("foo") == "bar"


@pytest.mark.redismod
async def test_json_get_jset(r: redis.Redis):
    assert await r.json().set("foo", Path.root_path(), "bar")
    assert "bar" == await r.json().get("foo")
    assert await r.json().get("baz") is None
    assert 1 == await r.json().delete("foo")
    assert await r.exists("foo") == 0


@pytest.mark.redismod
async def test_nonascii_setgetdelete(r: redis.Redis):
    assert await r.json().set("notascii", Path.root_path(), "hyvää-élève")
    assert "hyvää-élève" == await r.json().get("notascii", no_escape=True)
    assert 1 == await r.json().delete("notascii")
    assert await r.exists("notascii") == 0


@pytest.mark.redismod
async def test_jsonsetexistentialmodifiersshouldsucceed(r: redis.Redis):
    obj = {"foo": "bar"}
    assert await r.json().set("obj", Path.root_path(), obj)

    # Test that flags prevent updates when conditions are unmet
    assert await r.json().set("obj", Path("foo"), "baz", nx=True) is None
    assert await r.json().set("obj", Path("qaz"), "baz", xx=True) is None

    # Test that flags allow updates when conditions are met
    assert await r.json().set("obj", Path("foo"), "baz", xx=True)
    assert await r.json().set("obj", Path("qaz"), "baz", nx=True)

    # Test that flags are mutually exlusive
    with pytest.raises(Exception):
        await r.json().set("obj", Path("foo"), "baz", nx=True, xx=True)


@pytest.mark.redismod
async def test_mgetshouldsucceed(r: redis.Redis):
    await r.json().set("1", Path.root_path(), 1)
    await r.json().set("2", Path.root_path(), 2)
    assert await r.json().mget(["1"], Path.root_path()) == [1]

    assert await r.json().mget([1, 2], Path.root_path()) == [1, 2]


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
async def test_clear(r: redis.Redis):
    await r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 1 == await r.json().clear("arr", Path.root_path())
    assert [] == await r.json().get("arr")


@pytest.mark.redismod
async def test_type(r: redis.Redis):
    await r.json().set("1", Path.root_path(), 1)
    assert "integer" == await r.json().type("1", Path.root_path())
    assert "integer" == await r.json().type("1")


@pytest.mark.redismod
async def test_numincrby(r):
    await r.json().set("num", Path.root_path(), 1)
    assert 2 == await r.json().numincrby("num", Path.root_path(), 1)
    assert 2.5 == await r.json().numincrby("num", Path.root_path(), 0.5)
    assert 1.25 == await r.json().numincrby("num", Path.root_path(), -1.25)


@pytest.mark.redismod
async def test_nummultby(r: redis.Redis):
    await r.json().set("num", Path.root_path(), 1)

    with pytest.deprecated_call():
        assert 2 == await r.json().nummultby("num", Path.root_path(), 2)
        assert 5 == await r.json().nummultby("num", Path.root_path(), 2.5)
        assert 2.5 == await r.json().nummultby("num", Path.root_path(), 0.5)


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
async def test_toggle(r: redis.Redis):
    await r.json().set("bool", Path.root_path(), False)
    assert await r.json().toggle("bool", Path.root_path())
    assert await r.json().toggle("bool", Path.root_path()) is False
    # check non-boolean value
    await r.json().set("num", Path.root_path(), 1)
    with pytest.raises(exceptions.ResponseError):
        await r.json().toggle("num", Path.root_path())


@pytest.mark.redismod
async def test_strappend(r: redis.Redis):
    await r.json().set("jsonkey", Path.root_path(), "foo")
    assert 6 == await r.json().strappend("jsonkey", "bar")
    assert "foobar" == await r.json().get("jsonkey", Path.root_path())


@pytest.mark.redismod
async def test_strlen(r: redis.Redis):
    await r.json().set("str", Path.root_path(), "foo")
    assert 3 == await r.json().strlen("str", Path.root_path())
    await r.json().strappend("str", "bar", Path.root_path())
    assert 6 == await r.json().strlen("str", Path.root_path())
    assert 6 == await r.json().strlen("str")


@pytest.mark.redismod
async def test_arrappend(r: redis.Redis):
    await r.json().set("arr", Path.root_path(), [1])
    assert 2 == await r.json().arrappend("arr", Path.root_path(), 2)
    assert 4 == await r.json().arrappend("arr", Path.root_path(), 3, 4)
    assert 7 == await r.json().arrappend("arr", Path.root_path(), *[5, 6, 7])


@pytest.mark.redismod
async def test_arrindex(r: redis.Redis):
    r_path = Path.root_path()
    await r.json().set("arr", r_path, [0, 1, 2, 3, 4])
    assert 1 == await r.json().arrindex("arr", r_path, 1)
    assert -1 == await r.json().arrindex("arr", r_path, 1, 2)
    assert 4 == await r.json().arrindex("arr", r_path, 4)
    assert 4 == await r.json().arrindex("arr", r_path, 4, start=0)
    assert 4 == await r.json().arrindex("arr", r_path, 4, start=0, stop=5000)
    assert -1 == await r.json().arrindex("arr", r_path, 4, start=0, stop=-1)
    assert -1 == await r.json().arrindex("arr", r_path, 4, start=1, stop=3)


@pytest.mark.redismod
async def test_arrinsert(r: redis.Redis):
    await r.json().set("arr", Path.root_path(), [0, 4])
    assert 5 - -await r.json().arrinsert("arr", Path.root_path(), 1, *[1, 2, 3])
    assert [0, 1, 2, 3, 4] == await r.json().get("arr")

    # test prepends
    await r.json().set("val2", Path.root_path(), [5, 6, 7, 8, 9])
    await r.json().arrinsert("val2", Path.root_path(), 0, ["some", "thing"])
    assert await r.json().get("val2") == [["some", "thing"], 5, 6, 7, 8, 9]


@pytest.mark.redismod
async def test_arrlen(r: redis.Redis):
    await r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 5 == await r.json().arrlen("arr", Path.root_path())
    assert 5 == await r.json().arrlen("arr")
    assert await r.json().arrlen("fakekey") is None


@pytest.mark.redismod
async def test_arrpop(r: redis.Redis):
    await r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 4 == await r.json().arrpop("arr", Path.root_path(), 4)
    assert 3 == await r.json().arrpop("arr", Path.root_path(), -1)
    assert 2 == await r.json().arrpop("arr", Path.root_path())
    assert 0 == await r.json().arrpop("arr", Path.root_path(), 0)
    assert [1] == await r.json().get("arr")

    # test out of bounds
    await r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 4 == await r.json().arrpop("arr", Path.root_path(), 99)

    # none test
    await r.json().set("arr", Path.root_path(), [])
    assert await r.json().arrpop("arr") is None


@pytest.mark.redismod
async def test_arrtrim(r: redis.Redis):
    await r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 3 == await r.json().arrtrim("arr", Path.root_path(), 1, 3)
    assert [1, 2, 3] == await r.json().get("arr")

    # <0 test, should be 0 equivalent
    await r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == await r.json().arrtrim("arr", Path.root_path(), -1, 3)

    # testing stop > end
    await r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 2 == await r.json().arrtrim("arr", Path.root_path(), 3, 99)

    # start > array size and stop
    await r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == await r.json().arrtrim("arr", Path.root_path(), 9, 1)

    # all larger
    await r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == await r.json().arrtrim("arr", Path.root_path(), 9, 11)


@pytest.mark.redismod
async def test_resp(r: redis.Redis):
    obj = {"foo": "bar", "baz": 1, "qaz": True}
    await r.json().set("obj", Path.root_path(), obj)
    assert "bar" == await r.json().resp("obj", Path("foo"))
    assert 1 == await r.json().resp("obj", Path("baz"))
    assert await r.json().resp("obj", Path("qaz"))
    assert isinstance(await r.json().resp("obj"), list)


@pytest.mark.redismod
async def test_objkeys(r: redis.Redis):
    obj = {"foo": "bar", "baz": "qaz"}
    await r.json().set("obj", Path.root_path(), obj)
    keys = await r.json().objkeys("obj", Path.root_path())
    keys.sort()
    exp = list(obj.keys())
    exp.sort()
    assert exp == keys

    await r.json().set("obj", Path.root_path(), obj)
    keys = await r.json().objkeys("obj")
    assert keys == list(obj.keys())

    assert await r.json().objkeys("fakekey") is None


@pytest.mark.redismod
async def test_objlen(r: redis.Redis):
    obj = {"foo": "bar", "baz": "qaz"}
    await r.json().set("obj", Path.root_path(), obj)
    assert len(obj) == await r.json().objlen("obj", Path.root_path())

    await r.json().set("obj", Path.root_path(), obj)
    assert len(obj) == await r.json().objlen("obj")


# @pytest.mark.redismod
# async def test_json_commands_in_pipeline(r: redis.Redis):
#     async with r.json().pipeline() as p:
#         p.set("foo", Path.root_path(), "bar")
#         p.get("foo")
#         p.delete("foo")
#         assert [True, "bar", 1] == await p.execute()
#     assert await r.keys() == []
#     assert await r.get("foo") is None

#     # now with a true, json object
#     await r.flushdb()
#     p = await r.json().pipeline()
#     d = {"hello": "world", "oh": "snap"}
#     with pytest.deprecated_call():
#         p.jsonset("foo", Path.root_path(), d)
#         p.jsonget("foo")
#     p.exists("notarealkey")
#     p.delete("foo")
#     assert [True, d, 0, 1] == p.execute()
#     assert await r.keys() == []
#     assert await r.get("foo") is None


@pytest.mark.redismod
async def test_json_delete_with_dollar(r: redis.Redis):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert await r.json().set("doc1", "$", doc1)
    assert await r.json().delete("doc1", "$..a") == 2
    res = await r.json().get("doc1", "$")
    assert res == [{"nested": {"b": 3}}]

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    assert await r.json().set("doc2", "$", doc2)
    assert await r.json().delete("doc2", "$..a") == 1
    res = await r.json().get("doc2", "$")
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
    assert await r.json().set("doc3", "$", doc3)
    assert await r.json().delete("doc3", '$.[0]["nested"]..ciao') == 3

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
    res = await r.json().get("doc3", "$")
    assert res == doc3val

    # Test async default path
    assert await r.json().delete("doc3") == 1
    assert await r.json().get("doc3", "$") is None

    await r.json().delete("not_a_document", "..a")


@pytest.mark.redismod
async def test_json_forget_with_dollar(r: redis.Redis):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert await r.json().set("doc1", "$", doc1)
    assert await r.json().forget("doc1", "$..a") == 2
    res = await r.json().get("doc1", "$")
    assert res == [{"nested": {"b": 3}}]

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    assert await r.json().set("doc2", "$", doc2)
    assert await r.json().forget("doc2", "$..a") == 1
    res = await r.json().get("doc2", "$")
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
    assert await r.json().set("doc3", "$", doc3)
    assert await r.json().forget("doc3", '$.[0]["nested"]..ciao') == 3

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
    res = await r.json().get("doc3", "$")
    assert res == doc3val

    # Test async default path
    assert await r.json().forget("doc3") == 1
    assert await r.json().get("doc3", "$") is None

    await r.json().forget("not_a_document", "..a")


@pytest.mark.redismod
async def test_json_mget_dollar(r: redis.Redis):
    # Test mget with multi paths
    await r.json().set(
        "doc1",
        "$",
        {"a": 1, "b": 2, "nested": {"a": 3}, "c": None, "nested2": {"a": None}},
    )
    await r.json().set(
        "doc2",
        "$",
        {"a": 4, "b": 5, "nested": {"a": 6}, "c": None, "nested2": {"a": [None]}},
    )
    # Compare also to single JSON.GET
    assert await r.json().get("doc1", "$..a") == [1, 3, None]
    assert await r.json().get("doc2", "$..a") == [4, 6, [None]]

    # Test mget with single path
    await r.json().mget("doc1", "$..a") == [1, 3, None]
    # Test mget with multi path
    res = await r.json().mget(["doc1", "doc2"], "$..a")
    assert res == [[1, 3, None], [4, 6, [None]]]

    # Test missing key
    res = await r.json().mget(["doc1", "missing_doc"], "$..a")
    assert res == [[1, 3, None], None]
    res = await r.json().mget(["missing_doc1", "missing_doc2"], "$..a")
    assert res == [None, None]


@pytest.mark.redismod
async def test_numby_commands_dollar(r: redis.Redis):

    # Test NUMINCRBY
    await r.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})
    # Test multi
    assert await r.json().numincrby("doc1", "$..a", 2) == [None, 4, 7.0, None]

    res = await r.json().numincrby("doc1", "$..a", 2.5)
    assert res == [None, 6.5, 9.5, None]
    # Test single
    assert await r.json().numincrby("doc1", "$.b[1].a", 2) == [11.5]

    assert await r.json().numincrby("doc1", "$.b[2].a", 2) == [None]
    assert await r.json().numincrby("doc1", "$.b[1].a", 3.5) == [15.0]

    # Test NUMMULTBY
    await r.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})

    # test list
    with pytest.deprecated_call():
        res = await r.json().nummultby("doc1", "$..a", 2)
        assert res == [None, 4, 10, None]
        res = await r.json().nummultby("doc1", "$..a", 2.5)
        assert res == [None, 10.0, 25.0, None]

    # Test single
    with pytest.deprecated_call():
        assert await r.json().nummultby("doc1", "$.b[1].a", 2) == [50.0]
        assert await r.json().nummultby("doc1", "$.b[2].a", 2) == [None]
        assert await r.json().nummultby("doc1", "$.b[1].a", 3) == [150.0]

    # test missing keys
    with pytest.raises(exceptions.ResponseError):
        await r.json().numincrby("non_existing_doc", "$..a", 2)
        await r.json().nummultby("non_existing_doc", "$..a", 2)

    # Test legacy NUMINCRBY
    await r.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})
    await r.json().numincrby("doc1", ".b[0].a", 3) == 5

    # Test legacy NUMMULTBY
    await r.json().set("doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]})

    with pytest.deprecated_call():
        await r.json().nummultby("doc1", ".b[0].a", 3) == 6


@pytest.mark.redismod
async def test_strappend_dollar(r: redis.Redis):

    await r.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    # Test multi
    await r.json().strappend("doc1", "bar", "$..a") == [6, 8, None]

    await r.json().get("doc1", "$") == [
        {"a": "foobar", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}
    ]
    # Test single
    await r.json().strappend("doc1", "baz", "$.nested1.a") == [11]

    await r.json().get("doc1", "$") == [
        {"a": "foobar", "nested1": {"a": "hellobarbaz"}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().strappend("non_existing_doc", "$..a", "err")

    # Test multi
    await r.json().strappend("doc1", "bar", ".*.a") == 8
    await r.json().get("doc1", "$") == [
        {"a": "foo", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}
    ]

    # Test missing path
    with pytest.raises(exceptions.ResponseError):
        await r.json().strappend("doc1", "piu")


@pytest.mark.redismod
async def test_strlen_dollar(r: redis.Redis):

    # Test multi
    await r.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    assert await r.json().strlen("doc1", "$..a") == [3, 5, None]

    res2 = await r.json().strappend("doc1", "bar", "$..a")
    res1 = await r.json().strlen("doc1", "$..a")
    assert res1 == res2

    # Test single
    await r.json().strlen("doc1", "$.nested1.a") == [8]
    await r.json().strlen("doc1", "$.nested2.a") == [None]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().strlen("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrappend_dollar(r: redis.Redis):
    await r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    await r.json().arrappend("doc1", "$..a", "bar", "racuda") == [3, 5, None]
    assert await r.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
            "nested2": {"a": 31},
        }
    ]

    # Test single
    assert await r.json().arrappend("doc1", "$.nested1.a", "baz") == [6]
    assert await r.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
            "nested2": {"a": 31},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().arrappend("non_existing_doc", "$..a")

    # Test legacy
    await r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (all paths are updated, but return result of last path)
    assert await r.json().arrappend("doc1", "..a", "bar", "racuda") == 5

    assert await r.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
            "nested2": {"a": 31},
        }
    ]
    # Test single
    assert await r.json().arrappend("doc1", ".nested1.a", "baz") == 6
    assert await r.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
            "nested2": {"a": 31},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().arrappend("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrinsert_dollar(r: redis.Redis):
    await r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    res = await r.json().arrinsert("doc1", "$..a", "1", "bar", "racuda")
    assert res == [3, 5, None]

    assert await r.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", "bar", "racuda", None, "world"]},
            "nested2": {"a": 31},
        }
    ]
    # Test single
    assert await r.json().arrinsert("doc1", "$.nested1.a", -2, "baz") == [6]
    assert await r.json().get("doc1", "$") == [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", "bar", "racuda", "baz", None, "world"]},
            "nested2": {"a": 31},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().arrappend("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrlen_dollar(r: redis.Redis):

    await r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi
    assert await r.json().arrlen("doc1", "$..a") == [1, 3, None]
    res = await r.json().arrappend("doc1", "$..a", "non", "abba", "stanza")
    assert res == [4, 6, None]

    await r.json().clear("doc1", "$.a")
    assert await r.json().arrlen("doc1", "$..a") == [0, 6, None]
    # Test single
    assert await r.json().arrlen("doc1", "$.nested1.a") == [6]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().arrappend("non_existing_doc", "$..a")

    await r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (return result of last path)
    assert await r.json().arrlen("doc1", "$..a") == [1, 3, None]
    assert await r.json().arrappend("doc1", "..a", "non", "abba", "stanza") == 6

    # Test single
    assert await r.json().arrlen("doc1", ".nested1.a") == 6

    # Test missing key
    assert await r.json().arrlen("non_existing_doc", "..a") is None


@pytest.mark.redismod
async def test_arrpop_dollar(r: redis.Redis):
    await r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # # # Test multi
    assert await r.json().arrpop("doc1", "$..a", 1) == ['"foo"', None, None]

    assert await r.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().arrpop("non_existing_doc", "..a")

    # # Test legacy
    await r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (all paths are updated, but return result of last path)
    await r.json().arrpop("doc1", "..a", "1") is None
    assert await r.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}
    ]

    # # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().arrpop("non_existing_doc", "..a")


@pytest.mark.redismod
async def test_arrtrim_dollar(r: redis.Redis):

    await r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    assert await r.json().arrtrim("doc1", "$..a", "1", -1) == [0, 2, None]
    assert await r.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": [None, "world"]}, "nested2": {"a": 31}}
    ]

    assert await r.json().arrtrim("doc1", "$..a", "1", "1") == [0, 1, None]
    assert await r.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}
    ]
    # Test single
    assert await r.json().arrtrim("doc1", "$.nested1.a", 1, 0) == [0]
    assert await r.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": []}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().arrtrim("non_existing_doc", "..a", "0", 1)

    # Test legacy
    await r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi (all paths are updated, but return result of last path)
    assert await r.json().arrtrim("doc1", "..a", "1", "-1") == 2

    # Test single
    assert await r.json().arrtrim("doc1", ".nested1.a", "1", "1") == 1
    assert await r.json().get("doc1", "$") == [
        {"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().arrtrim("non_existing_doc", "..a", 1, 1)


@pytest.mark.redismod
async def test_objkeys_dollar(r: redis.Redis):
    await r.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )

    # Test single
    assert await r.json().objkeys("doc1", "$.nested1.a") == [["foo", "bar"]]

    # Test legacy
    assert await r.json().objkeys("doc1", ".*.a") == ["foo", "bar"]
    # Test single
    assert await r.json().objkeys("doc1", ".nested2.a") == ["baz"]

    # Test missing key
    assert await r.json().objkeys("non_existing_doc", "..a") is None

    # Test non existing doc
    with pytest.raises(exceptions.ResponseError):
        assert await r.json().objkeys("non_existing_doc", "$..a") == []

    assert await r.json().objkeys("doc1", "$..nowhere") == []


@pytest.mark.redismod
async def test_objlen_dollar(r: redis.Redis):
    await r.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )
    # Test multi
    assert await r.json().objlen("doc1", "$..a") == [None, 2, 1]
    # Test single
    assert await r.json().objlen("doc1", "$.nested1.a") == [2]

    # Test missing key, and path
    with pytest.raises(exceptions.ResponseError):
        await r.json().objlen("non_existing_doc", "$..a")

    assert await r.json().objlen("doc1", "$.nowhere") == []

    # Test legacy
    assert await r.json().objlen("doc1", ".*.a") == 2

    # Test single
    assert await r.json().objlen("doc1", ".nested2.a") == 1

    # Test missing key
    assert await r.json().objlen("non_existing_doc", "..a") is None

    # Test missing path
    # with pytest.raises(exceptions.ResponseError):
    await r.json().objlen("doc1", ".nowhere")


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
async def test_type_dollar(r: redis.Redis):
    jdata, jtypes = load_types_data("a")
    await r.json().set("doc1", "$", jdata)
    # Test multi
    assert await r.json().type("doc1", "$..a") == jtypes

    # Test single
    assert await r.json().type("doc1", "$.nested2.a") == [jtypes[1]]

    # Test missing key
    assert await r.json().type("non_existing_doc", "..a") is None


@pytest.mark.redismod
async def test_clear_dollar(r: redis.Redis):

    await r.json().set(
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
    assert await r.json().clear("doc1", "$..a") == 3

    assert await r.json().get("doc1", "$") == [
        {"nested1": {"a": {}}, "a": [], "nested2": {"a": "claro"}, "nested3": {"a": {}}}
    ]

    # Test single
    await r.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        },
    )
    assert await r.json().clear("doc1", "$.nested1.a") == 1
    assert await r.json().get("doc1", "$") == [
        {
            "nested1": {"a": {}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        }
    ]

    # Test missing path (async defaults to root)
    assert await r.json().clear("doc1") == 1
    assert await r.json().get("doc1", "$") == [{}]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().clear("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_toggle_dollar(r: redis.Redis):
    await r.json().set(
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
    assert await r.json().toggle("doc1", "$..a") == [None, 1, None, 0]
    assert await r.json().get("doc1", "$") == [
        {
            "a": ["foo"],
            "nested1": {"a": True},
            "nested2": {"a": 31},
            "nested3": {"a": False},
        }
    ]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await r.json().toggle("non_existing_doc", "$..a")
