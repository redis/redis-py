import pytest
import redis.asyncio as redis
from redis import exceptions
from redis.commands.json.path import Path
from tests.conftest import assert_resp_response, skip_ifmodversion_lt


@pytest.mark.redismod
async def test_json_setbinarykey(decoded_r: redis.Redis):
    d = {"hello": "world", b"some": "value"}
    with pytest.raises(TypeError):
        decoded_r.json().set("somekey", Path.root_path(), d)
    assert await decoded_r.json().set("somekey", Path.root_path(), d, decode_keys=True)


@pytest.mark.redismod
async def test_json_setgetdeleteforget(decoded_r: redis.Redis):
    assert await decoded_r.json().set("foo", Path.root_path(), "bar")
    assert_resp_response(decoded_r, await decoded_r.json().get("foo"), "bar", [["bar"]])
    assert await decoded_r.json().get("baz") is None
    assert await decoded_r.json().delete("foo") == 1
    assert await decoded_r.json().forget("foo") == 0  # second delete
    assert await decoded_r.exists("foo") == 0


@pytest.mark.redismod
async def test_jsonget(decoded_r: redis.Redis):
    await decoded_r.json().set("foo", Path.root_path(), "bar")
    assert_resp_response(decoded_r, await decoded_r.json().get("foo"), "bar", [["bar"]])


@pytest.mark.redismod
async def test_json_get_jset(decoded_r: redis.Redis):
    assert await decoded_r.json().set("foo", Path.root_path(), "bar")
    assert_resp_response(decoded_r, await decoded_r.json().get("foo"), "bar", [["bar"]])
    assert await decoded_r.json().get("baz") is None
    assert 1 == await decoded_r.json().delete("foo")
    assert await decoded_r.exists("foo") == 0


@pytest.mark.redismod
async def test_nonascii_setgetdelete(decoded_r: redis.Redis):
    assert await decoded_r.json().set("notascii", Path.root_path(), "hyvää-élève")
    res = "hyvää-élève"
    assert_resp_response(
        decoded_r, await decoded_r.json().get("notascii", no_escape=True), res, [[res]]
    )
    assert 1 == await decoded_r.json().delete("notascii")
    assert await decoded_r.exists("notascii") == 0


@pytest.mark.redismod
@skip_ifmodversion_lt("2.6.0", "ReJSON")
async def test_json_merge(decoded_r: redis.Redis):
    # Test with root path $
    assert await decoded_r.json().set(
        "person_data",
        "$",
        {"person1": {"personal_data": {"name": "John"}}},
    )
    assert await decoded_r.json().merge(
        "person_data", "$", {"person1": {"personal_data": {"hobbies": "reading"}}}
    )
    assert await decoded_r.json().get("person_data") == {
        "person1": {"personal_data": {"name": "John", "hobbies": "reading"}}
    }

    # Test with root path path $.person1.personal_data
    assert await decoded_r.json().merge(
        "person_data", "$.person1.personal_data", {"country": "Israel"}
    )
    assert await decoded_r.json().get("person_data") == {
        "person1": {
            "personal_data": {"name": "John", "hobbies": "reading", "country": "Israel"}
        }
    }

    # Test with null value to delete a value
    assert await decoded_r.json().merge(
        "person_data", "$.person1.personal_data", {"name": None}
    )
    assert await decoded_r.json().get("person_data") == {
        "person1": {"personal_data": {"country": "Israel", "hobbies": "reading"}}
    }


@pytest.mark.redismod
async def test_jsonsetexistentialmodifiersshouldsucceed(decoded_r: redis.Redis):
    obj = {"foo": "bar"}
    assert await decoded_r.json().set("obj", Path.root_path(), obj)

    # Test that flags prevent updates when conditions are unmet
    assert await decoded_r.json().set("obj", Path("foo"), "baz", nx=True) is None
    assert await decoded_r.json().set("obj", Path("qaz"), "baz", xx=True) is None

    # Test that flags allow updates when conditions are met
    assert await decoded_r.json().set("obj", Path("foo"), "baz", xx=True)
    assert await decoded_r.json().set("obj", Path("qaz"), "baz", nx=True)

    # Test that flags are mutually exlusive
    with pytest.raises(Exception):
        await decoded_r.json().set("obj", Path("foo"), "baz", nx=True, xx=True)


@pytest.mark.redismod
async def test_mgetshouldsucceed(decoded_r: redis.Redis):
    await decoded_r.json().set("1", Path.root_path(), 1)
    await decoded_r.json().set("2", Path.root_path(), 2)
    assert await decoded_r.json().mget(["1"], Path.root_path()) == [1]

    assert await decoded_r.json().mget([1, 2], Path.root_path()) == [1, 2]


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_ifmodversion_lt("2.6.0", "ReJSON")
async def test_mset(decoded_r: redis.Redis):
    await decoded_r.json().mset(
        [("1", Path.root_path(), 1), ("2", Path.root_path(), 2)]
    )

    assert await decoded_r.json().mget(["1"], Path.root_path()) == [1]
    assert await decoded_r.json().mget(["1", "2"], Path.root_path()) == [1, 2]


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
async def test_clear(decoded_r: redis.Redis):
    await decoded_r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 1 == await decoded_r.json().clear("arr", Path.root_path())
    assert_resp_response(decoded_r, await decoded_r.json().get("arr"), [], [[[]]])


@pytest.mark.redismod
async def test_type(decoded_r: redis.Redis):
    await decoded_r.json().set("1", Path.root_path(), 1)
    assert_resp_response(
        decoded_r,
        await decoded_r.json().type("1", Path.root_path()),
        "integer",
        ["integer"],
    )
    assert_resp_response(
        decoded_r, await decoded_r.json().type("1"), "integer", ["integer"]
    )


@pytest.mark.redismod
async def test_numincrby(decoded_r):
    await decoded_r.json().set("num", Path.root_path(), 1)
    assert_resp_response(
        decoded_r, await decoded_r.json().numincrby("num", Path.root_path(), 1), 2, [2]
    )
    res = await decoded_r.json().numincrby("num", Path.root_path(), 0.5)
    assert_resp_response(decoded_r, res, 2.5, [2.5])
    res = await decoded_r.json().numincrby("num", Path.root_path(), -1.25)
    assert_resp_response(decoded_r, res, 1.25, [1.25])


@pytest.mark.redismod
async def test_nummultby(decoded_r: redis.Redis):
    await decoded_r.json().set("num", Path.root_path(), 1)

    with pytest.deprecated_call():
        res = await decoded_r.json().nummultby("num", Path.root_path(), 2)
        assert_resp_response(decoded_r, res, 2, [2])
        res = await decoded_r.json().nummultby("num", Path.root_path(), 2.5)
        assert_resp_response(decoded_r, res, 5, [5])
        res = await decoded_r.json().nummultby("num", Path.root_path(), 0.5)
        assert_resp_response(decoded_r, res, 2.5, [2.5])


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
async def test_toggle(decoded_r: redis.Redis):
    await decoded_r.json().set("bool", Path.root_path(), False)
    assert await decoded_r.json().toggle("bool", Path.root_path())
    assert await decoded_r.json().toggle("bool", Path.root_path()) is False
    # check non-boolean value
    await decoded_r.json().set("num", Path.root_path(), 1)
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().toggle("num", Path.root_path())


@pytest.mark.redismod
async def test_strappend(decoded_r: redis.Redis):
    await decoded_r.json().set("jsonkey", Path.root_path(), "foo")
    assert 6 == await decoded_r.json().strappend("jsonkey", "bar")
    res = await decoded_r.json().get("jsonkey", Path.root_path())
    assert_resp_response(decoded_r, res, "foobar", [["foobar"]])


@pytest.mark.redismod
async def test_strlen(decoded_r: redis.Redis):
    await decoded_r.json().set("str", Path.root_path(), "foo")
    assert 3 == await decoded_r.json().strlen("str", Path.root_path())
    await decoded_r.json().strappend("str", "bar", Path.root_path())
    assert 6 == await decoded_r.json().strlen("str", Path.root_path())
    assert 6 == await decoded_r.json().strlen("str")


@pytest.mark.redismod
async def test_arrappend(decoded_r: redis.Redis):
    await decoded_r.json().set("arr", Path.root_path(), [1])
    assert 2 == await decoded_r.json().arrappend("arr", Path.root_path(), 2)
    assert 4 == await decoded_r.json().arrappend("arr", Path.root_path(), 3, 4)
    assert 7 == await decoded_r.json().arrappend("arr", Path.root_path(), *[5, 6, 7])


@pytest.mark.redismod
async def test_arrindex(decoded_r: redis.Redis):
    r_path = Path.root_path()
    await decoded_r.json().set("arr", r_path, [0, 1, 2, 3, 4])
    assert 1 == await decoded_r.json().arrindex("arr", r_path, 1)
    assert -1 == await decoded_r.json().arrindex("arr", r_path, 1, 2)
    assert 4 == await decoded_r.json().arrindex("arr", r_path, 4)
    assert 4 == await decoded_r.json().arrindex("arr", r_path, 4, start=0)
    assert 4 == await decoded_r.json().arrindex("arr", r_path, 4, start=0, stop=5000)
    assert -1 == await decoded_r.json().arrindex("arr", r_path, 4, start=0, stop=-1)
    assert -1 == await decoded_r.json().arrindex("arr", r_path, 4, start=1, stop=3)


@pytest.mark.redismod
async def test_arrinsert(decoded_r: redis.Redis):
    await decoded_r.json().set("arr", Path.root_path(), [0, 4])
    assert 5 == await decoded_r.json().arrinsert("arr", Path.root_path(), 1, *[1, 2, 3])
    res = [0, 1, 2, 3, 4]
    assert_resp_response(decoded_r, await decoded_r.json().get("arr"), res, [[res]])

    # test prepends
    await decoded_r.json().set("val2", Path.root_path(), [5, 6, 7, 8, 9])
    await decoded_r.json().arrinsert("val2", Path.root_path(), 0, ["some", "thing"])
    res = [["some", "thing"], 5, 6, 7, 8, 9]
    assert_resp_response(decoded_r, await decoded_r.json().get("val2"), res, [[res]])


@pytest.mark.redismod
async def test_arrlen(decoded_r: redis.Redis):
    await decoded_r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 5 == await decoded_r.json().arrlen("arr", Path.root_path())
    assert 5 == await decoded_r.json().arrlen("arr")
    assert await decoded_r.json().arrlen("fakekey") is None


@pytest.mark.redismod
async def test_arrpop(decoded_r: redis.Redis):
    await decoded_r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 4 == await decoded_r.json().arrpop("arr", Path.root_path(), 4)
    assert 3 == await decoded_r.json().arrpop("arr", Path.root_path(), -1)
    assert 2 == await decoded_r.json().arrpop("arr", Path.root_path())
    assert 0 == await decoded_r.json().arrpop("arr", Path.root_path(), 0)
    assert_resp_response(decoded_r, await decoded_r.json().get("arr"), [1], [[[1]]])

    # test out of bounds
    await decoded_r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 4 == await decoded_r.json().arrpop("arr", Path.root_path(), 99)

    # none test
    await decoded_r.json().set("arr", Path.root_path(), [])
    assert await decoded_r.json().arrpop("arr") is None


@pytest.mark.redismod
async def test_arrtrim(decoded_r: redis.Redis):
    await decoded_r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 3 == await decoded_r.json().arrtrim("arr", Path.root_path(), 1, 3)
    res = await decoded_r.json().get("arr")
    assert_resp_response(decoded_r, res, [1, 2, 3], [[[1, 2, 3]]])

    # <0 test, should be 0 equivalent
    await decoded_r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == await decoded_r.json().arrtrim("arr", Path.root_path(), -1, 3)

    # testing stop > end
    await decoded_r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 2 == await decoded_r.json().arrtrim("arr", Path.root_path(), 3, 99)

    # start > array size and stop
    await decoded_r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == await decoded_r.json().arrtrim("arr", Path.root_path(), 9, 1)

    # all larger
    await decoded_r.json().set("arr", Path.root_path(), [0, 1, 2, 3, 4])
    assert 0 == await decoded_r.json().arrtrim("arr", Path.root_path(), 9, 11)


@pytest.mark.redismod
async def test_resp(decoded_r: redis.Redis):
    obj = {"foo": "bar", "baz": 1, "qaz": True}
    await decoded_r.json().set("obj", Path.root_path(), obj)
    assert "bar" == await decoded_r.json().resp("obj", Path("foo"))
    assert 1 == await decoded_r.json().resp("obj", Path("baz"))
    assert await decoded_r.json().resp("obj", Path("qaz"))
    assert isinstance(await decoded_r.json().resp("obj"), list)


@pytest.mark.redismod
async def test_objkeys(decoded_r: redis.Redis):
    obj = {"foo": "bar", "baz": "qaz"}
    await decoded_r.json().set("obj", Path.root_path(), obj)
    keys = await decoded_r.json().objkeys("obj", Path.root_path())
    keys.sort()
    exp = list(obj.keys())
    exp.sort()
    assert exp == keys

    await decoded_r.json().set("obj", Path.root_path(), obj)
    keys = await decoded_r.json().objkeys("obj")
    assert keys == list(obj.keys())

    assert await decoded_r.json().objkeys("fakekey") is None


@pytest.mark.redismod
async def test_objlen(decoded_r: redis.Redis):
    obj = {"foo": "bar", "baz": "qaz"}
    await decoded_r.json().set("obj", Path.root_path(), obj)
    assert len(obj) == await decoded_r.json().objlen("obj", Path.root_path())

    await decoded_r.json().set("obj", Path.root_path(), obj)
    assert len(obj) == await decoded_r.json().objlen("obj")


# @pytest.mark.redismod
# async def test_json_commands_in_pipeline(decoded_r: redis.Redis):
#     async with decoded_r.json().pipeline() as p:
#         p.set("foo", Path.root_path(), "bar")
#         p.get("foo")
#         p.delete("foo")
#         assert [True, "bar", 1] == await p.execute()
#     assert await decoded_r.keys() == []
#     assert await decoded_r.get("foo") is None

#     # now with a true, json object
#     await decoded_r.flushdb()
#     p = await decoded_r.json().pipeline()
#     d = {"hello": "world", "oh": "snap"}
#     with pytest.deprecated_call():
#         p.jsonset("foo", Path.root_path(), d)
#         p.jsonget("foo")
#     p.exists("notarealkey")
#     p.delete("foo")
#     assert [True, d, 0, 1] == p.execute()
#     assert await decoded_r.keys() == []
#     assert await decoded_r.get("foo") is None


@pytest.mark.redismod
async def test_json_delete_with_dollar(decoded_r: redis.Redis):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert await decoded_r.json().set("doc1", "$", doc1)
    assert await decoded_r.json().delete("doc1", "$..a") == 2
    res = [{"nested": {"b": 3}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    assert await decoded_r.json().set("doc2", "$", doc2)
    assert await decoded_r.json().delete("doc2", "$..a") == 1
    res = await decoded_r.json().get("doc2", "$")
    res = [{"nested": {"b": [True, "a", "b"]}, "b": ["a", "b"]}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc2", "$"), res, [res])

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
    assert await decoded_r.json().set("doc3", "$", doc3)
    assert await decoded_r.json().delete("doc3", '$.[0]["nested"]..ciao') == 3

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
    res = await decoded_r.json().get("doc3", "$")
    assert_resp_response(decoded_r, res, doc3val, [doc3val])

    # Test async default path
    assert await decoded_r.json().delete("doc3") == 1
    assert await decoded_r.json().get("doc3", "$") is None

    await decoded_r.json().delete("not_a_document", "..a")


@pytest.mark.redismod
async def test_json_forget_with_dollar(decoded_r: redis.Redis):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert await decoded_r.json().set("doc1", "$", doc1)
    assert await decoded_r.json().forget("doc1", "$..a") == 2
    res = [{"nested": {"b": 3}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [True, "a", "b"]}}
    assert await decoded_r.json().set("doc2", "$", doc2)
    assert await decoded_r.json().forget("doc2", "$..a") == 1
    res = [{"nested": {"b": [True, "a", "b"]}, "b": ["a", "b"]}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc2", "$"), res, [res])

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
    assert await decoded_r.json().set("doc3", "$", doc3)
    assert await decoded_r.json().forget("doc3", '$.[0]["nested"]..ciao') == 3

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
    res = await decoded_r.json().get("doc3", "$")
    assert_resp_response(decoded_r, res, doc3val, [doc3val])

    # Test async default path
    assert await decoded_r.json().forget("doc3") == 1
    assert await decoded_r.json().get("doc3", "$") is None

    await decoded_r.json().forget("not_a_document", "..a")


@pytest.mark.redismod
async def test_json_mget_dollar(decoded_r: redis.Redis):
    # Test mget with multi paths
    await decoded_r.json().set(
        "doc1",
        "$",
        {"a": 1, "b": 2, "nested": {"a": 3}, "c": None, "nested2": {"a": None}},
    )
    await decoded_r.json().set(
        "doc2",
        "$",
        {"a": 4, "b": 5, "nested": {"a": 6}, "c": None, "nested2": {"a": [None]}},
    )
    # Compare also to single JSON.GET
    res = [1, 3, None]
    assert_resp_response(
        decoded_r, await decoded_r.json().get("doc1", "$..a"), res, [res]
    )
    res = [4, 6, [None]]
    assert_resp_response(
        decoded_r, await decoded_r.json().get("doc2", "$..a"), res, [res]
    )

    # Test mget with single path
    await decoded_r.json().mget("doc1", "$..a") == [1, 3, None]
    # Test mget with multi path
    res = await decoded_r.json().mget(["doc1", "doc2"], "$..a")
    assert res == [[1, 3, None], [4, 6, [None]]]

    # Test missing key
    res = await decoded_r.json().mget(["doc1", "missing_doc"], "$..a")
    assert res == [[1, 3, None], None]
    res = await decoded_r.json().mget(["missing_doc1", "missing_doc2"], "$..a")
    assert res == [None, None]


@pytest.mark.redismod
async def test_numby_commands_dollar(decoded_r: redis.Redis):
    # Test NUMINCRBY
    await decoded_r.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )
    # Test multi
    assert await decoded_r.json().numincrby("doc1", "$..a", 2) == [None, 4, 7.0, None]

    res = await decoded_r.json().numincrby("doc1", "$..a", 2.5)
    assert res == [None, 6.5, 9.5, None]
    # Test single
    assert await decoded_r.json().numincrby("doc1", "$.b[1].a", 2) == [11.5]

    assert await decoded_r.json().numincrby("doc1", "$.b[2].a", 2) == [None]
    assert await decoded_r.json().numincrby("doc1", "$.b[1].a", 3.5) == [15.0]

    # Test NUMMULTBY
    await decoded_r.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )

    # test list
    with pytest.deprecated_call():
        res = await decoded_r.json().nummultby("doc1", "$..a", 2)
        assert res == [None, 4, 10, None]
        res = await decoded_r.json().nummultby("doc1", "$..a", 2.5)
        assert res == [None, 10.0, 25.0, None]

    # Test single
    with pytest.deprecated_call():
        assert await decoded_r.json().nummultby("doc1", "$.b[1].a", 2) == [50.0]
        assert await decoded_r.json().nummultby("doc1", "$.b[2].a", 2) == [None]
        assert await decoded_r.json().nummultby("doc1", "$.b[1].a", 3) == [150.0]

    # test missing keys
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().numincrby("non_existing_doc", "$..a", 2)
        await decoded_r.json().nummultby("non_existing_doc", "$..a", 2)

    # Test legacy NUMINCRBY
    await decoded_r.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )
    await decoded_r.json().numincrby("doc1", ".b[0].a", 3) == 5

    # Test legacy NUMMULTBY
    await decoded_r.json().set(
        "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
    )

    with pytest.deprecated_call():
        await decoded_r.json().nummultby("doc1", ".b[0].a", 3) == 6


@pytest.mark.redismod
async def test_strappend_dollar(decoded_r: redis.Redis):
    await decoded_r.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    # Test multi
    await decoded_r.json().strappend("doc1", "bar", "$..a") == [6, 8, None]

    res = [{"a": "foobar", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test single
    await decoded_r.json().strappend("doc1", "baz", "$.nested1.a") == [11]

    res = [{"a": "foobar", "nested1": {"a": "hellobarbaz"}, "nested2": {"a": 31}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().strappend("non_existing_doc", "$..a", "err")

    # Test multi
    await decoded_r.json().strappend("doc1", "bar", ".*.a") == 8
    res = [{"a": "foobar", "nested1": {"a": "hellobarbazbar"}, "nested2": {"a": 31}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test missing path
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().strappend("doc1", "piu")


@pytest.mark.redismod
async def test_strlen_dollar(decoded_r: redis.Redis):
    # Test multi
    await decoded_r.json().set(
        "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
    )
    assert await decoded_r.json().strlen("doc1", "$..a") == [3, 5, None]

    res2 = await decoded_r.json().strappend("doc1", "bar", "$..a")
    res1 = await decoded_r.json().strlen("doc1", "$..a")
    assert res1 == res2

    # Test single
    await decoded_r.json().strlen("doc1", "$.nested1.a") == [8]
    await decoded_r.json().strlen("doc1", "$.nested2.a") == [None]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().strlen("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrappend_dollar(decoded_r: redis.Redis):
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    await decoded_r.json().arrappend("doc1", "$..a", "bar", "racuda") == [3, 5, None]
    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test single
    assert await decoded_r.json().arrappend("doc1", "$.nested1.a", "baz") == [6]
    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().arrappend("non_existing_doc", "$..a")

    # Test legacy
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (all paths are updated, but return result of last path)
    assert await decoded_r.json().arrappend("doc1", "..a", "bar", "racuda") == 5

    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])
    # Test single
    assert await decoded_r.json().arrappend("doc1", ".nested1.a", "baz") == 6
    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().arrappend("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrinsert_dollar(decoded_r: redis.Redis):
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    res = await decoded_r.json().arrinsert("doc1", "$..a", "1", "bar", "racuda")
    assert res == [3, 5, None]

    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", "bar", "racuda", None, "world"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])
    # Test single
    assert await decoded_r.json().arrinsert("doc1", "$.nested1.a", -2, "baz") == [6]
    res = [
        {
            "a": ["foo", "bar", "racuda"],
            "nested1": {"a": ["hello", "bar", "racuda", "baz", None, "world"]},
            "nested2": {"a": 31},
        }
    ]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().arrappend("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_arrlen_dollar(decoded_r: redis.Redis):
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi
    assert await decoded_r.json().arrlen("doc1", "$..a") == [1, 3, None]
    res = await decoded_r.json().arrappend("doc1", "$..a", "non", "abba", "stanza")
    assert res == [4, 6, None]

    await decoded_r.json().clear("doc1", "$.a")
    assert await decoded_r.json().arrlen("doc1", "$..a") == [0, 6, None]
    # Test single
    assert await decoded_r.json().arrlen("doc1", "$.nested1.a") == [6]

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().arrappend("non_existing_doc", "$..a")

    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (return result of last path)
    assert await decoded_r.json().arrlen("doc1", "$..a") == [1, 3, None]
    assert await decoded_r.json().arrappend("doc1", "..a", "non", "abba", "stanza") == 6

    # Test single
    assert await decoded_r.json().arrlen("doc1", ".nested1.a") == 6

    # Test missing key
    assert await decoded_r.json().arrlen("non_existing_doc", "..a") is None


@pytest.mark.redismod
async def test_arrpop_dollar(decoded_r: redis.Redis):
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi
    assert await decoded_r.json().arrpop("doc1", "$..a", 1) == ['"foo"', None, None]

    res = [{"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().arrpop("non_existing_doc", "..a")

    # # Test legacy
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi (all paths are updated, but return result of last path)
    await decoded_r.json().arrpop("doc1", "..a", "1") is None
    res = [{"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().arrpop("non_existing_doc", "..a")


@pytest.mark.redismod
async def test_arrtrim_dollar(decoded_r: redis.Redis):
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )
    # Test multi
    assert await decoded_r.json().arrtrim("doc1", "$..a", "1", -1) == [0, 2, None]
    res = [{"a": [], "nested1": {"a": [None, "world"]}, "nested2": {"a": 31}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    assert await decoded_r.json().arrtrim("doc1", "$..a", "1", "1") == [0, 1, None]
    res = [{"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])
    # Test single
    assert await decoded_r.json().arrtrim("doc1", "$.nested1.a", 1, 0) == [0]
    res = [{"a": [], "nested1": {"a": []}, "nested2": {"a": 31}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().arrtrim("non_existing_doc", "..a", "0", 1)

    # Test legacy
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "a": ["foo"],
            "nested1": {"a": ["hello", None, "world"]},
            "nested2": {"a": 31},
        },
    )

    # Test multi (all paths are updated, but return result of last path)
    assert await decoded_r.json().arrtrim("doc1", "..a", "1", "-1") == 2

    # Test single
    assert await decoded_r.json().arrtrim("doc1", ".nested1.a", "1", "1") == 1
    res = [{"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().arrtrim("non_existing_doc", "..a", 1, 1)


@pytest.mark.redismod
async def test_objkeys_dollar(decoded_r: redis.Redis):
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )

    # Test single
    assert await decoded_r.json().objkeys("doc1", "$.nested1.a") == [["foo", "bar"]]

    # Test legacy
    assert await decoded_r.json().objkeys("doc1", ".*.a") == ["foo", "bar"]
    # Test single
    assert await decoded_r.json().objkeys("doc1", ".nested2.a") == ["baz"]

    # Test missing key
    assert await decoded_r.json().objkeys("non_existing_doc", "..a") is None

    # Test non existing doc
    with pytest.raises(exceptions.ResponseError):
        assert await decoded_r.json().objkeys("non_existing_doc", "$..a") == []

    assert await decoded_r.json().objkeys("doc1", "$..nowhere") == []


@pytest.mark.redismod
async def test_objlen_dollar(decoded_r: redis.Redis):
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": {"baz": 50}},
        },
    )
    # Test multi
    assert await decoded_r.json().objlen("doc1", "$..a") == [None, 2, 1]
    # Test single
    assert await decoded_r.json().objlen("doc1", "$.nested1.a") == [2]

    # Test missing key, and path
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().objlen("non_existing_doc", "$..a")

    assert await decoded_r.json().objlen("doc1", "$.nowhere") == []

    # Test legacy
    assert await decoded_r.json().objlen("doc1", ".*.a") == 2

    # Test single
    assert await decoded_r.json().objlen("doc1", ".nested2.a") == 1

    # Test missing key
    assert await decoded_r.json().objlen("non_existing_doc", "..a") is None

    # Test missing path
    # with pytest.raises(exceptions.ResponseError):
    await decoded_r.json().objlen("doc1", ".nowhere")


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
async def test_type_dollar(decoded_r: redis.Redis):
    jdata, jtypes = load_types_data("a")
    await decoded_r.json().set("doc1", "$", jdata)
    # Test multi
    assert_resp_response(
        decoded_r, await decoded_r.json().type("doc1", "$..a"), jtypes, [jtypes]
    )

    # Test single
    res = await decoded_r.json().type("doc1", "$.nested2.a")
    assert_resp_response(decoded_r, res, [jtypes[1]], [[jtypes[1]]])

    # Test missing key
    assert_resp_response(
        decoded_r, await decoded_r.json().type("non_existing_doc", "..a"), None, [None]
    )


@pytest.mark.redismod
async def test_clear_dollar(decoded_r: redis.Redis):
    await decoded_r.json().set(
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
    assert await decoded_r.json().clear("doc1", "$..a") == 3

    res = [
        {"nested1": {"a": {}}, "a": [], "nested2": {"a": "claro"}, "nested3": {"a": {}}}
    ]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test single
    await decoded_r.json().set(
        "doc1",
        "$",
        {
            "nested1": {"a": {"foo": 10, "bar": 20}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        },
    )
    assert await decoded_r.json().clear("doc1", "$.nested1.a") == 1
    res = [
        {
            "nested1": {"a": {}},
            "a": ["foo"],
            "nested2": {"a": "claro"},
            "nested3": {"a": {"baz": 50}},
        }
    ]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test missing path (async defaults to root)
    assert await decoded_r.json().clear("doc1") == 1
    assert_resp_response(
        decoded_r, await decoded_r.json().get("doc1", "$"), [{}], [[{}]]
    )

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().clear("non_existing_doc", "$..a")


@pytest.mark.redismod
async def test_toggle_dollar(decoded_r: redis.Redis):
    await decoded_r.json().set(
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
    assert await decoded_r.json().toggle("doc1", "$..a") == [None, 1, None, 0]
    res = [
        {
            "a": ["foo"],
            "nested1": {"a": True},
            "nested2": {"a": 31},
            "nested3": {"a": False},
        }
    ]
    assert_resp_response(decoded_r, await decoded_r.json().get("doc1", "$"), res, [res])

    # Test missing key
    with pytest.raises(exceptions.ResponseError):
        await decoded_r.json().toggle("non_existing_doc", "$..a")
