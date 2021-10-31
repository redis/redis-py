import pytest
import redis
from redis.commands.json.path import Path
from redis import exceptions
from .conftest import skip_ifmodversion_lt
from .testdata.jsontestdata import nested_large_key


@pytest.fixture
def client(modclient):
    modclient.flushdb()
    return modclient


@pytest.mark.redismod
def test_json_setbinarykey(client):
    d = {"hello": "world", b"some": "value"}
    with pytest.raises(TypeError):
        client.json().set("somekey", Path.rootPath(), d)
    assert client.json().set("somekey", Path.rootPath(), d, decode_keys=True)


@pytest.mark.redismod
def test_json_setgetdeleteforget(client):
    assert client.json().set("foo", Path.rootPath(), "bar")
    assert client.json().get("foo") == "bar"
    assert client.json().get("baz") is None
    assert client.json().delete("foo") == 1
    assert client.json().forget("foo") == 0  # second delete
    assert client.exists("foo") == 0


@pytest.mark.redismod
def test_justaget(client):
    client.json().set("foo", Path.rootPath(), "bar")
    assert client.json().get("foo") == "bar"


@pytest.mark.redismod
def test_json_get_jset(client):
    assert client.json().set("foo", Path.rootPath(), "bar")
    assert "bar" == client.json().get("foo")
    assert client.json().get("baz") is None
    assert 1 == client.json().delete("foo")
    assert client.exists("foo") == 0


@pytest.mark.redismod
def test_nonascii_setgetdelete(client):
    assert client.json().set("notascii", Path.rootPath(),
                             "hyvää-élève") is True
    assert "hyvää-élève" == client.json().get("notascii", no_escape=True)
    assert 1 == client.json().delete("notascii")
    assert client.exists("notascii") == 0


@pytest.mark.redismod
def test_jsonsetexistentialmodifiersshouldsucceed(client):
    obj = {"foo": "bar"}
    assert client.json().set("obj", Path.rootPath(), obj)

    # Test that flags prevent updates when conditions are unmet
    assert client.json().set("obj", Path("foo"), "baz", nx=True) is None
    assert client.json().set("obj", Path("qaz"), "baz", xx=True) is None

    # Test that flags allow updates when conditions are met
    assert client.json().set("obj", Path("foo"), "baz", xx=True)
    assert client.json().set("obj", Path("qaz"), "baz", nx=True)

    # Test that flags are mutually exlusive
    with pytest.raises(Exception):
        client.json().set("obj", Path("foo"), "baz", nx=True, xx=True)


@pytest.mark.redismod
def test_mgetshouldsucceed(client):
    client.json().set("1", Path.rootPath(), 1)
    client.json().set("2", Path.rootPath(), 2)
    r = client.json().mget(Path.rootPath(), "1", "2")
    e = [1, 2]
    assert e == r


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
def test_clearShouldSucceed(client):
    client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 1 == client.json().clear("arr", Path.rootPath())
    assert [] == client.json().get("arr")


@pytest.mark.redismod
def test_typeshouldsucceed(client):
    client.json().set("1", Path.rootPath(), 1)
    assert b"integer" == client.json().type("1")


@pytest.mark.redismod
def test_numincrbyshouldsucceed(client):
    client.json().set("num", Path.rootPath(), 1)
    assert 2 == client.json().numincrby("num", Path.rootPath(), 1)
    assert 2.5 == client.json().numincrby("num", Path.rootPath(), 0.5)
    assert 1.25 == client.json().numincrby("num", Path.rootPath(), -1.25)


@pytest.mark.redismod
def test_nummultbyshouldsucceed(client):
    client.json().set("num", Path.rootPath(), 1)
    assert 2 == client.json().nummultby("num", Path.rootPath(), 2)
    assert 5 == client.json().nummultby("num", Path.rootPath(), 2.5)
    assert 2.5 == client.json().nummultby("num", Path.rootPath(), 0.5)


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "ReJSON")  # todo: update after the release
def test_toggleShouldSucceed(client):
    client.json().set("bool", Path.rootPath(), False)
    assert client.json().toggle("bool", Path.rootPath())
    assert not client.json().toggle("bool", Path.rootPath())
    # check non-boolean value
    client.json().set("num", Path.rootPath(), 1)
    with pytest.raises(redis.exceptions.ResponseError):
        client.json().toggle("num", Path.rootPath())


@pytest.mark.redismod
def test_strappendshouldsucceed(client):
    client.json().set("str", Path.rootPath(), "foo")
    assert 6 == client.json().strappend("str", "bar", Path.rootPath())
    assert "foobar" == client.json().get("str", Path.rootPath())


@pytest.mark.redismod
def test_debug(client):
    client.json().set("str", Path.rootPath(), "foo")
    assert 24 == client.json().debug("str", Path.rootPath())


@pytest.mark.redismod
def test_strlenshouldsucceed(client):
    client.json().set("str", Path.rootPath(), "foo")
    assert 3 == client.json().strlen("str", Path.rootPath())
    client.json().strappend("str", "bar", Path.rootPath())
    assert 6 == client.json().strlen("str", Path.rootPath())


@pytest.mark.redismod
def test_arrappendshouldsucceed(client):
    client.json().set("arr", Path.rootPath(), [1])
    assert 2 == client.json().arrappend("arr", Path.rootPath(), 2)
    assert 4 == client.json().arrappend("arr", Path.rootPath(), 3, 4)
    assert 7 == client.json().arrappend("arr", Path.rootPath(), *[5, 6, 7])


@pytest.mark.redismod
def testArrIndexShouldSucceed(client):
    client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 1 == client.json().arrindex("arr", Path.rootPath(), 1)
    assert -1 == client.json().arrindex("arr", Path.rootPath(), 1, 2)


@pytest.mark.redismod
def test_arrinsertshouldsucceed(client):
    client.json().set("arr", Path.rootPath(), [0, 4])
    assert 5 - -client.json().arrinsert(
        "arr",
        Path.rootPath(),
        1,
        *[
            1,
            2,
            3,
        ]
    )
    assert [0, 1, 2, 3, 4] == client.json().get("arr")


@pytest.mark.redismod
def test_arrlenshouldsucceed(client):
    client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 5 == client.json().arrlen("arr", Path.rootPath())


@pytest.mark.redismod
def test_arrpopshouldsucceed(client):
    client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 4 == client.json().arrpop("arr", Path.rootPath(), 4)
    assert 3 == client.json().arrpop("arr", Path.rootPath(), -1)
    assert 2 == client.json().arrpop("arr", Path.rootPath())
    assert 0 == client.json().arrpop("arr", Path.rootPath(), 0)
    assert [1] == client.json().get("arr")


@pytest.mark.redismod
def test_arrtrimshouldsucceed(client):
    client.json().set("arr", Path.rootPath(), [0, 1, 2, 3, 4])
    assert 3 == client.json().arrtrim("arr", Path.rootPath(), 1, 3)
    assert [1, 2, 3] == client.json().get("arr")


@pytest.mark.redismod
def test_respshouldsucceed(client):
    obj = {"foo": "bar", "baz": 1, "qaz": True}
    client.json().set("obj", Path.rootPath(), obj)
    assert b"bar" == client.json().resp("obj", Path("foo"))
    assert 1 == client.json().resp("obj", Path("baz"))
    assert client.json().resp("obj", Path("qaz"))


@pytest.mark.redismod
def test_objkeysshouldsucceed(client):
    obj = {"foo": "bar", "baz": "qaz"}
    client.json().set("obj", Path.rootPath(), obj)
    keys = client.json().objkeys("obj", Path.rootPath())
    keys.sort()
    exp = list(obj.keys())
    exp.sort()
    assert exp == keys


@pytest.mark.redismod
def test_objlenshouldsucceed(client):
    obj = {"foo": "bar", "baz": "qaz"}
    client.json().set("obj", Path.rootPath(), obj)
    assert len(obj) == client.json().objlen("obj", Path.rootPath())


# @pytest.mark.pipeline
# @pytest.mark.redismod
# def test_pipelineshouldsucceed(client):
#     p = client.json().pipeline()
#     p.set("foo", Path.rootPath(), "bar")
#     p.get("foo")
#     p.delete("foo")
#     assert [True, "bar", 1] == p.execute()
#     assert client.keys() == []
#     assert client.get("foo") is None


######### DUAL

@pytest.mark.redismod
def test_json_delete_with_dollar(client):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert client.json().set("doc1", "$", doc1)
    assert client.json().delete("doc1", "$..a") == 2
    r = client.json().get("doc1", "$")
    assert r == [{"nested": {"b": 3}}]


    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], 
            "nested": {"b":[True, "a","b"]}}
    assert client.json().set('doc2', '$', doc2)
    assert client.json().delete("doc2", "$..a") == 1
    res = client.json().get("doc2", "$")
    assert res == [{"nested":{"b":[True,"a","b"]},"b":["a","b"]}]

    doc3 = [{"ciao":["non ancora"],"nested":[{"ciao":[1,"a"]}, 
            {"ciao":[2,"a"]}, 
            {"ciaoc":[3,"non","ciao"]}, 
            {"ciao":[4,"a"]}, {"e":[5,"non","ciao"]}]}]
    assert client.json().set('doc3', '$', doc3)
    assert client.json().delete('doc3', '$.[0]["nested"]..ciao') == 3

    doc3val = [[{"ciao":["non ancora"],
                "nested":[{},{},
                {"ciaoc":[3,"non","ciao"]},{},{"e":[5,"non","ciao"]}]}]]
    res = client.json().get('doc3', '$')
    assert res == doc3val

    # Test default path
    assert client.json().delete("doc3") == 1
    assert client.json().get("doc3", "$") is None

    client.json().delete("not_a_document", "..a")

@pytest.mark.redismod
def test_json_forget_with_dollar(client):
    doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
    assert client.json().set("doc1", "$", doc1)
    assert client.json().forget("doc1", "$..a") == 2
    r = client.json().get("doc1", "$")
    assert r == [{"nested": {"b": 3}}]


    doc2 = {"a": {"a": 2, "b": 3}, "b": ["a", "b"], 
            "nested": {"b":[True, "a","b"]}}
    assert client.json().set('doc2', '$', doc2)
    assert client.json().forget("doc2", "$..a") == 1
    res = client.json().get("doc2", "$")
    assert res == [{"nested":{"b":[True,"a","b"]},"b":["a","b"]}]

    doc3 = [{"ciao":["non ancora"],"nested":[{"ciao":[1,"a"]}, 
            {"ciao":[2,"a"]}, 
            {"ciaoc":[3,"non","ciao"]}, 
            {"ciao":[4,"a"]}, {"e":[5,"non","ciao"]}]}]
    assert client.json().set('doc3', '$', doc3)
    assert client.json().forget('doc3', '$.[0]["nested"]..ciao') == 3

    doc3val = [[{"ciao":["non ancora"],
                "nested":[{},{},
                {"ciaoc":[3,"non","ciao"]},{},{"e":[5,"non","ciao"]}]}]]
    res = client.json().get('doc3', '$')
    assert res == doc3val

    # Test default path
    assert client.json().forget("doc3") == 1
    assert client.json().get("doc3", "$") is None

    client.json().forget("not_a_document", "..a")

@pytest.mark.redismod
def test_set_and_get_with_dollar(client):
    # Test set and get on large nested key
    assert client.json().set("doc1", "$", nested_large_key, "XX") is None
    assert client.json().set("doc1", "$", nested_large_key, "NX")
    assert client.json().get('doc1', '$') == [nested_large_key]
    assert client.json().set("doc1", "$", nested_large_key, "NX") is None

    # Test single path
    assert client.json().get('doc1', '$..tm') == [[46,876.85],[134.761,"jcoels",None]]

    # Test multi get and set
    assert client.json().get('doc1', '$..foobar') == [3.141592,1.61803398875]

    # Set multi existing values
    client.json().set('doc1', '$..foobar', '"new_val"')
    assert client.json().get('doc1', '$..foobar') == ["new_val","new_val"]

    # Test multi set and get on small nested key
    nested_simple_key = {"a":1,"nested":{"a":2,"b":3}}
    client.json().set('doc2', '$', nested_simple_key)
    assert client.json().get('doc2', '$') == [nested_simple_key]
    # Set multi existing values
    client.json().set('doc2', '$..a', '4.2')
    assert client.json().get('doc2', '$') == \
        [{"a":4.2,"nested":{"a":4.2,"b":3}}]


    # Test multi paths
    assert client.json().get('doc1', '$..tm', '$..nu') == \
        [[[46,876.85],[134.761,"jcoels",None]],[[377,"qda",True]]]
    # Test multi paths - if one path is none-legacy - result format is not legacy
    assert client.json().get('doc1', '..tm', '$..nu') == \
        [[[46,876.85],[134.761,"jcoels",None]],[[377,"qda",True]]]

    # Test missing key
    assert client.json().get('docX', '..tm', '$..nu') is None
    # Test missing path
    assert client.json().get('doc1', '..tm', '$..back_in_nov') == \
        [[[46,876.85],[134.761,"jcoels",None]],[]]
    assert client.json().get('doc2', '..a', '..b', '$.back_in_nov') == \
        [[4.2,4.2],[3],[]]

    # Test legacy multi path (all paths are legacy)
    client.json().get('doc1', '..nu', '..tm') == \
        {"..nu":[377,"qda",True],"..tm":[46,876.85]}
    # Test legacy single path
    client.json().get('doc1', '..tm') == '[46,876.85]'

    # Test missing legacy path (should return an error for a missing path)
    client.json().set('doc2', '$.nested.b', None)

    with pytest.raises(exceptions.DataError):
        client.json.get('doc2', '.a', '.nested.b', '.back_in_nov', '.ttyl')
        client.json.get('JSON.GET', 'doc2', '.back_in_nov')

@pytest.mark.redismod
def test_json_mget_dollar(client):
    # Test mget with multi paths
    client.json().set('doc1', '$', {"a":1, "b": 2, "nested": {"a": 3}, "c": None, "nested2": {"a": None}})
    client.json().set('doc2', '$', {"a":4, "b": 5, "nested": {"a": 6}, "c": None, "nested2": {"a": [None]}})
    # Compare also to single JSON.GET
    assert client.json().get('doc1', '$..a') == [1,3,None]
    assert client.json().get('doc2', '$..a') == [4,6,[None]]

    # Test mget with single path
    client.json().mget('doc1', '$..a') == [1,3,None]
    # Test mget with multi path
    client.json().mget('doc1', 'doc2', '$..a') == [[1,3,None], [4,6,[None]]]

    # Test missing key
    client.json().mget('doc1', 'missing_doc', '$..a') == [[1,3,None], None]
    res = client.json().mget('missing_doc1', 'missing_doc2', '$..a')
    assert res == [None, None]

@pytest.mark.redismod
def test_numby_commands_dollar(env):

    r = env

    # Test NUMINCRBY
    client.json().set('doc1', '$', {"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]})
    # Test multi
    assert client.json().numincrby('doc1', '$..a', '2') == [None, 4, 7.0, None]

    assert client.json().numincrby('doc1', '$..a', '2.5') == [None, 6.5, 9.5, None]
    # Test single
    assert client.json().numincrby('doc1', '$.b[1].a', '2')  == [11.5]

    assert client.json().numincrby('doc1', '$.b[2].a', '2') == [None]
    assert client.json().numincrby('doc1', '$.b[1].a', '3.5') == [15.0]

    # Test NUMMULTBY
    client.json().set('doc1', '$', {"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]})

    assert client.json().nummultby('doc1', '$..a', '2') == [None, 4, 10, None]
    assert client.json().nummultby('doc1', '$..a', '2.5') == [None,10.0,25.0,None]
    # Test single
    assert client.json().nummultby('doc1', '$.b[1].a', '2') == [50.0]
    assert client.json().nummultby('doc1', '$.b[2].a', '2') == [None]
    assert client.json().nummultby('doc1', '$.b[1].a', '3') == [150.0]

    # Test NUMPOWBY
    client.json().set('doc1', '$', {"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]})
    # Test multi
    assert client.json().numpowby('doc1', '$..a', '2') == [None, 4, 25, None]
    #  Avoid json.loads to verify the underlying type (integer/float)
    assert client.json().numpowby('doc1', '$..a', '2') == [None,16,625.0,None]

    # Test single
    assert client.json().numpowby('JSON.NUMPOWBY', 'doc1', '$.b[1].a', '2') == [390625.0]
    assert client.json().numpowby('JSON.NUMPOWBY', 'doc1', '$.b[2].a', '2') == [None]
    assert client.json().numpowby('JSON.NUMPOWBY', 'doc1', '$.b[1].a', '3') == [5.960464477539062e16]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().numincrby('non_existing_doc', '$..a', '2')
        client.json().nummultby('non_existing_doc', '$..a', '2')

    # TODO fixme
    r.expect('JSON.NUMPOWBY', 'non_existing_doc', '$..a', '2')

    # Test legacy NUMINCRBY
    client.json().set('doc1', '$', {"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]})
    client.json().numincrby('doc1', '.b[0].a', '3') == 5

    # Test legacy NUMMULTBY
    client.json().set('doc1', '$', {"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]})
    client.json().nummultby('doc1', '.b[0].a', '3') == 6

@pytest.mark.redismod
def test_strappend_dollar(client):

    client.json().set('doc1', '$', {"a":"foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}})
    # Test multi
    client.json().strappend('doc1', '$..a', '"bar"') == [6, 8, None]


    client.json().get('doc1', '$') == [{"a":"foobar","nested1":{"a":"hellobar"},"nested2":{"a":31}}]
    # Test single
    client.json().strappend('doc1', '$.nested1.a', '"baz"') == [11]

    client.json().get('doc1', '$') == [{"a":"foobar","nested1":{"a":"hellobarbaz"},"nested2":{"a":31}}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().strappend('non_existing_doc', '$..a', '"err"')

    # Test multi
    client.json().strappend('doc1', '.*.a', '"bar"') == 8
    client.json.get('doc1', '$') == [{"a":"foo","nested1":{"a":"hellobar"},"nested2":{"a":31}}]

    # Test missing path
    with pytest.raises(exceptions.DataError):
        client.json().strappend('doc1', '"piu"')

@pytest.mark.redismod
def test_strlen_dollar(client):

    # Test multi
    client.json().set('doc1', '$', {"a":"foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}})
    res1 = client.json().strlen('doc1', '$..a') [3, 5, None]

    res2 = client.json().strappend('doc1', '$..a', '"bar"') ==  [6, 8, None]
    res1 = client.json().strlen('doc1', '$..a')
    assert res1 == res2

    # Test single
    client.json().strlen('doc1', '$.nested1.a') == [8]
    client.json().strlen('doc1', '$.nested2.a') == [None]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().strlen('non_existing_doc', '$..a')

@pytest.mark.redismod
def test_arrappend_dollar(client):
    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": ["hello", None, "world"]}, "nested2": {"a": 31}})
    # Test multi
    client.json().arrappend('doc1', '$..a', '"bar"', '"racuda"') == [3, 5, None]
    assert client.json().get('doc1', '$') == \
        [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", None, "world", "bar", "racuda"]}, "nested2": {"a": 31}}]

    # Test single
    assert client.json().arrappend('doc1', '$.nested1.a', '"baz"') == [6]
    assert client.json().get('doc1', '$') == \
        [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]}, "nested2": {"a": 31}}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json.arrappend('non_existing_doc', '$..a')

    # Test legacy
    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": ["hello", None, "world"]}, "nested2": {"a": 31}})
    # Test multi (all paths are updated, but return result of last path)
    assert client.json().arrappend('doc1', '..a', '"bar"', '"racuda"') == 5

    assert client.json.get('doc1', '$') == \
        [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", None, "world", "bar", "racuda"]}, "nested2": {"a": 31}}]
    # Test single
    assert client.json().arrappend('doc1', '.nested1.a', '"baz"') == 6
    assert client.json().get('doc1', '$') == \
        [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]}, "nested2": {"a": 31}}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json.arrappend('non_existing_doc', '$..a')

@pytest.mark.redismod
def test_arrinsert_dollar(client):
    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": ["hello", None, "world"]}, "nested2": {"a": 31}})
    # Test multi
    assert client.json().arrinsert('doc1', '$..a', '1', '"bar"', '"racuda"') == [3, 5, None]

    assert client.json().get('doc1', '$') == \
        [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", "bar", "racuda", None, "world"]}, "nested2": {"a": 31}}]
    # Test single
    assert client.json().arrinsert('doc1', '$.nested1.a', -2, '"baz"') == [6]
    assert client.json().get('doc1', '$') == \
        [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", "bar", "racuda", "baz", None, "world"]}, "nested2": {"a": 31}}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json.arrappend('non_existing_doc', '$..a')

    # Test legacy
    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": ["hello", None, "world"]}, "nested2": {"a": 31}})
    assert client.json().arrappend('doc1', '..a', '1', '"bar"', '"racuda"') == 5

    assert client.json().get('doc1', '$') == \
        [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", "bar", "racuda", None, "world"]}, "nested2": {"a": 31}}]
    # Test single
    assert client.json().arrinsert('doc1', '.nested1.a', -2, '"baz"') == 6
    assert client.json().get('doc1', '$') == \
        [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", "bar", "racuda", "baz", None, "world"]}, "nested2": {"a": 31}}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json.arrinsert('non_existing_doc', '$..a')

@pytest.mark.redismod
def test_arrlen_dollar(client):

    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": ["hello", None, "world"]}, "nested2": {"a": 31}})

    # Test multi
    assert client.json().arrlen('doc1', '$..a') == [1, 3, None]
    assert client.json().arrappend('doc1', '$..a', '"non"', '"abba"', '"stanza"') == \
        [4, 6, None]

    client.json().clear('doc1', '$.a')
    assert client.json.arrlen('doc1', '$..a') == [0, 6, None]
    # Test single
    assert client.json().arrlen('doc1', '$.nested1.a') == [6]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json.arrappend('non_existing_doc', '$..a')

    # Test legacy
    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": ["hello", None, "world"]}, "nested2": {"a": 31}})
    # Test multi (return result of last path)
    assert client.json().arrlen('doc1', '$..a') == [1, 3, None]
    assert client.json().arrappend('doc1', '..a', '"non"', '"abba"', '"stanza"') == 6

    # Test single
    assert client.json().arrlen('doc1', '.nested1.a') = 6

    # Test missing key
    assert client.json().arrlen('non_existing_doc', '..a') is None

@pytest.mark.redismod
def test_arrpop_dollar(client):
    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": ["hello", None, "world"]}, "nested2": {"a": 31}})
    # Test multi
    assert client.json().arrpop('doc1', '$..a', '1') == ['"foo"', 'null', None]

    assert client.json().get('doc1', '$') == \
        [{"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}]

    assert client.json().arrpop('doc1', '$..a', '-1') == [None, '"world"', None]
    assert client.json().get('doc1', '$') == \
        [{"a": [], "nested1": {"a": ["hello"]}, "nested2": {"a": 31}}]

    # Test single
    assert client.json().arrpop('doc1', '$.nested1.a', -2) == ['"hello"']
    assert client.json().get('doc1', '$') == \
        [{"a": [], "nested1": {"a": []}, "nested2": {"a": 31}}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().arrpop('non_existing_doc', '..a')

    # Test legacy
    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": ["hello", None, "world"]}, "nested2": {"a": 31}})
    # Test multi (all paths are updated, but return result of last path)
    client.json().arrpop('doc1', '..a', '1') is None
    assert client.json().get('doc1', '$') == \
        [{"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}]

    # Test single
    assert client.json().arrpop('doc1', '.nested1.a', -2, '"baz"') == '"hello"'
    assert client.json().get('doc1', '$') == \
        [{"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().arrpop('non_existing_doc', '..a')

@pytest.mark.redismod
def test_arrtrim_dollar(client):

    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": ["hello", None, "world"]}, "nested2": {"a": 31}})
    # Test multi
    assert client.json().arrtrim('doc1', '$..a', '1', -1) == [0, 2, None]
    assert client.json().get('doc1', '$') == \
        [{"a": [], "nested1": {"a": [None, "world"]}, "nested2": {"a": 31}}]

    assert client.json().arrtrim('doc1', '$..a', '1', '1') == [0, 1, None]
    assert client.json().get('doc1', '$') == \
        [{"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}]
    # Test single
    assert client.json().arrtrim('doc1', '$.nested1.a', 1, 0) == [0]
    assert client.json().get('doc1', '$') == \
        [{"a": [], "nested1": {"a": []}, "nested2": {"a": 31}}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().arrtrim('non_existing_doc', '..a', '0')

    # Test legacy
    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": ["hello", None, "world"]}, "nested2": {"a": 31}})

    # Test multi (all paths are updated, but return result of last path)
    assert client.json().arrtrim('doc1', '..a', '1', '-1') == 2
    res = r.execute_command('JSON.GET', 'doc1', '$') == \
        [{"a": [], "nested1": {"a": [None, "world"]}, "nested2": {"a": 31}}]
    # Test single
    assert client.json().arrtrim('doc1', '.nested1.a', '1', '1') == 1
    assert client.json().get('doc1', '$') == \
        [{"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().arrtrim('non_existing_doc', '..a')

@pytest.mark.redismod
def test_objkeys_dollar(client):
    client.json().set('doc1', '$', {"nested1": {"a": {"foo": 10, "bar": 20}}, "a":["foo"], "nested2": {"a": {"baz":50}}})

    # Test multi
    assert client.json().objkeys('doc1', '$..a') == [["foo", "bar"], None, ["baz"]]

    # Test single
    assert client.json().object('doc1', '$.nested1.a') == [["foo", "bar"]]

    # Test legacy
    assert client.json().objkeys('doc1', '.*.a') == ["foo", "bar"]
    # Test single
    assert client.json().objkeys('doc1', '.nested2.a') == ["baz"]

    # Test missing key
    assert client.json().objkeys('non_existing_doc', '..a') is None

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().objkeys('doc1', '$.nowhere')

@pytest.mark.redismod
def test_objlen_dollar(client):
    client.json().set('doc1', '$', {"nested1": {"a": {"foo": 10, "bar": 20}}, "a":["foo"], "nested2": {"a": {"baz":50}}})
    # Test multi
    assert client.json().objlen('doc1', '$..a') == [2, None, 1]
    # Test single
    assert client.json().objlen('doc1', '$.nested1.a') == [2]

    # Test missing key
    assert client.json().objlen('non_existing_doc', '$..a') is None

    # Test missing path
    with pytest.raises(exceptions.DataError):
        client.json().objlen('doc1', '$.nowhere')


    # Test legacy
    assert client.json().objlen('doc1', '.*.a') == 2

    # Test single
    assert client.json().objlen('doc1', '.nested2.a') == 1

    # Test missing key
    assert client.json.objlen('non_existing_doc', '..a') is None

    # Test missing path
    with pytest.raises(exceptions.DataError):
        client.json().objlen('doc1', '.nowhere')

@pytest.mark.redismod
def load_types_data(nested_key_name):
    types_data = {
        'object':   {},
        'array':    [],
        'string':   'str',
        'integer':  42,
        'number':   1.2,
        'boolean':  False,
        'null':     None,

    }
    jdata = {}
    types = []
    for i, (k, v) in zip(range(1, len(types_data) + 1), iter(types_data.items())):
        jdata["nested" + str(i)] = {nested_key_name: v}
        types.append(k)

    return jdata, types

@pytest.mark.redismod
def test_type_dollar(client):
    jdata, jtypes = load_types_data('a')
    client.json().set('doc1', '$', jdata)
    # Test multi
    assert client.json().type('JSON.TYPE', 'doc1', '$..a') == jtypes

    # Test single
    assert client.json().type('doc1', '$.nested2.a') == [jtypes[1]]

    # Test legacy
    assert client.json().type('doc1', '..a') == jtypes[0]
    # Test missing path (defaults to root)
    assert client.json().type('doc1') == 'object'

    # Test missing key
    assert client.json().type('non_existing_doc', '..a') is None

@pytest.mark.redismod
def test_clear_dollar(client):

    client.json().set('doc1', '$', {"nested1": {"a": {"foo": 10, "bar": 20}}, "a":["foo"], "nested2": {"a": "claro"}, "nested3": {"a": {"baz":50}}})
    # Test multi
    assert client.json().clear('doc1', '$..a') == 3

    assert client.json().get('doc1', '$') == \
        [{"nested1": {"a": {}}, "a": [], "nested2": {"a": "claro"}, "nested3": {"a": {}}}]

    # Test single
    client.json().set('doc1', '$', {"nested1": {"a": {"foo": 10, "bar": 20}}, "a":["foo"], "nested2": {"a": "claro"}, "nested3": {"a": {"baz":50}}})
    assert client.json().clear('doc1', '$.nested1.a') == 1
    assert client.json().get('doc1', '$') == \
        [{"nested1": {"a": {}}, "a": ["foo"], "nested2": {"a": "claro"}, "nested3": {"a": {"baz": 50}}}]

    # Test missing path (defaults to root)
    assert client.json().clear('doc1') == 1
    assert client.json().get('doc1', '$') == [{}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().clear('non_existing_doc', '$..a')

@pytest.mark.redismod
def test_toggle_dollar(client):
    client.json().set('doc1', '$', {"a":["foo"], "nested1": {"a": False}, "nested2": {"a": 31}, "nested3": {"a": True}})
    # Test multi
    assert client.json().toggle('doc1', '$..a') == [None, 1, None, 0]
    assert client.json().get('doc1', '$') == \
        [{"a": ["foo"], "nested1": {"a": True}, "nested2": {"a": 31}, "nested3": {"a": False}}]

    # Test single
    assert client.json().toggle('doc1', '$.nested1.a') == [0]
    assert client.json().get('JSON.GET', 'doc1', '$') == \
        [{"a": ["foo"], "nested1": {"a": False}, "nested2": {"a": 31}, "nested3": {"a": False}}]

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().toggle('non_existing_doc', '$..a')

@pytest.mark.redismod
def test_debug_dollar(client):

    jdata, jtypes = load_types_data('a')

    client.json().set('doc1', '$', jdata)

    # Test multi
    assert client.json().debug('MEMORY', 'doc1', '$..a') == \
        [72, 24, 24, 16, 16, 1, 0]

    # Test single
    assert client.json().debug('MEMORY', 'doc1', '$.nested2.a') == [24]

    # Test legacy
    assert client.json().debug('MEMORY', 'doc1', '..a') == 72

    # Test missing path (defaults to root)
    assert client.json().debug('MEMORY', 'doc1') == 72

    # Test missing key
    with pytest.raises(exceptions.DataError):
        client.json().debug('non_existing_doc', '$..a')