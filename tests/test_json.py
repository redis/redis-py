import pytest
import redis
from redis.commands.json.path import Path
from .conftest import skip_ifmodversion_lt


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
