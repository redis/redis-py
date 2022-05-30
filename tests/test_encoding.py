import pytest

import redis
from redis.commands.packer import SPEEDUPS

from .conftest import _get_client

pytestmark = pytest.mark.onlynoncluster


@pytest.fixture(
    params=[
        (False, "utf-8"),
        (False, "utf-16"),
        pytest.param(
            (True, "utf-8"),
            marks=pytest.mark.skipif(
                not SPEEDUPS, reason="CommandPacker speedups are not installed"
            ),
        ),
        pytest.param(
            (True, "utf-16"),
            marks=pytest.mark.skipif(
                not SPEEDUPS, reason="CommandPacker speedups are not installed"
            ),
        ),
    ],
    ids=["Python-utf-8", "Python-utf-16", "C-utf-8", "C-utf-16"],
)
def create_client(request):
    speedups, encoding = request.param

    def _create_client(**kwargs):
        client = _get_client(
            redis.Redis,
            request,
            db=0,
            encoding=encoding,
            single_connection_client=True,
            **kwargs,
        )
        client.connection.command_packer.speedups = speedups
        return client

    return _create_client


@pytest.fixture()
def r(create_client):
    return create_client()


class TestEncoding:
    @pytest.fixture()
    def r(self, create_client):
        return create_client(decode_responses=True)

    @pytest.fixture()
    def r_no_decode(self, create_client):
        return create_client(decode_responses=False)

    def test_simple_encoding(self, r_no_decode):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        r_no_decode["unicode-string"] = unicode_string.encode("utf-8")
        cached_val = r_no_decode["unicode-string"]
        assert isinstance(cached_val, bytes)
        assert unicode_string == cached_val.decode("utf-8")

    def test_simple_encoding_and_decoding(self, r):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        r["unicode-string"] = unicode_string
        cached_val = r["unicode-string"]
        assert isinstance(cached_val, str)
        assert unicode_string == cached_val

    def test_memoryview_encoding(self, r_no_decode):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        unicode_string_view = memoryview(unicode_string.encode("utf-8"))
        r_no_decode["unicode-string-memoryview"] = unicode_string_view
        cached_val = r_no_decode["unicode-string-memoryview"]
        # The cached value won't be a memoryview because it's a copy from Redis
        assert isinstance(cached_val, bytes)
        assert unicode_string == cached_val.decode("utf-8")

    def test_memoryview_encoding_and_decoding(self, r):
        if r.connection.encoder.encoding == "utf-16":
            pytest.skip()

        unicode_string = chr(3456) + "abcd" + chr(3421)
        unicode_string_view = memoryview(unicode_string.encode("utf-8"))
        r["unicode-string-memoryview"] = unicode_string_view
        cached_val = r["unicode-string-memoryview"]
        assert isinstance(cached_val, str)
        assert unicode_string == cached_val

    def test_list_encoding(self, r):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        result = [unicode_string, unicode_string, unicode_string]
        r.rpush("a", *result)
        assert r.lrange("a", 0, -1) == result


class TestEncodingErrors:
    def test_ignore(self, create_client):
        r = create_client(decode_responses=True, encoding_errors="ignore")
        if r.connection.encoder.encoding == "utf-16":
            pytest.skip()

        r.set("a", b"foo\xff")
        assert r.get("a") == "foo"

    def test_replace(self, create_client):
        r = create_client(decode_responses=True, encoding_errors="replace")
        if r.connection.encoder.encoding == "utf-16":
            pytest.skip()

        r.set("a", b"foo\xff")
        assert r.get("a") == "foo\ufffd"


class TestMemoryviewsAreNotPacked:
    def test_memoryviews_are_not_packed(self, r):
        arg = memoryview(b"some_arg")
        arg_list = ("SOME_COMMAND", arg)
        c = r.connection
        cmd = c.pack_command(*arg_list)
        assert cmd[1] is arg
        cmds = c.pack_commands(arg for arg in [arg_list, arg_list])
        assert cmds[1] is arg
        assert cmds[3] is arg


class TestCommandsAreNotEncoded:
    def test_basic_command(self, r):
        r.set("hello", "world")


class TestInvalidUserInput:
    def test_boolean_fails(self, r):
        with pytest.raises(redis.DataError):
            r.set("a", True)

    def test_none_fails(self, r):
        with pytest.raises(redis.DataError):
            r.set("a", None)

    def test_user_type_fails(self, r):
        class Foo:
            def __str__(self):
                return "Foo"

        with pytest.raises(redis.DataError):
            r.set("a", Foo())
