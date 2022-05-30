import sys

import pytest

if sys.version_info[0:2] == (3, 6):
    import pytest as pytest_asyncio
else:
    import pytest_asyncio

import redis.asyncio as redis
from redis.commands.packer import SPEEDUPS
from redis.exceptions import DataError

pytestmark = [pytest.mark.asyncio, pytest.mark.onlynoncluster]


@pytest_asyncio.fixture(
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
def create_client(request, create_redis):
    speedups, encoding = request.param

    async def _create_client(**kwargs):
        client = await create_redis(
            db=0, encoding=encoding, single_connection_client=True, **kwargs
        )
        client.connection.command_packer.speedups = speedups
        return client

    return _create_client


@pytest_asyncio.fixture()
async def r(create_client):
    return await create_client()


class TestEncoding:
    @pytest_asyncio.fixture()
    async def r(self, create_client):
        return await create_client(decode_responses=True)

    @pytest_asyncio.fixture()
    async def r_no_decode(self, create_client):
        return await create_client(decode_responses=False)

    async def test_simple_encoding(self, r_no_decode: redis.Redis):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        await r_no_decode.set("unicode-string", unicode_string.encode("utf-8"))
        cached_val = await r_no_decode.get("unicode-string")
        assert isinstance(cached_val, bytes)
        assert unicode_string == cached_val.decode("utf-8")

    async def test_simple_encoding_and_decoding(self, r: redis.Redis):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        await r.set("unicode-string", unicode_string)
        cached_val = await r.get("unicode-string")
        assert isinstance(cached_val, str)
        assert unicode_string == cached_val

    async def test_memoryview_encoding(self, r_no_decode: redis.Redis):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        unicode_string_view = memoryview(unicode_string.encode("utf-8"))
        await r_no_decode.set("unicode-string-memoryview", unicode_string_view)
        cached_val = await r_no_decode.get("unicode-string-memoryview")
        # The cached value won't be a memoryview because it's a copy from Redis
        assert isinstance(cached_val, bytes)
        assert unicode_string == cached_val.decode("utf-8")

    async def test_memoryview_encoding_and_decoding(self, r: redis.Redis):
        if r.connection.encoder.encoding == "utf-16":
            pytest.skip()

        unicode_string = chr(3456) + "abcd" + chr(3421)
        unicode_string_view = memoryview(unicode_string.encode("utf-8"))
        await r.set("unicode-string-memoryview", unicode_string_view)
        cached_val = await r.get("unicode-string-memoryview")
        assert isinstance(cached_val, str)
        assert unicode_string == cached_val

    async def test_list_encoding(self, r: redis.Redis):
        unicode_string = chr(3456) + "abcd" + chr(3421)
        result = [unicode_string, unicode_string, unicode_string]
        await r.rpush("a", *result)
        assert await r.lrange("a", 0, -1) == result


class TestEncodingErrors:
    async def test_ignore(self, create_client):
        r = await create_client(decode_responses=True, encoding_errors="ignore")
        if r.connection.encoder.encoding == "utf-16":
            pytest.skip()

        await r.set("a", b"foo\xff")
        assert await r.get("a") == "foo"

    async def test_replace(self, create_client):
        r = await create_client(decode_responses=True, encoding_errors="replace")
        if r.connection.encoder.encoding == "utf-16":
            pytest.skip()

        await r.set("a", b"foo\xff")
        assert await r.get("a") == "foo\ufffd"


class TestMemoryviewsAreNotPacked:
    async def test_memoryviews_are_not_packed(self, r):
        arg = memoryview(b"some_arg")
        arg_list = ("SOME_COMMAND", arg)
        c = r.connection
        cmd = c.pack_command(*arg_list)
        assert cmd[1] is arg
        cmds = c.pack_commands(arg for arg in [arg_list, arg_list])
        assert cmds[1] is arg
        assert cmds[3] is arg


class TestCommandsAreNotEncoded:
    async def test_basic_command(self, r):
        await r.set("hello", "world")


class TestInvalidUserInput:
    async def test_boolean_fails(self, r: redis.Redis):
        with pytest.raises(DataError):
            await r.set("a", True)

    async def test_none_fails(self, r: redis.Redis):
        with pytest.raises(DataError):
            await r.set("a", None)  # type: ignore

    async def test_user_type_fails(self, r: redis.Redis):
        class Foo:
            def __str__(self):
                return "Foo"

        with pytest.raises(DataError):
            await r.set("a", Foo())  # type: ignore
