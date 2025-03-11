import pytest
import redis
from tests.conftest import skip_if_server_version_lt

from .compat import aclosing, mock
from .conftest import wait_for_command


class TestPipeline:
    async def test_pipeline_is_true(self, r):
        """Ensure pipeline instances are not false-y"""
        async with r.pipeline() as pipe:
            assert pipe

    async def test_pipeline(self, r):
        async with r.pipeline() as pipe:
            (
                pipe.set("a", "a1")
                .get("a")
                .zadd("z", {"z1": 1})
                .zadd("z", {"z2": 4})
                .zincrby("z", 1, "z1")
            )
            assert await pipe.execute() == [
                True,
                b"a1",
                True,
                True,
                2.0,
            ]

    async def test_pipeline_memoryview(self, r):
        async with r.pipeline() as pipe:
            (pipe.set("a", memoryview(b"a1")).get("a"))
            assert await pipe.execute() == [True, b"a1"]

    async def test_pipeline_length(self, r):
        async with r.pipeline() as pipe:
            # Initially empty.
            assert len(pipe) == 0

            # Fill 'er up!
            pipe.set("a", "a1").set("b", "b1").set("c", "c1")
            assert len(pipe) == 3

            # Execute calls reset(), so empty once again.
            await pipe.execute()
            assert len(pipe) == 0

    async def test_pipeline_no_transaction(self, r):
        async with r.pipeline(transaction=False) as pipe:
            pipe.set("a", "a1").set("b", "b1").set("c", "c1")
            assert await pipe.execute() == [True, True, True]
            assert await r.get("a") == b"a1"
            assert await r.get("b") == b"b1"
            assert await r.get("c") == b"c1"

    @pytest.mark.onlynoncluster
    async def test_pipeline_no_transaction_watch(self, r):
        await r.set("a", 0)

        async with r.pipeline(transaction=False) as pipe:
            await pipe.watch("a")
            a = await pipe.get("a")

            pipe.multi()
            pipe.set("a", int(a) + 1)
            assert await pipe.execute() == [True]

    @pytest.mark.onlynoncluster
    async def test_pipeline_no_transaction_watch_failure(self, r):
        await r.set("a", 0)

        async with r.pipeline(transaction=False) as pipe:
            await pipe.watch("a")
            a = await pipe.get("a")

            await r.set("a", "bad")

            pipe.multi()
            pipe.set("a", int(a) + 1)

            with pytest.raises(redis.WatchError):
                await pipe.execute()

            assert await r.get("a") == b"bad"

    async def test_exec_error_in_response(self, r):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        await r.set("c", "a")
        async with r.pipeline() as pipe:
            pipe.set("a", 1).set("b", 2).lpush("c", 3).set("d", 4)
            result = await pipe.execute(raise_on_error=False)

            assert result[0]
            assert await r.get("a") == b"1"
            assert result[1]
            assert await r.get("b") == b"2"

            # we can't lpush to a key that's a string value, so this should
            # be a ResponseError exception
            assert isinstance(result[2], redis.ResponseError)
            assert await r.get("c") == b"a"

            # since this isn't a transaction, the other commands after the
            # error are still executed
            assert result[3]
            assert await r.get("d") == b"4"

            # make sure the pipe was restored to a working state
            assert await pipe.set("z", "zzz").execute() == [True]
            assert await r.get("z") == b"zzz"

    async def test_exec_error_raised(self, r):
        await r.set("c", "a")
        async with r.pipeline() as pipe:
            pipe.set("a", 1).set("b", 2).lpush("c", 3).set("d", 4)
            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()
            assert str(ex.value).startswith(
                "Command # 3 (LPUSH c 3) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert await pipe.set("z", "zzz").execute() == [True]
            assert await r.get("z") == b"zzz"

    @pytest.mark.onlynoncluster
    async def test_transaction_with_empty_error_command(self, r):
        """
        Commands with custom EMPTY_ERROR functionality return their default
        values in the pipeline no matter the raise_on_error preference
        """
        for error_switch in (True, False):
            async with r.pipeline() as pipe:
                pipe.set("a", 1).mget([]).set("c", 3)
                result = await pipe.execute(raise_on_error=error_switch)

                assert result[0]
                assert result[1] == []
                assert result[2]

    @pytest.mark.onlynoncluster
    async def test_pipeline_with_empty_error_command(self, r):
        """
        Commands with custom EMPTY_ERROR functionality return their default
        values in the pipeline no matter the raise_on_error preference
        """
        for error_switch in (True, False):
            async with r.pipeline(transaction=False) as pipe:
                pipe.set("a", 1).mget([]).set("c", 3)
                result = await pipe.execute(raise_on_error=error_switch)

                assert result[0]
                assert result[1] == []
                assert result[2]

    async def test_parse_error_raised(self, r):
        async with r.pipeline() as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set("a", 1).zrem("b").set("b", 2)
            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()

            assert str(ex.value).startswith(
                "Command # 2 (ZREM b) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert await pipe.set("z", "zzz").execute() == [True]
            assert await r.get("z") == b"zzz"

    @pytest.mark.onlynoncluster
    async def test_parse_error_raised_transaction(self, r):
        async with r.pipeline() as pipe:
            pipe.multi()
            # the zrem is invalid because we don't pass any keys to it
            pipe.set("a", 1).zrem("b").set("b", 2)
            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()

            assert str(ex.value).startswith(
                "Command # 2 (ZREM b) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert await pipe.set("z", "zzz").execute() == [True]
            assert await r.get("z") == b"zzz"

    @pytest.mark.onlynoncluster
    async def test_watch_succeed(self, r):
        await r.set("a", 1)
        await r.set("b", 2)

        async with r.pipeline() as pipe:
            await pipe.watch("a", "b")
            assert pipe.watching
            a_value = await pipe.get("a")
            b_value = await pipe.get("b")
            assert a_value == b"1"
            assert b_value == b"2"
            pipe.multi()

            pipe.set("c", 3)
            assert await pipe.execute() == [True]
            assert not pipe.watching

    @pytest.mark.onlynoncluster
    async def test_watch_failure(self, r):
        await r.set("a", 1)
        await r.set("b", 2)

        async with r.pipeline() as pipe:
            await pipe.watch("a", "b")
            await r.set("b", 3)
            pipe.multi()
            pipe.get("a")
            with pytest.raises(redis.WatchError):
                await pipe.execute()

            assert not pipe.watching

    @pytest.mark.onlynoncluster
    async def test_watch_failure_in_empty_transaction(self, r):
        await r.set("a", 1)
        await r.set("b", 2)

        async with r.pipeline() as pipe:
            await pipe.watch("a", "b")
            await r.set("b", 3)
            pipe.multi()
            with pytest.raises(redis.WatchError):
                await pipe.execute()

            assert not pipe.watching

    @pytest.mark.onlynoncluster
    async def test_unwatch(self, r):
        await r.set("a", 1)
        await r.set("b", 2)

        async with r.pipeline() as pipe:
            await pipe.watch("a", "b")
            await r.set("b", 3)
            await pipe.unwatch()
            assert not pipe.watching
            pipe.get("a")
            assert await pipe.execute() == [b"1"]

    @pytest.mark.onlynoncluster
    async def test_watch_exec_no_unwatch(self, r):
        await r.set("a", 1)
        await r.set("b", 2)

        async with r.monitor() as m:
            async with r.pipeline() as pipe:
                await pipe.watch("a", "b")
                assert pipe.watching
                a_value = await pipe.get("a")
                b_value = await pipe.get("b")
                assert a_value == b"1"
                assert b_value == b"2"
                pipe.multi()
                pipe.set("c", 3)
                assert await pipe.execute() == [True]
                assert not pipe.watching

            unwatch_command = await wait_for_command(r, m, "UNWATCH")
            assert unwatch_command is None, "should not send UNWATCH"

    @pytest.mark.onlynoncluster
    async def test_watch_reset_unwatch(self, r):
        await r.set("a", 1)

        async with r.monitor() as m:
            async with r.pipeline() as pipe:
                await pipe.watch("a")
                assert pipe.watching
                await pipe.reset()
                assert not pipe.watching

            unwatch_command = await wait_for_command(r, m, "UNWATCH")
            assert unwatch_command is not None
            assert unwatch_command["command"] == "UNWATCH"

    @pytest.mark.onlynoncluster
    async def test_aclose_is_reset(self, r):
        async with r.pipeline() as pipe:
            called = 0

            async def mock_reset():
                nonlocal called
                called += 1

            with mock.patch.object(pipe, "reset", mock_reset):
                await pipe.aclose()
                assert called == 1

    @pytest.mark.onlynoncluster
    async def test_aclosing(self, r):
        async with aclosing(r.pipeline()):
            pass

    @pytest.mark.onlynoncluster
    async def test_transaction_callable(self, r):
        await r.set("a", 1)
        await r.set("b", 2)
        has_run = []

        async def my_transaction(pipe):
            a_value = await pipe.get("a")
            assert a_value in (b"1", b"2")
            b_value = await pipe.get("b")
            assert b_value == b"2"

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                await r.incr("a")
                has_run.append("it has")

            pipe.multi()
            pipe.set("c", int(a_value) + int(b_value))

        result = await r.transaction(my_transaction, "a", "b")
        assert result == [True]
        assert await r.get("c") == b"4"

    @pytest.mark.onlynoncluster
    async def test_transaction_callable_returns_value_from_callable(self, r):
        async def callback(pipe):
            # No need to do anything here since we only want the return value
            return "a"

        res = await r.transaction(callback, "my-key", value_from_callable=True)
        assert res == "a"

    async def test_exec_error_in_no_transaction_pipeline(self, r):
        await r.set("a", 1)
        async with r.pipeline(transaction=False) as pipe:
            pipe.llen("a")
            pipe.expire("a", 100)

            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()

            assert str(ex.value).startswith(
                "Command # 1 (LLEN a) of pipeline caused error: "
            )

        assert await r.get("a") == b"1"

    async def test_exec_error_in_no_transaction_pipeline_unicode_command(self, r):
        key = chr(3456) + "abcd" + chr(3421)
        await r.set(key, 1)
        async with r.pipeline(transaction=False) as pipe:
            pipe.llen(key)
            pipe.expire(key, 100)

            with pytest.raises(redis.ResponseError) as ex:
                await pipe.execute()

            expected = f"Command # 1 (LLEN {key}) of pipeline caused error: "
            assert str(ex.value).startswith(expected)

        assert await r.get(key) == b"1"

    async def test_pipeline_with_bitfield(self, r):
        async with r.pipeline() as pipe:
            pipe.set("a", "1")
            bf = pipe.bitfield("b")
            pipe2 = (
                bf.set("u8", 8, 255)
                .get("u8", 0)
                .get("u4", 8)  # 1111
                .get("u4", 12)  # 1111
                .get("u4", 13)  # 1110
                .execute()
            )
            pipe.get("a")
            response = await pipe.execute()

            assert pipe == pipe2
            assert response == [True, [0, 0, 15, 15, 14], b"1"]

    async def test_pipeline_get(self, r):
        await r.set("a", "a1")
        async with r.pipeline() as pipe:
            pipe.get("a")
            assert await pipe.execute() == [b"a1"]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.0.0")
    async def test_pipeline_discard(self, r):
        # empty pipeline should raise an error
        async with r.pipeline() as pipe:
            pipe.set("key", "someval")
            await pipe.discard()
            with pytest.raises(redis.exceptions.ResponseError):
                await pipe.execute()

        # setting a pipeline and discarding should do the same
        async with r.pipeline() as pipe:
            pipe.set("key", "someval")
            pipe.set("someotherkey", "val")
            response = await pipe.execute()
            pipe.set("key", "another value!")
            await pipe.discard()
            pipe.set("key", "another vae!")
            with pytest.raises(redis.exceptions.ResponseError):
                await pipe.execute()

            pipe.set("foo", "bar")
            response = await pipe.execute()
        assert response[0]
        assert await r.get("foo") == b"bar"

    @pytest.mark.onlynoncluster
    async def test_send_set_commands_over_async_pipeline(self, r: redis.asyncio.Redis):
        pipe = r.pipeline()
        pipe.hset("hash:1", "foo", "bar")
        pipe.hset("hash:1", "bar", "foo")
        pipe.hset("hash:1", "baz", "bar")
        pipe.hgetall("hash:1")
        resp = await pipe.execute()
        assert resp == [1, 1, 1, {b"bar": b"foo", b"baz": b"bar", b"foo": b"bar"}]
