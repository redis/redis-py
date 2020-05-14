from __future__ import unicode_literals
import pytest

import redis
from redis._compat import unichr, unicode
from .conftest import wait_for_command


class TestPipeline(object):
    def test_pipeline_is_true(self, r):
        "Ensure pipeline instances are not false-y"
        with r.pipeline() as pipe:
            assert pipe

    def test_pipeline(self, r):
        with r.pipeline() as pipe:
            (pipe.set('a', 'a1')
                 .get('a')
                 .zadd('z', {'z1': 1})
                 .zadd('z', {'z2': 4})
                 .zincrby('z', 1, 'z1')
                 .zrange('z', 0, 5, withscores=True))
            assert pipe.execute() == \
                [
                    True,
                    b'a1',
                    True,
                    True,
                    2.0,
                    [(b'z1', 2.0), (b'z2', 4)],
                ]

    def test_pipeline_memoryview(self, r):
        with r.pipeline() as pipe:
            (pipe.set('a', memoryview(b'a1'))
                 .get('a'))
            assert pipe.execute() == \
                [
                    True,
                    b'a1',
                ]

    def test_pipeline_length(self, r):
        with r.pipeline() as pipe:
            # Initially empty.
            assert len(pipe) == 0

            # Fill 'er up!
            pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
            assert len(pipe) == 3

            # Execute calls reset(), so empty once again.
            pipe.execute()
            assert len(pipe) == 0

    def test_pipeline_no_transaction(self, r):
        with r.pipeline(transaction=False) as pipe:
            pipe.set('a', 'a1').set('b', 'b1').set('c', 'c1')
            assert pipe.execute() == [True, True, True]
            assert r['a'] == b'a1'
            assert r['b'] == b'b1'
            assert r['c'] == b'c1'

    def test_pipeline_no_transaction_watch(self, r):
        r['a'] = 0

        with r.pipeline(transaction=False) as pipe:
            pipe.watch('a')
            a = pipe.get('a')

            pipe.multi()
            pipe.set('a', int(a) + 1)
            assert pipe.execute() == [True]

    def test_pipeline_no_transaction_watch_failure(self, r):
        r['a'] = 0

        with r.pipeline(transaction=False) as pipe:
            pipe.watch('a')
            a = pipe.get('a')

            r['a'] = 'bad'

            pipe.multi()
            pipe.set('a', int(a) + 1)

            with pytest.raises(redis.WatchError):
                pipe.execute()

            assert r['a'] == b'bad'

    def test_exec_error_in_response(self, r):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        r['c'] = 'a'
        with r.pipeline() as pipe:
            pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4)
            result = pipe.execute(raise_on_error=False)

            assert result[0]
            assert r['a'] == b'1'
            assert result[1]
            assert r['b'] == b'2'

            # we can't lpush to a key that's a string value, so this should
            # be a ResponseError exception
            assert isinstance(result[2], redis.ResponseError)
            assert r['c'] == b'a'

            # since this isn't a transaction, the other commands after the
            # error are still executed
            assert result[3]
            assert r['d'] == b'4'

            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]
            assert r['z'] == b'zzz'

    def test_exec_error_raised(self, r):
        r['c'] = 'a'
        with r.pipeline() as pipe:
            pipe.set('a', 1).set('b', 2).lpush('c', 3).set('d', 4)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()
            assert unicode(ex.value).startswith('Command # 3 (LPUSH c 3) of '
                                                'pipeline caused error: ')

            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]
            assert r['z'] == b'zzz'

    def test_transaction_with_empty_error_command(self, r):
        """
        Commands with custom EMPTY_ERROR functionality return their default
        values in the pipeline no matter the raise_on_error preference
        """
        for error_switch in (True, False):
            with r.pipeline() as pipe:
                pipe.set('a', 1).mget([]).set('c', 3)
                result = pipe.execute(raise_on_error=error_switch)

                assert result[0]
                assert result[1] == []
                assert result[2]

    def test_pipeline_with_empty_error_command(self, r):
        """
        Commands with custom EMPTY_ERROR functionality return their default
        values in the pipeline no matter the raise_on_error preference
        """
        for error_switch in (True, False):
            with r.pipeline(transaction=False) as pipe:
                pipe.set('a', 1).mget([]).set('c', 3)
                result = pipe.execute(raise_on_error=error_switch)

                assert result[0]
                assert result[1] == []
                assert result[2]

    def test_parse_error_raised(self, r):
        with r.pipeline() as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set('a', 1).zrem('b').set('b', 2)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert unicode(ex.value).startswith('Command # 2 (ZREM b) of '
                                                'pipeline caused error: ')

            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]
            assert r['z'] == b'zzz'

    def test_parse_error_raised_transaction(self, r):
        with r.pipeline() as pipe:
            pipe.multi()
            # the zrem is invalid because we don't pass any keys to it
            pipe.set('a', 1).zrem('b').set('b', 2)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert unicode(ex.value).startswith('Command # 2 (ZREM b) of '
                                                'pipeline caused error: ')

            # make sure the pipe was restored to a working state
            assert pipe.set('z', 'zzz').execute() == [True]
            assert r['z'] == b'zzz'

    def test_watch_succeed(self, r):
        r['a'] = 1
        r['b'] = 2

        with r.pipeline() as pipe:
            pipe.watch('a', 'b')
            assert pipe.watching
            a_value = pipe.get('a')
            b_value = pipe.get('b')
            assert a_value == b'1'
            assert b_value == b'2'
            pipe.multi()

            pipe.set('c', 3)
            assert pipe.execute() == [True]
            assert not pipe.watching

    def test_watch_failure(self, r):
        r['a'] = 1
        r['b'] = 2

        with r.pipeline() as pipe:
            pipe.watch('a', 'b')
            r['b'] = 3
            pipe.multi()
            pipe.get('a')
            with pytest.raises(redis.WatchError):
                pipe.execute()

            assert not pipe.watching

    def test_watch_failure_in_empty_transaction(self, r):
        r['a'] = 1
        r['b'] = 2

        with r.pipeline() as pipe:
            pipe.watch('a', 'b')
            r['b'] = 3
            pipe.multi()
            with pytest.raises(redis.WatchError):
                pipe.execute()

            assert not pipe.watching

    def test_unwatch(self, r):
        r['a'] = 1
        r['b'] = 2

        with r.pipeline() as pipe:
            pipe.watch('a', 'b')
            r['b'] = 3
            pipe.unwatch()
            assert not pipe.watching
            pipe.get('a')
            assert pipe.execute() == [b'1']

    def test_watch_exec_no_unwatch(self, r):
        r['a'] = 1
        r['b'] = 2

        with r.monitor() as m:
            with r.pipeline() as pipe:
                pipe.watch('a', 'b')
                assert pipe.watching
                a_value = pipe.get('a')
                b_value = pipe.get('b')
                assert a_value == b'1'
                assert b_value == b'2'
                pipe.multi()
                pipe.set('c', 3)
                assert pipe.execute() == [True]
                assert not pipe.watching

            unwatch_command = wait_for_command(r, m, 'UNWATCH')
            assert unwatch_command is None, "should not send UNWATCH"

    def test_watch_reset_unwatch(self, r):
        r['a'] = 1

        with r.monitor() as m:
            with r.pipeline() as pipe:
                pipe.watch('a')
                assert pipe.watching
                pipe.reset()
                assert not pipe.watching

            unwatch_command = wait_for_command(r, m, 'UNWATCH')
            assert unwatch_command is not None
            assert unwatch_command['command'] == 'UNWATCH'

    def test_transaction_callable(self, r):
        r['a'] = 1
        r['b'] = 2
        has_run = []

        def my_transaction(pipe):
            a_value = pipe.get('a')
            assert a_value in (b'1', b'2')
            b_value = pipe.get('b')
            assert b_value == b'2'

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                r.incr('a')
                has_run.append('it has')

            pipe.multi()
            pipe.set('c', int(a_value) + int(b_value))

        result = r.transaction(my_transaction, 'a', 'b')
        assert result == [True]
        assert r['c'] == b'4'

    def test_transaction_callable_returns_value_from_callable(self, r):
        def callback(pipe):
            # No need to do anything here since we only want the return value
            return 'a'

        res = r.transaction(callback, 'my-key', value_from_callable=True)
        assert res == 'a'

    def test_exec_error_in_no_transaction_pipeline(self, r):
        r['a'] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.llen('a')
            pipe.expire('a', 100)

            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert unicode(ex.value).startswith('Command # 1 (LLEN a) of '
                                                'pipeline caused error: ')

        assert r['a'] == b'1'

    def test_exec_error_in_no_transaction_pipeline_unicode_command(self, r):
        key = unichr(3456) + 'abcd' + unichr(3421)
        r[key] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.llen(key)
            pipe.expire(key, 100)

            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            expected = unicode('Command # 1 (LLEN %s) of pipeline caused '
                               'error: ') % key
            assert unicode(ex.value).startswith(expected)

        assert r[key] == b'1'

    def test_pipeline_with_bitfield(self, r):
        with r.pipeline() as pipe:
            pipe.set('a', '1')
            bf = pipe.bitfield('b')
            pipe2 = (bf
                     .set('u8', 8, 255)
                     .get('u8', 0)
                     .get('u4', 8)  # 1111
                     .get('u4', 12)  # 1111
                     .get('u4', 13)  # 1110
                     .execute())
            pipe.get('a')
            response = pipe.execute()

            assert pipe == pipe2
            assert response == [True, [0, 0, 15, 15, 14], b'1']
