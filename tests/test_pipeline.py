from contextlib import closing
from unittest import mock

import pytest
from redis import RedisClusterException
import redis
from redis.client import Pipeline
from redis.event import EventDispatcher, EventListenerInterface, OnErrorEvent
from redis.observability import recorder
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector

from .conftest import skip_if_server_version_lt, wait_for_command


class TestPipeline:
    def test_pipeline_is_true(self, r):
        "Ensure pipeline instances are not false-y"
        with r.pipeline() as pipe:
            assert pipe

    def test_pipeline(self, r):
        with r.pipeline() as pipe:
            (
                pipe.set("a", "a1")
                .get("a")
                .zadd("z", {"z1": 1})
                .zadd("z", {"z2": 4})
                .zincrby("z", 1, "z1")
            )
            assert pipe.execute() == [
                True,
                b"a1",
                True,
                True,
                2.0,
            ]

    def test_pipeline_memoryview(self, r):
        with r.pipeline() as pipe:
            (pipe.set("a", memoryview(b"a1")).get("a"))
            assert pipe.execute() == [True, b"a1"]

    def test_pipeline_length(self, r):
        with r.pipeline() as pipe:
            # Initially empty.
            assert len(pipe) == 0

            # Fill 'er up!
            pipe.set("a", "a1").set("b", "b1").set("c", "c1")
            assert len(pipe) == 3

            # Execute calls reset(), so empty once again.
            pipe.execute()
            assert len(pipe) == 0

    def test_pipeline_no_transaction(self, r):
        with r.pipeline(transaction=False) as pipe:
            pipe.set("a", "a1").set("b", "b1").set("c", "c1")
            assert pipe.execute() == [True, True, True]
            assert r["a"] == b"a1"
            assert r["b"] == b"b1"
            assert r["c"] == b"c1"

    @pytest.mark.onlynoncluster
    def test_pipeline_no_transaction_watch(self, r):
        r["a"] = 0

        with r.pipeline(transaction=False) as pipe:
            pipe.watch("a")
            a = pipe.get("a")

            pipe.multi()
            pipe.set("a", int(a) + 1)
            assert pipe.execute() == [True]

    @pytest.mark.onlynoncluster
    def test_pipeline_no_transaction_watch_failure(self, r):
        r["a"] = 0

        with r.pipeline(transaction=False) as pipe:
            pipe.watch("a")
            a = pipe.get("a")

            r["a"] = "bad"

            pipe.multi()
            pipe.set("a", int(a) + 1)

            with pytest.raises(redis.WatchError):
                pipe.execute()

            assert r["a"] == b"bad"

    def test_exec_error_in_response(self, r):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        r["c"] = "a"
        with r.pipeline() as pipe:
            pipe.set("a", 1).set("b", 2).lpush("c", 3).set("d", 4)
            result = pipe.execute(raise_on_error=False)

            assert result[0]
            assert r["a"] == b"1"
            assert result[1]
            assert r["b"] == b"2"

            # we can't lpush to a key that's a string value, so this should
            # be a ResponseError exception
            assert isinstance(result[2], redis.ResponseError)
            assert r["c"] == b"a"

            # since this isn't a transaction, the other commands after the
            # error are still executed
            assert result[3]
            assert r["d"] == b"4"

            # make sure the pipe was restored to a working state
            assert pipe.set("z", "zzz").execute() == [True]
            assert r["z"] == b"zzz"

    def test_exec_error_raised(self, r):
        r["c"] = "a"
        with r.pipeline() as pipe:
            pipe.set("a", 1).set("b", 2).lpush("c", 3).set("d", 4)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()
            assert str(ex.value).startswith(
                "Command # 3 (LPUSH c 3) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert pipe.set("z", "zzz").execute() == [True]
            assert r["z"] == b"zzz"

    @pytest.mark.onlynoncluster
    def test_transaction_with_empty_error_command(self, r):
        """
        Commands with custom EMPTY_ERROR functionality return their default
        values in the pipeline no matter the raise_on_error preference
        """
        for error_switch in (True, False):
            with r.pipeline() as pipe:
                pipe.set("a", 1).mget([]).set("c", 3)
                result = pipe.execute(raise_on_error=error_switch)

                assert result[0]
                assert result[1] == []
                assert result[2]

    @pytest.mark.onlynoncluster
    def test_pipeline_with_empty_error_command(self, r):
        """
        Commands with custom EMPTY_ERROR functionality return their default
        values in the pipeline no matter the raise_on_error preference
        """
        for error_switch in (True, False):
            with r.pipeline(transaction=False) as pipe:
                pipe.set("a", 1).mget([]).set("c", 3)
                result = pipe.execute(raise_on_error=error_switch)

                assert result[0]
                assert result[1] == []
                assert result[2]

    def test_parse_error_raised(self, r):
        with r.pipeline() as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set("a", 1).zrem("b").set("b", 2)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert str(ex.value).startswith(
                "Command # 2 (ZREM b) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert pipe.set("z", "zzz").execute() == [True]
            assert r["z"] == b"zzz"

    @pytest.mark.onlynoncluster
    def test_parse_error_raised_transaction(self, r):
        with r.pipeline() as pipe:
            pipe.multi()
            # the zrem is invalid because we don't pass any keys to it
            pipe.set("a", 1).zrem("b").set("b", 2)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert str(ex.value).startswith(
                "Command # 2 (ZREM b) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert pipe.set("z", "zzz").execute() == [True]
            assert r["z"] == b"zzz"

    @pytest.mark.onlynoncluster
    def test_watch_succeed(self, r):
        r["a"] = 1
        r["b"] = 2

        with r.pipeline() as pipe:
            pipe.watch("a", "b")
            assert pipe.watching
            a_value = pipe.get("a")
            b_value = pipe.get("b")
            assert a_value == b"1"
            assert b_value == b"2"
            pipe.multi()

            pipe.set("c", 3)
            assert pipe.execute() == [True]
            assert not pipe.watching

    @pytest.mark.onlynoncluster
    def test_watch_failure(self, r):
        r["a"] = 1
        r["b"] = 2

        with r.pipeline() as pipe:
            pipe.watch("a", "b")
            r["b"] = 3
            pipe.multi()
            pipe.get("a")
            with pytest.raises(redis.WatchError):
                pipe.execute()

            assert not pipe.watching

    @pytest.mark.onlynoncluster
    def test_watch_failure_in_empty_transaction(self, r):
        r["a"] = 1
        r["b"] = 2

        with r.pipeline() as pipe:
            pipe.watch("a", "b")
            r["b"] = 3
            pipe.multi()
            with pytest.raises(redis.WatchError):
                pipe.execute()

            assert not pipe.watching

    @pytest.mark.onlynoncluster
    def test_unwatch(self, r):
        r["a"] = 1
        r["b"] = 2

        with r.pipeline() as pipe:
            pipe.watch("a", "b")
            r["b"] = 3
            pipe.unwatch()
            assert not pipe.watching
            pipe.get("a")
            assert pipe.execute() == [b"1"]

    @pytest.mark.onlynoncluster
    def test_watch_exec_no_unwatch(self, r):
        r["a"] = 1
        r["b"] = 2

        with r.monitor() as m:
            with r.pipeline() as pipe:
                pipe.watch("a", "b")
                assert pipe.watching
                a_value = pipe.get("a")
                b_value = pipe.get("b")
                assert a_value == b"1"
                assert b_value == b"2"
                pipe.multi()
                pipe.set("c", 3)
                assert pipe.execute() == [True]
                assert not pipe.watching

            unwatch_command = wait_for_command(r, m, "UNWATCH")
            assert unwatch_command is None, "should not send UNWATCH"

    @pytest.mark.onlynoncluster
    def test_watch_reset_unwatch(self, r):
        r["a"] = 1

        with r.monitor() as m:
            with r.pipeline() as pipe:
                pipe.watch("a")
                assert pipe.watching
                pipe.reset()
                assert not pipe.watching

            unwatch_command = wait_for_command(r, m, "UNWATCH")
            assert unwatch_command is not None
            assert unwatch_command["command"] == "UNWATCH"

    @pytest.mark.onlynoncluster
    def test_close_is_reset(self, r):
        with r.pipeline() as pipe:
            called = 0

            def mock_reset():
                nonlocal called
                called += 1

            with mock.patch.object(pipe, "reset", mock_reset):
                pipe.close()
                assert called == 1

    @pytest.mark.onlynoncluster
    def test_closing(self, r):
        with closing(r.pipeline()):
            pass

    @pytest.mark.onlynoncluster
    def test_transaction_callable(self, r):
        r["a"] = 1
        r["b"] = 2
        has_run = []

        def my_transaction(pipe):
            a_value = pipe.get("a")
            assert a_value in (b"1", b"2")
            b_value = pipe.get("b")
            assert b_value == b"2"

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                r.incr("a")
                has_run.append("it has")

            pipe.multi()
            pipe.set("c", int(a_value) + int(b_value))

        result = r.transaction(my_transaction, "a", "b")
        assert result == [True]
        assert r["c"] == b"4"

    @pytest.mark.onlynoncluster
    def test_transaction_callable_returns_value_from_callable(self, r):
        def callback(pipe):
            # No need to do anything here since we only want the return value
            return "a"

        res = r.transaction(callback, "my-key", value_from_callable=True)
        assert res == "a"

    def test_exec_error_in_no_transaction_pipeline(self, r):
        r["a"] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.llen("a")
            pipe.expire("a", 100)

            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert str(ex.value).startswith(
                "Command # 1 (LLEN a) of pipeline caused error: "
            )

        assert r["a"] == b"1"

    def test_exec_error_in_no_transaction_pipeline_unicode_command(self, r):
        key = chr(3456) + "abcd" + chr(3421)
        r[key] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.llen(key)
            pipe.expire(key, 100)

            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            expected = f"Command # 1 (LLEN {key}) of pipeline caused error: "
            assert str(ex.value).startswith(expected)

        assert r[key] == b"1"

    def test_exec_error_in_pipeline_truncated(self, r):
        key = "a" * 50
        a_value = "a" * 20
        b_value = "b" * 20

        r[key] = 1
        with r.pipeline(transaction=False) as pipe:
            pipe.hset(key, mapping={"field_a": a_value, "field_b": b_value})
            pipe.expire(key, 100)

            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            expected = f"Command # 1 (HSET {key} field_a {a_value} field_b...) of pipeline caused error: "
            assert str(ex.value).startswith(expected)

    def test_pipeline_with_bitfield(self, r):
        with r.pipeline() as pipe:
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
            response = pipe.execute()

            assert pipe == pipe2
            assert response == [True, [0, 0, 15, 15, 14], b"1"]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.0.0")
    def test_pipeline_discard(self, r):
        # empty pipeline should raise an error
        with r.pipeline() as pipe:
            pipe.set("key", "someval")
            pipe.discard()
            with pytest.raises(redis.exceptions.ResponseError):
                pipe.execute()

        # setting a pipeline and discarding should do the same
        with r.pipeline() as pipe:
            pipe.set("key", "someval")
            pipe.set("someotherkey", "val")
            response = pipe.execute()
            pipe.set("key", "another value!")
            pipe.discard()
            pipe.set("key", "another vae!")
            with pytest.raises(redis.exceptions.ResponseError):
                pipe.execute()

            pipe.set("foo", "bar")
            response = pipe.execute()
        assert response[0]
        assert r.get("foo") == b"bar"

    @pytest.mark.onlynoncluster
    def test_send_set_commands_over_pipeline(self, r: redis.Redis):
        pipe = r.pipeline()
        pipe.hset("hash:1", "foo", "bar")
        pipe.hset("hash:1", "bar", "foo")
        pipe.hset("hash:1", "baz", "bar")
        pipe.hgetall("hash:1")
        resp = pipe.execute()
        assert resp == [1, 1, 1, {b"bar": b"foo", b"baz": b"bar", b"foo": b"bar"}]

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    def test_pipeline_with_msetex(self, r):
        r.delete("key1", "key2", "key1_transaction", "key2_transaction")

        p = r.pipeline()
        with pytest.raises(RedisClusterException):
            p.msetex({"key1": "value1", "key2": "value2"}, ex=1000)

        p_transaction = r.pipeline(transaction=True)
        with pytest.raises(RedisClusterException):
            p_transaction.msetex(
                {"key1_transaction": "value1", "key2_transaction": "value2"}, ex=10
            )


class TestPipelineEventEmission:
    """
    Unit tests that verify AfterCommandExecutionEvent is properly emitted from Pipeline
    and delivered to the Meter through the event dispatcher chain.

    These tests use fully mocked connection and connection pool - no real Redis
    or OTel integration is used.
    """

    @pytest.fixture
    def mock_connection(self):
        """Create a mock connection with required attributes."""
        conn = mock.MagicMock()
        conn.host = 'localhost'
        conn.port = 6379
        conn.db = 0

        # Mock retry to just execute the function directly
        def mock_call_with_retry(do, fail, is_retryable=None, with_failure_count=False):
            return do()
        conn.retry.call_with_retry = mock_call_with_retry

        return conn

    @pytest.fixture
    def mock_connection_pool(self, mock_connection):
        """Create a mock connection pool."""
        pool = mock.MagicMock()
        pool.get_connection.return_value = mock_connection
        pool.get_encoder.return_value = mock.MagicMock()
        return pool

    @pytest.fixture
    def mock_meter(self):
        """Create a mock Meter that tracks all instrument calls."""
        meter = mock.MagicMock()

        # Create mock histogram for operation duration
        self.operation_duration = mock.MagicMock()

        def create_histogram_side_effect(name, **kwargs):
            if name == 'db.client.operation.duration':
                return self.operation_duration
            return mock.MagicMock()

        meter.create_counter.return_value = mock.MagicMock()
        meter.create_up_down_counter.return_value = mock.MagicMock()
        meter.create_histogram.side_effect = create_histogram_side_effect

        return meter

    @pytest.fixture
    def setup_pipeline_with_otel(self, mock_connection_pool, mock_connection, mock_meter):
        """
        Setup a Pipeline with mocked connection and OTel collector.
        Returns tuple of (pipeline, operation_duration_mock).
        """

        # Reset any existing collector state
        recorder.reset_collector()

        # Create config with COMMAND group enabled
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        # Create collector with mocked meter
        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Patch the recorder to use our collector
        with mock.patch.object(
            recorder,
            '_get_or_create_collector',
            return_value=collector
        ):
            # Create event dispatcher (real one, to test the full chain)
            event_dispatcher = EventDispatcher()

            # Create pipeline with mocked connection pool
            pipeline = Pipeline(
                connection_pool=mock_connection_pool,
                response_callbacks={},
                transaction=True,
                shard_hint=None,
                event_dispatcher=event_dispatcher,
            )

            yield pipeline, self.operation_duration

        # Cleanup
        recorder.reset_collector()

    def test_pipeline_execute_emits_event_to_meter(self, setup_pipeline_with_otel):
        """
        Test that executing a pipeline emits AfterCommandExecutionEvent
        which is delivered to the Meter's histogram.record() method.
        """
        pipeline, operation_duration_mock = setup_pipeline_with_otel

        # Mock _execute_transaction to return successful responses
        pipeline._execute_transaction = mock.MagicMock(
            return_value=[True, True, b'value1']
        )

        # Queue commands in the pipeline
        pipeline.command_stack = [
            (('SET', 'key1', 'value1'), {}),
            (('SET', 'key2', 'value2'), {}),
            (('GET', 'key1'), {}),
        ]

        # Execute the pipeline
        pipeline.execute()

        # Verify the Meter's histogram.record() was called
        operation_duration_mock.record.assert_called_once()

        # Get the call arguments
        call_args = operation_duration_mock.record.call_args

        # Verify duration was recorded (first positional arg)
        duration = call_args[0][0]
        assert isinstance(duration, float)
        assert duration >= 0

        # Verify attributes
        attrs = call_args[1]['attributes']
        assert attrs['db.operation.name'] == 'MULTI'
        assert attrs['db.operation.batch.size'] == 3
        assert attrs['server.address'] == 'localhost'
        assert attrs['server.port'] == 6379
        assert attrs['db.namespace'] == '0'

    def test_pipeline_no_transaction_emits_pipeline_command_name(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that executing a pipeline without transaction
        emits AfterCommandExecutionEvent with command_name='PIPELINE'.
        """
        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, '_get_or_create_collector', return_value=collector):
            event_dispatcher = EventDispatcher()

            # Create pipeline with transaction=False
            pipeline = Pipeline(
                connection_pool=mock_connection_pool,
                response_callbacks={},
                transaction=False,  # Non-transaction mode
                shard_hint=None,
                event_dispatcher=event_dispatcher,
            )

            pipeline._execute_pipeline = mock.MagicMock(return_value=[True, True])
            pipeline.command_stack = [
                (('SET', 'key1', 'value1'), {}),
                (('SET', 'key2', 'value2'), {}),
            ]

            pipeline.execute()

            # Verify command name is PIPELINE
            call_args = self.operation_duration.record.call_args
            attrs = call_args[1]['attributes']
            assert attrs['db.operation.name'] == 'PIPELINE'

        recorder.reset_collector()

    def test_pipeline_error_emits_event_with_error(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that when a pipeline execution raises an exception,
        AfterCommandExecutionEvent is still emitted with error information.
        """
        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, '_get_or_create_collector', return_value=collector):
            event_dispatcher = EventDispatcher()

            pipeline = Pipeline(
                connection_pool=mock_connection_pool,
                response_callbacks={},
                transaction=False,
                shard_hint=None,
                event_dispatcher=event_dispatcher,
            )

            # Make execute raise an exception
            test_error = redis.ResponseError("WRONGTYPE Operation error")
            pipeline._execute_pipeline = mock.MagicMock(side_effect=test_error)
            pipeline.command_stack = [(('LPUSH', 'string_key', 'value'), {})]

            # Execute should raise the error
            with pytest.raises(redis.ResponseError):
                pipeline.execute()

            # Verify the Meter's histogram.record() was still called
            self.operation_duration.record.assert_called_once()

            # Verify error type is recorded in attributes
            call_args = self.operation_duration.record.call_args
            attrs = call_args[1]['attributes']
            assert attrs['db.operation.name'] == 'PIPELINE'
            assert 'error.type' in attrs

        recorder.reset_collector()

    def test_pipeline_batch_size_recorded_correctly(self, setup_pipeline_with_otel):
        """
        Test that the batch_size attribute correctly reflects
        the number of commands in the pipeline.
        """
        pipeline, operation_duration_mock = setup_pipeline_with_otel

        pipeline._execute_transaction = mock.MagicMock(
            return_value=[True, True, True, True, True]
        )

        # Queue exactly 5 commands
        pipeline.command_stack = [
            (('SET', 'key1', 'v1'), {}),
            (('SET', 'key2', 'v2'), {}),
            (('SET', 'key3', 'v3'), {}),
            (('SET', 'key4', 'v4'), {}),
            (('SET', 'key5', 'v5'), {}),
        ]

        pipeline.execute()

        # Verify batch_size is 5
        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]['attributes']
        assert attrs['db.operation.batch.size'] == 5

    def test_pipeline_server_attributes_recorded(self, setup_pipeline_with_otel):
        """
        Test that server address, port, and db namespace are correctly recorded.
        """
        pipeline, operation_duration_mock = setup_pipeline_with_otel

        pipeline._execute_transaction = mock.MagicMock(return_value=[True])
        pipeline.command_stack = [(('PING',), {})]

        pipeline.execute()

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]['attributes']

        # Verify server attributes match mock connection
        assert attrs['server.address'] == 'localhost'
        assert attrs['server.port'] == 6379
        assert attrs['db.namespace'] == '0'

    def test_multiple_pipeline_executions_emit_multiple_events(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that each pipeline execution emits a separate event to the Meter.
        """
        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, '_get_or_create_collector', return_value=collector):
            event_dispatcher = EventDispatcher()

            # First pipeline execution
            pipeline1 = Pipeline(
                connection_pool=mock_connection_pool,
                response_callbacks={},
                transaction=True,
                shard_hint=None,
                event_dispatcher=event_dispatcher,
            )
            pipeline1._execute_transaction = mock.MagicMock(return_value=[True])
            pipeline1.command_stack = [(('SET', 'key1', 'value1'), {})]
            pipeline1.execute()

            # Second pipeline execution
            pipeline2 = Pipeline(
                connection_pool=mock_connection_pool,
                response_callbacks={},
                transaction=True,
                shard_hint=None,
                event_dispatcher=event_dispatcher,
            )
            pipeline2._execute_transaction = mock.MagicMock(return_value=[True, True])
            pipeline2.command_stack = [
                (('SET', 'key2', 'value2'), {}),
                (('SET', 'key3', 'value3'), {}),
            ]
            pipeline2.execute()

            # Verify histogram.record() was called twice
            assert self.operation_duration.record.call_count == 2

        recorder.reset_collector()

    def test_empty_pipeline_does_not_emit_event(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that an empty pipeline (no commands) does not emit an event.
        """
        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(recorder, '_get_or_create_collector', return_value=collector):
            event_dispatcher = EventDispatcher()

            pipeline = Pipeline(
                connection_pool=mock_connection_pool,
                response_callbacks={},
                transaction=True,
                shard_hint=None,
                event_dispatcher=event_dispatcher,
            )

            # Empty command stack
            pipeline.command_stack = []

            # Execute empty pipeline
            result = pipeline.execute()

            # Should return empty list
            assert result == []

            # No event should be emitted for empty pipeline
            self.operation_duration.record.assert_not_called()

        recorder.reset_collector()

    def test_pipeline_retry_emits_event_on_each_attempt(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that when a pipeline is retried, an AfterCommandExecutionEvent
        is emitted for each retry attempt with retry_attempts attribute.
        """
        # Create connection with retry behavior
        mock_connection = mock.MagicMock()
        mock_connection.host = 'localhost'
        mock_connection.port = 6379
        mock_connection.db = 0

        # Track retry attempts
        max_retries = 2

        def call_with_retry_impl(do, fail, is_retryable=None, with_failure_count=False):
            """Simulate retry behavior - fail twice, then succeed."""
            for attempt in range(max_retries + 1):
                try:
                    return do()
                except redis.ConnectionError as e:
                    if attempt < max_retries:
                        if with_failure_count:
                            fail(e, attempt + 1)
                        else:
                            fail(e)
                    else:
                        raise

        mock_connection.retry.call_with_retry = call_with_retry_impl
        mock_connection.retry.get_retries.return_value = max_retries

        mock_connection_pool.get_connection.return_value = mock_connection

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(
            recorder, '_get_or_create_collector', return_value=collector
        ):
            event_dispatcher = EventDispatcher()

            pipeline = Pipeline(
                connection_pool=mock_connection_pool,
                response_callbacks={},
                transaction=False,
                shard_hint=None,
                event_dispatcher=event_dispatcher,
            )

            # Make pipeline fail twice then succeed
            call_count = [0]

            def execute_pipeline_impl(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] <= 2:
                    raise redis.ConnectionError("Connection failed")
                return [True, True]

            pipeline._execute_pipeline = mock.MagicMock(
                side_effect=execute_pipeline_impl
            )
            pipeline.command_stack = [
                (('SET', 'key1', 'value1'), {}),
                (('SET', 'key2', 'value2'), {}),
            ]

            # Execute pipeline - should retry twice then succeed
            pipeline.execute()

            # Verify histogram.record() was called 3 times:
            # 2 retry attempts + 1 final success
            assert self.operation_duration.record.call_count == 3

            calls = self.operation_duration.record.call_args_list

            # First two calls should have error.type (retry attempts)
            assert 'error.type' in calls[0][1]['attributes']
            assert 'error.type' in calls[1][1]['attributes']

            # Last call should be success (no error.type)
            assert 'error.type' not in calls[2][1]['attributes']

            # All calls should have batch_size
            for call in calls:
                assert call[1]['attributes']['db.operation.batch.size'] == 2

        recorder.reset_collector()

    def test_pipeline_retry_exhausted_emits_final_error_event(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that when all retries are exhausted, a final AfterCommandExecutionEvent
        is emitted with the error.
        """
        mock_connection = mock.MagicMock()
        mock_connection.host = 'localhost'
        mock_connection.port = 6379
        mock_connection.db = 0

        max_retries = 2

        def call_with_retry_impl(do, fail, is_retryable=None, with_failure_count=False):
            """Simulate retry behavior - always fail."""
            for attempt in range(max_retries + 1):
                try:
                    return do()
                except redis.ConnectionError as e:
                    if attempt < max_retries:
                        if with_failure_count:
                            fail(e, attempt + 1)
                        else:
                            fail(e)
                    else:
                        raise

        mock_connection.retry.call_with_retry = call_with_retry_impl
        mock_connection.retry.get_retries.return_value = max_retries

        mock_connection_pool.get_connection.return_value = mock_connection

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(
            recorder, '_get_or_create_collector', return_value=collector
        ):
            event_dispatcher = EventDispatcher()

            pipeline = Pipeline(
                connection_pool=mock_connection_pool,
                response_callbacks={},
                transaction=False,
                shard_hint=None,
                event_dispatcher=event_dispatcher,
            )

            # Make pipeline always fail
            pipeline._execute_pipeline = mock.MagicMock(
                side_effect=redis.ConnectionError("Connection failed")
            )
            pipeline.command_stack = [
                (('SET', 'key1', 'value1'), {}),
            ]

            # Execute pipeline - should fail after all retries
            with pytest.raises(redis.ConnectionError):
                pipeline.execute()

            # Verify histogram.record() was called 3 times:
            # 2 retry attempts + 1 final error
            assert self.operation_duration.record.call_count == 3

            calls = self.operation_duration.record.call_args_list

            # All calls should have error.type
            for call in calls:
                assert 'error.type' in call[1]['attributes']
                assert call[1]['attributes']['db.operation.name'] == 'PIPELINE'

        recorder.reset_collector()

    def test_pipeline_on_error_event_emitted_on_retry(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that OnErrorEvent is emitted during pipeline retry attempts.
        """

        mock_connection = mock.MagicMock()
        mock_connection.host = 'localhost'
        mock_connection.port = 6379
        mock_connection.db = 0

        max_retries = 1

        def call_with_retry_impl(do, fail, is_retryable=None, with_failure_count=False):
            """Simulate retry behavior - fail once, then succeed."""
            for attempt in range(max_retries + 1):
                try:
                    return do()
                except redis.ConnectionError as e:
                    if attempt < max_retries:
                        if with_failure_count:
                            fail(e, attempt + 1)
                        else:
                            fail(e)
                    else:
                        raise

        mock_connection.retry.call_with_retry = call_with_retry_impl
        mock_connection.retry.get_retries.return_value = max_retries

        mock_connection_pool.get_connection.return_value = mock_connection

        # Track OnErrorEvent dispatches
        error_events = []

        class ErrorEventTracker(EventListenerInterface):
            def listen(self, event: object):
                if isinstance(event, OnErrorEvent):
                    error_events.append(event)

        event_dispatcher = EventDispatcher()
        tracker = ErrorEventTracker()
        event_dispatcher.register_listeners({OnErrorEvent: [tracker]})

        pipeline = Pipeline(
            connection_pool=mock_connection_pool,
            response_callbacks={},
            transaction=False,
            shard_hint=None,
            event_dispatcher=event_dispatcher,
        )

        # Make pipeline fail once then succeed
        call_count = [0]

        def execute_pipeline_impl(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise redis.ConnectionError("Connection failed")
            return [True]

        pipeline._execute_pipeline = mock.MagicMock(
            side_effect=execute_pipeline_impl
        )
        pipeline.command_stack = [(('SET', 'key', 'value'), {})]

        # Execute pipeline
        pipeline.execute()

        # Verify OnErrorEvent was dispatched during retry
        assert len(error_events) == 1
        assert error_events[0].server_address == 'localhost'
        assert error_events[0].server_port == 6379
        assert error_events[0].retry_attempts == 1

    def test_pipeline_on_error_event_emitted_on_final_failure(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that OnErrorEvent is emitted when pipeline fails after all retries.
        """

        mock_connection = mock.MagicMock()
        mock_connection.host = 'localhost'
        mock_connection.port = 6379
        mock_connection.db = 0

        max_retries = 1

        def call_with_retry_impl(do, fail, is_retryable=None, with_failure_count=False):
            """Simulate retry behavior - always fail."""
            for attempt in range(max_retries + 1):
                try:
                    return do()
                except redis.ConnectionError as e:
                    if attempt < max_retries:
                        if with_failure_count:
                            fail(e, attempt + 1)
                        else:
                            fail(e)
                    else:
                        raise

        mock_connection.retry.call_with_retry = call_with_retry_impl
        mock_connection.retry.get_retries.return_value = max_retries

        mock_connection_pool.get_connection.return_value = mock_connection

        # Track OnErrorEvent dispatches
        error_events = []

        class ErrorEventTracker(EventListenerInterface):
            def listen(self, event: object):
                if isinstance(event, OnErrorEvent):
                    error_events.append(event)

        event_dispatcher = EventDispatcher()
        tracker = ErrorEventTracker()
        event_dispatcher.register_listeners({OnErrorEvent: [tracker]})

        pipeline = Pipeline(
            connection_pool=mock_connection_pool,
            response_callbacks={},
            transaction=False,
            shard_hint=None,
            event_dispatcher=event_dispatcher,
        )

        # Make pipeline always fail
        pipeline._execute_pipeline = mock.MagicMock(
            side_effect=redis.ConnectionError("Connection failed")
        )
        pipeline.command_stack = [(('SET', 'key', 'value'), {})]

        # Execute pipeline - should fail
        with pytest.raises(redis.ConnectionError):
            pipeline.execute()

        # Verify OnErrorEvent was dispatched:
        # 1 during retry + 1 on final failure
        assert len(error_events) == 2

        # First event is from retry
        assert error_events[0].retry_attempts == 1

        # Second event is from final failure (is_internal=False)
        assert error_events[1].is_internal is False