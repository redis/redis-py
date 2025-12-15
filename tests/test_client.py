from unittest import mock

import pytest

import redis
from redis.event import EventDispatcher
from redis.observability import recorder
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector


class TestRedisClientEventEmission:
    """
    Unit tests that verify AfterCommandExecutionEvent is properly emitted from Redis client
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
        conn.should_reconnect.return_value = False

        # Mock retry to just execute the function directly
        conn.retry.call_with_retry = lambda func, _: func()

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
    def setup_redis_client_with_otel(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Setup a Redis client with mocked connection and OTel collector.
        Returns tuple of (redis_client, operation_duration_mock).
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
            # Create Redis client with mocked connection pool
            client = redis.Redis(
                connection_pool=mock_connection_pool,
            )

            yield client, self.operation_duration

        # Cleanup
        recorder.reset_collector()

    def test_execute_command_emits_event_to_meter(self, setup_redis_client_with_otel):
        """
        Test that executing a command emits AfterCommandExecutionEvent
        which is delivered to the Meter's histogram.record() method.
        """
        client, operation_duration_mock = setup_redis_client_with_otel

        # Mock _send_command_parse_response to return a successful response
        client._send_command_parse_response = mock.MagicMock(return_value=True)

        # Execute a command
        client.execute_command('SET', 'key1', 'value1')

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
        assert attrs['db.operation.name'] == 'SET'
        assert attrs['server.address'] == 'localhost'
        assert attrs['server.port'] == 6379
        assert attrs['db.namespace'] == '0'

    def test_get_command_emits_event_to_meter(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that GET command emits AfterCommandExecutionEvent with correct command name.
        """

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(
            recorder, '_get_or_create_collector', return_value=collector
        ):
            client = redis.Redis(
                connection_pool=mock_connection_pool,
            )

            client._send_command_parse_response = mock.MagicMock(return_value=b'value1')

            # Execute GET command
            client.execute_command('GET', 'key1')

            # Verify command name is GET
            call_args = self.operation_duration.record.call_args
            attrs = call_args[1]['attributes']
            assert attrs['db.operation.name'] == 'GET'

        recorder.reset_collector()

    def test_command_error_emits_event_with_error(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that when a command execution raises an exception,
        AfterCommandExecutionEvent is still emitted with error information.
        """

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(
            recorder, '_get_or_create_collector', return_value=collector
        ):
            client = redis.Redis(
                connection_pool=mock_connection_pool,
            )

            # Make command raise an exception
            test_error = redis.ResponseError("WRONGTYPE Operation error")
            client._send_command_parse_response = mock.MagicMock(side_effect=test_error)

            # Execute should raise the error
            with pytest.raises(redis.ResponseError):
                client.execute_command('LPUSH', 'string_key', 'value')

            # Verify the Meter's histogram.record() was still called
            self.operation_duration.record.assert_called_once()

            # Verify error type is recorded in attributes
            call_args = self.operation_duration.record.call_args
            attrs = call_args[1]['attributes']
            assert attrs['db.operation.name'] == 'LPUSH'
            assert 'error.type' in attrs

        recorder.reset_collector()

    def test_server_attributes_recorded_correctly(self, setup_redis_client_with_otel):
        """
        Test that server address, port, and db namespace are correctly recorded.
        """
        client, operation_duration_mock = setup_redis_client_with_otel

        client._send_command_parse_response = mock.MagicMock(return_value=b'PONG')

        client.execute_command('PING')

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]['attributes']

        # Verify server attributes match mock connection
        assert attrs['server.address'] == 'localhost'
        assert attrs['server.port'] == 6379
        assert attrs['db.namespace'] == '0'

    def test_multiple_commands_emit_multiple_events(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that each command execution emits a separate event to the Meter.
        """

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(
            recorder, '_get_or_create_collector', return_value=collector
        ):
            client = redis.Redis(
                connection_pool=mock_connection_pool,
            )

            client._send_command_parse_response = mock.MagicMock(return_value=True)

            # Execute multiple commands
            client.execute_command('SET', 'key1', 'value1')
            client.execute_command('SET', 'key2', 'value2')
            client.execute_command('GET', 'key1')

            # Verify histogram.record() was called three times
            assert self.operation_duration.record.call_count == 3

            # Verify command names in order
            calls = self.operation_duration.record.call_args_list
            assert calls[0][1]['attributes']['db.operation.name'] == 'SET'
            assert calls[1][1]['attributes']['db.operation.name'] == 'SET'
            assert calls[2][1]['attributes']['db.operation.name'] == 'GET'

        recorder.reset_collector()

    def test_different_db_namespace_recorded(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that different db namespace values are correctly recorded.
        """

        # Create connection with different db
        mock_connection = mock.MagicMock()
        mock_connection.host = 'redis.example.com'
        mock_connection.port = 6380
        mock_connection.db = 5
        mock_connection.should_reconnect.return_value = False
        mock_connection.retry.call_with_retry = lambda func, _: func()

        mock_connection_pool.get_connection.return_value = mock_connection

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(
            recorder, '_get_or_create_collector', return_value=collector
        ):
            client = redis.Redis(
                connection_pool=mock_connection_pool,
            )

            client._send_command_parse_response = mock.MagicMock(return_value=True)

            client.execute_command('SET', 'key', 'value')

            call_args = self.operation_duration.record.call_args
            attrs = call_args[1]['attributes']

            # Verify different server attributes
            assert attrs['server.address'] == 'redis.example.com'
            assert attrs['server.port'] == 6380
            assert attrs['db.namespace'] == '5'

        recorder.reset_collector()

    def test_duration_is_positive(self, setup_redis_client_with_otel):
        """
        Test that the recorded duration is a positive float value.
        """
        client, operation_duration_mock = setup_redis_client_with_otel

        client._send_command_parse_response = mock.MagicMock(return_value=True)

        client.execute_command('SET', 'key', 'value')

        call_args = operation_duration_mock.record.call_args
        duration = call_args[0][0]

        assert isinstance(duration, float)
        assert duration >= 0

    def test_no_batch_size_for_single_command(self, setup_redis_client_with_otel):
        """
        Test that single commands do not include batch_size attribute
        (batch_size is only for pipeline operations).
        """
        client, operation_duration_mock = setup_redis_client_with_otel

        client._send_command_parse_response = mock.MagicMock(return_value=True)

        client.execute_command('SET', 'key', 'value')

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]['attributes']

        # batch_size should not be present for single commands
        assert 'db.operation.batch_size' not in attrs