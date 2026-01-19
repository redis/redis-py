"""
Unit tests for redis.observability.recorder module.

These tests verify that recorder functions correctly pass arguments through
to the underlying OTel Meter instruments (Counter, Histogram, UpDownCounter).
The MeterProvider is mocked to verify the actual integration point where
metrics are exported to OTel.
"""

import pytest
from unittest.mock import MagicMock, patch

from redis.observability import recorder
from redis.observability.attributes import (
    ConnectionState,
    PubSubDirection,
    # Connection pool attributes
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_CONNECTION_STATE,
    REDIS_CLIENT_CONNECTION_PUBSUB,
    # Server attributes
    SERVER_ADDRESS,
    SERVER_PORT,
    # Database attributes
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_OPERATION_BATCH_SIZE,
    DB_RESPONSE_STATUS_CODE,
    # Error attributes
    ERROR_TYPE,
    # Network attributes
    NETWORK_PEER_ADDRESS,
    NETWORK_PEER_PORT,
    # Redis-specific attributes
    REDIS_CLIENT_OPERATION_RETRY_ATTEMPTS,
    REDIS_CLIENT_CONNECTION_CLOSE_REASON,
    REDIS_CLIENT_CONNECTION_NOTIFICATION,
    REDIS_CLIENT_PUBSUB_MESSAGE_DIRECTION,
    REDIS_CLIENT_PUBSUB_CHANNEL,
    REDIS_CLIENT_PUBSUB_SHARDED,
    # Streaming attributes
    REDIS_CLIENT_STREAM_NAME,
    REDIS_CLIENT_CONSUMER_GROUP,
    REDIS_CLIENT_CONSUMER_NAME, DB_CLIENT_CONNECTION_NAME,
)
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector, CloseReason
from redis.observability.recorder import record_operation_duration, record_connection_create_time, \
    record_connection_timeout, record_connection_wait_time, record_connection_use_time, \
    record_connection_closed, record_connection_relaxed_timeout, record_connection_handoff, record_error_count, \
    record_pubsub_message, reset_collector, record_streaming_lag


class MockInstruments:
    """Container for mock OTel instruments."""

    def __init__(self):
        # Counters
        self.client_errors = MagicMock()
        self.maintenance_notifications = MagicMock()
        self.connection_timeouts = MagicMock()
        self.connection_closed = MagicMock()
        self.connection_handoff = MagicMock()
        self.pubsub_messages = MagicMock()

        # Gauges
        self.connection_count = MagicMock()

        # UpDownCounters
        self.connection_relaxed_timeout = MagicMock()

        # Histograms
        self.connection_create_time = MagicMock()
        self.connection_wait_time = MagicMock()
        self.connection_use_time = MagicMock()
        self.operation_duration = MagicMock()
        self.stream_lag = MagicMock()


@pytest.fixture
def mock_instruments():
    """Create mock OTel instruments."""
    return MockInstruments()


@pytest.fixture
def mock_meter(mock_instruments):
    """Create a mock Meter that returns our mock instruments."""
    meter = MagicMock()

    def create_counter_side_effect(name, **kwargs):
        instrument_map = {
            'redis.client.errors': mock_instruments.client_errors,
            'redis.client.maintenance.notifications': mock_instruments.maintenance_notifications,
            'db.client.connection.timeouts': mock_instruments.connection_timeouts,
            'redis.client.connection.closed': mock_instruments.connection_closed,
            'redis.client.connection.handoff': mock_instruments.connection_handoff,
            'redis.client.pubsub.messages': mock_instruments.pubsub_messages,
        }
        return instrument_map.get(name, MagicMock())

    def create_gauge_side_effect(name, **kwargs):
        instrument_map = {
            'db.client.connection.count': mock_instruments.connection_count,
        }
        return instrument_map.get(name, MagicMock())

    def create_up_down_counter_side_effect(name, **kwargs):
        instrument_map = {
            'redis.client.connection.relaxed_timeout': mock_instruments.connection_relaxed_timeout,
        }
        return instrument_map.get(name, MagicMock())

    def create_histogram_side_effect(name, **kwargs):
        instrument_map = {
            'db.client.connection.create_time': mock_instruments.connection_create_time,
            'db.client.connection.wait_time': mock_instruments.connection_wait_time,
            'db.client.connection.use_time': mock_instruments.connection_use_time,
            'db.client.operation.duration': mock_instruments.operation_duration,
            'redis.client.stream.lag': mock_instruments.stream_lag,
        }
        return instrument_map.get(name, MagicMock())

    meter.create_counter.side_effect = create_counter_side_effect
    meter.create_gauge.side_effect = create_gauge_side_effect
    meter.create_observable_gauge.side_effect = create_gauge_side_effect
    meter.create_up_down_counter.side_effect = create_up_down_counter_side_effect
    meter.create_histogram.side_effect = create_histogram_side_effect

    return meter


@pytest.fixture
def mock_config():
    """Create a config with all metric groups enabled."""
    config = OTelConfig(
        metric_groups=[
            MetricGroup.RESILIENCY,
            MetricGroup.CONNECTION_BASIC,
            MetricGroup.CONNECTION_ADVANCED,
            MetricGroup.COMMAND,
            MetricGroup.PUBSUB,
            MetricGroup.STREAMING,
        ]
    )
    return config


@pytest.fixture
def metrics_collector(mock_meter, mock_config):
    """Create a real RedisMetricsCollector with mocked Meter."""
    with patch('redis.observability.metrics.OTEL_AVAILABLE', True):
        from redis.observability.metrics import RedisMetricsCollector
        collector = RedisMetricsCollector(mock_meter, mock_config)
        return collector


@pytest.fixture
def setup_recorder(metrics_collector, mock_instruments):
    """
    Setup the recorder module with our collector that has mocked instruments.
    """
    from redis.observability import recorder

    # Reset the global collector before test
    recorder.reset_collector()

    # Patch _get_or_create_collector to return our collector with mocked instruments
    with patch.object(
        recorder,
        '_get_or_create_collector',
        return_value=metrics_collector
    ):
        yield mock_instruments

    # Reset after test
    recorder.reset_collector()


class TestRecordOperationDuration:
    """Tests for record_operation_duration - verifies Histogram.record() calls."""

    def test_record_operation_duration_success(self, setup_recorder):
        """Test that operation duration is recorded to the histogram with correct attributes."""

        instruments = setup_recorder

        record_operation_duration(
            command_name='SET',
            duration_seconds=0.005,
            server_address='localhost',
            server_port=6379,
            db_namespace='0',
            error=None,
        )

        # Verify histogram.record() was called
        instruments.operation_duration.record.assert_called_once()
        call_args = instruments.operation_duration.record.call_args

        # Verify duration value
        assert call_args[0][0] == 0.005

        # Verify attributes
        attrs = call_args[1]['attributes']
        assert attrs[SERVER_ADDRESS] == 'localhost'
        assert attrs[SERVER_PORT] == 6379
        assert attrs[DB_NAMESPACE] == '0'
        assert attrs[DB_OPERATION_NAME] == 'SET'

    def test_record_operation_duration_with_error(self, setup_recorder):
        """Test that error information is included in attributes."""

        instruments = setup_recorder

        error = ConnectionError("Connection refused")
        record_operation_duration(
            command_name='GET',
            duration_seconds=0.001,
            server_address='localhost',
            server_port=6379,
            error=error,
        )

        instruments.operation_duration.record.assert_called_once()
        call_args = instruments.operation_duration.record.call_args

        attrs = call_args[1]['attributes']
        assert attrs[DB_OPERATION_NAME] == 'GET'
        assert attrs[DB_RESPONSE_STATUS_CODE] == 'error'
        assert attrs[ERROR_TYPE] == 'ConnectionError'


class TestRecordConnectionCreateTime:
    """Tests for record_connection_create_time - verifies Histogram.record() calls."""

    def test_record_connection_create_time(self, setup_recorder):
        """Test that connection creation time is recorded with pool name."""

        instruments = setup_recorder

        record_connection_create_time(
            connection_pool='ConnectionPool<localhost:6379>',
            duration_seconds=0.025,
        )

        instruments.connection_create_time.record.assert_called_once()
        call_args = instruments.connection_create_time.record.call_args

        # Verify duration value
        assert call_args[0][0] == 0.025

        # Verify attributes
        attrs = call_args[1]['attributes']
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "'ConnectionPool<localhost:6379>'"


class TestRecordConnectionTimeout:
    """Tests for record_connection_timeout - verifies Counter.add() calls."""

    def test_record_connection_timeout(self, setup_recorder):
        """Test recording connection timeout event."""

        instruments = setup_recorder

        record_connection_timeout(
            pool_name='ConnectionPool<localhost:6379>',
        )

        instruments.connection_timeouts.add.assert_called_once()
        call_args = instruments.connection_timeouts.add.call_args

        # Counter increments by 1
        assert call_args[0][0] == 1

        attrs = call_args[1]['attributes']
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == 'ConnectionPool<localhost:6379>'


class TestRecordConnectionWaitTime:
    """Tests for record_connection_wait_time - verifies Histogram.record() calls."""

    def test_record_connection_wait_time(self, setup_recorder):
        """Test recording connection wait time."""

        instruments = setup_recorder

        record_connection_wait_time(
            pool_name='ConnectionPool<localhost:6379>',
            duration_seconds=0.010,
        )

        instruments.connection_wait_time.record.assert_called_once()
        call_args = instruments.connection_wait_time.record.call_args

        assert call_args[0][0] == 0.010
        attrs = call_args[1]['attributes']
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == 'ConnectionPool<localhost:6379>'


class TestRecordConnectionUseTime:
    """Tests for record_connection_use_time - verifies Histogram.record() calls."""

    def test_record_connection_use_time(self, setup_recorder):
        """Test recording connection use time."""

        instruments = setup_recorder

        record_connection_use_time(
            pool_name='ConnectionPool<localhost:6379>',
            duration_seconds=0.050,
        )

        instruments.connection_use_time.record.assert_called_once()
        call_args = instruments.connection_use_time.record.call_args

        assert call_args[0][0] == 0.050
        attrs = call_args[1]['attributes']
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == 'ConnectionPool<localhost:6379>'


class TestRecordConnectionClosed:
    """Tests for record_connection_closed - verifies Counter.add() calls."""

    def test_record_connection_closed_with_reason(self, setup_recorder):
        """Test recording connection closed with reason."""

        instruments = setup_recorder

        record_connection_closed(
            close_reason=CloseReason.HEALTHCHECK_FAILED,
        )

        instruments.connection_closed.add.assert_called_once()
        call_args = instruments.connection_closed.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]['attributes']
        assert attrs[REDIS_CLIENT_CONNECTION_CLOSE_REASON] == CloseReason.HEALTHCHECK_FAILED.value

    def test_record_connection_closed_with_error(self, setup_recorder):
        """Test recording connection closed with error type."""

        instruments = setup_recorder

        error = ConnectionResetError("Connection reset by peer")
        record_connection_closed(
            close_reason=CloseReason.ERROR,
            error_type=error,
        )

        instruments.connection_closed.add.assert_called_once()
        attrs = instruments.connection_closed.add.call_args[1]['attributes']
        assert attrs[REDIS_CLIENT_CONNECTION_CLOSE_REASON] == 'error'
        assert attrs[ERROR_TYPE] == 'ConnectionResetError'


class TestRecordConnectionRelaxedTimeout:
    """Tests for record_connection_relaxed_timeout - verifies UpDownCounter.add() calls."""

    def test_record_connection_relaxed_timeout_relaxed(self, setup_recorder):
        """Test recording relaxed timeout increments counter by 1."""

        instruments = setup_recorder

        record_connection_relaxed_timeout(
            connection_name='Connection<localhost:6379>',
            maint_notification='MOVING',
            relaxed=True,
        )

        instruments.connection_relaxed_timeout.add.assert_called_once()
        call_args = instruments.connection_relaxed_timeout.add.call_args

        # relaxed=True means count up (+1)
        assert call_args[0][0] == 1
        attrs = call_args[1]['attributes']
        assert attrs[DB_CLIENT_CONNECTION_NAME] == 'Connection<localhost:6379>'
        assert attrs[REDIS_CLIENT_CONNECTION_NOTIFICATION] == 'MOVING'

    def test_record_connection_relaxed_timeout_unrelaxed(self, setup_recorder):
        """Test recording unrelaxed timeout decrements counter by 1."""

        instruments = setup_recorder

        record_connection_relaxed_timeout(
            connection_name='ConnectionPool<localhost:6379>',
            maint_notification='MIGRATING',
            relaxed=False,
        )

        instruments.connection_relaxed_timeout.add.assert_called_once()
        call_args = instruments.connection_relaxed_timeout.add.call_args

        # relaxed=False means count down (-1)
        assert call_args[0][0] == -1
        attrs = call_args[1]['attributes']
        assert attrs[REDIS_CLIENT_CONNECTION_NOTIFICATION] == 'MIGRATING'


class TestRecordConnectionHandoff:
    """Tests for record_connection_handoff - verifies Counter.add() calls."""

    def test_record_connection_handoff(self, setup_recorder):
        """Test recording connection handoff event."""

        instruments = setup_recorder

        record_connection_handoff(
            pool_name='ConnectionPool<localhost:6379>',
        )

        instruments.connection_handoff.add.assert_called_once()
        call_args = instruments.connection_handoff.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]['attributes']
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == 'ConnectionPool<localhost:6379>'


class TestRecordErrorCount:
    """Tests for record_error_count - verifies Counter.add() calls."""

    def test_record_error_count(self, setup_recorder):
        """Test recording error count with all attributes."""

        instruments = setup_recorder

        error = ConnectionError("Connection refused")
        record_error_count(
            server_address='localhost',
            server_port=6379,
            network_peer_address='127.0.0.1',
            network_peer_port=6379,
            error_type=error,
            retry_attempts=3,
            is_internal=True,
        )

        instruments.client_errors.add.assert_called_once()
        call_args = instruments.client_errors.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]['attributes']
        assert attrs[SERVER_ADDRESS] == 'localhost'
        assert attrs[SERVER_PORT] == 6379
        assert attrs[NETWORK_PEER_ADDRESS] == '127.0.0.1'
        assert attrs[NETWORK_PEER_PORT] == 6379
        assert attrs[ERROR_TYPE] == 'ConnectionError'
        assert attrs[REDIS_CLIENT_OPERATION_RETRY_ATTEMPTS] == 3

    def test_record_error_count_with_is_internal_false(self, setup_recorder):
        """Test recording error count with is_internal=False."""

        instruments = setup_recorder

        error = TimeoutError("Connection timed out")
        record_error_count(
            server_address='localhost',
            server_port=6379,
            network_peer_address='127.0.0.1',
            network_peer_port=6379,
            error_type=error,
            retry_attempts=2,
            is_internal=False,
        )

        instruments.client_errors.add.assert_called_once()
        call_args = instruments.client_errors.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]['attributes']
        assert attrs[ERROR_TYPE] == 'TimeoutError'
        assert attrs[REDIS_CLIENT_OPERATION_RETRY_ATTEMPTS] == 2


class TestRecordMaintNotificationCount:
    """Tests for record_maint_notification_count - verifies Counter.add() calls."""

    def test_record_maint_notification_count(self, setup_recorder):
        """Test recording maintenance notification count with all attributes."""

        instruments = setup_recorder

        recorder.record_maint_notification_count(
            server_address='localhost',
            server_port=6379,
            network_peer_address='127.0.0.1',
            network_peer_port=6379,
            maint_notification='MOVING',
        )

        instruments.maintenance_notifications.add.assert_called_once()
        call_args = instruments.maintenance_notifications.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]['attributes']
        assert attrs[SERVER_ADDRESS] == 'localhost'
        assert attrs[SERVER_PORT] == 6379
        assert attrs[NETWORK_PEER_ADDRESS] == '127.0.0.1'
        assert attrs[NETWORK_PEER_PORT] == 6379
        assert attrs[REDIS_CLIENT_CONNECTION_NOTIFICATION] == 'MOVING'

    def test_record_maint_notification_count_migrating(self, setup_recorder):
        """Test recording maintenance notification count with MIGRATING type."""

        instruments = setup_recorder

        recorder.record_maint_notification_count(
            server_address='redis-primary',
            server_port=6380,
            network_peer_address='10.0.0.1',
            network_peer_port=6380,
            maint_notification='MIGRATING',
        )

        instruments.maintenance_notifications.add.assert_called_once()
        call_args = instruments.maintenance_notifications.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]['attributes']
        assert attrs[SERVER_ADDRESS] == 'redis-primary'
        assert attrs[SERVER_PORT] == 6380
        assert attrs[REDIS_CLIENT_CONNECTION_NOTIFICATION] == 'MIGRATING'


class TestRecordPubsubMessage:
    """Tests for record_pubsub_message - verifies Counter.add() calls."""

    def test_record_pubsub_message_publish(self, setup_recorder):
        """Test recording published message."""

        instruments = setup_recorder

        record_pubsub_message(
            direction=PubSubDirection.PUBLISH,
            channel='my-channel',
            sharded=False,
        )

        instruments.pubsub_messages.add.assert_called_once()
        call_args = instruments.pubsub_messages.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]['attributes']
        assert attrs[REDIS_CLIENT_PUBSUB_MESSAGE_DIRECTION] == PubSubDirection.PUBLISH.value
        assert attrs[REDIS_CLIENT_PUBSUB_CHANNEL] == 'my-channel'
        assert attrs[REDIS_CLIENT_PUBSUB_SHARDED] is False

    def test_record_pubsub_message_receive_sharded(self, setup_recorder):
        """Test recording received message on sharded channel."""

        instruments = setup_recorder

        record_pubsub_message(
            direction=PubSubDirection.RECEIVE,
            channel='sharded-channel',
            sharded=True,
        )

        instruments.pubsub_messages.add.assert_called_once()
        attrs = instruments.pubsub_messages.add.call_args[1]['attributes']
        assert attrs[REDIS_CLIENT_PUBSUB_MESSAGE_DIRECTION] == PubSubDirection.RECEIVE.value
        assert attrs[REDIS_CLIENT_PUBSUB_CHANNEL] == 'sharded-channel'
        assert attrs[REDIS_CLIENT_PUBSUB_SHARDED] is True


class TestRecordStreamingLag:
    """Tests for record_streaming_lag - verifies Histogram.record() calls."""

    def test_record_streaming_lag_with_all_attributes(self, setup_recorder):
        """Test recording streaming lag with all attributes."""

        instruments = setup_recorder

        record_streaming_lag(
            lag_seconds=0.150,
            stream_name='my-stream',
            consumer_group='my-group',
            consumer_name='consumer-1',
        )

        instruments.stream_lag.record.assert_called_once()
        call_args = instruments.stream_lag.record.call_args

        # Verify lag value
        assert call_args[0][0] == 0.150

        # Verify attributes
        attrs = call_args[1]['attributes']
        assert attrs[REDIS_CLIENT_STREAM_NAME] == 'my-stream'
        assert attrs[REDIS_CLIENT_CONSUMER_GROUP] == 'my-group'
        assert attrs[REDIS_CLIENT_CONSUMER_NAME] == 'consumer-1'

    def test_record_streaming_lag_minimal(self, setup_recorder):
        """Test recording streaming lag with only required attributes."""

        instruments = setup_recorder

        record_streaming_lag(
            lag_seconds=0.025,
        )

        instruments.stream_lag.record.assert_called_once()
        call_args = instruments.stream_lag.record.call_args

        # Verify lag value
        assert call_args[0][0] == 0.025

    def test_record_streaming_lag_with_stream_only(self, setup_recorder):
        """Test recording streaming lag with stream name only."""

        instruments = setup_recorder

        record_streaming_lag(
            lag_seconds=0.500,
            stream_name='events-stream',
        )

        instruments.stream_lag.record.assert_called_once()
        attrs = instruments.stream_lag.record.call_args[1]['attributes']
        assert attrs[REDIS_CLIENT_STREAM_NAME] == 'events-stream'


class TestRecorderDisabled:
    """Tests for recorder behavior when observability is disabled."""

    def test_record_operation_duration_when_disabled(self):
        """Test that recording does nothing when collector is None."""

        reset_collector()

        with patch.object(recorder, '_get_or_create_collector', return_value=None):
            # Should not raise any exception
            record_operation_duration(
                command_name='SET',
                duration_seconds=0.005,
                server_address='localhost',
                server_port=6379,
            )

        reset_collector()

    def test_is_enabled_returns_false_when_disabled(self):
        """Test is_enabled returns False when collector is None."""
        reset_collector()

        with patch.object(recorder, '_get_or_create_collector', return_value=None):
            assert recorder.is_enabled() is False

        recorder.reset_collector()

    def test_all_record_functions_safe_when_disabled(self):
        """Test that all record functions are safe to call when disabled."""

        reset_collector()

        with patch.object(recorder, '_get_or_create_collector', return_value=None):
            # None of these should raise
            recorder.record_connection_create_time('pool', 0.1)
            recorder.record_connection_timeout('pool')
            recorder.record_connection_wait_time('pool', 0.1)
            recorder.record_connection_use_time('pool', 0.1)
            recorder.record_connection_closed('pool')
            recorder.record_connection_relaxed_timeout('pool', 'MOVING', True)
            recorder.record_connection_handoff('pool')
            recorder.record_error_count('host', 6379, '127.0.0.1', 6379, Exception(), 0)
            recorder.record_maint_notification_count('host', 6379, '127.0.0.1', 6379, 'MOVING')
            recorder.record_pubsub_message(PubSubDirection.PUBLISH)
            recorder.record_streaming_lag(0.1, 'stream', 'group', 'consumer')

        recorder.reset_collector()


class TestResetCollector:
    """Tests for reset_collector function."""

    def test_reset_collector_clears_global(self):
        """Test that reset_collector clears the global collector."""

        reset_collector()
        assert recorder._metrics_collector is None


class TestMetricGroupsDisabled:
    """Tests for verifying metrics are not sent to Meter when their MetricGroup is disabled.

    These tests call recorder.record_*() functions and verify that no calls
    are made to the underlying Meter instruments (.add() or .record()).
    """

    def _create_collector_with_disabled_groups(self, mock_instruments, enabled_groups):
        """Helper to create a collector with specific metric groups enabled."""
        mock_meter = MagicMock()

        def create_counter_side_effect(name, **kwargs):
            instrument_map = {
                'redis.client.errors': mock_instruments.client_errors,
                'redis.client.maintenance.notifications': mock_instruments.maintenance_notifications,
                'db.client.connection.timeouts': mock_instruments.connection_timeouts,
                'redis.client.connection.closed': mock_instruments.connection_closed,
                'redis.client.connection.handoff': mock_instruments.connection_handoff,
                'redis.client.pubsub.messages': mock_instruments.pubsub_messages,
            }
            return instrument_map.get(name, MagicMock())

        def create_gauge_side_effect(name, **kwargs):
            instrument_map = {
                'db.client.connection.count': mock_instruments.connection_count,
            }
            return instrument_map.get(name, MagicMock())

        def create_up_down_counter_side_effect(name, **kwargs):
            instrument_map = {
                'redis.client.connection.relaxed_timeout': mock_instruments.connection_relaxed_timeout,
            }
            return instrument_map.get(name, MagicMock())

        def create_histogram_side_effect(name, **kwargs):
            instrument_map = {
                'db.client.connection.create_time': mock_instruments.connection_create_time,
                'db.client.connection.wait_time': mock_instruments.connection_wait_time,
                'db.client.connection.use_time': mock_instruments.connection_use_time,
                'db.client.operation.duration': mock_instruments.operation_duration,
                'redis.client.stream.lag': mock_instruments.stream_lag,
            }
            return instrument_map.get(name, MagicMock())

        mock_meter.create_counter.side_effect = create_counter_side_effect
        # The RedisMetricsCollector uses create_observable_gauge in the implementation,
        # so we need to mock that here to ensure the tests observe the correct behavior.
        mock_meter.create_observable_gauge.side_effect = create_gauge_side_effect
        # Keep create_gauge mocked as well in case it is used elsewhere.
        mock_meter.create_gauge.side_effect = create_gauge_side_effect
        mock_meter.create_up_down_counter.side_effect = create_up_down_counter_side_effect
        mock_meter.create_histogram.side_effect = create_histogram_side_effect

        config = OTelConfig(metric_groups=enabled_groups)

        with patch('redis.observability.metrics.OTEL_AVAILABLE', True):
            return RedisMetricsCollector(mock_meter, config)

    def test_record_operation_duration_no_meter_call_when_command_disabled(self):
        """Test that record_operation_duration makes no Meter calls when COMMAND group is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.RESILIENCY]  # No COMMAND
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            record_operation_duration(
                command_name='SET',
                duration_seconds=0.005,
                server_address='localhost',
                server_port=6379,
            )

        # Verify no call to the histogram's record method
        instruments.operation_duration.record.assert_not_called()

    def test_record_connection_create_time_no_meter_call_when_connection_basic_disabled(self):
        """Test that record_connection_create_time makes no Meter calls when CONNECTION_BASIC is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND]  # No CONNECTION_BASIC
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            record_connection_create_time(
                connection_pool='test-pool',
                duration_seconds=0.050,
            )

        # Verify no call to the histogram's record method
        instruments.connection_create_time.record.assert_not_called()

    def test_record_connection_wait_time_no_meter_call_when_connection_advanced_disabled(self):
        """Test that record_connection_wait_time makes no Meter calls when CONNECTION_ADVANCED is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND]  # No CONNECTION_ADVANCED
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            record_connection_wait_time(
                pool_name='test-pool',
                duration_seconds=0.010,
            )

        # Verify no call to the histogram's record method
        instruments.connection_wait_time.record.assert_not_called()

    def test_record_connection_use_time_no_meter_call_when_connection_advanced_disabled(self):
        """Test that record_connection_use_time makes no Meter calls when CONNECTION_ADVANCED is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND]  # No CONNECTION_ADVANCED
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            record_connection_use_time(
                pool_name='test-pool',
                duration_seconds=0.100,
            )

        # Verify no call to the histogram's record method
        instruments.connection_use_time.record.assert_not_called()

    def test_record_connection_closed_no_meter_call_when_connection_advanced_disabled(self):
        """Test that record_connection_closed makes no Meter calls when CONNECTION_ADVANCED is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND]  # No CONNECTION_ADVANCED
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            record_connection_closed(
                close_reason=CloseReason.APPLICATION_CLOSE,
            )

        # Verify no call to the counter's add method
        instruments.connection_closed.add.assert_not_called()

    def test_record_connection_relaxed_timeout_no_meter_call_when_connection_basic_disabled(self):
        """Test that record_connection_relaxed_timeout makes no Meter calls when CONNECTION_BASIC is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND]  # No CONNECTION_BASIC
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            record_connection_relaxed_timeout(
                connection_name='test-pool',
                maint_notification='MOVING',
                relaxed=True,
            )

        # Verify no call to the up_down_counter's add method
        instruments.connection_relaxed_timeout.add.assert_not_called()

    def test_record_pubsub_message_no_meter_call_when_pubsub_disabled(self):
        """Test that record_pubsub_message makes no Meter calls when PUBSUB group is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND]  # No PUBSUB
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            record_pubsub_message(
                direction=PubSubDirection.PUBLISH,
                channel='test-channel',
            )

        # Verify no call to the counter's add method
        instruments.pubsub_messages.add.assert_not_called()

    def test_record_streaming_lag_no_meter_call_when_streaming_disabled(self):
        """Test that record_streaming_lag makes no Meter calls when STREAMING group is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND]  # No STREAMING
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            record_streaming_lag(
                lag_seconds=0.150,
                stream_name='test-stream',
                consumer_group='test-group',
                consumer_name='test-consumer',
            )

        # Verify no call to the histogram's record method
        instruments.stream_lag.record.assert_not_called()

    def test_record_error_count_no_meter_call_when_resiliency_disabled(self):
        """Test that record_error_count makes no Meter calls when RESILIENCY group is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND]  # No RESILIENCY
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            record_error_count(
                server_address='localhost',
                server_port=6379,
                network_peer_address='127.0.0.1',
                network_peer_port=6379,
                error_type=Exception('test error'),
                retry_attempts=0,
            )

        # Verify no call to the counter's add method
        instruments.client_errors.add.assert_not_called()

    def test_record_maint_notification_count_no_meter_call_when_resiliency_disabled(self):
        """Test that record_maint_notification_count makes no Meter calls when RESILIENCY group is disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND]  # No RESILIENCY
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            recorder.record_maint_notification_count(
                server_address='localhost',
                server_port=6379,
                network_peer_address='127.0.0.1',
                network_peer_port=6379,
                maint_notification='MOVING',
            )

        # Verify no call to the counter's add method
        instruments.maintenance_notifications.add.assert_not_called()

    def test_all_record_functions_no_meter_calls_when_all_groups_disabled(self):
        """Test that all record_* functions make no Meter calls when all groups are disabled."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            []  # No metric groups enabled
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            # Call all record functions
            record_operation_duration('GET', 0.001, 'localhost', 6379)
            record_connection_create_time('pool', 0.050)
            record_connection_timeout('pool')
            record_connection_wait_time('pool', 0.010)
            record_connection_use_time('pool', 0.100)
            record_connection_closed('pool', 'shutdown')
            record_connection_relaxed_timeout('pool', 'MOVING', True)
            record_connection_handoff('pool')
            record_error_count('localhost', 6379, '127.0.0.1', 6379, Exception('err'), 0)
            recorder.record_maint_notification_count('localhost', 6379, '127.0.0.1', 6379, 'MOVING')
            record_pubsub_message(PubSubDirection.PUBLISH, 'channel')
            record_streaming_lag(0.150, 'stream', 'group', 'consumer')

        # Verify no Meter instrument methods were called
        instruments.operation_duration.record.assert_not_called()
        instruments.connection_create_time.record.assert_not_called()
        instruments.connection_count.set.assert_not_called()
        instruments.connection_timeouts.add.assert_not_called()
        instruments.connection_wait_time.record.assert_not_called()
        instruments.connection_use_time.record.assert_not_called()
        instruments.connection_closed.add.assert_not_called()
        instruments.connection_relaxed_timeout.add.assert_not_called()
        instruments.connection_handoff.add.assert_not_called()
        instruments.client_errors.add.assert_not_called()
        instruments.maintenance_notifications.add.assert_not_called()
        instruments.pubsub_messages.add.assert_not_called()
        instruments.stream_lag.record.assert_not_called()

    def test_enabled_group_receives_meter_calls_disabled_group_does_not(self):
        """Test that only enabled groups receive Meter calls."""
        instruments = MockInstruments()
        collector = self._create_collector_with_disabled_groups(
            instruments,
            [MetricGroup.COMMAND, MetricGroup.PUBSUB]  # Only COMMAND and PUBSUB enabled
        )

        recorder.reset_collector()
        with patch.object(recorder, '_get_or_create_collector', return_value=collector):
            # Call functions from enabled groups
            record_operation_duration('GET', 0.001, 'localhost', 6379)
            record_pubsub_message(PubSubDirection.PUBLISH, 'channel')

            # Call functions from disabled groups
            record_error_count('localhost', 6379, '127.0.0.1', 6379, Exception('err'), 0)
            recorder.record_maint_notification_count('localhost', 6379, '127.0.0.1', 6379, 'MOVING')
            record_streaming_lag(0.150, 'stream', 'group', 'consumer')

        # Enabled groups should have received Meter calls
        instruments.operation_duration.record.assert_called_once()
        instruments.pubsub_messages.add.assert_called_once()

        # Disabled groups should NOT have received Meter calls
        instruments.connection_count.set.assert_not_called()
        instruments.client_errors.add.assert_not_called()
        instruments.maintenance_notifications.add.assert_not_called()
        instruments.stream_lag.record.assert_not_called()
