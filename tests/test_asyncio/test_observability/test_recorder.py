"""
Unit tests for redis.asyncio.observability.recorder module.

These tests verify that async recorder functions correctly pass arguments through
to the underlying OTel Meter instruments (Counter, Histogram, UpDownCounter).
The MeterProvider is mocked to verify the actual integration point where
metrics are exported to OTel.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from redis.asyncio.observability import recorder
from redis.observability.attributes import (
    PubSubDirection,
    SERVER_ADDRESS,
    SERVER_PORT,
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_RESPONSE_STATUS_CODE,
    ERROR_TYPE,
    DB_CLIENT_CONNECTION_POOL_NAME,
)
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector, CloseReason
from redis.observability.registry import get_observables_registry_instance


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
            "redis.client.errors": mock_instruments.client_errors,
            "redis.client.maintenance.notifications": mock_instruments.maintenance_notifications,
            "db.client.connection.timeouts": mock_instruments.connection_timeouts,
            "redis.client.connection.closed": mock_instruments.connection_closed,
            "redis.client.connection.handoff": mock_instruments.connection_handoff,
            "redis.client.pubsub.messages": mock_instruments.pubsub_messages,
        }
        return instrument_map.get(name, MagicMock())

    def create_gauge_side_effect(name, **kwargs):
        instrument_map = {
            "db.client.connection.count": mock_instruments.connection_count,
        }
        return instrument_map.get(name, MagicMock())

    def create_up_down_counter_side_effect(name, **kwargs):
        instrument_map = {
            "redis.client.connection.relaxed_timeout": mock_instruments.connection_relaxed_timeout,
        }
        return instrument_map.get(name, MagicMock())

    def create_histogram_side_effect(name, **kwargs):
        instrument_map = {
            "db.client.connection.create_time": mock_instruments.connection_create_time,
            "db.client.connection.wait_time": mock_instruments.connection_wait_time,
            "db.client.connection.use_time": mock_instruments.connection_use_time,
            "db.client.operation.duration": mock_instruments.operation_duration,
            "redis.client.stream.lag": mock_instruments.stream_lag,
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
    return OTelConfig(
        metric_groups=[
            MetricGroup.RESILIENCY,
            MetricGroup.CONNECTION_BASIC,
            MetricGroup.CONNECTION_ADVANCED,
            MetricGroup.COMMAND,
            MetricGroup.PUBSUB,
            MetricGroup.STREAMING,
        ]
    )


@pytest.fixture
def metrics_collector(mock_meter, mock_config):
    """Create a real RedisMetricsCollector with mocked Meter."""
    with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
        collector = RedisMetricsCollector(mock_meter, mock_config)
        return collector


@pytest.fixture
def setup_async_recorder(metrics_collector, mock_instruments):
    """
    Setup the async recorder module with our collector that has mocked instruments.
    """
    # Reset the global collector before test
    recorder.reset_collector()
    get_observables_registry_instance().clear()

    # Patch _get_or_create_collector to return our collector with mocked instruments
    with patch.object(
        recorder,
        "_get_or_create_collector",
        new_callable=lambda: AsyncMock(return_value=metrics_collector),
    ):
        yield mock_instruments

    # Reset after test
    recorder.reset_collector()
    get_observables_registry_instance().clear()


@pytest.mark.asyncio
class TestRecordOperationDuration:
    """Tests for record_operation_duration - verifies Histogram.record() calls."""

    async def test_record_operation_duration_success(self, setup_async_recorder):
        """Test that operation duration is recorded to the histogram with correct attributes."""
        instruments = setup_async_recorder

        await recorder.record_operation_duration(
            command_name="SET",
            duration_seconds=0.005,
            server_address="localhost",
            server_port=6379,
            db_namespace="0",
            error=None,
        )

        # Verify histogram.record() was called
        instruments.operation_duration.record.assert_called_once()
        call_args = instruments.operation_duration.record.call_args

        # Verify duration value
        assert call_args[0][0] == 0.005

        # Verify attributes
        attrs = call_args[1]["attributes"]
        assert attrs[SERVER_ADDRESS] == "localhost"
        assert attrs[SERVER_PORT] == 6379
        assert attrs[DB_NAMESPACE] == "0"
        assert attrs[DB_OPERATION_NAME] == "SET"

    async def test_record_operation_duration_with_error(self, setup_async_recorder):
        """Test that error information is included in attributes."""
        instruments = setup_async_recorder

        error = ConnectionError("Connection refused")
        await recorder.record_operation_duration(
            command_name="GET",
            duration_seconds=0.001,
            server_address="localhost",
            server_port=6379,
            error=error,
        )

        instruments.operation_duration.record.assert_called_once()
        call_args = instruments.operation_duration.record.call_args

        attrs = call_args[1]["attributes"]
        assert attrs[DB_OPERATION_NAME] == "GET"
        assert attrs[DB_RESPONSE_STATUS_CODE] == "error"
        assert attrs[ERROR_TYPE] == "ConnectionError"


@pytest.mark.asyncio
class TestRecordConnectionCreateTime:
    """Tests for record_connection_create_time - verifies Histogram.record() calls."""

    async def test_record_connection_create_time(self, setup_async_recorder):
        """Test that connection create time is recorded correctly."""
        instruments = setup_async_recorder

        mock_pool = MagicMock()
        mock_pool.__class__.__name__ = "ConnectionPool"
        mock_pool.connection_kwargs = {"host": "localhost", "port": 6379, "db": 0}

        await recorder.record_connection_create_time(
            connection_pool=mock_pool,
            duration_seconds=0.050,
        )

        instruments.connection_create_time.record.assert_called_once()
        call_args = instruments.connection_create_time.record.call_args

        assert call_args[0][0] == 0.050
        attrs = call_args[1]["attributes"]
        # Pool name is generated from class name and connection kwargs
        assert "localhost:6379/0" in attrs[DB_CLIENT_CONNECTION_POOL_NAME]


@pytest.mark.asyncio
class TestRecordConnectionTimeout:
    """Tests for record_connection_timeout - verifies Counter.add() calls."""

    async def test_record_connection_timeout(self, setup_async_recorder):
        """Test that connection timeout is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_connection_timeout(pool_name="test_pool")

        instruments.connection_timeouts.add.assert_called_once()
        call_args = instruments.connection_timeouts.add.call_args

        assert call_args[0][0] == 1
        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "test_pool"


@pytest.mark.asyncio
class TestRecordConnectionWaitTime:
    """Tests for record_connection_wait_time - verifies Histogram.record() calls."""

    async def test_record_connection_wait_time(self, setup_async_recorder):
        """Test that connection wait time is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_connection_wait_time(
            pool_name="test_pool",
            duration_seconds=0.010,
        )

        instruments.connection_wait_time.record.assert_called_once()
        call_args = instruments.connection_wait_time.record.call_args

        assert call_args[0][0] == 0.010
        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "test_pool"


@pytest.mark.asyncio
class TestRecordConnectionUseTime:
    """Tests for record_connection_use_time - verifies Histogram.record() calls."""

    async def test_record_connection_use_time(self, setup_async_recorder):
        """Test that connection use time is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_connection_use_time(
            pool_name="test_pool",
            duration_seconds=0.100,
        )

        instruments.connection_use_time.record.assert_called_once()
        call_args = instruments.connection_use_time.record.call_args

        assert call_args[0][0] == 0.100
        attrs = call_args[1]["attributes"]
        assert attrs[DB_CLIENT_CONNECTION_POOL_NAME] == "test_pool"


@pytest.mark.asyncio
class TestRecordConnectionClosed:
    """Tests for record_connection_closed - verifies Counter.add() calls."""

    async def test_record_connection_closed(self, setup_async_recorder):
        """Test that connection closed is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_connection_closed(
            close_reason=CloseReason.ERROR,
            error_type=ConnectionError("Connection lost"),
        )

        instruments.connection_closed.add.assert_called_once()


@pytest.mark.asyncio
class TestRecordConnectionRelaxedTimeout:
    """Tests for record_connection_relaxed_timeout - verifies UpDownCounter calls."""

    async def test_record_connection_relaxed_timeout_relaxed(
        self, setup_async_recorder
    ):
        """Test that relaxed timeout is recorded correctly when relaxed=True."""
        instruments = setup_async_recorder

        await recorder.record_connection_relaxed_timeout(
            connection_name="conn1",
            maint_notification="MOVING",
            relaxed=True,
        )

        instruments.connection_relaxed_timeout.add.assert_called_once()
        call_args = instruments.connection_relaxed_timeout.add.call_args
        assert call_args[0][0] == 1  # +1 for relaxed

    async def test_record_connection_relaxed_timeout_unrelaxed(
        self, setup_async_recorder
    ):
        """Test that relaxed timeout is recorded correctly when relaxed=False."""
        instruments = setup_async_recorder

        await recorder.record_connection_relaxed_timeout(
            connection_name="conn1",
            maint_notification="MOVING",
            relaxed=False,
        )

        instruments.connection_relaxed_timeout.add.assert_called_once()
        call_args = instruments.connection_relaxed_timeout.add.call_args
        assert call_args[0][0] == -1  # -1 for unrelaxed


@pytest.mark.asyncio
class TestRecordConnectionHandoff:
    """Tests for record_connection_handoff - verifies Counter.add() calls."""

    async def test_record_connection_handoff(self, setup_async_recorder):
        """Test that connection handoff is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_connection_handoff(pool_name="test_pool")

        instruments.connection_handoff.add.assert_called_once()
        call_args = instruments.connection_handoff.add.call_args
        assert call_args[0][0] == 1


@pytest.mark.asyncio
class TestRecordErrorCount:
    """Tests for record_error_count - verifies Counter.add() calls."""

    async def test_record_error_count(self, setup_async_recorder):
        """Test that error count is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_error_count(
            server_address="localhost",
            server_port=6379,
            network_peer_address="127.0.0.1",
            network_peer_port=6379,
            error_type=ConnectionError("Connection refused"),
            retry_attempts=3,
            is_internal=True,
        )

        instruments.client_errors.add.assert_called_once()
        call_args = instruments.client_errors.add.call_args
        assert call_args[0][0] == 1


@pytest.mark.asyncio
class TestRecordPubsubMessage:
    """Tests for record_pubsub_message - verifies Counter.add() calls."""

    async def test_record_pubsub_message_publish(self, setup_async_recorder):
        """Test that pubsub publish message is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_pubsub_message(
            direction=PubSubDirection.PUBLISH,
            channel="test_channel",
            sharded=False,
        )

        instruments.pubsub_messages.add.assert_called_once()

    async def test_record_pubsub_message_receive(self, setup_async_recorder):
        """Test that pubsub receive message is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_pubsub_message(
            direction=PubSubDirection.RECEIVE,
            channel="test_channel",
            sharded=True,
        )

        instruments.pubsub_messages.add.assert_called_once()


@pytest.mark.asyncio
class TestRecordStreamingLag:
    """Tests for record_streaming_lag - verifies Histogram.record() calls."""

    async def test_record_streaming_lag(self, setup_async_recorder):
        """Test that streaming lag is recorded correctly."""
        instruments = setup_async_recorder

        await recorder.record_streaming_lag(
            lag_seconds=0.150,
            stream_name="test_stream",
            consumer_group="test_group",
            consumer_name="test_consumer",
        )

        instruments.stream_lag.record.assert_called_once()
        call_args = instruments.stream_lag.record.call_args
        assert call_args[0][0] == 0.150


@pytest.mark.asyncio
class TestRecorderDisabled:
    """Tests for recorder behavior when observability is disabled."""

    async def test_record_operation_duration_when_disabled(self):
        """Test that recording does nothing when collector is None."""
        recorder.reset_collector()

        with patch.object(
            recorder,
            "_get_or_create_collector",
            new_callable=lambda: AsyncMock(return_value=None),
        ):
            # Should not raise any exception
            await recorder.record_operation_duration(
                command_name="SET",
                duration_seconds=0.005,
                server_address="localhost",
                server_port=6379,
            )

        recorder.reset_collector()

    async def test_is_enabled_returns_false_when_disabled(self):
        """Test is_enabled returns False when collector is None."""
        recorder.reset_collector()

        with patch.object(
            recorder,
            "_get_or_create_collector",
            new_callable=lambda: AsyncMock(return_value=None),
        ):
            assert await recorder.is_enabled() is False

        recorder.reset_collector()

    async def test_all_record_functions_safe_when_disabled(self):
        """Test that all record functions are safe to call when disabled."""
        recorder.reset_collector()

        with patch.object(
            recorder,
            "_get_or_create_collector",
            new_callable=lambda: AsyncMock(return_value=None),
        ):
            # None of these should raise
            mock_pool = MagicMock()
            mock_pool.pool_name = "test_pool"

            await recorder.record_connection_create_time(mock_pool, 0.1)
            await recorder.record_connection_timeout("pool")
            await recorder.record_connection_wait_time("pool", 0.1)
            await recorder.record_connection_use_time("pool", 0.1)
            await recorder.record_connection_closed()
            await recorder.record_connection_relaxed_timeout("pool", "MOVING", True)
            await recorder.record_connection_handoff("pool")
            await recorder.record_error_count(
                "host", 6379, "127.0.0.1", 6379, Exception(), 0
            )
            await recorder.record_maint_notification_count(
                "host", 6379, "127.0.0.1", 6379, "MOVING"
            )
            await recorder.record_pubsub_message(PubSubDirection.PUBLISH)
            await recorder.record_streaming_lag(0.1, "stream", "group", "consumer")

        recorder.reset_collector()


@pytest.mark.asyncio
class TestObservableGaugeIntegration:
    """Integration tests for observable gauge pattern with registry."""

    @pytest.fixture
    def clean_registry(self):
        """Ensure clean registry before and after test."""
        registry = get_observables_registry_instance()
        registry.clear()
        yield
        registry.clear()

    async def test_full_observable_gauge_flow(
        self, clean_registry, mock_meter, mock_config
    ):
        """Test the complete flow: init -> register -> callback invocation."""

        # Create mock meter and collector
        captured_callback = None

        def capture_callback(name, **kwargs):
            nonlocal captured_callback
            captured_callback = kwargs.get("callbacks", [None])[0]
            return MagicMock()

        mock_meter.create_observable_gauge.side_effect = capture_callback

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, mock_config)

        with patch.object(
            recorder,
            "_get_or_create_collector",
            new_callable=lambda: AsyncMock(return_value=collector),
        ):
            # Step 1: Initialize the observable gauge
            await recorder.init_connection_count()

            # Step 2: Register pool callbacks
            mock_pool = MagicMock()
            mock_pool.get_connection_count.return_value = [
                (5, {"state": "idle", "pool": "pool1"}),
            ]
            await recorder.register_pools_connection_count([mock_pool])

            # Step 3: Simulate OTel calling the observable callback (sync)
            assert captured_callback is not None
            # The callback is now sync, so we call it directly
            observations = captured_callback(None)

            # Verify the observation was created correctly
            assert len(observations) == 1
            assert observations[0].value == 5

        recorder.reset_collector()

    async def test_observable_gauge_with_empty_registry(
        self, clean_registry, mock_meter, mock_config
    ):
        """Test observable gauge returns empty list when no callbacks registered."""
        captured_callback = None

        def capture_callback(name, **kwargs):
            nonlocal captured_callback
            captured_callback = kwargs.get("callbacks", [None])[0]
            return MagicMock()

        mock_meter.create_observable_gauge.side_effect = capture_callback

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, mock_config)

        with patch.object(
            recorder,
            "_get_or_create_collector",
            new_callable=lambda: AsyncMock(return_value=collector),
        ):
            await recorder.init_connection_count()

            # Don't register any pools - registry is empty
            assert captured_callback is not None
            # The callback is now sync, so we call it directly
            observations = captured_callback(None)

            # Should return empty list, not raise an error
            assert observations == []

        recorder.reset_collector()
