"""
Unit tests that verify error metrics are properly recorded from async Redis client
via record_error_count function calls.

These tests use fully mocked connection and connection pool - no real Redis
or OTel integration is used.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

import redis.asyncio as redis
from redis.asyncio.observability import recorder as async_recorder
from redis.observability.attributes import (
    SERVER_ADDRESS,
    SERVER_PORT,
)
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector


@pytest.mark.asyncio
class TestAsyncRedisClientErrorMetricsRecording:
    """
    Unit tests that verify error metrics are properly recorded from async Redis client
    via record_error_count function calls.

    These tests use fully mocked connection and connection pool - no real Redis
    or OTel integration is used.
    """

    @pytest.fixture
    def mock_async_connection(self):
        """Create a mock async connection with required attributes."""
        conn = MagicMock()
        conn.host = "localhost"
        conn.port = 6379
        conn.db = 0
        conn.should_reconnect.return_value = False

        # Create a real Retry object that just executes the function directly
        async def mock_call_with_retry(
            do, fail, is_retryable=None, with_failure_count=False
        ):
            return await do()

        conn.retry = MagicMock()
        conn.retry.call_with_retry = mock_call_with_retry
        conn.retry.get_retries.return_value = 0

        return conn

    @pytest.fixture
    def mock_async_connection_pool(self, mock_async_connection):
        """Create a mock async connection pool."""
        pool = MagicMock()
        pool.get_connection = AsyncMock(return_value=mock_async_connection)
        pool.release = AsyncMock()
        pool.get_encoder.return_value = MagicMock()
        pool.get_protocol.return_value = 2
        return pool

    @pytest.fixture
    def mock_error_meter(self):
        """Create a mock Meter that tracks error counter calls."""
        meter = MagicMock()

        # Create mock counter for client errors
        self.client_errors = MagicMock()

        def create_counter_side_effect(name, **kwargs):
            if name == "redis.client.errors":
                return self.client_errors
            return MagicMock()

        meter.create_counter.side_effect = create_counter_side_effect
        meter.create_up_down_counter.return_value = MagicMock()
        meter.create_histogram.return_value = MagicMock()
        meter.create_observable_gauge.return_value = MagicMock()

        return meter

    async def test_execute_command_error_records_error_count(
        self, mock_async_connection_pool, mock_async_connection, mock_error_meter
    ):
        """
        Test that when a command execution raises an exception,
        error count is recorded via record_error_count.
        """
        async_recorder.reset_collector()
        # Enable RESILIENCY metric group for error counting
        config = OTelConfig(metric_groups=[MetricGroup.RESILIENCY])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_error_meter, config)

        with patch.object(
            async_recorder,
            "_get_or_create_collector",
            new=AsyncMock(return_value=collector),
        ):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            # Make command raise an exception
            test_error = redis.ResponseError("WRONGTYPE Operation error")

            async def raise_error(*args, **kwargs):
                raise test_error

            client._send_command_parse_response = raise_error

            # Execute should raise the error
            with pytest.raises(redis.ResponseError):
                await client.execute_command("LPUSH", "string_key", "value")

            # Verify record_error_count was called (via client_errors counter)
            self.client_errors.add.assert_called_once()

            # Verify error type is recorded in attributes
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert "error.type" in attrs
            assert attrs["error.type"] == "ResponseError"

        async_recorder.reset_collector()

    async def test_connection_error_records_error_count(
        self, mock_async_connection_pool, mock_async_connection, mock_error_meter
    ):
        """
        Test that ConnectionError is recorded via record_error_count.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.RESILIENCY])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_error_meter, config)

        with patch.object(
            async_recorder,
            "_get_or_create_collector",
            new=AsyncMock(return_value=collector),
        ):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            # Make command raise a ConnectionError
            async def raise_connection_error(*args, **kwargs):
                raise redis.ConnectionError("Connection refused")

            client._send_command_parse_response = raise_connection_error

            # Execute should raise the error
            with pytest.raises(redis.ConnectionError):
                await client.execute_command("GET", "key")

            # Verify record_error_count was called
            self.client_errors.add.assert_called_once()

            # Verify error type is ConnectionError
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert attrs["error.type"] == "ConnectionError"

        async_recorder.reset_collector()

    async def test_timeout_error_records_error_count(
        self, mock_async_connection_pool, mock_async_connection, mock_error_meter
    ):
        """
        Test that TimeoutError is recorded via record_error_count.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.RESILIENCY])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_error_meter, config)

        with patch.object(
            async_recorder,
            "_get_or_create_collector",
            new=AsyncMock(return_value=collector),
        ):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            # Make command raise a TimeoutError
            async def raise_timeout_error(*args, **kwargs):
                raise redis.TimeoutError("Connection timed out")

            client._send_command_parse_response = raise_timeout_error

            # Execute should raise the error
            with pytest.raises(redis.TimeoutError):
                await client.execute_command("GET", "key")

            # Verify record_error_count was called
            self.client_errors.add.assert_called_once()

            # Verify error type is TimeoutError
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert attrs["error.type"] == "TimeoutError"

        async_recorder.reset_collector()

    async def test_error_count_includes_server_attributes(
        self, mock_async_connection_pool, mock_async_connection, mock_error_meter
    ):
        """
        Test that error count includes server address and port attributes.
        """
        async_recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.RESILIENCY])

        with patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_error_meter, config)

        with patch.object(
            async_recorder,
            "_get_or_create_collector",
            new=AsyncMock(return_value=collector),
        ):
            client = redis.Redis(connection_pool=mock_async_connection_pool)

            async def raise_error(*args, **kwargs):
                raise redis.ResponseError("Error")

            client._send_command_parse_response = raise_error

            with pytest.raises(redis.ResponseError):
                await client.execute_command("SET", "key", "value")

            # Verify server attributes are recorded
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert attrs[SERVER_ADDRESS] == "localhost"
            assert attrs[SERVER_PORT] == 6379

        async_recorder.reset_collector()
