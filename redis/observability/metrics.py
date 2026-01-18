"""
OpenTelemetry metrics collector for redis-py.

This module defines and manages all metric instruments according to
OTel semantic conventions for database clients.
"""

import logging
import time
from enum import Enum
from typing import Any, Dict, Optional, Callable, List

from redis.observability.attributes import AttributeBuilder, ConnectionState, REDIS_CLIENT_CONNECTION_NOTIFICATION, \
    REDIS_CLIENT_CONNECTION_CLOSE_REASON, ERROR_TYPE, PubSubDirection
from redis.observability.config import OTelConfig, MetricGroup

logger = logging.getLogger(__name__)

# Optional imports - OTel SDK may not be installed
try:
    from opentelemetry.metrics import Counter, Histogram, Meter, UpDownCounter

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    Counter = None
    Histogram = None
    Meter = None
    UpDownCounter = None

class CloseReason(Enum):
    APPLICATION_CLOSE = "application_close"
    ERROR = "error"
    HEALTHCHECK_FAILED = "healthcheck_failed"


class RedisMetricsCollector:
    """
    Collects and records OpenTelemetry metrics for Redis operations.

    This class manages all metric instruments and provides methods to record
    various Redis operations including connection pool events, command execution,
    and cluster-specific operations.

    Args:
        meter: OpenTelemetry Meter instance
        config: OTel configuration object
    """

    METER_NAME = "redis-py"
    METER_VERSION = "1.0.0"

    def __init__(self, meter: Meter, config: OTelConfig):
        if not OTEL_AVAILABLE:
            raise ImportError(
                "OpenTelemetry API is not installed. "
                "Install it with: pip install opentelemetry-api"
            )

        self.meter = meter
        self.config = config
        self.attr_builder = AttributeBuilder()
        self.connection_count = None

        # Initialize enabled metric instruments

        if MetricGroup.RESILIENCY in self.config.metric_groups:
            self._init_resiliency_metrics()

        if MetricGroup.COMMAND in self.config.metric_groups:
            self._init_command_metrics()

        if MetricGroup.CONNECTION_BASIC in self.config.metric_groups:
            self._init_connection_basic_metrics()

        if MetricGroup.CONNECTION_ADVANCED in self.config.metric_groups:
            self._init_connection_advanced_metrics()

        if MetricGroup.PUBSUB in self.config.metric_groups:
            self._init_pubsub_metrics()

        if MetricGroup.STREAMING in self.config.metric_groups:
            self._init_streaming_metrics()

        logger.info("RedisMetricsCollector initialized")

    def _init_resiliency_metrics(self) -> None:
        """Initialize resiliency metrics."""
        self.client_errors = self.meter.create_counter(
            name="redis.client.errors",
            unit="{error}",
            description="A counter of all errors (both returned to the user and handled internally in the client library)",
        )

        self.maintenance_notifications = self.meter.create_counter(
            name="redis.client.maintenance.notifications",
            unit="{notification}",
            description="Tracks server-side maintenance notifications"
        )

    def _init_connection_basic_metrics(self) -> None:
        """Initialize basic connection metrics."""
        self.connection_create_time = self.meter.create_histogram(
            name="db.client.connection.create_time",
            unit="{seconds}",
            description="Time to create a new connection",
        )

        self.connection_relaxed_timeout = self.meter.create_up_down_counter(
            name="redis.client.connection.relaxed_timeout",
            unit="{relaxation}",
            description="Counts up for relaxed timeout, counts down for unrelaxed timeout",
        )

        self.connection_handoff = self.meter.create_counter(
            name="redis.client.connection.handoff",
            unit="{handoff}",
            description="Connections that have been handed off (e.g., after a MOVING notification)",
        )

    def _init_connection_advanced_metrics(self) -> None:
        """Initialize advanced connection metrics."""
        self.connection_timeouts = self.meter.create_counter(
            name="db.client.connection.timeouts",
            unit="{timeout}",
            description="The number of connection timeouts that have occurred trying to obtain a connection from the pool.",
        )

        self.connection_wait_time = self.meter.create_histogram(
            name="db.client.connection.wait_time",
            unit="{seconds}",
            description="Time to obtain an open connection from the pool",
        )

        self.connection_use_time = self.meter.create_histogram(
            name="db.client.connection.use_time",
            unit="{seconds}",
            description="Time between borrowing and returning a connection",
        )

        self.connection_closed = self.meter.create_counter(
            name="redis.client.connection.closed",
            unit="{connection}",
            description="Total number of closed connections",
        )


    def _init_command_metrics(self) -> None:
        """Initialize command execution metric instruments."""
        self.operation_duration = self.meter.create_histogram(
            name="db.client.operation.duration",
            unit="{seconds}",
            description="Command execution duration",
        )

    def _init_pubsub_metrics(self) -> None:
        """Initialize PubSub metric instruments."""
        self.pubsub_messages = self.meter.create_counter(
            name="redis.client.pubsub.messages",
            unit="{message}",
            description="Tracks published and received messages",
        )

    def _init_streaming_metrics(self) -> None:
        """Initialize Streaming metric instruments."""
        self.stream_lag = self.meter.create_histogram(
            name="redis.client.stream.lag",
            unit="{seconds}",
            description="End-to-end lag per message, showing how stale are the messages when the application starts processing them."
        )

    # Resiliency metric recording methods

    def record_error_count(
            self,
            server_address: str,
            server_port: int,
            network_peer_address: str,
            network_peer_port: int,
            error_type: Exception,
            retry_attempts: int,
            is_internal: bool,
    ):
        """
        Record error count

        Args:
            server_address: Server address
            server_port: Server port
            network_peer_address: Network peer address
            network_peer_port: Network peer port
            error_type: Error type
            retry_attempts: Retry attempts
            is_internal: Whether the error is internal (e.g., timeout, network error)
        """
        if not hasattr(self, "client_errors"):
            return

        attrs = self.attr_builder.build_base_attributes(
            server_address=server_address,
            server_port=server_port,
        )
        attrs.update(
            self.attr_builder.build_operation_attributes(
                network_peer_address=network_peer_address,
                network_peer_port=network_peer_port,
                retry_attempts=retry_attempts,
            )
        )

        attrs.update(
            self.attr_builder.build_error_attributes(
                error_type=error_type,
                is_internal=is_internal,
            )
        )

        self.client_errors.add(1, attributes=attrs)

    def record_maint_notification_count(
            self,
            server_address: str,
            server_port: int,
            network_peer_address: str,
            network_peer_port: int,
            maint_notification: str
    ):
        """
        Record maintenance notification count

        Args:
            server_address: Server address
            server_port: Server port
            network_peer_address: Network peer address
            network_peer_port: Network peer port
            maint_notification: Maintenance notification
        """
        if not hasattr(self, "maintenance_notifications"):
            return

        attrs = self.attr_builder.build_base_attributes(
            server_address=server_address,
            server_port=server_port,
        )

        attrs.update(
            self.attr_builder.build_operation_attributes(
                network_peer_address=network_peer_address,
                network_peer_port=network_peer_port,
            )
        )

        attrs[REDIS_CLIENT_CONNECTION_NOTIFICATION] = maint_notification
        self.maintenance_notifications.add(1, attributes=attrs)

    def init_connection_count(
            self,
            callback: Callable,
    ) -> None:
        """
        Initialize observable gauge for connection count metric.

        Args:
            callback: Callback function to retrieve connection count
        """
        if not MetricGroup.CONNECTION_BASIC in self.config.metric_groups:
            return

        self.connection_count = self.meter.create_observable_gauge(
            name="db.client.connection.count",
            unit="connections",
            description="Number of connections in the pool",
            callbacks=[callback],
        )

    def record_connection_timeout(self, pool_name: str) -> None:
        """
        Record a connection timeout event.

        Args:
            pool_name: Connection pool name
        """
        if not hasattr(self, "connection_timeouts"):
            return

        attrs = self.attr_builder.build_connection_attributes(pool_name=pool_name)
        self.connection_timeouts.add(1, attributes=attrs)

    def record_connection_create_time(
            self,
            connection_pool: "ConnectionPoolInterface",
            duration_seconds: float,
    ) -> None:
        """
        Record time taken to create a new connection.

        Args:
            connection_pool: Connection pool implementation
            duration_seconds: Creation time in seconds
        """
        if not hasattr(self, "connection_create_time"):
            return

        attrs = self.attr_builder.build_connection_attributes(pool_name=repr(connection_pool))
        self.connection_create_time.record(duration_seconds, attributes=attrs)

    def record_connection_wait_time(
            self,
            pool_name: str,
            duration_seconds: float,
    ) -> None:
        """
        Record time taken to obtain a connection from the pool.

        Args:
            pool_name: Connection pool name
            duration_seconds: Wait time in seconds
        """
        if not hasattr(self, "connection_wait_time"):
            return

        attrs = self.attr_builder.build_connection_attributes(pool_name=pool_name)
        self.connection_wait_time.record(duration_seconds, attributes=attrs)

    def record_connection_use_time(
            self,
            pool_name: str,
            duration_seconds: float,
    ) -> None:
        """
        Record time a connection was in use (borrowed from pool).

        Args:
            pool_name: Connection pool name
            duration_seconds: Use time in seconds
        """
        if not hasattr(self, "connection_use_time"):
            return

        attrs = self.attr_builder.build_connection_attributes(pool_name=pool_name)
        self.connection_use_time.record(duration_seconds, attributes=attrs)

    # Command execution metric recording methods

    def record_operation_duration(
            self,
            command_name: str,
            duration_seconds: float,
            server_address: Optional[str] = None,
            server_port: Optional[int] = None,
            db_namespace: Optional[int] = None,
            batch_size: Optional[int] = None,
            error_type: Optional[Exception] = None,
            network_peer_address: Optional[str] = None,
            network_peer_port: Optional[int] = None,
            retry_attempts: Optional[int] = None,
            is_blocking: Optional[bool] = None,
    ) -> None:

        """
        Record command execution duration.

        Args:
            command_name: Redis command name (e.g., 'GET', 'SET', 'MULTI')
            duration_seconds: Execution time in seconds
            server_address: Redis server address
            server_port: Redis server port
            db_namespace: Redis database index
            batch_size: Number of commands in batch (for pipelines/transactions)
            error_type: Error type if operation failed
            network_peer_address: Resolved peer address
            network_peer_port: Peer port number
            retry_attempts: Number of retry attempts made
            is_blocking: Whether the operation is a blocking command
        """
        if not hasattr(self, "operation_duration"):
            return

        # Check if this command should be tracked
        if not self.config.should_track_command(command_name):
            return

        # Build attributes
        attrs = self.attr_builder.build_base_attributes(
            server_address=server_address,
            server_port=server_port,
            db_namespace=db_namespace,
        )

        attrs.update(
            self.attr_builder.build_operation_attributes(
                command_name=command_name,
                batch_size=batch_size,
                network_peer_address=network_peer_address,
                network_peer_port=network_peer_port,
                retry_attempts=retry_attempts,
                is_blocking=is_blocking,
            )
        )

        attrs.update(
            self.attr_builder.build_error_attributes(
                error_type=error_type,
            )
        )
        self.operation_duration.record(duration_seconds, attributes=attrs)

    def record_connection_closed(
            self,
            close_reason: Optional[CloseReason] = None,
            error_type: Optional[Exception] = None,
    ) -> None:
        """
        Record a connection closed event.

        Args:
            close_reason: Reason for closing (e.g. 'error', 'application_close')
            error_type: Error type if closed due to error
        """
        if not hasattr(self, "connection_closed"):
            return

        attrs = self.attr_builder.build_connection_attributes()
        if close_reason:
            attrs[REDIS_CLIENT_CONNECTION_CLOSE_REASON] = close_reason.value

        attrs.update(
            self.attr_builder.build_error_attributes(
                error_type=error_type,
            )
        )

        self.connection_closed.add(1, attributes=attrs)

    def record_connection_relaxed_timeout(
            self,
            connection_name: str,
            maint_notification: str,
            relaxed: bool,
    ) -> None:
        """
        Record a connection timeout relaxation event.

        Args:
            connection_name: Connection name
            maint_notification: Maintenance notification type
            relaxed: True to count up (relaxed), False to count down (unrelaxed)
        """
        if not hasattr(self, "connection_relaxed_timeout"):
            return

        attrs = self.attr_builder.build_connection_attributes(connection_name=connection_name)
        attrs[REDIS_CLIENT_CONNECTION_NOTIFICATION] = maint_notification
        self.connection_relaxed_timeout.add(1 if relaxed else -1, attributes=attrs)

    def record_connection_handoff(
            self,
            pool_name: str,
    ) -> None:
        """
        Record a connection handoff event (e.g., after MOVING notification).

        Args:
            pool_name: Connection pool name
        """
        if not hasattr(self, "connection_handoff"):
            return

        attrs = self.attr_builder.build_connection_attributes(pool_name=pool_name)
        self.connection_handoff.add(1, attributes=attrs)

    # PubSub metric recording methods

    def record_pubsub_message(
            self,
            direction: PubSubDirection,
            channel: Optional[str] = None,
            sharded: Optional[bool] = None,
    ) -> None:
        """
        Record a PubSub message (published or received).

        Args:
            direction: Message direction ('publish' or 'receive')
            channel: Pub/Sub channel name
            sharded: True if sharded Pub/Sub channel
        """
        if not hasattr(self, "pubsub_messages"):
            return

        attrs = self.attr_builder.build_pubsub_message_attributes(
            direction=direction,
            channel=channel,
            sharded=sharded,
        )
        self.pubsub_messages.add(1, attributes=attrs)

    # Streaming metric recording methods

    def record_streaming_lag(
            self,
            lag_seconds: float,
            stream_name: Optional[str] = None,
            consumer_group: Optional[str] = None,
            consumer_name: Optional[str] = None,
    ) -> None:
        """
        Record the lag of a streaming message.

        Args:
            lag_seconds: Lag in seconds
            stream_name: Stream name
            consumer_group: Consumer group name
            consumer_name: Consumer name
        """
        if not hasattr(self, "stream_lag"):
            return

        attrs = self.attr_builder.build_streaming_attributes(
            stream_name=stream_name,
            consumer_group=consumer_group,
            consumer_name=consumer_name,
        )
        self.stream_lag.record(lag_seconds, attributes=attrs)

    # Utility methods

    @staticmethod
    def monotonic_time() -> float:
        """
        Get monotonic time for duration measurements.

        Returns:
            Current monotonic time in seconds
        """
        return time.monotonic()

    def __repr__(self) -> str:
        return f"RedisMetricsCollector(meter={self.meter}, config={self.config})"