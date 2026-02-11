"""
Async-compatible API for recording observability metrics.

This module provides an async-safe interface for Redis async client code to record
metrics without needing to know about OpenTelemetry internals. It reuses the same
RedisMetricsCollector and configuration as the sync recorder.

Usage in Redis async client code:
    from redis.asyncio.observability.recorder import record_operation_duration

    start_time = time.monotonic()
    # ... execute Redis command ...
    await record_operation_duration(
        command_name='SET',
        duration_seconds=time.monotonic() - start_time,
        server_address='localhost',
        server_port=6379,
        db_namespace='0',
        error=None
    )
"""

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from redis.observability.attributes import (
    PubSubDirection,
)
from redis.observability.metrics import CloseReason, RedisMetricsCollector
from redis.observability.providers import get_observability_instance
from redis.observability.registry import get_observables_registry_instance
from redis.utils import str_if_bytes

if TYPE_CHECKING:
    from redis.connection import ConnectionPoolInterface
    from redis.observability.config import OTelConfig

# Global metrics collector instance (lazy-initialized)
_async_metrics_collector: Optional[RedisMetricsCollector] = None
_collector_lock = asyncio.Lock()

CONNECTION_COUNT_REGISTRY_KEY = "connection_count"
CSC_ITEMS_REGISTRY_KEY = "csc_items"


async def _get_or_create_collector() -> Optional[RedisMetricsCollector]:
    """
    Get or create the global metrics collector with async-safe initialization.

    Returns:
        RedisMetricsCollector instance if observability is enabled, None otherwise
    """
    global _async_metrics_collector

    if _async_metrics_collector is not None:
        return _async_metrics_collector

    async with _collector_lock:
        # Double-check after acquiring lock
        if _async_metrics_collector is not None:
            return _async_metrics_collector

        try:
            manager = get_observability_instance().get_provider_manager()
            if manager is None or not manager.config.enabled_telemetry:
                return None

            # Get meter from the global MeterProvider
            meter = manager.get_meter_provider().get_meter(
                RedisMetricsCollector.METER_NAME, RedisMetricsCollector.METER_VERSION
            )

            _async_metrics_collector = RedisMetricsCollector(meter, manager.config)
            return _async_metrics_collector

        except ImportError:
            # Observability module not available
            return None
        except Exception:
            # Any other error - don't break Redis operations
            return None


async def _get_config() -> Optional["OTelConfig"]:
    """
    Get the OTel configuration from the observability manager.

    Returns:
        OTelConfig instance if observability is enabled, None otherwise
    """
    try:
        manager = get_observability_instance().get_provider_manager()
        if manager is None:
            return None
        return manager.config
    except Exception:
        return None


async def record_operation_duration(
    command_name: str,
    duration_seconds: float,
    server_address: Optional[str] = None,
    server_port: Optional[int] = None,
    db_namespace: Optional[str] = None,
    error: Optional[Exception] = None,
    is_blocking: Optional[bool] = None,
    batch_size: Optional[int] = None,
    retry_attempts: Optional[int] = None,
) -> None:
    """
    Record a Redis command execution duration.

    This is an async-safe API that Redis async client code can call directly.
    If observability is not enabled, this returns immediately with zero overhead.

    Args:
        command_name: Redis command name (e.g., 'GET', 'SET')
        duration_seconds: Command execution time in seconds
        server_address: Redis server address
        server_port: Redis server port
        db_namespace: Redis database index
        error: Exception if command failed, None if successful
        is_blocking: Whether the operation is a blocking command
        batch_size: Number of commands in batch (for pipelines/transactions)
        retry_attempts: Number of retry attempts made

    Example:
        >>> start = time.monotonic()
        >>> # ... execute command ...
        >>> await record_operation_duration('SET', time.monotonic() - start, 'localhost', 6379, '0')
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    collector.record_operation_duration(
        command_name=command_name,
        duration_seconds=duration_seconds,
        server_address=server_address,
        server_port=server_port,
        db_namespace=db_namespace,
        error_type=error,
        network_peer_address=server_address,
        network_peer_port=server_port,
        is_blocking=is_blocking,
        batch_size=batch_size,
        retry_attempts=retry_attempts,
    )


async def record_connection_create_time(
    connection_pool: "ConnectionPoolInterface",
    duration_seconds: float,
) -> None:
    """
    Record connection creation time.

    Args:
        connection_pool: Connection pool implementation
        duration_seconds: Time taken to create connection in seconds
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    collector.record_connection_create_time(
        connection_pool=connection_pool,
        duration_seconds=duration_seconds,
    )


async def init_connection_count() -> None:
    """
    Initialize observable gauge for connection count metric.
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    def observable_callback(__):
        observables_registry = get_observables_registry_instance()
        callbacks = observables_registry.get(CONNECTION_COUNT_REGISTRY_KEY)
        observations = []

        for callback in callbacks:
            observations.extend(callback())

        return observations

    collector.init_connection_count(
        callback=observable_callback,
    )


async def register_pools_connection_count(
    connection_pools: List["ConnectionPoolInterface"],
) -> None:
    """
    Add connection pools to connection count observable registry.
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    # Lazy import
    from opentelemetry.metrics import Observation

    def connection_count_callback():
        observations = []
        for connection_pool in connection_pools:
            for count, attributes in connection_pool.get_connection_count():
                observations.append(Observation(count, attributes=attributes))
        return observations

    observables_registry = get_observables_registry_instance()
    observables_registry.register(CONNECTION_COUNT_REGISTRY_KEY, connection_count_callback)


async def record_connection_timeout(
    pool_name: str,
) -> None:
    """
    Record a connection timeout event.

    Args:
        pool_name: Connection pool identifier
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    collector.record_connection_timeout(
        pool_name=pool_name,
    )


async def record_connection_wait_time(
    pool_name: str,
    duration_seconds: float,
) -> None:
    """
    Record time taken to obtain a connection from the pool.

    Args:
        pool_name: Connection pool identifier
        duration_seconds: Wait time in seconds
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    collector.record_connection_wait_time(
        pool_name=pool_name,
        duration_seconds=duration_seconds,
    )


async def record_connection_use_time(
    pool_name: str,
    duration_seconds: float,
) -> None:
    """
    Record time a connection was in use (borrowed from pool).

    Args:
        pool_name: Connection pool identifier
        duration_seconds: Use time in seconds
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    collector.record_connection_use_time(
        pool_name=pool_name,
        duration_seconds=duration_seconds,
    )


async def record_connection_closed(
    close_reason: Optional[CloseReason] = None,
    error_type: Optional[Exception] = None,
) -> None:
    """
    Record a connection closed event.

    Args:
        close_reason: Reason for closing (e.g. 'error', 'application_close')
        error_type: Error type if closed due to error
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    collector.record_connection_closed(
        close_reason=close_reason,
        error_type=error_type,
    )


async def record_connection_relaxed_timeout(
    connection_name: str,
    maint_notification: str,
    relaxed: bool,
) -> None:
    """
    Record a connection timeout relaxation event.

    Args:
        connection_name: Connection identifier
        maint_notification: Maintenance notification type
        relaxed: True to count up (relaxed), False to count down (unrelaxed)
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    collector.record_connection_relaxed_timeout(
        connection_name=connection_name,
        maint_notification=maint_notification,
        relaxed=relaxed,
    )


async def record_connection_handoff(
    pool_name: str,
) -> None:
    """
    Record a connection handoff event (e.g., after MOVING notification).

    Args:
        pool_name: Connection pool identifier
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    collector.record_connection_handoff(
        pool_name=pool_name,
    )


async def record_error_count(
    server_address: str,
    server_port: int,
    network_peer_address: str,
    network_peer_port: int,
    error_type: Exception,
    retry_attempts: int,
    is_internal: bool = True,
) -> None:
    """
    Record error count.

    Args:
        server_address: Server address
        server_port: Server port
        network_peer_address: Network peer address
        network_peer_port: Network peer port
        error_type: Error type (Exception)
        retry_attempts: Retry attempts
        is_internal: Whether the error is internal (e.g., timeout, network error)
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    collector.record_error_count(
        server_address=server_address,
        server_port=server_port,
        network_peer_address=network_peer_address,
        network_peer_port=network_peer_port,
        error_type=error_type,
        retry_attempts=retry_attempts,
        is_internal=is_internal,
    )


async def record_pubsub_message(
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
    collector = await _get_or_create_collector()
    if collector is None:
        return

    # Check if channel names should be hidden
    effective_channel = channel
    if channel is not None:
        config = await _get_config()
        if config is not None and config.hide_pubsub_channel_names:
            effective_channel = None

    collector.record_pubsub_message(
        direction=direction,
        channel=effective_channel,
        sharded=sharded,
    )


async def record_streaming_lag(
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
    collector = await _get_or_create_collector()
    if collector is None:
        return

    # Check if stream names should be hidden
    effective_stream_name = stream_name
    if stream_name is not None:
        config = await _get_config()
        if config is not None and config.hide_stream_names:
            effective_stream_name = None

    collector.record_streaming_lag(
        lag_seconds=lag_seconds,
        stream_name=effective_stream_name,
        consumer_group=consumer_group,
        consumer_name=consumer_name,
    )


async def record_streaming_lag_from_response(
    response,
    consumer_group: Optional[str] = None,
    consumer_name: Optional[str] = None,
) -> None:
    """
    Record streaming lag from XREAD/XREADGROUP response.

    Parses the response and calculates lag for each message based on message ID timestamp.

    Args:
        response: Response from XREAD/XREADGROUP command
        consumer_group: Consumer group name (for XREADGROUP)
        consumer_name: Consumer name (for XREADGROUP)
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    if not response:
        return

    now = datetime.now().timestamp()

    # Check if stream names should be hidden
    config = await _get_config()
    hide_stream_names = config is not None and config.hide_stream_names

    # RESP3 format: dict
    if isinstance(response, dict):
        for stream_name, stream_messages in response.items():
            effective_stream_name = (
                None if hide_stream_names else str_if_bytes(stream_name)
            )
            for messages in stream_messages:
                for message in messages:
                    message_id, _ = message
                    message_id = str_if_bytes(message_id)
                    timestamp, _ = message_id.split("-")
                    # Ensure lag is non-negative (clock skew can cause negative values)
                    lag_seconds = max(0.0, now - int(timestamp) / 1000)

                    collector.record_streaming_lag(
                        lag_seconds=lag_seconds,
                        stream_name=effective_stream_name,
                        consumer_group=consumer_group,
                        consumer_name=consumer_name,
                    )
    else:
        # RESP2 format: list
        for stream_entry in response:
            stream_name = str_if_bytes(stream_entry[0])
            effective_stream_name = None if hide_stream_names else stream_name

            for message in stream_entry[1]:
                message_id, _ = message
                message_id = str_if_bytes(message_id)
                timestamp, _ = message_id.split("-")
                # Ensure lag is non-negative (clock skew can cause negative values)
                lag_seconds = max(0.0, now - int(timestamp) / 1000)

                collector.record_streaming_lag(
                    lag_seconds=lag_seconds,
                    stream_name=effective_stream_name,
                    consumer_group=consumer_group,
                    consumer_name=consumer_name,
                )


async def record_maint_notification_count(
    server_address: str,
    server_port: int,
    network_peer_address: str,
    network_peer_port: int,
    maint_notification: str,
) -> None:
    """
    Record a maintenance notification count.

    Args:
        server_address: Server address
        server_port: Server port
        network_peer_address: Network peer address
        network_peer_port: Network peer port
        maint_notification: Maintenance notification type (e.g., 'MOVING', 'MIGRATING')
    """
    collector = await _get_or_create_collector()
    if collector is None:
        return

    collector.record_maint_notification_count(
        server_address=server_address,
        server_port=server_port,
        network_peer_address=network_peer_address,
        network_peer_port=network_peer_port,
        maint_notification=maint_notification,
    )


def reset_collector() -> None:
    """
    Reset the global async collector (used for testing or re-initialization).
    """
    global _async_metrics_collector
    _async_metrics_collector = None


async def is_enabled() -> bool:
    """
    Check if observability is enabled.

    Returns:
        True if metrics are being collected, False otherwise
    """
    collector = await _get_or_create_collector()
    return collector is not None
