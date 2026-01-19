"""
Simple, clean API for recording observability metrics.

This module provides a straightforward interface for Redis core code to record
metrics without needing to know about OpenTelemetry internals.

Usage in Redis core code:
    from redis.observability.recorder import record_operation_duration

    start_time = time.monotonic()
    # ... execute Redis command ...
    record_operation_duration(
        command_name='SET',
        duration_seconds=time.monotonic() - start_time,
        server_address='localhost',
        server_port=6379,
        db_namespace='0',
        error=None
    )
"""

import time
from typing import Optional, Callable, List

from redis.connection import ConnectionPoolInterface
from redis.observability.attributes import PubSubDirection, ConnectionState, CSCResult, CSCReason, AttributeBuilder
from redis.observability.attributes import PubSubDirection, ConnectionState
from redis.observability.metrics import RedisMetricsCollector, CloseReason
from redis.observability.providers import get_observability_instance
from redis.observability.registry import get_observables_registry_instance

# Global metrics collector instance (lazy-initialized)
_metrics_collector: Optional[RedisMetricsCollector] = None

CONNECTION_COUNT_REGISTRY_KEY = "connection_count"
CSC_ITEMS_REGISTRY_KEY = "csc_items"


def record_operation_duration(
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

    This is a simple, clean API that Redis core code can call directly.
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
        >>> record_operation_duration('SET', time.monotonic() - start, 'localhost', 6379, '0')
    """
    global _metrics_collector

    # Fast path: if collector not initialized, observability is disabled
    if _metrics_collector is None:
        # Try to initialize (only once)
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return  # Observability not enabled

    # Record the metric
    # try:
    _metrics_collector.record_operation_duration(
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
    # except Exception:
    #     # Don't let metric recording errors break Redis operations
    #     pass


def record_connection_create_time(
        connection_pool: "ConnectionPoolInterface",
        duration_seconds: float,
) -> None:
    """
    Record connection creation time.

    Args:
        connection_pool: Connection pool implementation
        duration_seconds: Time taken to create connection in seconds

    Example:
        >>> start = time.monotonic()
        >>> # ... create connection ...
        >>> record_connection_create_time('ConnectionPool<localhost:6379>', time.monotonic() - start)
    """
    global _metrics_collector

    # Fast path: if collector not initialized, observability is disabled
    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # try:
    _metrics_collector.record_connection_create_time(
        connection_pool=connection_pool,
        duration_seconds=duration_seconds,
    )
    # except Exception:
    #     pass


def init_connection_count() -> None:
    """
    Initialize observable gauge for connection count metric.
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    def observable_callback(__):
        observables_registry = get_observables_registry_instance()
        callbacks = observables_registry.get(CONNECTION_COUNT_REGISTRY_KEY)
        observations = []

        for callback in callbacks:
            observations.extend(callback())

        return observations

    # try:
    _metrics_collector.init_connection_count(
        callback=observable_callback,
    )
    # except Exception:
    #     pass

def register_pools_connection_count(
        connection_pools: List["ConnectionPoolInterface"],
) -> None:
    """
    Add connection pools to connection count observable registry.
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
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

def record_connection_timeout(
        pool_name: str,
) -> None:
    """
    Record a connection timeout event.

    Args:
        pool_name: Connection pool identifier

    Example:
        >>> record_connection_timeout('ConnectionPool<localhost:6379>')
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # try:
    _metrics_collector.record_connection_timeout(
        pool_name=pool_name,
    )
    # except Exception:
    #     pass


def record_connection_wait_time(
        pool_name: str,
        duration_seconds: float,
) -> None:
    """
    Record time taken to obtain a connection from the pool.

    Args:
        pool_name: Connection pool identifier
        duration_seconds: Wait time in seconds

    Example:
        >>> start = time.monotonic()
        >>> # ... wait for connection from pool ...
        >>> record_connection_wait_time('ConnectionPool<localhost:6379>', time.monotonic() - start)
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # try:
    _metrics_collector.record_connection_wait_time(
        pool_name=pool_name,
        duration_seconds=duration_seconds,
    )
    # except Exception:
    #     pass


def record_connection_use_time(
        pool_name: str,
        duration_seconds: float,
) -> None:
    """
    Record time a connection was in use (borrowed from pool).

    Args:
        pool_name: Connection pool identifier
        duration_seconds: Use time in seconds

    Example:
        >>> start = time.monotonic()
        >>> # ... use connection ...
        >>> record_connection_use_time('ConnectionPool<localhost:6379>', time.monotonic() - start)
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # try:
    _metrics_collector.record_connection_use_time(
        pool_name=pool_name,
        duration_seconds=duration_seconds,
    )
    # except Exception:
    #     pass


def record_connection_closed(
        close_reason: Optional[CloseReason] = None,
        error_type: Optional[Exception] = None,
) -> None:
    """
    Record a connection closed event.

    Args:
        close_reason: Reason for closing (e.g. 'error', 'application_close')
        error_type: Error type if closed due to error

    Example:
        >>> record_connection_closed('ConnectionPool<localhost:6379>', 'idle_timeout')
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # try:
    _metrics_collector.record_connection_closed(
        close_reason=close_reason,
        error_type=error_type,
    )
    # except Exception:
    #     pass


def record_connection_relaxed_timeout(
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

    Example:
        >>> record_connection_relaxed_timeout('Connection<localhost:6379>', 'MOVING', True)
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # try:
    _metrics_collector.record_connection_relaxed_timeout(
        connection_name=connection_name,
        maint_notification=maint_notification,
        relaxed=relaxed,
    )
    # except Exception:
    #     pass


def record_connection_handoff(
        pool_name: str,
) -> None:
    """
    Record a connection handoff event (e.g., after MOVING notification).

    Args:
        pool_name: Connection pool identifier

    Example:
        >>> record_connection_handoff('ConnectionPool<localhost:6379>')
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # try:
    _metrics_collector.record_connection_handoff(
        pool_name=pool_name,
    )
    # except Exception:
    #     pass


def record_error_count(
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

    Example:
        >>> record_error_count('localhost', 6379, 'localhost', 6379, ConnectionError(), 3)
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # try:
    _metrics_collector.record_error_count(
        server_address=server_address,
        server_port=server_port,
        network_peer_address=network_peer_address,
        network_peer_port=network_peer_port,
        error_type=error_type,
        retry_attempts=retry_attempts,
        is_internal=is_internal,
    )
    # except Exception:
    #     pass


def record_pubsub_message(
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

    Example:
        >>> record_pubsub_message(PubSubDirection.PUBLISH, 'channel', False)
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    _metrics_collector.record_pubsub_message(
        direction=direction,
        channel=channel,
        sharded=sharded,
    )


def record_streaming_lag(
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
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # try:
    _metrics_collector.record_streaming_lag(
        lag_seconds=lag_seconds,
        stream_name=stream_name,
        consumer_group=consumer_group,
        consumer_name=consumer_name,
    )
    # except Exception:
    #     pass


def record_maint_notification_count(
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

    Example:
        >>> record_maint_notification_count('localhost', 6379, 'localhost', 6379, 'MOVING')
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # try:
    _metrics_collector.record_maint_notification_count(
        server_address=server_address,
        server_port=server_port,
        network_peer_address=network_peer_address,
        network_peer_port=network_peer_port,
        maint_notification=maint_notification,
    )
    # except Exception:
    #     pass

def record_csc_request(
        db_namespace: Optional[int] = None,
        result: Optional[CSCResult] = None,
):
    """
    Record a Client Side Caching (CSC) request.

    Args:
        db_namespace: Redis database index
        result: CSC result ('hit' or 'miss')
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    _metrics_collector.record_csc_request(
        db_namespace=db_namespace,
        result=result,
    )

def init_csc_items() -> None:
    """
    Initialize observable gauge for CSC items metric.
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    def observable_callback(__):
        observables_registry = get_observables_registry_instance()
        callbacks = observables_registry.get(CSC_ITEMS_REGISTRY_KEY)
        observations = []

        for callback in callbacks:
            observations.extend(callback())

        return observations

    _metrics_collector.init_csc_items(
        callback=observable_callback,
    )

def register_csc_items_callback(
        callback: Callable,
        db_namespace: Optional[int] = None,
) -> None:
    """
    Adds given callback to CSC items observable registry.
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    # Lazy import
    from opentelemetry.metrics import Observation

    def csc_items_callback():
        return [Observation(callback(), attributes=AttributeBuilder.build_csc_attributes(db_namespace=db_namespace))]

    observables_registry = get_observables_registry_instance()
    observables_registry.register(CSC_ITEMS_REGISTRY_KEY, csc_items_callback)

def record_csc_eviction(
        count: int,
        db_namespace: Optional[int] = None,
        reason: Optional[CSCReason] = None,
) -> None:
    """
    Record a Client Side Caching (CSC) eviction.

    Args:
        count: Number of evictions
        db_namespace: Redis database index
        reason: Reason for eviction
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    _metrics_collector.record_csc_eviction(
        count=count,
        db_namespace=db_namespace,
        reason=reason,
    )

def record_csc_network_saved(
        bytes_saved: int,
        db_namespace: Optional[int] = None,
) -> None:
    """
    Record the number of bytes saved by using Client Side Caching (CSC).

    Args:
        bytes_saved: Number of bytes saved
        db_namespace: Redis database index
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()
        if _metrics_collector is None:
            return

    _metrics_collector.record_csc_network_saved(
        bytes_saved=bytes_saved,
        db_namespace=db_namespace,
    )

def _get_or_create_collector() -> Optional[RedisMetricsCollector]:
    """
    Get or create the global metrics collector.

    Returns:
        RedisMetricsCollector instance if observability is enabled, None otherwise
    """
    try:
        manager = get_observability_instance().get_provider_manager()
        if manager is None or not manager.config.enabled_telemetry:
            return None

        # Get meter from the global MeterProvider
        meter = manager.get_meter_provider().get_meter(
            RedisMetricsCollector.METER_NAME,
            RedisMetricsCollector.METER_VERSION
        )

        return RedisMetricsCollector(meter, manager.config)

    except ImportError:
        # Observability module not available
        return None
    except Exception:
        # Any other error - don't break Redis operations
        return None


def reset_collector() -> None:
    """
    Reset the global collector (used for testing or re-initialization).
    """
    global _metrics_collector
    _metrics_collector = None


def is_enabled() -> bool:
    """
    Check if observability is enabled.

    Returns:
        True if metrics are being collected, False otherwise
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = _get_or_create_collector()

    return _metrics_collector is not None