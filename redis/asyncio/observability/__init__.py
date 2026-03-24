"""
Async observability module for Redis async clients.

This module provides async-safe APIs for recording Redis metrics using OpenTelemetry.

Usage:
    from redis.asyncio.observability.recorder import record_operation_duration

Configuration is shared with the sync observability module:
    from redis.observability import get_observability_instance, OTelConfig

    otel = get_observability_instance()
    otel.init(OTelConfig())
"""
