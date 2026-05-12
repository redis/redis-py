"""
OpenTelemetry observability module for redis-py.

This module provides APIs for collecting and exporting Redis metrics using OpenTelemetry.

Usage:
    from redis.observability import get_observability_instance, OTelConfig

    otel = get_observability_instance()
    otel.init(OTelConfig())
"""

from redis.observability.config import MetricGroup, OTelConfig, TelemetryOption
from redis.observability.providers import (
    ObservabilityInstance,
    get_observability_instance,
    reset_observability_instance,
)

__all__ = [
    "OTelConfig",
    "MetricGroup",
    "TelemetryOption",
    "ObservabilityInstance",
    "get_observability_instance",
    "reset_observability_instance",
]
