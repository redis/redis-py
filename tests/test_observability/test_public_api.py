"""
Unit tests for redis.observability public API exports.

These tests verify that all symbols exported from redis.observability
are correctly re-exported and match the original implementations.
"""


class TestPublicAPIExports:
    """Tests for public API exports from redis.observability."""

    def test_otel_config_reexport(self):
        """Test that OTelConfig is correctly re-exported."""
        from redis.observability import OTelConfig
        from redis.observability.config import OTelConfig as OriginalOTelConfig

        assert OTelConfig is OriginalOTelConfig

    def test_metric_group_reexport(self):
        """Test that MetricGroup is correctly re-exported."""
        from redis.observability import MetricGroup
        from redis.observability.config import MetricGroup as OriginalMetricGroup

        assert MetricGroup is OriginalMetricGroup

    def test_telemetry_option_reexport(self):
        """Test that TelemetryOption is correctly re-exported."""
        from redis.observability import TelemetryOption
        from redis.observability.config import (
            TelemetryOption as OriginalTelemetryOption,
        )

        assert TelemetryOption is OriginalTelemetryOption

    def test_observability_instance_reexport(self):
        """Test that ObservabilityInstance is correctly re-exported."""
        from redis.observability import ObservabilityInstance
        from redis.observability.providers import (
            ObservabilityInstance as OriginalObservabilityInstance,
        )

        assert ObservabilityInstance is OriginalObservabilityInstance

    def test_get_observability_instance_reexport(self):
        """Test that get_observability_instance is correctly re-exported."""
        from redis.observability import get_observability_instance
        from redis.observability.providers import (
            get_observability_instance as original_get_observability_instance,
        )

        assert get_observability_instance is original_get_observability_instance

    def test_reset_observability_instance_reexport(self):
        """Test that reset_observability_instance is correctly re-exported."""
        from redis.observability import reset_observability_instance
        from redis.observability.providers import (
            reset_observability_instance as original_reset_observability_instance,
        )

        assert reset_observability_instance is original_reset_observability_instance

    def test_all_exports_defined(self):
        """Test that __all__ contains all expected exports."""
        import redis.observability as obs

        expected_exports = {
            "OTelConfig",
            "MetricGroup",
            "TelemetryOption",
            "ObservabilityInstance",
            "get_observability_instance",
            "reset_observability_instance",
        }

        assert set(obs.__all__) == expected_exports

    def test_all_exports_are_accessible(self):
        """Test that all items in __all__ are actually accessible."""
        import redis.observability as obs

        for name in obs.__all__:
            assert hasattr(obs, name), f"{name} is in __all__ but not accessible"
            assert getattr(obs, name) is not None, f"{name} is None"
