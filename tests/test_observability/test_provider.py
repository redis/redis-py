"""
Unit tests for redis.observability.providers module.

These tests verify the OTelProviderManager and ObservabilityInstance classes including:
- Provider initialization and configuration
- MeterProvider retrieval and validation
- Shutdown and force flush operations
- Singleton pattern behavior
- Context manager support
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from redis.observability.config import OTelConfig, TelemetryOption
from redis.observability.providers import (
    OTelProviderManager,
    ObservabilityInstance,
    get_observability_instance,
)


class TestOTelProviderManagerInit:
    """Tests for OTelProviderManager initialization."""

    def test_init_with_config(self):
        """Test that OTelProviderManager initializes with config."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        assert manager.config is config
        assert manager._meter_provider is None

    def test_init_with_custom_config(self):
        """Test initialization with custom config."""
        config = OTelConfig(
            enabled_telemetry=[TelemetryOption.METRICS],
            metrics_sample_percentage=50.0,
        )
        manager = OTelProviderManager(config)

        assert manager.config.metrics_sample_percentage == 50.0


class TestOTelProviderManagerGetMeterProvider:
    """Tests for get_meter_provider method."""

    def test_get_meter_provider_returns_none_when_disabled(self):
        """Test that get_meter_provider returns None when telemetry is disabled."""
        config = OTelConfig(enabled_telemetry=[])
        manager = OTelProviderManager(config)

        result = manager.get_meter_provider()

        assert result is None

    def test_get_meter_provider_raises_when_no_global_provider(self):
        """Test that get_meter_provider raises RuntimeError when no global provider is set."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        with patch('opentelemetry.metrics') as mock_metrics:
            from opentelemetry.metrics import NoOpMeterProvider
            mock_metrics.get_meter_provider.return_value = NoOpMeterProvider()

            with pytest.raises(RuntimeError) as exc_info:
                manager.get_meter_provider()

            assert "no global MeterProvider is configured" in str(exc_info.value)

    def test_get_meter_provider_returns_global_provider(self):
        """Test that get_meter_provider returns the global MeterProvider."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        mock_provider = Mock()
        # Make sure it's not a NoOpMeterProvider
        mock_provider.__class__.__name__ = 'MeterProvider'

        with patch('opentelemetry.metrics') as mock_metrics:
            mock_metrics.get_meter_provider.return_value = mock_provider
            result = manager.get_meter_provider()

        assert result is mock_provider

    def test_get_meter_provider_caches_provider(self):
        """Test that get_meter_provider caches the provider."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        mock_provider = Mock()

        with patch('opentelemetry.metrics') as mock_metrics:
            mock_metrics.get_meter_provider.return_value = mock_provider

            # Call twice
            result1 = manager.get_meter_provider()
            result2 = manager.get_meter_provider()

        # Should only call get_meter_provider once due to caching
        assert mock_metrics.get_meter_provider.call_count == 1
        assert result1 is result2


class TestOTelProviderManagerShutdown:
    """Tests for shutdown method."""

    def test_shutdown_calls_force_flush(self):
        """Test that shutdown calls force_flush."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        with patch.object(manager, 'force_flush', return_value=True) as mock_flush:
            result = manager.shutdown(timeout_millis=5000)

        mock_flush.assert_called_once_with(timeout_millis=5000)
        assert result is True

    def test_shutdown_with_default_timeout(self):
        """Test shutdown with default timeout."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        with patch.object(manager, 'force_flush', return_value=True) as mock_flush:
            manager.shutdown()

        mock_flush.assert_called_once_with(timeout_millis=30000)


class TestOTelProviderManagerForceFlush:
    """Tests for force_flush method."""

    def test_force_flush_returns_true_when_no_provider(self):
        """Test that force_flush returns True when no provider is set."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        result = manager.force_flush()

        assert result is True

    def test_force_flush_calls_provider_force_flush(self):
        """Test that force_flush calls the provider's force_flush."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        mock_provider = Mock()
        manager._meter_provider = mock_provider

        result = manager.force_flush(timeout_millis=5000)

        mock_provider.force_flush.assert_called_once_with(timeout_millis=5000)
        assert result is True

    def test_force_flush_returns_false_on_exception(self):
        """Test that force_flush returns False when an exception occurs."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        mock_provider = Mock()
        mock_provider.force_flush.side_effect = Exception("Flush failed")
        manager._meter_provider = mock_provider

        result = manager.force_flush()

        assert result is False


class TestOTelProviderManagerContextManager:
    """Tests for context manager support."""

    def test_context_manager_enter_returns_self(self):
        """Test that __enter__ returns self."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        result = manager.__enter__()

        assert result is manager

    def test_context_manager_exit_calls_shutdown(self):
        """Test that __exit__ calls shutdown."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        with patch.object(manager, 'shutdown') as mock_shutdown:
            manager.__exit__(None, None, None)

        mock_shutdown.assert_called_once()

    def test_context_manager_with_statement(self):
        """Test using OTelProviderManager with 'with' statement."""
        config = OTelConfig()

        with patch.object(OTelProviderManager, 'shutdown') as mock_shutdown:
            with OTelProviderManager(config) as manager:
                assert manager.config is config

            mock_shutdown.assert_called_once()


class TestOTelProviderManagerRepr:
    """Tests for __repr__ method."""

    def test_repr_contains_config(self):
        """Test that repr contains config information."""
        config = OTelConfig()
        manager = OTelProviderManager(config)

        repr_str = repr(manager)

        assert 'OTelProviderManager' in repr_str
        assert 'config=' in repr_str


class TestObservabilityInstanceInit:
    """Tests for ObservabilityInstance initialization."""

    def test_init_creates_empty_instance(self):
        """Test that ObservabilityInstance initializes with no provider manager."""
        instance = ObservabilityInstance()

        assert instance._provider_manager is None

    def test_init_method_creates_provider_manager(self):
        """Test that init() creates a provider manager."""
        instance = ObservabilityInstance()
        config = OTelConfig()

        result = instance.init(config)

        assert instance._provider_manager is not None
        assert instance._provider_manager.config is config
        assert result is instance  # Returns self for chaining

    def test_init_method_replaces_existing_manager(self):
        """Test that init() replaces existing provider manager."""
        instance = ObservabilityInstance()
        config1 = OTelConfig(metrics_sample_percentage=50.0)
        config2 = OTelConfig(metrics_sample_percentage=75.0)

        instance.init(config1)
        old_manager = instance._provider_manager

        with patch.object(old_manager, 'shutdown') as mock_shutdown:
            instance.init(config2)

        mock_shutdown.assert_called_once()
        assert instance._provider_manager.config.metrics_sample_percentage == 75.0


class TestObservabilityInstanceIsEnabled:
    """Tests for is_enabled method."""

    def test_is_enabled_returns_false_when_not_initialized(self):
        """Test that is_enabled returns False when not initialized."""
        instance = ObservabilityInstance()

        assert instance.is_enabled() is False

    def test_is_enabled_returns_true_when_initialized_and_enabled(self):
        """Test that is_enabled returns True when initialized with enabled config."""
        instance = ObservabilityInstance()
        config = OTelConfig()

        instance.init(config)

        assert instance.is_enabled() is True

    def test_is_enabled_returns_false_when_telemetry_disabled(self):
        """Test that is_enabled returns False when telemetry is disabled."""
        instance = ObservabilityInstance()
        config = OTelConfig(enabled_telemetry=[])

        instance.init(config)

        assert instance.is_enabled() is False


class TestObservabilityInstanceGetProviderManager:
    """Tests for get_provider_manager method."""

    def test_get_provider_manager_returns_none_when_not_initialized(self):
        """Test that get_provider_manager returns None when not initialized."""
        instance = ObservabilityInstance()

        assert instance.get_provider_manager() is None

    def test_get_provider_manager_returns_manager_when_initialized(self):
        """Test that get_provider_manager returns the manager when initialized."""
        instance = ObservabilityInstance()
        config = OTelConfig()

        instance.init(config)

        manager = instance.get_provider_manager()
        assert manager is not None
        assert manager.config is config


class TestObservabilityInstanceShutdown:
    """Tests for shutdown method."""

    def test_shutdown_returns_true_when_not_initialized(self):
        """Test that shutdown returns True when not initialized."""
        instance = ObservabilityInstance()

        result = instance.shutdown()

        assert result is True

    def test_shutdown_calls_provider_manager_shutdown(self):
        """Test that shutdown calls the provider manager's shutdown."""
        instance = ObservabilityInstance()
        config = OTelConfig()
        instance.init(config)

        with patch.object(instance._provider_manager, 'shutdown', return_value=True) as mock_shutdown:
            result = instance.shutdown(timeout_millis=5000)

        mock_shutdown.assert_called_once_with(5000)
        assert result is True
        assert instance._provider_manager is None

    def test_shutdown_clears_provider_manager(self):
        """Test that shutdown clears the provider manager."""
        instance = ObservabilityInstance()
        config = OTelConfig()
        instance.init(config)

        with patch.object(instance._provider_manager, 'shutdown', return_value=True):
            instance.shutdown()

        assert instance._provider_manager is None


class TestObservabilityInstanceForceFlush:
    """Tests for force_flush method."""

    def test_force_flush_returns_true_when_not_initialized(self):
        """Test that force_flush returns True when not initialized."""
        instance = ObservabilityInstance()

        result = instance.force_flush()

        assert result is True

    def test_force_flush_calls_provider_manager_force_flush(self):
        """Test that force_flush calls the provider manager's force_flush."""
        instance = ObservabilityInstance()
        config = OTelConfig()
        instance.init(config)

        with patch.object(instance._provider_manager, 'force_flush', return_value=True) as mock_flush:
            result = instance.force_flush(timeout_millis=5000)

        mock_flush.assert_called_once_with(5000)
        assert result is True


class TestGetObservabilityInstance:
    """Tests for get_observability_instance function."""

    def test_get_observability_instance_returns_singleton(self):
        """Test that get_observability_instance returns the same instance."""
        # Reset the global instance for this test
        import redis.observability.providers as providers
        original_instance = providers._observability_instance

        try:
            providers._observability_instance = None

            instance1 = get_observability_instance()
            instance2 = get_observability_instance()

            assert instance1 is instance2
        finally:
            # Restore original instance
            providers._observability_instance = original_instance

    def test_get_observability_instance_creates_new_if_none(self):
        """Test that get_observability_instance creates a new instance if none exists."""
        import redis.observability.providers as providers
        original_instance = providers._observability_instance

        try:
            providers._observability_instance = None

            instance = get_observability_instance()

            assert instance is not None
            assert isinstance(instance, ObservabilityInstance)
        finally:
            providers._observability_instance = original_instance

    def test_get_observability_instance_returns_existing(self):
        """Test that get_observability_instance returns existing instance."""
        import redis.observability.providers as providers
        original_instance = providers._observability_instance

        try:
            existing = ObservabilityInstance()
            providers._observability_instance = existing

            instance = get_observability_instance()

            assert instance is existing
        finally:
            providers._observability_instance = original_instance


class TestObservabilityInstanceIntegration:
    """Integration tests for ObservabilityInstance."""

    def test_full_lifecycle(self):
        """Test full lifecycle: init -> use -> shutdown."""
        instance = ObservabilityInstance()
        config = OTelConfig()

        # Initialize
        result = instance.init(config)
        assert result is instance
        assert instance.is_enabled() is True

        # Get provider manager
        manager = instance.get_provider_manager()
        assert manager is not None

        # Force flush (with mocked provider)
        with patch.object(manager, 'force_flush', return_value=True):
            flush_result = instance.force_flush()
            assert flush_result is True

        # Shutdown
        with patch.object(manager, 'shutdown', return_value=True):
            shutdown_result = instance.shutdown()
            assert shutdown_result is True

        assert instance._provider_manager is None
        assert instance.is_enabled() is False

    def test_reinitialize_after_shutdown(self):
        """Test that instance can be reinitialized after shutdown."""
        instance = ObservabilityInstance()
        config1 = OTelConfig(metrics_sample_percentage=50.0)
        config2 = OTelConfig(metrics_sample_percentage=75.0)

        # First initialization
        instance.init(config1)
        with patch.object(instance._provider_manager, 'shutdown', return_value=True):
            instance.shutdown()

        # Second initialization
        instance.init(config2)

        assert instance.is_enabled() is True
        assert instance._provider_manager.config.metrics_sample_percentage == 75.0
