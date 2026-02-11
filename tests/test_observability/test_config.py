"""
Unit tests for redis.observability.config module.

These tests verify the OTelConfig class behavior including:
- Default configuration values
- Custom configuration via constructor parameters
- Validation of configuration values
- Command filtering (include/exclude lists)
- Runtime configuration changes
"""

import pytest

from redis.observability.config import OTelConfig, MetricGroup, TelemetryOption


class TestOTelConfigDefaults:
    """Tests for OTelConfig default values."""

    def test_default_enabled_telemetry(self):
        """Test that default telemetry is METRICS only."""
        config = OTelConfig()
        assert config.enabled_telemetry == TelemetryOption.METRICS

    def test_default_metric_groups(self):
        """Test that default metric groups are COMMAND, CONNECTION_BASIC, RESILIENCY."""
        config = OTelConfig()
        expected = MetricGroup.CONNECTION_BASIC | MetricGroup.RESILIENCY
        assert config.metric_groups == expected

    def test_default_include_commands_is_none(self):
        """Test that include_commands is None by default."""
        config = OTelConfig()
        assert config.include_commands is None

    def test_default_exclude_commands_is_empty_set(self):
        """Test that exclude_commands is empty set by default."""
        config = OTelConfig()
        assert config.exclude_commands == set()

    def test_is_enabled_returns_true_by_default(self):
        """Test that is_enabled returns True with default config."""
        config = OTelConfig()
        assert config.is_enabled() is True


class TestOTelConfigEnabledTelemetry:
    """Tests for enabled_telemetry configuration."""

    def test_single_telemetry_option(self):
        """Test setting a single telemetry option."""
        config = OTelConfig(enabled_telemetry=[TelemetryOption.METRICS])
        assert config.enabled_telemetry == TelemetryOption.METRICS

    def test_empty_telemetry_list_disables_all(self):
        """Test that empty telemetry list disables all telemetry."""
        config = OTelConfig(enabled_telemetry=[])
        assert config.enabled_telemetry == TelemetryOption(0)
        assert config.is_enabled() is False


class TestOTelConfigMetricGroups:
    """Tests for metric_groups configuration."""

    def test_single_metric_group(self):
        """Test setting a single metric group."""
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])
        assert config.metric_groups == MetricGroup.COMMAND

    def test_multiple_metric_groups(self):
        """Test setting multiple metric groups."""
        config = OTelConfig(
            metric_groups=[MetricGroup.COMMAND, MetricGroup.PUBSUB]
        )
        assert MetricGroup.COMMAND in config.metric_groups
        assert MetricGroup.PUBSUB in config.metric_groups

    def test_all_metric_groups(self):
        """Test setting all metric groups."""
        config = OTelConfig(
            metric_groups=[
                MetricGroup.RESILIENCY,
                MetricGroup.CONNECTION_BASIC,
                MetricGroup.CONNECTION_ADVANCED,
                MetricGroup.COMMAND,
                MetricGroup.CSC,
                MetricGroup.STREAMING,
                MetricGroup.PUBSUB,
            ]
        )
        assert MetricGroup.RESILIENCY in config.metric_groups
        assert MetricGroup.CONNECTION_BASIC in config.metric_groups
        assert MetricGroup.CONNECTION_ADVANCED in config.metric_groups
        assert MetricGroup.COMMAND in config.metric_groups
        assert MetricGroup.CSC in config.metric_groups
        assert MetricGroup.STREAMING in config.metric_groups
        assert MetricGroup.PUBSUB in config.metric_groups

    def test_empty_metric_groups_list(self):
        """Test that empty metric groups list results in no groups enabled."""
        config = OTelConfig(metric_groups=[])
        assert config.metric_groups == MetricGroup(0)


class TestOTelConfigIncludeCommands:
    """Tests for include_commands configuration."""

    def test_include_commands_single(self):
        """Test include_commands with single command."""
        config = OTelConfig(include_commands=['GET'])
        assert config.include_commands == {'GET'}

    def test_include_commands_multiple(self):
        """Test include_commands with multiple commands."""
        config = OTelConfig(include_commands=['GET', 'SET', 'DEL'])
        assert config.include_commands == {'GET', 'SET', 'DEL'}

    def test_include_commands_empty_list(self):
        """Test include_commands with empty list results in empty set."""
        config = OTelConfig(include_commands=[])
        assert config.include_commands == None


class TestOTelConfigExcludeCommands:
    """Tests for exclude_commands configuration."""

    def test_exclude_commands_single(self):
        """Test exclude_commands with single command."""
        config = OTelConfig(exclude_commands=['PING'])
        assert config.exclude_commands == {'PING'}

    def test_exclude_commands_multiple(self):
        """Test exclude_commands with multiple commands."""
        config = OTelConfig(exclude_commands=['PING', 'INFO', 'DEBUG'])
        assert config.exclude_commands == {'PING', 'INFO', 'DEBUG'}

    def test_exclude_commands_empty_list(self):
        """Test exclude_commands with empty list results in empty set."""
        config = OTelConfig(exclude_commands=[])
        assert config.exclude_commands == set()


class TestOTelConfigShouldTrackCommand:
    """Tests for should_track_command method."""

    def test_should_track_command_default_tracks_all(self):
        """Test that all commands are tracked by default."""
        config = OTelConfig()
        assert config.should_track_command('GET') is True
        assert config.should_track_command('SET') is True
        assert config.should_track_command('PING') is True

    def test_should_track_command_case_insensitive(self):
        """Test that command matching is case-insensitive."""
        config = OTelConfig(include_commands=['GET'])
        assert config.should_track_command('GET') is True
        assert config.should_track_command('get') is True
        assert config.should_track_command('Get') is True

    def test_should_track_command_with_include_list(self):
        """Test that only included commands are tracked."""
        config = OTelConfig(include_commands=['GET', 'SET'])
        assert config.should_track_command('GET') is True
        assert config.should_track_command('SET') is True
        assert config.should_track_command('DEL') is False
        assert config.should_track_command('PING') is False

    def test_should_track_command_with_exclude_list(self):
        """Test that excluded commands are not tracked."""
        config = OTelConfig(exclude_commands=['PING', 'INFO'])
        assert config.should_track_command('GET') is True
        assert config.should_track_command('SET') is True
        assert config.should_track_command('PING') is False
        assert config.should_track_command('INFO') is False

    def test_should_track_command_include_takes_precedence(self):
        """Test that include_commands takes precedence over exclude_commands."""
        # When include_commands is set, exclude_commands is ignored
        config = OTelConfig(
            include_commands=['GET', 'SET'],
            exclude_commands=['GET']  # This should be ignored
        )
        assert config.should_track_command('GET') is True
        assert config.should_track_command('SET') is True
        assert config.should_track_command('DEL') is False

    def test_should_track_command_empty_include_tracks_all(self):
        """Test that empty include list tracks all commands."""
        config = OTelConfig(include_commands=[])
        assert config.should_track_command('GET') is True
        assert config.should_track_command('SET') is True


class TestOTelConfigRepr:
    """Tests for __repr__ method."""

    def test_repr_contains_enabled_telemetry(self):
        """Test that repr contains enabled_telemetry."""
        config = OTelConfig()
        repr_str = repr(config)
        assert 'enabled_telemetry' in repr_str


class TestOTelConfigPrivacyControls:
    """Tests for privacy control configuration options."""

    def test_default_hide_pubsub_channel_names_is_false(self):
        """Test that hide_pubsub_channel_names is False by default."""
        config = OTelConfig()
        assert config.hide_pubsub_channel_names is False

    def test_default_hide_stream_names_is_false(self):
        """Test that hide_stream_names is False by default."""
        config = OTelConfig()
        assert config.hide_stream_names is False

    def test_hide_pubsub_channel_names_can_be_enabled(self):
        """Test that hide_pubsub_channel_names can be set to True."""
        config = OTelConfig(hide_pubsub_channel_names=True)
        assert config.hide_pubsub_channel_names is True

    def test_hide_stream_names_can_be_enabled(self):
        """Test that hide_stream_names can be set to True."""
        config = OTelConfig(hide_stream_names=True)
        assert config.hide_stream_names is True

    def test_both_privacy_controls_can_be_enabled(self):
        """Test that both privacy controls can be enabled together."""
        config = OTelConfig(
            hide_pubsub_channel_names=True,
            hide_stream_names=True,
        )
        assert config.hide_pubsub_channel_names is True
        assert config.hide_stream_names is True


class TestOTelConfigHistogramBuckets:
    """Tests for custom histogram bucket boundary configuration."""

    def test_default_operation_duration_buckets(self):
        """Test that default operation duration buckets are set correctly."""
        from redis.observability.config import default_operation_duration_buckets
        config = OTelConfig()
        assert config.buckets_operation_duration == default_operation_duration_buckets()

    def test_default_stream_processing_duration_buckets(self):
        """Test that default stream processing duration buckets are set correctly."""
        from redis.observability.config import default_histogram_buckets
        config = OTelConfig()
        assert config.buckets_stream_processing_duration == default_histogram_buckets()

    def test_default_connection_create_time_buckets(self):
        """Test that default connection create time buckets are set correctly."""
        from redis.observability.config import default_histogram_buckets
        config = OTelConfig()
        assert config.buckets_connection_create_time == default_histogram_buckets()

    def test_default_connection_wait_time_buckets(self):
        """Test that default connection wait time buckets are set correctly."""
        from redis.observability.config import default_histogram_buckets
        config = OTelConfig()
        assert config.buckets_connection_wait_time == default_histogram_buckets()

    def test_custom_operation_duration_buckets(self):
        """Test that custom operation duration buckets can be set."""
        custom_buckets = [0.001, 0.01, 0.1, 1.0, 10.0]
        config = OTelConfig(buckets_operation_duration=custom_buckets)
        assert config.buckets_operation_duration == custom_buckets

    def test_custom_stream_processing_duration_buckets(self):
        """Test that custom stream processing duration buckets can be set."""
        custom_buckets = [0.01, 0.1, 1.0, 5.0]
        config = OTelConfig(buckets_stream_processing_duration=custom_buckets)
        assert config.buckets_stream_processing_duration == custom_buckets

    def test_custom_connection_create_time_buckets(self):
        """Test that custom connection create time buckets can be set."""
        custom_buckets = [0.001, 0.005, 0.01, 0.05, 0.1]
        config = OTelConfig(buckets_connection_create_time=custom_buckets)
        assert config.buckets_connection_create_time == custom_buckets

    def test_custom_connection_wait_time_buckets(self):
        """Test that custom connection wait time buckets can be set."""
        custom_buckets = [0.0001, 0.001, 0.01, 0.1]
        config = OTelConfig(buckets_connection_wait_time=custom_buckets)
        assert config.buckets_connection_wait_time == custom_buckets

    def test_all_custom_buckets_can_be_set_together(self):
        """Test that all custom bucket configurations can be set together."""
        op_buckets = [0.001, 0.01, 0.1]
        stream_buckets = [0.01, 0.1, 1.0]
        create_buckets = [0.001, 0.005, 0.01]
        wait_buckets = [0.0001, 0.001, 0.01]

        config = OTelConfig(
            buckets_operation_duration=op_buckets,
            buckets_stream_processing_duration=stream_buckets,
            buckets_connection_create_time=create_buckets,
            buckets_connection_wait_time=wait_buckets,
        )

        assert config.buckets_operation_duration == op_buckets
        assert config.buckets_stream_processing_duration == stream_buckets
        assert config.buckets_connection_create_time == create_buckets
        assert config.buckets_connection_wait_time == wait_buckets

    def test_empty_buckets_list_is_allowed(self):
        """Test that empty bucket lists are allowed (OTel SDK will use defaults)."""
        config = OTelConfig(buckets_operation_duration=[])
        assert config.buckets_operation_duration == []

    def test_single_bucket_boundary_is_allowed(self):
        """Test that a single bucket boundary is allowed."""
        config = OTelConfig(buckets_operation_duration=[1.0])
        assert config.buckets_operation_duration == [1.0]


class TestDefaultBucketFunctions:
    """Tests for default bucket boundary functions."""

    def test_default_operation_duration_buckets_returns_sequence(self):
        """Test that default_operation_duration_buckets returns a sequence."""
        from redis.observability.config import default_operation_duration_buckets
        buckets = default_operation_duration_buckets()
        assert isinstance(buckets, (list, tuple))
        assert len(buckets) > 0

    def test_default_histogram_buckets_returns_sequence(self):
        """Test that default_histogram_buckets returns a sequence."""
        from redis.observability.config import default_histogram_buckets
        buckets = default_histogram_buckets()
        assert isinstance(buckets, (list, tuple))
        assert len(buckets) > 0

    def test_default_operation_duration_buckets_are_sorted(self):
        """Test that default operation duration buckets are in ascending order."""
        from redis.observability.config import default_operation_duration_buckets
        buckets = list(default_operation_duration_buckets())
        assert buckets == sorted(buckets)

    def test_default_histogram_buckets_are_sorted(self):
        """Test that default histogram buckets are in ascending order."""
        from redis.observability.config import default_histogram_buckets
        buckets = list(default_histogram_buckets())
        assert buckets == sorted(buckets)

    def test_default_operation_duration_buckets_are_positive(self):
        """Test that all default operation duration bucket values are positive."""
        from redis.observability.config import default_operation_duration_buckets
        buckets = default_operation_duration_buckets()
        assert all(b > 0 for b in buckets)

    def test_default_histogram_buckets_are_positive(self):
        """Test that all default histogram bucket values are positive."""
        from redis.observability.config import default_histogram_buckets
        buckets = default_histogram_buckets()
        assert all(b > 0 for b in buckets)


class TestMetricGroupEnum:
    """Tests for MetricGroup IntFlag enum."""

    def test_metric_group_values_are_unique(self):
        """Test that all MetricGroup values are unique powers of 2."""
        values = [
            MetricGroup.RESILIENCY,
            MetricGroup.CONNECTION_BASIC,
            MetricGroup.CONNECTION_ADVANCED,
            MetricGroup.COMMAND,
            MetricGroup.CSC,
            MetricGroup.STREAMING,
            MetricGroup.PUBSUB,
        ]
        # Each value should be a power of 2
        for value in values:
            assert value & (value - 1) == 0  # Power of 2 check

    def test_metric_group_can_be_combined(self):
        """Test that MetricGroup values can be combined with bitwise OR."""
        combined = MetricGroup.COMMAND | MetricGroup.PUBSUB
        assert MetricGroup.COMMAND in combined
        assert MetricGroup.PUBSUB in combined
        assert MetricGroup.STREAMING not in combined

    def test_metric_group_membership_check(self):
        """Test checking membership in combined MetricGroup."""
        combined = MetricGroup.RESILIENCY | MetricGroup.CONNECTION_BASIC
        assert bool(combined & MetricGroup.RESILIENCY)
        assert bool(combined & MetricGroup.CONNECTION_BASIC)
        assert not bool(combined & MetricGroup.COMMAND)
