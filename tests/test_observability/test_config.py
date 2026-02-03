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
