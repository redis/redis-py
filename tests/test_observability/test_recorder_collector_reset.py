"""The reset-vs-resolution window, tested against the recorder's real resolver.

An earlier version of this file drove `_get_or_create_collector` through a
stand-in, which could not distinguish the guarded resolver from the unguarded one
and therefore proved nothing. This drives the real resolver and injects the reset
where it actually matters: during the provider-manager read, after the "resolved"
flag has been set and before the collector is published.

A fresh recorder module is used so the module-global caches start clean, and the
collector class is replaced by a stand-in that keeps the `METER_NAME` /
`METER_VERSION` attributes the resolver reads - replacing it with a plain
callable raises AttributeError there, which the resolver's broad `except`
swallows, and the resolution then looks like a miss for the wrong reason.
"""

import importlib
from unittest.mock import MagicMock, patch

import pytest


pytestmark = pytest.mark.fixed_client


def _fresh_recorder():
    """Reload the recorder so its module-global caches start from zero."""
    module = importlib.import_module("redis.observability.recorder")
    return importlib.reload(module)


class _Config:
    enabled_telemetry = True


class _MeterProvider:
    def __init__(self, hooks):
        self._hooks = hooks

    def get_meter(self, *_args, **_kwargs):
        # The read a concurrent init() can land in.
        self._hooks()
        return MagicMock(name="meter")


class _ProviderManager:
    def __init__(self, hooks):
        self.config = _Config()
        self._provider = _MeterProvider(hooks)

    def get_meter_provider(self):
        return self._provider


class _Observability:
    def __init__(self, manager):
        self._manager = manager

    def get_provider_manager(self):
        return self._manager


class _StandInCollector:
    """Keeps the class attributes the resolver reads off the collector class."""

    METER_NAME = "redis-py"
    METER_VERSION = "1"

    def __init__(self, *_args, **_kwargs):
        pass

    def record_operation_duration(self, **_kwargs):
        pass


def _run_recording(recorder_module, injecting):
    """One recording, with `injecting` called during the meter read."""
    manager = _ProviderManager(injecting)
    with (
        patch.object(
            recorder_module,
            "get_observability_instance",
            lambda: _Observability(manager),
        ),
        patch.object(recorder_module, "RedisMetricsCollector", _StandInCollector),
    ):
        recorder_module.reset_collector()
        recorder_module.record_operation_duration("GET", 0.001, "localhost", 6379)


def test_resolution_spanning_a_reset_does_not_cache_its_answer():
    """A reset during the resolution must leave the cache unresolved.

    The resolution started before the reset and finishes after it, so the
    collector it builds belongs to the configuration the reset replaced.
    Publishing it would pin a stale value into the cache; leaving the flag set
    would be worse, because the next caller would read a cached miss and metrics
    would stay off until the next init.
    """
    module = _fresh_recorder()
    resets = []

    def inject_reset():
        if not resets:
            resets.append(1)
            module.reset_collector()

    _run_recording(module, inject_reset)

    assert resets == [1], "the reset must land inside the resolution"
    assert module._metrics_collector is None, (
        "the collector was built against the configuration the reset replaced, "
        "so it must not be published as the cache"
    )
    assert module._collector_resolved is False, (
        "the cache must be left unresolved; a cached miss here is what silences "
        "metrics until the next init"
    )


def test_resolution_without_a_reset_does_cache_its_answer():
    """The control: with no reset the resolution publishes normally.

    Without this, the test above would also pass if the resolver simply never
    cached anything.
    """
    module = _fresh_recorder()
    _run_recording(module, lambda: None)

    assert module._metrics_collector is not None, (
        "an uninterrupted resolution must cache the collector it built"
    )
    assert module._collector_resolved is True

    module.reset_collector()
