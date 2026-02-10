"""
Async registry for observable metrics callbacks.

This module provides an async-safe registry for storing async callbacks used by
observable metrics (gauges). Uses asyncio.Lock for coroutine-safe access.

Usage:
    from redis.asyncio.observability.registry import get_async_observables_registry_instance

    registry = await get_async_observables_registry_instance()
    await registry.register("connection_count", my_async_callback)
    callbacks = await registry.get("connection_count")
"""

import asyncio
from typing import Awaitable, Callable, Dict, List, Optional

from opentelemetry.metrics import Observation

# Type alias for async callbacks only
AsyncObservableCallback = Callable[[], Awaitable[List[Observation]]]


class AsyncObservablesRegistry:
    """
    Async-safe registry for storing async callbacks for observable metrics.
    """

    def __init__(self, registry: Dict[str, List[AsyncObservableCallback]] = None):
        self._registry = registry or {}
        self._lock = asyncio.Lock()

    async def register(self, name: str, callback: AsyncObservableCallback) -> None:
        """
        Register an async callback for an observable metric.
        """
        async with self._lock:
            self._registry.setdefault(name, []).append(callback)

    async def get(self, name: str) -> List[AsyncObservableCallback]:
        """
        Get all callbacks for an observable metric.
        """
        async with self._lock:
            return self._registry.get(name, [])

    async def clear(self) -> None:
        """
        Clear the registry.
        """
        async with self._lock:
            self._registry.clear()

    def __len__(self) -> int:
        """
        Get the number of registered callbacks.
        """
        return len(self._registry)


# Global singleton instance
_async_observables_registry_instance: Optional[AsyncObservablesRegistry] = None


async def get_async_observables_registry_instance() -> AsyncObservablesRegistry:
    """
    Get the global async observables registry singleton instance.

    This is the async equivalent of get_observables_registry_instance().

    Returns:
        The global AsyncObservablesRegistry singleton

    Example:
        >>> registry = await get_async_observables_registry_instance()
        >>> await registry.register('my_metric', my_async_callback)
    """
    global _async_observables_registry_instance

    if _async_observables_registry_instance is None:
        _async_observables_registry_instance = AsyncObservablesRegistry()

    return _async_observables_registry_instance


def reset_async_observables_registry() -> None:
    """
    Reset the global async observables registry singleton.

    This is primarily used for testing to ensure a clean state
    between test runs.

    Warning:
        This will clear all registered callbacks. Use with caution
        in production code.
    """
    global _async_observables_registry_instance
    _async_observables_registry_instance = None
